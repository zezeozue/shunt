//! Periodic host machine and process resource sampler.
//!
//! Ultra-efficient, zero-allocation polling of `/proc/loadavg`, `/proc/stat`, `/proc/meminfo`,
//! `statvfs`, active run process trees, and git worktrees as specified in design sections 5.3 and 8.
//!
//! Persistent file handles are reused across ticks with `pread` into stack buffers to avoid
//! file open/close syscalls and heap reallocations.
//!
//! ## Submodules
//! - [`proc_tree`]: Process tree walking, `/proc/<pid>/stat` & `cmdline` parsing, and `procs/slot:<k>` slices.
//! - [`worktree`]: Git worktree HEAD movement classification and `git ls-files` content hashing.
//! - [`timer`]: `timerfd_create(CLOCK_MONOTONIC)` cadence and waker-driven background thread.

pub mod proc_tree;
pub mod timer;
pub mod worktree;

#[cfg(test)]
mod tests;

pub use proc_tree::{
    find_process_tree, is_process_zombie, read_process_cmdline, read_process_cpu_ticks,
    read_process_ppid, read_process_rss_bytes, read_process_stat_and_comm,
    read_process_stat_and_comm_slice, read_process_stat_fields, sample_pid_tree, ProcStat,
};
pub use timer::{spawn_sampler, spawn_sampler_with_home, spawn_sampler_with_waker, SamplerWaker};
pub(crate) use worktree::wait_child_exit;
pub use worktree::{
    run_git, run_git_in_worktree, scan_worktree_files, scan_worktree_latest_mtime_ns,
};

use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use tacho_core::{AgentRole, Input, SessionKey};

use crate::appender::AppenderHandle;
use crate::cgroup::AgentCgroup;

use proc_tree::{CpuAccount, TrackedProc};
use worktree::WorktreeSnapshot;

static CLK_TCK: OnceLock<u64> = OnceLock::new();
static PAGE_SIZE: OnceLock<u64> = OnceLock::new();
static CGROUP_ATTACH_ERR_LOGGED: AtomicBool = AtomicBool::new(false);

/// Returns the clock ticks per second (cached sysconf(_SC_CLK_TCK)).
#[inline]
pub fn get_clock_ticks_per_sec() -> u64 {
    *CLK_TCK.get_or_init(|| {
        let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if tck <= 0 {
            100
        } else {
            tck as u64
        }
    })
}

/// Returns the page size in bytes (cached sysconf(_SC_PAGESIZE)).
#[inline]
pub fn get_page_size() -> u64 {
    *PAGE_SIZE.get_or_init(|| {
        let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if ps <= 0 {
            4096
        } else {
            ps as u64
        }
    })
}

/// Parsed host memory statistics from `/proc/meminfo` (all in bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemInfoStats {
    pub used_bytes: u64,
    pub avail_bytes: u64,
    pub cached_bytes: u64,
    pub swap_used_bytes: u64,
}

/// Persistent `/proc` sampler maintaining open file handles and scratch buffers
/// to eliminate open/close syscalls and heap allocations during polling ticks.
pub struct ProcSampler {
    cpu_accounts: HashMap<SessionKey, CpuAccount>,
    /// Start time of each run's root process when first sampled, to detect pid reuse.
    root_start_ticks: HashMap<SessionKey, u64>,
    loadavg_file: Option<File>,
    stat_file: Option<File>,
    meminfo_file: Option<File>,
    mount_cstrings: Vec<(String, CString)>,
    last_samples: HashMap<String, i64>,
    prev_cpu: Option<(u64, u64)>, // (busy, total)
    clk_tck: u64,
    page_size: u64,
    tree_pids_scratch: Vec<u32>,
    visited_scratch: HashSet<u32>,
    cgroup_pids_scratch: Vec<u32>,
    reaped_sessions: HashSet<SessionKey>,
    attach_attempted: HashSet<SessionKey>,
    proc_slots: HashMap<SessionKey, HashMap<u32, TrackedProc>>,
    session_run_tracks: HashMap<SessionKey, String>,
    worktree_states: HashMap<String, WorktreeSnapshot>,
    cgroup_enabled: bool,
    dry_run: bool,
    launcher: Option<std::sync::Arc<dyn crate::exec::Launcher>>,
    worktree_scan_secs: u64,
    home_dir: Option<std::path::PathBuf>,
    fleet_store: Option<crate::fleet::FleetStore>,
    /// `AppenderHandle::decide_seq` at the last decide pass (`None` before the first).
    last_decide_seq: Option<u64>,
    last_decide_ns: u64,
    /// Coarse refresh for time-based decide rules; see `SamplerConfig::decide_refresh_secs`.
    decide_refresh_ns: u64,
    /// Number of consecutive failed autonomous decide ticks (reset to `0` on success).
    decide_failures: u32,
    /// Earliest timestamp (`now_ns`) at which an autonomous decide tick may retry after failure.
    decide_backoff_until_ns: u64,
    /// Optional forge override for autonomous decide ticks (e.g. for deterministic testing).
    decide_forge: Option<std::sync::Arc<dyn tacho_core::decide::plugin::ForgePlugin + Send + Sync>>,
    /// Set by [`SamplerWaker::request_decide`] to force a decide pass on the next tick.
    decide_requested: std::sync::Arc<AtomicBool>,
}

impl ProcSampler {
    /// Creates a new `ProcSampler`, pre-opening persistent handles for `/proc` virtual files.
    pub fn new(mounts: &[String]) -> Self {
        let mount_cstrings: Vec<(String, CString)> = mounts
            .iter()
            .filter_map(|m| CString::new(m.as_str()).ok().map(|c| (m.clone(), c)))
            .collect();

        Self {
            cpu_accounts: HashMap::new(),
            root_start_ticks: HashMap::new(),
            loadavg_file: File::open("/proc/loadavg").ok(),
            stat_file: File::open("/proc/stat").ok(),
            meminfo_file: File::open("/proc/meminfo").ok(),
            mount_cstrings,
            last_samples: HashMap::new(),
            prev_cpu: None,
            clk_tck: get_clock_ticks_per_sec(),
            page_size: get_page_size(),
            tree_pids_scratch: Vec::with_capacity(32),
            visited_scratch: HashSet::with_capacity(32),
            cgroup_pids_scratch: Vec::with_capacity(32),
            reaped_sessions: HashSet::new(),
            attach_attempted: HashSet::new(),
            proc_slots: HashMap::new(),
            session_run_tracks: HashMap::new(),
            worktree_states: HashMap::new(),
            cgroup_enabled: true,
            dry_run: false,
            launcher: None,
            worktree_scan_secs: 30,
            home_dir: None,
            fleet_store: None,
            last_decide_seq: None,
            last_decide_ns: 0,
            decide_refresh_ns: 60 * 1_000_000_000,
            decide_failures: 0,
            decide_backoff_until_ns: 0,
            decide_forge: None,
            decide_requested: std::sync::Arc::new(AtomicBool::new(false)),
        }
    }

    /// Sets the process launcher used by autonomous ticks.
    pub fn set_launcher(&mut self, launcher: Option<std::sync::Arc<dyn crate::exec::Launcher>>) {
        self.launcher = launcher;
    }

    /// Sets whether cgroup accounting is enabled.
    pub fn set_cgroup_enabled(&mut self, enabled: bool) {
        self.cgroup_enabled = enabled;
    }

    /// Keeps autonomous ticks read-only when observing another fleet.
    pub fn set_dry_run(&mut self, enabled: bool) {
        self.dry_run = enabled;
    }

    /// Sets the minimum interval between worktree file-change scans in seconds.
    pub fn set_worktree_scan_secs(&mut self, secs: u64) {
        self.worktree_scan_secs = secs;
    }

    /// Sets `$TACHO_HOME` so sampling ticks also run autonomous `decide` passes when one is
    /// due (see [`ProcSampler::decide_due`]).
    pub fn set_home_dir(&mut self, home_dir: std::path::PathBuf) {
        self.fleet_store = None;
        self.home_dir = Some(home_dir);
    }

    /// Returns `$TACHO_HOME` if configured on this sampler.
    pub fn home_dir(&self) -> Option<&std::path::Path> {
        self.home_dir.as_deref()
    }

    /// Sets the coarse refresh interval for time-based decide rules.
    pub fn set_decide_refresh_secs(&mut self, secs: u64) {
        self.decide_refresh_ns = secs.max(1).saturating_mul(1_000_000_000);
    }

    /// Sets an explicit [`tacho_core::decide::plugin::ForgePlugin`] implementation for autonomous decide ticks.
    pub fn set_decide_forge(
        &mut self,
        forge: Option<std::sync::Arc<dyn tacho_core::decide::plugin::ForgePlugin + Send + Sync>>,
    ) {
        self.decide_forge = forge;
    }

    /// Shares the flag that forces a decide pass on the next tick when set.
    pub(crate) fn set_decide_request_flag(&mut self, flag: std::sync::Arc<AtomicBool>) {
        self.decide_requested = flag;
    }

    /// Whether a decide pass is due at `now_ns`: on the first tick, after a decide-relevant
    /// event was committed (`decide_seq` moved), on explicit request, or once the coarse
    /// refresh interval has elapsed for rules that depend on time alone.
    ///
    /// When a previous tick failed, autonomous ticks are held off until `decide_backoff_until_ns`
    /// elapses so a forge outage never hot-loops `gh` on every 2s sampler pass.
    fn decide_due(&self, decide_seq: u64, now_ns: u64) -> bool {
        if self.decide_requested.load(Ordering::Acquire) {
            return true;
        }
        if now_ns < self.decide_backoff_until_ns {
            return false;
        }
        self.last_decide_seq != Some(decide_seq)
            || now_ns.saturating_sub(self.last_decide_ns) >= self.decide_refresh_ns
    }

    /// Records a successful decide pass and clears any failure backoff.
    pub(crate) fn record_decide_success(&mut self, effective_seq: u64, now_ns: u64) {
        self.last_decide_seq = Some(effective_seq);
        self.last_decide_ns = now_ns;
        self.decide_failures = 0;
        self.decide_backoff_until_ns = 0;
    }

    /// Records a failed decide pass and schedules exponential backoff (`1x, 2x, 4x, 8x, 15x`
    /// of `decide_refresh_ns` — i.e. 60 s, 2 m, 4 m, 8 m, and up to 15 minutes when
    /// `decide_refresh_ns` is 60 s) so subsequent 2s sampler ticks do not spin on a failing forge.
    pub(crate) fn record_decide_failure(&mut self, effective_seq: u64, now_ns: u64) {
        self.last_decide_seq = Some(effective_seq);
        self.last_decide_ns = now_ns;
        self.decide_failures = self.decide_failures.saturating_add(1);
        let shift = (self.decide_failures - 1).min(4);
        let mult = (1u64 << shift).min(15);
        let backoff = self.decide_refresh_ns.saturating_mul(mult);
        self.decide_backoff_until_ns = now_ns.saturating_add(backoff);
    }

    /// Reads `/proc/loadavg` using `pread` at offset 0 into a stack buffer.
    #[inline]
    pub fn sample_loadavg(&mut self) -> Option<i64> {
        let mut buf = [0u8; 128];
        let file = self.loadavg_file.as_mut()?;
        let n = file.read_at(&mut buf, 0).ok()?;
        parse_loadavg_milli(&buf[..n])
    }

    /// Reads `/proc/stat` using `pread` at offset 0 into a stack buffer.
    #[inline]
    pub fn sample_cpu_jiffies(&mut self) -> Option<(u64, u64)> {
        let mut buf = [0u8; 2048];
        let file = self.stat_file.as_mut()?;
        let n = file.read_at(&mut buf, 0).ok()?;
        parse_cpu_jiffies(&buf[..n])
    }

    /// Reads `/proc/meminfo` using `pread` at offset 0 into a stack buffer.
    #[inline]
    pub fn sample_mem_used_bytes(&mut self) -> Option<u64> {
        self.sample_meminfo().map(|m| m.used_bytes)
    }

    /// Reads `/proc/meminfo` using `pread` at offset 0 and returns detailed memory counters.
    #[inline]
    pub fn sample_meminfo(&mut self) -> Option<MemInfoStats> {
        let mut buf = [0u8; 2048];
        let file = self.meminfo_file.as_mut()?;
        let n = file.read_at(&mut buf, 0).ok()?;
        parse_meminfo(&buf[..n])
    }

    /// Performs one complete sampling tick, emitting counter samples only for values that changed.
    pub fn sample_tick(
        &mut self,
        appender: &AppenderHandle,
        now_ns: u64,
        run_track_buf: &mut String,
    ) {
        // 1. /proc/loadavg -> machine/load1
        if let Some(load1_milli) = self.sample_loadavg() {
            send_sample_if_changed(
                appender,
                &mut self.last_samples,
                "machine/load1",
                load1_milli,
                Some("load x 1000"),
                now_ns,
            );
        }

        // 2. /proc/stat -> machine/cpu_busy
        if let Some((busy, total)) = self.sample_cpu_jiffies() {
            if let Some((prev_busy, prev_total)) = self.prev_cpu {
                let busy_delta = busy.saturating_sub(prev_busy);
                let total_delta = total.saturating_sub(prev_total);
                if let Some(per_mille) = busy_delta.saturating_mul(1000).checked_div(total_delta) {
                    send_sample_if_changed(
                        appender,
                        &mut self.last_samples,
                        "machine/cpu_busy",
                        per_mille.min(1000) as i64,
                        Some("per mille"),
                        now_ns,
                    );
                }
            }
            self.prev_cpu = Some((busy, total));
        }

        // 3. /proc/meminfo -> machine/mem_used, mem_avail, mem_cached, swap_used
        if let Some(mem) = self.sample_meminfo() {
            send_sample_if_changed(
                appender,
                &mut self.last_samples,
                "machine/mem_used",
                mem.used_bytes as i64,
                Some("bytes"),
                now_ns,
            );
            send_sample_if_changed(
                appender,
                &mut self.last_samples,
                "machine/mem_avail",
                mem.avail_bytes as i64,
                Some("bytes"),
                now_ns,
            );
            send_sample_if_changed(
                appender,
                &mut self.last_samples,
                "machine/mem_cached",
                mem.cached_bytes as i64,
                Some("bytes"),
                now_ns,
            );
            send_sample_if_changed(
                appender,
                &mut self.last_samples,
                "machine/swap_used",
                mem.swap_used_bytes as i64,
                Some("bytes"),
                now_ns,
            );
        }

        // 4. statvfs for configured mount points
        for i in 0..self.mount_cstrings.len() {
            let (ref mount_str, ref c_path) = self.mount_cstrings[i];
            if let Some(bytes_used) = sample_mount_statvfs(c_path) {
                run_track_buf.clear();
                run_track_buf.push_str("machine/disk:");
                run_track_buf.push_str(mount_str);
                send_sample_if_changed(
                    appender,
                    &mut self.last_samples,
                    run_track_buf,
                    bytes_used as i64,
                    Some("bytes used"),
                    now_ns,
                );
            }
        }

        // 5. Per-run process tree sampling, child process slices, worktree progress, and dead worker reaping
        let active_runs = appender.active_runs();

        // Close any leftover proc slices for runs that ended normally and left `active_runs`
        let ended_sessions: Vec<SessionKey> = self
            .proc_slots
            .keys()
            .filter(|s| !active_runs.iter().any(|r| &r.session == *s))
            .cloned()
            .collect();
        for s in ended_sessions {
            if let Some(mut slots) = self.proc_slots.remove(&s) {
                let run_track = self.session_run_tracks.remove(&s).unwrap_or_default();
                Self::close_all_proc_slices(&mut slots, &run_track, appender, now_ns);
            }
        }
        self.session_run_tracks
            .retain(|s, _| active_runs.iter().any(|r| &r.session == s));
        self.worktree_states.retain(|cwd, _| {
            active_runs
                .iter()
                .any(|r| r.cwd.as_deref() == Some(cwd.as_str()))
        });

        // Prune reaped_sessions and attach_attempted for runs that are no longer active
        self.reaped_sessions
            .retain(|s| active_runs.iter().any(|r| &r.session == s));
        self.attach_attempted
            .retain(|s| active_runs.iter().any(|r| &r.session == s));
        self.cpu_accounts
            .retain(|s, _| active_runs.iter().any(|r| &r.session == s));
        self.root_start_ticks
            .retain(|s, _| active_runs.iter().any(|r| &r.session == s));

        let live_state = appender.live_state();
        let mut polled_cwds = HashSet::new();
        for run in active_runs.iter() {
            if let Some(ref cwd) = run.cwd {
                if polled_cwds.insert(cwd.as_str()) {
                    // Prefer the Worker run with the most recent edit/command activity if multiple agents share cwd
                    let recent_ts = |agent_id: &str| -> u64 {
                        live_state
                            .agents
                            .get(agent_id)
                            .map(|a| {
                                let edit_ts = a.last_edit_ts_ns.unwrap_or(0);
                                let cmd_ts = a.recent_commands.last().map(|c| c.ts_ns).unwrap_or(0);
                                edit_ts.max(cmd_ts)
                            })
                            .unwrap_or(0)
                    };
                    let owner_agent = active_runs
                        .iter()
                        .filter(|r| {
                            r.cwd.as_deref() == Some(cwd.as_str()) && r.role == AgentRole::Worker
                        })
                        .max_by_key(|r| recent_ts(&r.agent_id))
                        .or_else(|| {
                            active_runs
                                .iter()
                                .filter(|r| {
                                    r.cwd.as_deref() == Some(cwd.as_str())
                                        && r.role != AgentRole::Orchestrator
                                })
                                .max_by_key(|r| recent_ts(&r.agent_id))
                        })
                        .map(|r| r.agent_id.as_str())
                        .unwrap_or(&run.agent_id);
                    self.poll_worktree_progress(owner_agent, cwd, appender, now_ns);
                }
            }

            if run.pid > 0 {
                if self.run_process_gone(run) && !self.reaped_sessions.contains(&run.session) {
                    if let Some(mut slots) = self.proc_slots.remove(&run.session) {
                        Self::close_all_proc_slices(&mut slots, &run.run_track, appender, now_ns);
                    }
                    self.reaped_sessions.insert(run.session.clone());
                    let _ = appender.send_input(
                        Input::RunEnd {
                            source_ts_ns: Some(now_ns),
                            session: run.session.clone(),
                            exit_code: Some(137),
                            reason: Some("worker process died unexpectedly".to_string()),
                        },
                        now_ns,
                    );
                    continue;
                }

                // Cgroup scope discovery / attachment (attach-once semantics). Only this run's
                // own scope counts: a sub-agent started inside another agent's scope must not
                // inherit that agent's totals.
                let run_n = run
                    .run_track
                    .rsplit("run:")
                    .next()
                    .and_then(|s| s.split('/').next())
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(1);
                let unit = crate::cgroup::unit_name(&run.agent_id, run_n, run.pid);
                let mut cgroup = AgentCgroup::of_pid(run.pid, &unit);
                if cgroup.is_none()
                    && self.cgroup_enabled
                    && !self.attach_attempted.contains(&run.session)
                {
                    self.attach_attempted.insert(run.session.clone());
                    if self.populate_tree_pids(run.pid) {
                        if let Err(e) = AgentCgroup::attach(&unit, &self.tree_pids_scratch) {
                            if !CGROUP_ATTACH_ERR_LOGGED.swap(true, Ordering::Relaxed) {
                                eprintln!(
                                    "[tacho-daemon] cgroup attach failed ({}), falling back to /proc sampling",
                                    e
                                );
                            }
                        } else {
                            cgroup = AgentCgroup::of_pid(run.pid, &unit);
                        }
                    }
                }

                if let Some(sample) = self.sample_tree_detailed(run.pid, cgroup.as_ref()) {
                    let cpu_us = self
                        .cpu_accounts
                        .entry(run.session.clone())
                        .or_default()
                        .update(&sample);
                    let cg_mem = cgroup
                        .as_ref()
                        .and_then(|cg| cg.memory_current())
                        .unwrap_or(0);
                    let rss_bytes = sample.rss_bytes.max(cg_mem);
                    let live_procs = sample.procs;
                    run_track_buf.clear();
                    run_track_buf.push_str(&run.run_track);
                    run_track_buf.push_str("/cpu");
                    send_sample_if_changed(
                        appender,
                        &mut self.last_samples,
                        run_track_buf,
                        cpu_us as i64,
                        Some("us"),
                        now_ns,
                    );

                    run_track_buf.truncate(run.run_track.len());
                    run_track_buf.push_str("/rss");
                    send_sample_if_changed(
                        appender,
                        &mut self.last_samples,
                        run_track_buf,
                        rss_bytes as i64,
                        Some("bytes"),
                        now_ns,
                    );

                    run_track_buf.truncate(run.run_track.len());
                    run_track_buf.push_str("/proc_count");
                    send_sample_if_changed(
                        appender,
                        &mut self.last_samples,
                        run_track_buf,
                        live_procs.len() as i64,
                        Some("procs"),
                        now_ns,
                    );

                    self.update_proc_slices(
                        &run.session,
                        run.pid,
                        &run.run_track,
                        &live_procs,
                        appender,
                        now_ns,
                    );
                }
            }
        }

        if let Some(ref home_dir) = self.home_dir {
            let decide_seq = appender.decide_seq();
            if self.decide_due(decide_seq, now_ns) {
                self.decide_requested.store(false, Ordering::Release);
                let latest_live = appender.live_state();
                let latest_runs = appender.active_runs();
                let effective_seq = appender.decide_seq();
                if self.fleet_store.is_none() {
                    match crate::fleet::FleetStore::open(&home_dir.join("events.sqlite")) {
                        Ok(s) => self.fleet_store = Some(s),
                        Err(e) => {
                            eprintln!("[tacho-daemon] failed to open FleetStore: {e}");
                            return;
                        }
                    }
                }
                if let Some(ref mut store) = self.fleet_store {
                    store.attach_appender(appender.clone());
                    let mut hands = crate::fleet::DaemonHands {
                        home: home_dir.clone(),
                        live_state: (*latest_live).clone(),
                        appender: Some(appender.clone()),
                        launcher: self.launcher.clone(),
                    };
                    let gh_forge = if self.decide_forge.is_none() {
                        std::env::var("TACHO_GH_REPO")
                            .ok()
                            .filter(|s| !s.trim().is_empty())
                            .map(tacho_core::decide::plugin::GitHubForge::new)
                    } else {
                        None
                    };
                    let mut opts = crate::exec::TickOptions::new(&latest_live);
                    opts.dry_run = self.dry_run;
                    if let Some(ref f) = self.decide_forge {
                        opts.forge = Some(f.as_ref());
                    } else if let Some(ref f) = gh_forge {
                        opts.forge = Some(f);
                    }
                    for run in latest_runs.iter() {
                        if run.pid > 0 {
                            opts.run_pids.insert(run.agent_id.clone(), run.pid);
                        }
                    }
                    match crate::exec::run_tick(home_dir, store, &mut hands, opts) {
                        Ok(_) => {
                            self.record_decide_success(effective_seq, now_ns);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            self.record_decide_success(effective_seq, now_ns);
                            self.decide_requested.store(true, Ordering::Release);
                        }
                        Err(e) => {
                            self.record_decide_failure(effective_seq, now_ns);
                            eprintln!("[tacho-daemon] autonomous decide tick failed: {e}");
                        }
                    }
                }
            }
        }
    }
}

/// Sends counter samples only when the value has changed since the previous sample.
///
/// Lookups are performed against `&str` BEFORE any string allocations occur.
#[inline]
pub fn send_sample_if_changed(
    appender: &AppenderHandle,
    last_samples: &mut HashMap<String, i64>,
    track_key: &str,
    value: i64,
    unit: Option<&str>,
    now_ns: u64,
) {
    if let Some(&last) = last_samples.get(track_key) {
        if last == value {
            return;
        }
    }
    last_samples.insert(track_key.to_string(), value);
    if let Err(e) = appender.send_input(
        Input::CounterSample {
            source_ts_ns: Some(now_ns),
            track_key: track_key.to_string(),
            value,
            unit: unit.map(|s| s.to_string()),
        },
        now_ns,
    ) {
        // Forget the value so the next tick resends it instead of deduplicating it away.
        last_samples.remove(track_key);
        eprintln!("[tacho-daemon] dropped counter sample for {track_key}: {e}");
    }
}

/// Parses unsigned 64-bit integer from ASCII byte slice without allocation.
#[inline]
pub fn parse_u64_ascii(bytes: &[u8]) -> Option<u64> {
    let mut val = 0u64;
    let mut empty = true;
    for &b in bytes {
        if b.is_ascii_digit() {
            val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
            empty = false;
        } else {
            break;
        }
    }
    if empty {
        None
    } else {
        Some(val)
    }
}

/// Parses signed 64-bit integer from ASCII byte slice without allocation.
#[inline]
pub fn parse_i64_ascii(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (is_neg, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    let val = parse_u64_ascii(digits)?;
    if is_neg {
        0i64.checked_sub_unsigned(val)
    } else {
        i64::try_from(val).ok()
    }
}

/// Parses 1-minute load average scaled by 1000 from `/proc/loadavg` bytes without allocation.
pub fn parse_loadavg_milli(buf: &[u8]) -> Option<i64> {
    let mut iter = buf
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|s| !s.is_empty());
    let first = iter.next()?;
    let dot_pos = first.iter().position(|&b| b == b'.');
    let (int_part, frac_part) = match dot_pos {
        Some(pos) => (&first[..pos], &first[pos + 1..]),
        None => (first, &b""[..]),
    };
    let whole = parse_i64_ascii(int_part)?;
    let mut milli = whole.checked_mul(1000)?;
    if !frac_part.is_empty() {
        let mut frac_val = 0i64;
        let mut divisor = 1i64;
        for &b in frac_part.iter().take(3) {
            if b.is_ascii_digit() {
                frac_val = frac_val * 10 + (b - b'0') as i64;
                divisor *= 10;
            }
        }
        while divisor < 1000 {
            frac_val *= 10;
            divisor *= 10;
        }
        milli += frac_val;
    }
    Some(milli)
}

/// Parses CPU busy and total jiffies from `/proc/stat` bytes without allocation.
pub fn parse_cpu_jiffies(buf: &[u8]) -> Option<(u64, u64)> {
    for line in buf.split(|&b| b == b'\n') {
        if line.starts_with(b"cpu ") {
            let mut fields = line[4..]
                .split(|&b| b == b' ' || b == b'\t')
                .filter(|s| !s.is_empty());
            let user = parse_u64_ascii(fields.next()?)?;
            let nice = parse_u64_ascii(fields.next()?)?;
            let system = parse_u64_ascii(fields.next()?)?;
            let idle = parse_u64_ascii(fields.next()?)?;
            let iowait = fields.next().and_then(parse_u64_ascii).unwrap_or(0);
            let irq = fields.next().and_then(parse_u64_ascii).unwrap_or(0);
            let softirq = fields.next().and_then(parse_u64_ascii).unwrap_or(0);
            let steal = fields.next().and_then(parse_u64_ascii).unwrap_or(0);

            let busy = user + nice + system + irq + softirq + steal;
            let total = busy + idle + iowait;
            return Some((busy, total));
        }
    }
    None
}

/// Parses used memory in bytes from `/proc/meminfo` bytes without allocation.
pub fn parse_mem_used_bytes(buf: &[u8]) -> Option<u64> {
    parse_meminfo(buf).map(|m| m.used_bytes)
}

/// Parses detailed memory statistics in bytes from `/proc/meminfo` bytes without allocation.
pub fn parse_meminfo(buf: &[u8]) -> Option<MemInfoStats> {
    let mut total_kb = None;
    let mut avail_kb = None;
    let mut buffers_kb = 0u64;
    let mut cached_kb = 0u64;
    let mut sreclaimable_kb = 0u64;
    let mut swap_total_kb = 0u64;
    let mut swap_free_kb = 0u64;

    let parse_first_num = |rest: &[u8]| -> Option<u64> {
        rest.split(|&b| b == b' ' || b == b'\t')
            .find(|s| !s.is_empty())
            .and_then(parse_u64_ascii)
    };

    for line in buf.split(|&b| b == b'\n') {
        if let Some(rest) = line.strip_prefix(b"MemTotal:") {
            total_kb = parse_first_num(rest);
        } else if let Some(rest) = line.strip_prefix(b"MemAvailable:") {
            avail_kb = parse_first_num(rest);
        } else if let Some(rest) = line.strip_prefix(b"Buffers:") {
            buffers_kb = parse_first_num(rest).unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix(b"Cached:") {
            cached_kb = parse_first_num(rest).unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix(b"SReclaimable:") {
            sreclaimable_kb = parse_first_num(rest).unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix(b"SwapTotal:") {
            swap_total_kb = parse_first_num(rest).unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix(b"SwapFree:") {
            swap_free_kb = parse_first_num(rest).unwrap_or(0);
        }
    }

    match (total_kb, avail_kb) {
        (Some(tot), Some(avail)) => Some(MemInfoStats {
            used_bytes: tot.saturating_sub(avail) * 1024,
            avail_bytes: avail * 1024,
            cached_bytes: (buffers_kb + cached_kb + sreclaimable_kb) * 1024,
            swap_used_bytes: swap_total_kb.saturating_sub(swap_free_kb) * 1024,
        }),
        _ => None,
    }
}

/// Samples mount usage via statvfs without string allocation.
#[inline]
pub fn sample_mount_statvfs(c_path: &CString) -> Option<u64> {
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let res = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if res == 0 {
        let total = (stat.f_blocks as u64) * (stat.f_frsize as u64);
        let free = (stat.f_bfree as u64) * (stat.f_frsize as u64);
        Some(total.saturating_sub(free))
    } else {
        None
    }
}

/// Reads `/proc/loadavg` and returns 1-minute load scaled by 1000.
pub fn read_loadavg_milli() -> Option<i64> {
    let mut buf = [0u8; 128];
    let file = File::open("/proc/loadavg").ok()?;
    let n = file.read_at(&mut buf, 0).ok()?;
    parse_loadavg_milli(&buf[..n])
}

/// Reads `/proc/stat` and returns (busy_jiffies, total_jiffies).
pub fn read_cpu_jiffies() -> Option<(u64, u64)> {
    let mut buf = [0u8; 2048];
    let file = File::open("/proc/stat").ok()?;
    let n = file.read_at(&mut buf, 0).ok()?;
    parse_cpu_jiffies(&buf[..n])
}

/// Reads `/proc/meminfo` and returns used memory in bytes (MemTotal - MemAvailable).
pub fn read_mem_used_bytes() -> Option<u64> {
    let mut buf = [0u8; 2048];
    let file = File::open("/proc/meminfo").ok()?;
    let n = file.read_at(&mut buf, 0).ok()?;
    parse_mem_used_bytes(&buf[..n])
}

/// Reads filesystem usage via statvfs.
pub fn read_mount_used_bytes(mount: &str) -> Option<u64> {
    let c_path = CString::new(mount).ok()?;
    sample_mount_statvfs(&c_path)
}
