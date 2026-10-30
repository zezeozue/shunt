//! The single plan executor: observe → `decide` → record → execute.
//!
//! Both the daemon (autonomous ticks, the `tick` API request) and the `tacho tick` CLI run
//! [`run_tick`]. There is one set of identity checks ([`identity`]), one observation path
//! ([`observe`]), one janitor ([`tidy`]), and one error policy:
//!
//! - Every action is recorded in the `decisions` journal with its reason (`because`), on
//!   success and on refusal or failure (`<action>_refused` / `<action>_failed`).
//! - A failed action never aborts the tick: it is recorded, an attention event is emitted, and
//!   the error is collected. After the plan's state is persisted the tick returns `Err` listing
//!   every failed action, so callers still learn about it. Only store (SQLite) errors abort.
//!
//! Callers provide [`Hands`] for physical actions; the daemon uses a [`Launcher`] adapter
//! to start workers and its appender to record trace events without reentering the API.
//!
//! Ticks are serialized in-process ([`TICK_LOCK`]) and across processes (an `flock` on
//! `$TACHO_HOME/decide.lock`), so a CLI tick and a daemon tick never execute two plans
//! computed from the same state.

pub mod identity;
pub mod observe;
pub mod tidy;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tacho_core::decide::dispatch::{base_key, resume_task, AgentRun, TodoItem};
use tacho_core::decide::watch::{idle_chains, ProcEntry, SuperviseAction};
use tacho_core::decide::{DecisionPlan, DecisionState, WorldObservation};
use tacho_core::TaskStatus;

use crate::fleet::{FleetState, FleetStore};
use identity::{AgentProcess, Identity, Signalled};

/// Error kind a [`Hands::deliver_tell`] returns when delivering would resume a session whose
/// old process may still own the worktree. Recorded as `tell_refused`, not as a failure.
pub const RESUME_REFUSED: io::ErrorKind = io::ErrorKind::ResourceBusy;

/// Starts project processes from either an API tick or the autonomous sampler.
pub trait Launcher: Send + Sync {
    /// Starts and records a worker in its allocated worktree.
    fn spawn_worker(&self, store: &mut FleetStore, item: &TodoItem) -> io::Result<u32>;
}

/// What a caller of [`run_tick`] can physically do.
pub trait Hands {
    /// Starts a worker for a dispatched item and records it (`dispatch/start`).
    /// `Ok(None)`: this process cannot start workers; the item stays queued.
    fn spawn_worker(&mut self, store: &mut FleetStore, item: &TodoItem) -> io::Result<Option<u32>>;

    /// Delivers a decided message to an agent and records the delivery.
    fn deliver_tell(
        &mut self,
        store: &mut FleetStore,
        agent_key: &str,
        msg: &str,
    ) -> io::Result<()>;

    /// Emits an instant trace event (e.g. `watch:attention`).
    fn emit(&mut self, track: &str, name: &str, because: &str);

    /// Emits a duration slice on `track_key`.
    fn emit_duration_slice(
        &mut self,
        store: &FleetStore,
        track_key: &str,
        name: &str,
        start_ts_ns: u64,
        end_ts_ns: u64,
        because: &str,
    ) -> io::Result<()> {
        store.emit_duration_slice(track_key, name, start_ts_ns, end_ts_ns, because)
    }

    /// Called once after a non-dry-run tick persisted its state.
    fn publish(&mut self, _store: &FleetStore) -> io::Result<()> {
        Ok(())
    }

    /// Delivers an owner notification (`subject`, `body`).
    ///
    /// The default implementation is inert (`Ok(false)`); only production `Hands`
    /// implementations (`DaemonHands`, `CliHands`) invoke the external notification hook.
    fn notify_owner(&mut self, _subject: &str, _body: &str) -> Result<bool, String> {
        Ok(false)
    }

    /// Prunes unused runner Docker volumes when `/` is low on free space (`< 40 GiB`).
    ///
    /// The default implementation is inert (`Ok(String::new())`); only production `Hands`
    /// implementations (`DaemonHands`, `CliHands`) invoke the root-disk prune helper.
    fn prune_root_disk_volumes(&mut self) -> Result<String, String> {
        Ok(String::new())
    }
}

/// Default path of the privileged runner-volume prune helper (`sudo -n /usr/local/sbin/tacho-runner-prune check`).
pub const DEFAULT_ROOT_PRUNE_HELPER: &str = "/usr/local/sbin/tacho-runner-prune";

/// Runs the root-disk runner-volume prune helper (`cmd_override`, `$TACHO_ROOT_PRUNE`, or
/// [`DEFAULT_ROOT_PRUNE_HELPER`]) with argument `"check"`.
pub fn prune_runner_volumes_command(cmd_override: Option<&Path>) -> Result<String, String> {
    let in_test = tacho_core::decide::plugin::running_in_test_harness();
    let resolved = match cmd_override {
        Some(p) => Some(p.to_path_buf()),
        None if in_test => std::env::var_os("TACHO_TEST_ROOT_PRUNE")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from),
        None => std::env::var_os("TACHO_ROOT_PRUNE")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from),
    };
    if resolved.is_none() && in_test {
        return Ok(String::new());
    }
    let (program, args): (String, Vec<String>) = if let Some(custom) = resolved {
        if !custom.is_file() {
            return Err(format!(
                "runner prune helper '{}' does not exist",
                custom.display()
            ));
        }
        (
            custom.to_string_lossy().to_string(),
            vec!["check".to_string()],
        )
    } else {
        let default_path = Path::new(DEFAULT_ROOT_PRUNE_HELPER);
        if !default_path.is_file() {
            return Ok(format!(
                "runner prune skipped: {DEFAULT_ROOT_PRUNE_HELPER} not installed"
            ));
        }
        (
            "sudo".to_string(),
            vec![
                "-n".to_string(),
                DEFAULT_ROOT_PRUNE_HELPER.to_string(),
                "check".to_string(),
            ],
        )
    };
    let out = std::process::Command::new(&program)
        .args(&args)
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("runner prune did not run: {e}"))?;
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let tail: String = combined
        .trim()
        .chars()
        .rev()
        .take(500)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    let code = out.status.code().unwrap_or(1);
    if !out.status.success() {
        return Err(format!("runner prune exited {code}: {tail}"));
    }
    Ok(format!("runner prune exited {code}: {tail}"))
}

/// Inputs of one tick besides fleet state.
pub struct TickOptions<'a> {
    pub dry_run: bool,
    /// Caller-supplied main sha (wins over everything else).
    pub explicit_main_sha: Option<String>,
    /// Checkout the tick runs against; `None` = unknown.
    pub repo: Option<PathBuf>,
    pub live_state: &'a tacho_core::LiveState,
    /// Root pid of each recorder-tracked run, by agent key.
    pub run_pids: BTreeMap<String, u32>,
    /// Process table to use; `None` scans `/proc`.
    pub procs: Option<Vec<ProcEntry>>,
    /// Optional forge plugin to refresh `open_prs`, `main_ci`, and remote `main_sha` before deciding.
    pub forge: Option<&'a dyn tacho_core::decide::plugin::ForgePlugin>,
    /// Timeout for each bounded forge/git refresh command (default 10s).
    pub refresh_timeout: std::time::Duration,
    /// Optional explicit epoch timestamp (seconds) for deterministic testing without sleeping.
    pub now_override: Option<i64>,
    /// Optional explicit `(total_bytes, avail_bytes)` of `/` for root-disk checks.
    pub root_disk_bytes: Option<(u64, u64)>,
}

impl<'a> TickOptions<'a> {
    pub fn new(live_state: &'a tacho_core::LiveState) -> Self {
        Self {
            dry_run: false,
            explicit_main_sha: None,
            repo: None,
            live_state,
            run_pids: BTreeMap::new(),
            procs: None,
            forge: None,
            refresh_timeout: std::time::Duration::from_secs(10),
            now_override: None,
            root_disk_bytes: None,
        }
    }
}

/// Result of one tick.
#[derive(Debug, Clone)]
pub struct TickOutcome {
    /// Recorded tick id; the previous id when this tick duplicated it; `None` on dry run.
    pub tick_id: Option<i64>,
    /// `true` when inputs and plan equalled the last recorded tick and nothing was executed.
    pub deduped: bool,
    pub state: DecisionState,
    pub obs: WorldObservation,
    pub plan: DecisionPlan,
}

/// The single owner of tick execution inside a process.
static TICK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Test-only overlap detector: counts ticks in flight and records the most ever seen at once.
#[cfg(test)]
pub(crate) mod tick_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
    pub(crate) static MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

    pub(crate) struct InFlight;

    pub(crate) fn enter() -> InFlight {
        let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_IN_FLIGHT.fetch_max(now, Ordering::SeqCst);
        InFlight
    }

    impl Drop for InFlight {
        fn drop(&mut self) {
            IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Cross-process tick lock: a blocking `flock` on `$TACHO_HOME/decide.lock`.
struct HomeLock(std::fs::File);

impl HomeLock {
    fn acquire(home: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(home)?;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(home.join("decide.lock"))?;
        loop {
            // SAFETY: flock on an fd we own; blocks in the kernel until the lock is free.
            if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(Self(f));
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }
}

impl Drop for HomeLock {
    fn drop(&mut self) {
        // SAFETY: unlocking an fd we own. Closing the fd releases the lock regardless, so a
        // failed unlock is only reported.
        if unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) } != 0 {
            eprintln!(
                "[tacho] failed to unlock decide.lock: {}",
                io::Error::last_os_error()
            );
        }
    }
}

/// Collects failed actions under the shared error policy.
struct Failures<'h> {
    hands: &'h mut dyn Hands,
    errors: Vec<String>,
}

impl Failures<'_> {
    /// Records a refused or failed action with its reason, emits attention, and (for
    /// failures) remembers it for the tick's result.
    fn record(
        &mut self,
        store: &FleetStore,
        category: &str,
        action: &str,
        target: &str,
        because: &str,
        is_failure: bool,
    ) -> io::Result<()> {
        store.record_decision(category, action, target, because)?;
        self.hands
            .emit(&attention_track(target), "watch:attention", because);
        eprintln!("[tacho] {because}");
        if is_failure {
            self.errors.push(format!("{action} {target}: {because}"));
        }
        Ok(())
    }
}

fn attention_track(target: &str) -> String {
    if target.starts_with("pid:") || target.is_empty() {
        "system".to_string()
    } else {
        format!("agent:{target}")
    }
}

/// Current epoch time in seconds.
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Main sha for the tick: explicit, else the refreshed remote main, else the checkout's local
/// main, else the newest sha fleet state knows. Empty when nothing is known.
fn resolve_main_sha(state: &FleetState, opts: &TickOptions<'_>, home: &Path) -> String {
    let non_empty = |s: &str| {
        let t = s.trim();
        (!t.is_empty()).then(|| t.to_string())
    };
    opts.explicit_main_sha
        .as_deref()
        .and_then(non_empty)
        .or_else(|| state.main_sha.as_deref().and_then(non_empty))
        .or_else(|| opts.repo.as_deref().and_then(observe::local_main_sha))
        .or_else(|| crate::fleet::known_main_sha(home, state, opts.live_state))
        .unwrap_or_default()
}

/// Observes, decides, records, and (unless `dry_run`) executes one tick. See module docs.
pub fn run_tick(
    home: &Path,
    store: &mut FleetStore,
    hands: &mut dyn Hands,
    opts: TickOptions<'_>,
) -> io::Result<TickOutcome> {
    // A tick that panicked left no lock-protected data behind (the guard protects `()`).
    let _serialized = TICK_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let _home_lock = HomeLock::acquire(home)?;
    #[cfg(test)]
    let _in_flight = tick_probe::enter();

    let tick_start_ns = now_ns();
    let mut refresh_errors = Vec::new();
    let mut state = store.load()?;
    if let Some(forge) = opts.forge {
        let outcome = observe::refresh_world(
            store,
            forge,
            opts.repo
                .as_deref()
                .or_else(|| state.repo_root.as_deref().map(Path::new)),
            hands,
            opts.refresh_timeout,
            !opts.dry_run,
        )?;
        refresh_errors = outcome.errors;
        if opts.dry_run && !refresh_errors.is_empty() {
            return Err(io::Error::other(refresh_errors.join("; ")));
        }
        state = if opts.dry_run {
            outcome.observed
        } else {
            store.load()?
        };
    }
    let now = opts.now_override.unwrap_or_else(now_secs);
    let procs = match opts.procs {
        Some(ref p) => p.clone(),
        None => observe::scan_procs(),
    };
    let main_sha = resolve_main_sha(&state, &opts, home);
    #[allow(clippy::unnecessary_lazy_evaluations)]
    let root_disk_bytes = opts.root_disk_bytes.or_else(|| {
        #[cfg(not(test))]
        if opts.procs.is_none() && opts.now_override.is_none() {
            return observe::statvfs_root_disk();
        }
        None
    });
    let host = observe::HostObs {
        now,
        live_state: opts.live_state,
        procs: &procs,
        run_pids: opts.run_pids.clone(),
        repo: opts
            .repo
            .as_deref()
            .or_else(|| state.repo_root.as_deref().map(Path::new)),
        main_sha,
        root_disk_bytes,
    };
    let (decision_state, world_obs) = observe::observe(&state, &host);
    let plan = tacho_core::decide::decide(&decision_state, &world_obs);

    if opts.dry_run {
        print_dry_run(&decision_state, &world_obs, &plan)?;
        return Ok(TickOutcome {
            tick_id: None,
            deduped: false,
            state: decision_state,
            obs: world_obs,
            plan,
        });
    }

    // Same inputs and same decision as the last recorded tick: nothing new to record or
    // execute. Point at the tick this one would duplicate (unless forge refresh failed, which
    // must surface as an error so the sampler backs off).
    if let Some(prev) = store.unchanged_since_latest_tick(&decision_state, &world_obs, &plan)? {
        if !refresh_errors.is_empty() {
            return Err(io::Error::other(format!(
                "tick {prev}: {} forge refresh error(s): {}",
                refresh_errors.len(),
                refresh_errors.join("; ")
            )));
        }
        return Ok(TickOutcome {
            tick_id: Some(prev),
            deduped: true,
            state: decision_state,
            obs: world_obs,
            plan,
        });
    }
    let tick_id = persist_plan_state(store, &state, &decision_state, &world_obs, &plan, now)?;

    let mut f = Failures {
        hands,
        errors: refresh_errors,
    };
    execute_actions(
        home, store, &mut f, &state, &world_obs, &plan, opts.forge, now,
    )?;

    let summary = format!(
        "active={}, dispatched={}, reaped={}",
        world_obs.live_keys.len(),
        plan.dispatched_tasks.len(),
        plan.reaped_agents.len()
    );
    f.hands.emit_duration_slice(
        store,
        "fleet/ticks",
        "tick",
        tick_start_ns,
        now_ns(),
        &summary,
    )?;
    if let Err(e) = f.hands.publish(store) {
        f.errors.push(format!("publish: {e}"));
    }
    println!("Tick complete at {now} ({summary})");

    if !f.errors.is_empty() {
        return Err(io::Error::other(format!(
            "tick {tick_id}: {} action(s) failed: {}",
            f.errors.len(),
            f.errors.join("; ")
        )));
    }
    Ok(TickOutcome {
        tick_id: Some(tick_id),
        deduped: false,
        state: decision_state,
        obs: world_obs,
        plan,
    })
}

fn dry_run_record(
    state: &DecisionState,
    observation: &WorldObservation,
    plan: &DecisionPlan,
    secrets: &[String],
) -> io::Result<serde_json::Value> {
    #[derive(serde::Serialize)]
    struct Preview<'a> {
        kind: &'static str,
        dry_run: bool,
        state: &'a DecisionState,
        observation: &'a WorldObservation,
        plan: &'a DecisionPlan,
    }
    fn scrub(value: &mut serde_json::Value, scrubber: &tacho_core::Scrubber, secrets: &[String]) {
        match value {
            serde_json::Value::String(text) => {
                *text = scrubber.scrub(text);
                for secret in secrets.iter().filter(|s| !s.is_empty()) {
                    *text = text.replace(secret, "<redacted>");
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    scrub(item, scrubber, secrets);
                }
            }
            serde_json::Value::Object(fields) => {
                for field in fields.values_mut() {
                    scrub(field, scrubber, secrets);
                }
            }
            _ => {}
        }
    }
    let mut record = serde_json::to_value(Preview {
        kind: "decision",
        dry_run: true,
        state,
        observation,
        plan,
    })
    .map_err(io::Error::other)?;
    scrub(&mut record, &tacho_core::Scrubber::enabled(), secrets);
    Ok(record)
}

fn print_dry_run(
    state: &DecisionState,
    observation: &WorldObservation,
    plan: &DecisionPlan,
) -> io::Result<()> {
    use std::io::Write as _;
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let secrets = tacho_core::decide::ops::snapshot::configured_secrets(&env);
    let record = dry_run_record(state, observation, plan, &secrets)?;
    writeln!(std::io::stdout().lock(), "{record}")
}

/// Fix and rebase hand-off tasks the plan asks for, appended to `todo` when not queued yet.
fn handoff_tasks(plan: &DecisionPlan, mut todo: Vec<TodoItem>, now: i64) -> Vec<TodoItem> {
    for (pr, rec) in &plan.fix_decisions.dispatch {
        let key = format!("fix-pr{pr}");
        if !todo.iter().any(|t| t.key == key) {
            todo.push(TodoItem {
                key,
                brief: format!(
                    "Fix red PR #{pr} at {} ({}):\n{}",
                    rec.sha, rec.summary, rec.log
                ),
                base: Some(pr.to_string()),
                priority: 1,
                created_at: now,
                ..Default::default()
            });
        }
    }
    for (pr, head) in &plan.rebase_handoffs {
        let key = format!("rebase-pr{pr}");
        if !todo.iter().any(|t| t.key == key) {
            todo.push(TodoItem {
                key,
                brief: format!("Rebase conflicting PR #{pr} at {head} onto main"),
                base: Some(pr.to_string()),
                priority: 1,
                created_at: now,
                ..Default::default()
            });
        }
    }
    todo
}

/// Persists `decide`'s next state. Runs before any action so actions that mutate the store
/// (spawns, resumes, aborts, worktree removal) are never overwritten by it.
fn persist_plan_state(
    store: &mut FleetStore,
    observed: &FleetState,
    decision_state: &DecisionState,
    observation: &WorldObservation,
    plan: &DecisionPlan,
    now: i64,
) -> io::Result<i64> {
    let next = &plan.next_state;
    let mut todo = plan.dispatched_tasks.clone();
    for item in handoff_tasks(plan, next.todo.clone(), now) {
        if !todo.iter().any(|t| t.key == item.key) {
            todo.push(item);
        }
    }
    let reaped = !plan.reaped_agents.is_empty();
    store.commit_tick(observed, now, (decision_state, observation, plan), |st| {
        st.todo = todo;
        st.agents = next.agents.clone();
        if reaped {
            // Exited lineages whose worktrees hold nothing more are finished without a PR.
            let idle = idle_chains(
                &st.agents,
                &st.retired,
                &BTreeSet::new(),
                &Default::default(),
            );
            for root in idle {
                if let Some(a) = st.agents.get_mut(&root) {
                    a.pr_none = true;
                }
            }
        }
        st.retired.extend(next.retired.iter().cloned());
        for (k, v) in &next.holds {
            st.holds.entry(k.clone()).or_insert_with(|| v.clone());
        }
        st.passed = next.passed.clone();
        st.failed = next.failed.clone();
        st.merged_prs = next.merged_prs.clone();
        st.closed_prs = next.closed_prs.clone();
        st.batches = next.batches.clone();
        st.batches.retain(|b| {
            !plan
                .cleared_main_broken
                .iter()
                .any(|c| c.batch_id == b.id && b.status == "main-broken")
        });
        st.flakes = next.flakes.clone();
        st.samples = next.samples.clone();
        st.nudged = next.nudged.clone();
        st.rebase_attempts = next.rebase_attempts.clone();
        st.fix_notified = next.fix_notified.clone();
        st.notified_failures = next.notified_keys.clone();
        st.inbox = next.inbox.clone();
        st.last_tick_ts = next.last_tick_ts;
        st.last_watchdog_alert_ts = next.last_watchdog_alert_ts;
        if let Some(ci) =
            observe::env_json::<tacho_core::decide::ops::snapshot::MainCiRun>("TACHO_MAIN_CI_JSON")
        {
            st.main_ci = Some(ci);
        }
    })
}

#[derive(Deserialize, Default)]
struct BatchIdentities {
    #[serde(default)]
    batch_identities: BTreeMap<String, RecordedIdentity>,
}

#[derive(Deserialize, Default, Clone)]
struct RecordedIdentity {
    pid: u32,
    #[serde(default)]
    starttime: Option<u64>,
    #[serde(default)]
    cgroup: Option<String>,
}

/// Recorded batch process identities (`$TACHO_HOME/fleet_meta.json`, written when a batch
/// is launched).
fn batch_identities(home: &Path) -> BTreeMap<String, RecordedIdentity> {
    std::fs::read_to_string(home.join("fleet_meta.json"))
        .ok()
        .and_then(|raw| serde_json::from_str::<BatchIdentities>(&raw).ok())
        .unwrap_or_default()
        .batch_identities
}

#[allow(clippy::too_many_arguments)]
fn execute_actions(
    home: &Path,
    store: &mut FleetStore,
    f: &mut Failures<'_>,
    state: &FleetState,
    obs: &WorldObservation,
    plan: &DecisionPlan,
    forge: Option<&dyn tacho_core::decide::plugin::ForgePlugin>,
    now: i64,
) -> io::Result<()> {
    // Duplicate batches: abort all but the oldest, signalling only verified processes.
    if !plan.duplicate_batch_aborts.is_empty() {
        let identities = batch_identities(home);
        for dup in &plan.duplicate_batch_aborts {
            let pid = state
                .batches
                .iter()
                .find(|b| &b.id == dup)
                .and_then(|b| b.pid);
            let outcome = match pid {
                // No process recorded: nothing runs, so only the record is aborted.
                None => Ok(Signalled::Gone),
                Some(pid) => {
                    let rec = identities.get(dup).filter(|r| r.pid == pid);
                    let id = Identity {
                        pid,
                        starttime: rec.and_then(|r| r.starttime),
                        cgroup: rec.and_then(|r| r.cgroup.as_deref()),
                    };
                    identity::signal_verified(&id, dup, libc::SIGTERM)
                }
            };
            match outcome {
                Ok(Signalled::Delivered | Signalled::Gone) => {
                    let because = format!("aborted duplicate batch {dup}");
                    store.mutate(
                        Some(("watch", "kill_duplicate_batch", dup, &because)),
                        |st| {
                            for b in st.batches.iter_mut().filter(|b| &b.id == dup) {
                                b.status = "aborted".to_string();
                                b.finished_at = Some(now);
                            }
                        },
                    )?;
                    println!("{because}");
                }
                Ok(Signalled::Refused) => f.record(
                    store,
                    "watch",
                    "kill_duplicate_batch_refused",
                    dup,
                    &format!("refused kill: no identity for duplicate batch {dup}"),
                    false,
                )?,
                Err(e) => f.record(
                    store,
                    "watch",
                    "kill_duplicate_batch_failed",
                    dup,
                    &format!("failed to abort duplicate batch {dup}: {e}"),
                    true,
                )?,
            }
        }
    }

    // Runaway burners.
    for b in &plan.burner_kills {
        let target = format!("pid:{}", b.pid);
        let id = Identity {
            pid: b.pid,
            starttime: b.starttime,
            cgroup: None,
        };
        let burner_cwd = std::fs::read_link(format!("/proc/{}/cwd", b.pid)).ok();
        match identity::signal_verified(&id, &b.comm, libc::SIGKILL) {
            Ok(Signalled::Delivered) => {
                if let Some(ref cwd) = burner_cwd.filter(|p| p.is_dir()) {
                    let run_dir = cwd.join(".tachorun");
                    let _ = std::fs::create_dir_all(&run_dir);
                    let notice = tacho_core::decide::watch::burner_notice(
                        1,
                        &tacho_core::decide::watch::WatchConfig::default(),
                    );
                    let _ = std::fs::write(run_dir.join("TACHO-NOTICE.txt"), notice);
                }
                let because = format!(
                    "killed runaway burner pid={} ({}) elapsed={}s",
                    b.pid, b.comm, b.elapsed_secs
                );
                store.record_decision("watch", "kill_burner", &target, &because)?;
                f.hands.emit("system", "watch:kill_burner", &because);
                println!("{because}");
            }
            Ok(Signalled::Gone) => store.record_decision(
                "watch",
                "kill_burner_gone",
                &target,
                &format!("burner pid={} ({}) exited before kill", b.pid, b.comm),
            )?,
            Ok(Signalled::Refused) => f.record(
                store,
                "watch",
                "kill_burner_refused",
                &target,
                &format!(
                    "refused kill of burner pid={} ({}): identity not verified",
                    b.pid, b.comm
                ),
                false,
            )?,
            Err(e) => f.record(
                store,
                "watch",
                "kill_burner_failed",
                &target,
                &format!("failed to kill burner pid={} ({}): {e}", b.pid, b.comm),
                true,
            )?,
        }
    }

    // Decided messages.
    for (key, msg) in &plan.tell_actions {
        deliver_tell(store, f, key, msg)?;
    }

    // Reaped agents: task state bands.
    for (k, found_pr, is_none) in &plan.reaped_agents {
        let (label, status, because) = if let Some(p) = found_pr {
            ("Review", TaskStatus::Review, format!("{k} opened PR #{p}"))
        } else if *is_none {
            (
                "Done",
                TaskStatus::Done,
                format!("{k} completed with PR: none"),
            )
        } else {
            (
                "Failed",
                TaskStatus::Failed,
                format!("{k} exited without PR"),
            )
        };
        store.record_decision("dispatch", "reap_agent", k, &because)?;
        store.emit_state_band(
            &format!("task:{}/state", base_key(k)),
            label,
            status,
            &because,
            now_ns(),
        )?;
    }

    // Supervision.
    for step in &plan.supervision_actions {
        match step {
            SuperviseAction::Ok => {}
            SuperviseAction::Resume { key, because } => {
                println!("Supervise: {because}");
                if let Some(a) = state.agents.get(key) {
                    supervise_resume(store, f, a, because, now)?;
                } else {
                    store.record_decision("watch", "supervise", key, because)?;
                }
                f.hands
                    .emit(&format!("agent:{key}"), "watch:supervise", because);
            }
            SuperviseAction::Nudge { key, because } => {
                println!("Supervise: {because}");
                store.record_decision("watch", "supervise_nudge", key, because)?;
                f.hands
                    .emit(&format!("agent:{key}"), "watch:supervise", because);
            }
            SuperviseAction::Escalate { key, because } => {
                println!("Supervise: {because}");
                if obs.main_checkout_workers.contains_key(key) {
                    if let Some(a) = state.agents.get(key) {
                        if let Some(pid) = a.pid {
                            let id = Identity {
                                pid,
                                starttime: a.starttime,
                                cgroup: a.cgroup.as_deref(),
                            };
                            match identity::signal_verified(&id, key, libc::SIGTERM) {
                                Ok(Signalled::Delivered) => {
                                    // Also signal the verified leader's process group so its children die too.
                                    let _ = identity::send_signal(-(pid as i32), libc::SIGTERM);
                                    store.mutate(
                                        Some(("watch", "supervise", key, because)),
                                        |st| {
                                            if let Some(ag) = st.agents.get_mut(key) {
                                                ag.finished_at.get_or_insert(now);
                                                ag.exit_code.get_or_insert(143);
                                                ag.status_note =
                                                    "stopped: directory resolves to main checkout"
                                                        .to_string();
                                            }
                                            st.holds = tacho_core::decide::dispatch::with_hold(
                                                &st.holds,
                                                base_key(key),
                                                because,
                                                now,
                                            );
                                        },
                                    )?;
                                }
                                Ok(Signalled::Gone) => {
                                    store.mutate(
                                        Some(("watch", "supervise", key, because)),
                                        |st| {
                                            if let Some(ag) = st.agents.get_mut(key) {
                                                ag.finished_at.get_or_insert(now);
                                                ag.exit_code.get_or_insert(143);
                                            }
                                            st.holds = tacho_core::decide::dispatch::with_hold(
                                                &st.holds,
                                                base_key(key),
                                                because,
                                                now,
                                            );
                                        },
                                    )?;
                                }
                                Ok(Signalled::Refused) => {
                                    f.record(
                                        store,
                                        "watch",
                                        "supervise_stop_refused",
                                        key,
                                        &format!(
                                            "refused to stop main-checkout worker {key}: identity not verified"
                                        ),
                                        false,
                                    )?;
                                }
                                Err(e) => {
                                    f.record(
                                        store,
                                        "watch",
                                        "supervise_stop_failed",
                                        key,
                                        &format!("failed to stop main-checkout worker {key}: {e}"),
                                        true,
                                    )?;
                                }
                            }
                        } else {
                            store.record_decision("watch", "supervise", key, because)?;
                        }
                    } else {
                        store.record_decision("watch", "supervise", key, because)?;
                    }
                } else {
                    // Ordinary stall escalation: notify the owner (via plan.owner_notifications)
                    // without killing the worker or holding the task.
                    store.record_decision("watch", "supervise_escalate", key, because)?;
                }
                f.hands
                    .emit(&format!("agent:{key}"), "watch:supervise", because);
            }
        }
    }

    for item in &plan.resumed_tasks {
        let because = format!("auto-queued resume {} after backoff", item.key);
        println!("{because}");
        store.record_decision("dispatch", "auto_resume", &item.key, &because)?;
    }
    for (pr, rec) in &plan.fix_decisions.dispatch {
        store.record_decision(
            "verify",
            "queue_fix",
            &format!("fix-pr{pr}"),
            &format!("PR #{pr} is red at {}: {}", rec.sha, rec.summary),
        )?;
    }
    for (pr, head) in &plan.rebase_handoffs {
        store.record_decision(
            "dispatch",
            "queue_rebase",
            &format!("rebase-pr{pr}"),
            &format!("PR #{pr} conflicts with main at {head}"),
        )?;
    }
    for c in &plan.cleared_main_broken {
        store.record_decision("verify", "clear_main_broken", &c.batch_id, &c.reason)?;
    }
    if !plan.cleared_main_broken.is_empty()
        && !plan
            .next_state
            .batches
            .iter()
            .any(|b| b.status == "main-broken")
    {
        if let Some(forge) = forge {
            let reason = plan
                .cleared_main_broken
                .iter()
                .map(|c| c.reason.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            if let Err(e) = forge.close_issue("main is red", &reason) {
                f.record(
                    store,
                    "watch",
                    "close_issue_failed",
                    "main is red",
                    &format!("failed to close 'main is red' issue: {e}"),
                    true,
                )?;
            }
        }
    }

    // Dispatch.
    for item in &plan.dispatched_tasks {
        match f.hands.spawn_worker(store, item) {
            Ok(Some(pid)) => println!("Dispatched {} (pid {pid})", item.key),
            Ok(None) => store.record_decision(
                "dispatch",
                "dispatch_queued",
                &item.key,
                "decided to dispatch; left queued for an executor that can start workers",
            )?,
            Err(e) => f.record(
                store,
                "dispatch",
                "start_failed",
                &item.key,
                &format!("failed to start {}: {e}", item.key),
                true,
            )?,
        }
    }

    // Janitor.
    if !plan.tidy_decisions.is_empty() {
        tidy::apply_tidy_decisions(store, &plan.tidy_decisions, now, false)?;
    }

    if let Some(ref disk) = plan.root_disk_action {
        if disk.prune_docker_volumes {
            match f.hands.prune_root_disk_volumes() {
                Ok(out) => {
                    let detail = if out.trim().is_empty() {
                        disk.summary.clone()
                    } else {
                        format!("{}: {}", disk.summary, out.trim())
                    };
                    store.record_decision(
                        "watch",
                        "prune_root_disk_volumes",
                        "root-disk",
                        &detail,
                    )?;
                }
                Err(e) => {
                    f.record(
                        store,
                        "watch",
                        "prune_root_disk_volumes_failed",
                        "root-disk",
                        &format!("{}: {e}", disk.summary),
                        true,
                    )?;
                }
            }
        }
        if let Some(forge) = forge {
            if disk.warn {
                let _ = forge.set_issue("The root disk is filling", &disk.summary);
            } else {
                let _ = forge.close_issue("The root disk is filling", "root disk is comfortable");
            }
        }
    }

    if let Some(target) = &plan.self_update.target_sha {
        println!("Self-update: {}", plan.self_update.because);
        store.record_decision("watch", "self_update", target, &plan.self_update.because)?;
    }
    for n in &plan.owner_notifications {
        let mut notify_failed = false;
        let mut delivered = false;
        match f.hands.notify_owner(&n.subject, &n.body) {
            Ok(sent) => delivered = sent,
            Err(e) => {
                notify_failed = true;
                f.record(
                    store,
                    "watch",
                    "notify_owner_failed",
                    &n.key,
                    &format!("failed to notify owner for {}: {e}", n.key),
                    true,
                )?;
            }
        }
        if let Some(forge) = forge {
            let issue_title = if n.kind == "main_red" || n.kind == "main-red" {
                "main is red".to_string()
            } else {
                format!("tacho alert: {}", n.key)
            };
            match forge.set_issue(&issue_title, &n.body) {
                Ok(issue) => delivered |= issue.is_some(),
                Err(e) => {
                    notify_failed = true;
                    f.record(
                        store,
                        "watch",
                        "set_issue_failed",
                        &n.key,
                        &format!("failed to set issue '{issue_title}' for {}: {e}", n.key),
                        true,
                    )?;
                }
            }
        }
        if notify_failed || !delivered {
            store.mutate(None, |st| {
                st.notified_failures.remove(&n.key);
            })?;
        } else {
            store.record_decision("watch", "notify_owner", &n.key, &n.because)?;
        }
    }
    if let Some(alert) = &plan.watchdog_alert {
        let mut alert_failed = false;
        let mut delivered = false;
        match f.hands.notify_owner("tacho is down", alert) {
            Ok(sent) => delivered = sent,
            Err(e) => {
                alert_failed = true;
                f.record(
                    store,
                    "watch",
                    "watchdog_alert_failed",
                    "tick",
                    &format!("failed to notify owner of watchdog alert: {e}"),
                    true,
                )?;
            }
        }
        if let Some(forge) = forge {
            match forge.set_issue("tacho is down", alert) {
                Ok(issue) => delivered |= issue.is_some(),
                Err(e) => {
                    alert_failed = true;
                    f.record(
                        store,
                        "watch",
                        "watchdog_alert_failed",
                        "tick",
                        &format!("failed to set 'tacho is down' issue: {e}"),
                        true,
                    )?;
                }
            }
        }
        if alert_failed || !delivered {
            let prev_ts = state.last_watchdog_alert_ts;
            store.mutate(None, |st| {
                st.last_watchdog_alert_ts = prev_ts;
            })?;
        } else {
            store.record_decision("watch", "watchdog_alert", "tick", alert)?;
        }
    }
    Ok(())
}

/// Delivers a tell; a refused resume (old process still owns the worktree) is recorded as
/// `tell_refused`, any other failure as `tell_failed`.
fn deliver_tell(
    store: &mut FleetStore,
    f: &mut Failures<'_>,
    key: &str,
    msg: &str,
) -> io::Result<()> {
    match f.hands.deliver_tell(store, key, msg) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == RESUME_REFUSED => {
            f.record(store, "watch", "tell_refused", key, &e.to_string(), false)
        }
        Err(e) => f.record(
            store,
            "watch",
            "tell_failed",
            key,
            &format!("failed to deliver message to {key}: {e}"),
            true,
        ),
    }
}

/// Carries out a supervisor `Resume` without ever restarting a running agent.
///
/// - Old process verified gone: queue a resume of the session.
/// - Verified and stopped: kill it, wait for its exit event, then queue the resume.
/// - Verified and running: deliver the guidance through the tell path.
/// - Identity unverified: refuse and record why; state is left untouched.
fn supervise_resume(
    store: &mut FleetStore,
    f: &mut Failures<'_>,
    a: &AgentRun,
    because: &str,
    now: i64,
) -> io::Result<()> {
    let key = a.key.as_str();
    let exit_code =
        match identity::classify_agent_process(a.pid, key, a.cgroup.as_deref(), a.starttime) {
            AgentProcess::Gone => a.exit_code,
            AgentProcess::Stopped(pid) => match identity::kill_stopped_and_wait(pid) {
                Ok(()) => Some(128 + libc::SIGKILL),
                Err(e) => {
                    return f.record(
                        store,
                        "watch",
                        "supervise_resume_failed",
                        key,
                        &format!("failed to stop {key} before resume: {e}"),
                        true,
                    )
                }
            },
            AgentProcess::Running => return deliver_tell(store, f, key, because),
            AgentProcess::Unverified => {
                return f.record(
                    store,
                    "watch",
                    "supervise_resume_refused",
                    key,
                    &format!("refused resume of {key}: no identity for pid {:?}", a.pid),
                    false,
                )
            }
        };
    let resumed = resume_task(a, because, "", now);
    store.mutate(Some(("watch", "supervise_resume", key, because)), |st| {
        if let Some(ag) = st.agents.get_mut(key) {
            ag.finished_at.get_or_insert(now);
            ag.exit_code = exit_code;
        }
        st.todo.push(resumed);
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod shadow_tests;
