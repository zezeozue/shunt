//! High-level lifecycle orchestrator for the tachograph flight recorder daemon.
//!
//! Ties together paths, configuration, WAL event log, appender thread,
//! sampler thread, socket servers, retention pruning, signal handling, and clean shutdown.

use std::fs;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::appender::{spawn_appender_with_silence, AppenderHandle};
use crate::config::Config;
use crate::log::EventLog;
use crate::paths::DaemonPaths;
use crate::recorder::Recorder;
use crate::sampler::{timer::spawn_sampler_with_launcher, SamplerWaker};
use crate::server::{DaemonLock, DaemonServer};
use crate::snapshot::{write_snapshot, write_snapshot_file_atomic, SnapshotStats};

/// Running daemon instance.
pub struct Daemon {
    pub paths: DaemonPaths,
    pub config: Config,
    pub appender: AppenderHandle,
    pub server: DaemonServer,
    pub shutdown_flag: Arc<AtomicBool>,
    retention_wake_tx: Option<crossbeam_channel::Sender<()>>,
    signal_handle: Option<signal_hook::iterator::Handle>,
    otlp_thread: Option<JoinHandle<()>>,
    otlp_shutdown_fds: Option<(std::os::unix::io::RawFd, std::os::unix::io::RawFd)>,
    sampler_thread: Option<JoinHandle<()>>,
    sampler_waker: SamplerWaker,
    retention_thread: Option<JoinHandle<()>>,
    appender_thread: Option<JoinHandle<()>>,
    signal_thread: Option<JoinHandle<()>>,
}

impl Daemon {
    /// Initializes and starts all daemon subsystems per design section 1 and 8.
    pub fn start(paths: DaemonPaths, config: Config) -> io::Result<Self> {
        Self::start_with_launcher(paths, config, None)
    }

    /// Starts the daemon with an adapter for project process execution.
    pub fn start_with_launcher(
        paths: DaemonPaths,
        config: Config,
        launcher: Option<Arc<dyn crate::exec::Launcher>>,
    ) -> io::Result<Self> {
        paths.ensure_dirs()?;

        // 0. Acquire single-instance advisory locks on both $TACHO_RUNTIME/daemon.lock and
        // $TACHO_HOME/daemon.lock BEFORE opening SQLite WAL or spawning background threads.
        let lock = DaemonLock::acquire_paths(&paths)?;
        if let Err(e) = crate::snapshot::write_file_atomic(
            &paths.runtime.join("daemon.pid"),
            std::process::id().to_string().as_bytes(),
        ) {
            eprintln!("[tacho-daemon] failed to write daemon.pid: {}", e);
        }

        // 1. Open SQLite WAL event log and initialize fleet / decision_ticks tables
        let log =
            EventLog::open(&paths.events_db()).map_err(|e| io::Error::other(e.to_string()))?;
        crate::fleet::FleetStore::open(&paths.events_db())?;

        // 2. Restart recovery: load all logged events and restore recorder state (design test 7)
        let logged_events = log
            .load_all()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let mut recorder = Recorder::restore(logged_events.iter());
        recorder.set_scrub(config.scrub);
        recorder.set_detector_config(tacho_core::DetectorConfig {
            silent_timeout_ns: config.detectors.silent_s * 1_000_000_000,
            loop_repeats: config.detectors.loop_count,
            spin_turns: config.detectors.spin_turns,
            burn_tokens: config.detectors.burn_tokens,
            burn_window_ns: config.detectors.burn_window_s * 1_000_000_000,
            failing_repeats: config.detectors.failing_count,
        });

        let on_stall = if config.sampler.dry_run {
            None
        } else {
            config.detectors.on_stall.clone()
        };
        let on_stall_timeout = Duration::from_secs(config.detectors.on_stall_timeout_secs);

        // Fallible setup that needs no threads happens first, so an error here cannot leak
        // half-started background threads.
        let otlp_listener = match config.otlp_addr.as_deref() {
            Some(addr) => Some((std::net::TcpListener::bind(addr)?, shutdown_pipe()?)),
            None => None,
        };

        // 3. Spawn appender thread (no in-memory rings; SQLite WAL is the sole source)
        let (appender, appender_thread) = spawn_appender_with_silence(
            recorder,
            log,
            65_536,
            on_stall,
            on_stall_timeout,
            Duration::from_secs(config.detectors.silent_s),
        );

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        // 4. Spawn sampler thread (also runs autonomous `decide` ticks on $TACHO_HOME)
        let (sampler_thread, sampler_waker) = match spawn_sampler_with_launcher(
            config.sampler.clone(),
            appender.clone(),
            shutdown_flag.clone(),
            Some(paths.home.clone()),
            launcher.clone(),
        ) {
            Ok(pair) => pair,
            Err(e) => {
                appender.shutdown();
                join_logged("appender", appender_thread);
                return Err(e);
            }
        };
        appender.set_sampler_waker(sampler_waker.clone());

        // 5. Start UNIX domain socket servers (api.sock, trace.sock) using the acquired lock
        let server = match DaemonServer::start_with_launcher(
            paths.clone(),
            lock,
            appender.clone(),
            shutdown_flag.clone(),
            config.sampler.dry_run,
            launcher,
        ) {
            Ok(server) => server,
            Err(e) => {
                // Unwind the threads started so far before reporting the error.
                shutdown_flag.store(true, Ordering::Release);
                appender.shutdown();
                drop(sampler_waker);
                join_logged("sampler", sampler_thread);
                join_logged("appender", appender_thread);
                return Err(e);
            }
        };
        server.set_sampler_waker(sampler_waker.clone());
        if let Err(e) = crate::snapshot::write_file_atomic(
            &paths.runtime.join("daemon.pid"),
            std::process::id().to_string().as_bytes(),
        ) {
            eprintln!("[tacho-daemon] failed to update daemon.pid: {}", e);
        }

        // 6. Spawn retention maintenance thread (hourly small-batch prune)
        let (retention_wake_tx, retention_wake_rx) = crossbeam_channel::bounded::<()>(1);
        let retention_thread = if config.sampler.dry_run {
            None
        } else {
            Some(spawn_retention_thread(
                config.retention_days,
                paths.clone(),
                shutdown_flag.clone(),
                retention_wake_rx,
            ))
        };

        // 7. Register SIGINT / SIGTERM signal handler
        let (signal_handle, signal_thread) =
            spawn_signal_handler(shutdown_flag.clone(), paths.clone(), appender.clone());

        // 8. Optional OTLP HTTP receiver (design section 1 and 8)
        let (otlp_thread, otlp_shutdown_fds) =
            if let Some((listener, (otlp_rx, otlp_tx))) = otlp_listener {
                let shutdown_clone = shutdown_flag.clone();
                let appender_clone = appender.clone();
                let otlp_cfg = tacho_ingest::otlp_server::OtlpServerConfig {
                    shutdown_fd: Some(otlp_rx),
                    ..Default::default()
                };
                let handle = std::thread::Builder::new()
                    .name("tacho-otlp".to_string())
                    .spawn(move || {
                        let on_inputs = move |inputs: Vec<tacho_core::Input>| {
                            let now_ns = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_nanos() as u64;
                            let batch: Vec<(tacho_core::Input, u64)> =
                                inputs.into_iter().map(|inp| (inp, now_ns)).collect();
                            if let Err(e) = appender_clone.send_input_batch(batch) {
                                eprintln!("[tacho-daemon] failed to enqueue OTLP batch: {}", e);
                            }
                        };
                        if let Err(e) = tacho_ingest::otlp_server::serve_otlp_with_config(
                            listener,
                            shutdown_clone,
                            otlp_cfg,
                            on_inputs,
                        ) {
                            eprintln!("[tacho-daemon] OTLP server stopped with error: {}", e);
                        }
                        if otlp_rx >= 0 {
                            unsafe { libc::close(otlp_rx) };
                        }
                    })
                    .expect("failed to spawn otlp thread");
                (Some(handle), Some((otlp_rx, otlp_tx)))
            } else {
                (None, None)
            };

        Ok(Self {
            paths,
            config,
            appender,
            server,
            shutdown_flag,
            retention_wake_tx: Some(retention_wake_tx),
            signal_handle,
            otlp_thread,
            otlp_shutdown_fds,
            sampler_thread: Some(sampler_thread),
            sampler_waker,
            retention_thread,
            appender_thread: Some(appender_thread),
            signal_thread,
        })
    }

    /// Captures an on-demand snapshot directly to `w`.
    pub fn snapshot(&self, since: Option<u64>, w: &mut impl Write) -> io::Result<SnapshotStats> {
        write_snapshot(&self.paths.events_db(), since, None, w)
    }

    /// Rebuilds a trace from the SQLite log.
    pub fn rebuild_from_log(
        &self,
        since: Option<u64>,
        until: Option<u64>,
        w: &mut impl Write,
    ) -> io::Result<SnapshotStats> {
        crate::snapshot::rebuild_from_log(&self.paths.events_db(), since, until, w)
    }

    /// Cleanly stops all daemon threads and writes final snapshot.
    pub fn stop(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        drop(self.retention_wake_tx.take());
        if let Some(h) = self.signal_handle.take() {
            h.close();
        }

        // Flush and shutdown appender
        self.appender.flush();
        self.appender.shutdown();

        // Stop server and clean sockets
        self.server.stop();

        // Write final trace to last.perfetto-trace
        if let Err(e) =
            write_snapshot_file_atomic(&self.paths.events_db(), &self.paths.last_trace())
        {
            eprintln!("[tacho-daemon] failed to write final trace: {}", e);
        }

        if let Some((_rx, tx)) = self.otlp_shutdown_fds.take() {
            if tx >= 0 {
                unsafe { libc::close(tx) };
            }
        }
        if let Some(t) = self.otlp_thread.take() {
            join_logged("otlp", t);
        }
        if let Some(t) = self.retention_thread.take() {
            join_logged("retention", t);
        }
        self.sampler_waker.wake();
        if let Some(t) = self.sampler_thread.take() {
            join_logged("sampler", t);
        }
        if let Some(t) = self.appender_thread.take() {
            join_logged("appender", t);
        }
        if let Some(t) = self.signal_thread.take() {
            join_logged("signal", t);
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
#[path = "daemon_shadow_tests.rs"]
mod shadow_tests;

/// Joins a daemon thread, reporting a panic instead of discarding it.
fn join_logged(name: &str, handle: JoinHandle<()>) {
    if handle.join().is_err() {
        eprintln!("[tacho-daemon] {name} thread panicked");
    }
}

/// Removes a socket file on shutdown; an already-missing file is fine.
fn remove_socket_logged(path: &std::path::Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => eprintln!("[tacho-daemon] failed to remove {}: {e}", path.display()),
    }
}

/// Creates the pipe whose write end, when closed, tells the OTLP server to exit.
fn shutdown_pipe() -> io::Result<(std::os::unix::io::RawFd, std::os::unix::io::RawFd)> {
    let mut fds = [-1i32; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((fds[0], fds[1]))
}

/// Spawns the retention pruning background thread.
///
/// Runs hourly, deleting rows older than `retention_days` in small batches of 1,000
/// while listening on `wake_rx` for immediate shutdown notification.
///
/// The hourly wait is a maintenance schedule (retention is defined in days), not a poll for
/// state. Between batches the thread only checks, without waiting, whether shutdown was
/// requested: each batch is its own short transaction, so the appender interleaves freely.
fn spawn_retention_thread(
    retention_days: u32,
    paths: DaemonPaths,
    stop_flag: Arc<AtomicBool>,
    wake_rx: crossbeam_channel::Receiver<()>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("tacho-retention".to_string())
        .spawn(move || {
            if retention_days == 0 {
                return;
            }

            let retention_ns = (retention_days as u64) * 86_400 * 1_000_000_000;

            while !stop_flag.load(Ordering::Acquire) {
                match wake_rx.recv_timeout(Duration::from_secs(3600)) {
                    Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                }
                if stop_flag.load(Ordering::Acquire) {
                    return;
                }

                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;

                let cutoff_ns = now_ns.saturating_sub(retention_ns);
                let cutoff_secs = (cutoff_ns / 1_000_000_000) as i64;
                match crate::fleet::FleetStore::open(&paths.events_db()) {
                    Ok(mut store) => {
                        if let Err(e) = store.prune_decision_ticks_older_than(cutoff_secs) {
                            eprintln!("[tacho-daemon] decision tick prune error: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[tacho-daemon] failed to open fleet store for tick prune: {}",
                            e
                        );
                    }
                }

                match EventLog::open(&paths.events_db()) {
                    Ok(mut log) => {
                        // Small-batch pruning: 1000 rows per batch, yielding between transactions
                        const BATCH_SIZE: usize = 1000;
                        loop {
                            if stop_flag.load(Ordering::Acquire) {
                                break;
                            }
                            match log.prune_older_than(cutoff_ns, BATCH_SIZE) {
                                Ok(0) => break, // Done
                                Ok(_) => match wake_rx.try_recv() {
                                    Ok(()) | Err(crossbeam_channel::TryRecvError::Disconnected) => {
                                        return;
                                    }
                                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                                },
                                Err(e) => {
                                    eprintln!("[tacho-daemon] retention prune error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[tacho-daemon] failed to open event log for retention prune: {}",
                            e
                        );
                    }
                }
            }
        })
        .expect("failed to spawn retention thread")
}

/// Spawns background signal handler thread for clean SIGINT / SIGTERM handling.
fn spawn_signal_handler(
    shutdown_flag: Arc<AtomicBool>,
    paths: DaemonPaths,
    appender: AppenderHandle,
) -> (
    Option<signal_hook::iterator::Handle>,
    Option<JoinHandle<()>>,
) {
    let mut signals = match signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
    ]) {
        Ok(s) => s,
        Err(_) => return (None, None),
    };
    let handle = signals.handle();
    let thread = std::thread::Builder::new()
        .name("tacho-signals".to_string())
        .spawn(move || {
            if let Some(_sig) = signals.forever().next() {
                shutdown_flag.store(true, Ordering::Release);
                appender.flush();

                // Save final trace
                if let Err(e) = write_snapshot_file_atomic(&paths.events_db(), &paths.last_trace())
                {
                    eprintln!("[tacho-daemon] failed to write final trace: {}", e);
                }

                remove_socket_logged(&paths.api_sock());
                remove_socket_logged(&paths.trace_sock());
            }
        })
        .expect("failed to spawn signal thread");
    (Some(handle), Some(thread))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_autonomously_runs_decide_without_api_tick() {
        let dir = tempfile::tempdir().unwrap();
        let paths =
            DaemonPaths::custom(dir.path().join("home"), dir.path().join("runtime")).unwrap();
        paths.ensure_dirs().unwrap();

        let mut store = crate::fleet::FleetStore::open(&paths.events_db()).unwrap();
        store
            .mutate(None, |st| {
                st.inbox
                    .entry("worker-1".to_string())
                    .or_default()
                    .push("Ship the autonomous decide loop".to_string());
            })
            .unwrap();

        let mut daemon = Daemon::start(paths.clone(), Config::default()).unwrap();
        let ts_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        daemon
            .appender
            .send_input(
                tacho_core::Input::RunStart {
                    source_ts_ns: Some(ts_ns),
                    provider: "claude".to_string(),
                    session_id: "sess-auto".to_string(),
                    agent: Some("worker-1".to_string()),
                    model: Some("claude-sonnet".to_string()),
                    task: Some("Autonomous decide".to_string()),
                    cwd: None,
                    pid: Some(std::process::id()),
                    role: None,
                    parent_session: None,
                    git_tree: None,
                },
                ts_ns,
            )
            .unwrap();
        assert!(daemon.appender.flush());
        // Wait for the sampler tick woken by `RunStart` commit to complete without any API call.
        daemon.sampler_waker.sample_now();

        let latest = store.load_latest_decision_tick().unwrap();
        assert!(
            latest.is_some(),
            "daemon must autonomously record a decision_ticks row without API tick/decide"
        );
        let (_id, _state, obs, plan) = latest.unwrap();
        assert!(
            !obs.main_sha.is_empty(),
            "recorded main_sha must never be empty"
        );
        assert!(
            plan.tell_actions.contains(&(
                "worker-1".to_string(),
                "Ship the autonomous decide loop".to_string()
            )),
            "expected tell_actions in autonomous decision tick, got: {:?}",
            plan.tell_actions
        );
        assert!(
            paths.home.join("inbox/worker-1.jsonl").exists(),
            "expected autonomous decide tick to deliver inbox message"
        );

        daemon.stop();
    }
}
