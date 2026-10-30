//! Fixed-rate `timerfd` tick source and background sampler loop.
//!
//! CPU and memory counters in `/proc` have no change notification, so rates can only be
//! obtained by sampling on a clock. One periodic `timerfd` keeps a steady cadence (a slow
//! tick does not shift later ones) without creating an fd per tick, while a pipe waker
//! allows prompt, sleep-free shutdown and on-demand synchronous sampling ticks.
//!
//! `decide` does not run on every tick: a tick runs it only when the appender committed a
//! decide-relevant event, when [`SamplerWaker::request_decide`] asked for it, or when the
//! coarse `decide_refresh_secs` interval has elapsed (see `ProcSampler::decide_due`).

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::appender::AppenderHandle;
use crate::config::SamplerConfig;

use super::ProcSampler;

/// Spawns the background sampler thread. It notices `stop_flag` at the next tick; use
/// [`spawn_sampler_with_waker`] to stop it immediately.
pub fn spawn_sampler(
    config: SamplerConfig,
    appender: AppenderHandle,
    stop_flag: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let control = Arc::new(SamplerControl::new(None));
    std::thread::Builder::new()
        .name("tacho-sampler".to_string())
        .spawn(move || run_sampler_loop(config, appender, stop_flag, None, control, None, None))
        .expect("failed to spawn sampler thread")
}

struct SamplerControl {
    write: Mutex<Option<std::os::fd::OwnedFd>>,
    tick_seq: Mutex<u64>,
    tick_cvar: Condvar,
    running: AtomicBool,
    /// Shared with the loop's `ProcSampler`; forces a decide pass on the next tick.
    decide_requested: Arc<AtomicBool>,
}

impl SamplerControl {
    fn new(write: Option<std::os::fd::OwnedFd>) -> Self {
        Self {
            write: Mutex::new(write),
            tick_seq: Mutex::new(0),
            tick_cvar: Condvar::new(),
            running: AtomicBool::new(true),
            decide_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    fn wake_pipe(&self) {
        use std::os::fd::AsRawFd;
        if let Some(fd) = self
            .write
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            let byte = [b't'];
            // SAFETY: writing one byte from a local buffer to a pipe fd we own.
            let n = unsafe { libc::write(fd.as_raw_fd(), byte.as_ptr().cast(), 1) };
            if n < 0 {
                let err = io::Error::last_os_error();
                // A full pipe already holds a pending wake, which is all a wake needs.
                if err.kind() != io::ErrorKind::WouldBlock {
                    eprintln!("[tacho-daemon] failed to wake sampler: {err}");
                }
            }
        }
    }
}

/// Ends a sampler's wait between ticks so it sees its stop flag without waiting out the
/// interval, or triggers an immediate synchronous sampling tick via [`SamplerWaker::sample_now`].
#[derive(Clone)]
pub struct SamplerWaker {
    control: Arc<SamplerControl>,
}

impl SamplerWaker {
    /// Closes the wake pipe so the sampler loop exits immediately once `stop_flag` is set.
    pub fn wake(&mut self) {
        let mut guard = self.control.write.lock().unwrap_or_else(|p| p.into_inner());
        drop(guard.take());
        self.control.tick_cvar.notify_all();
    }

    /// Asynchronously wakes the sampler loop to run an immediate tick without blocking the caller.
    pub fn trigger_tick(&self) {
        if self.control.running.load(Ordering::Acquire) {
            self.control.wake_pipe();
        }
    }

    /// Asks for a `decide` pass on an immediate tick, even if no decide-relevant event was
    /// committed since the last one. Does not block.
    pub fn request_decide(&self) {
        self.control.decide_requested.store(true, Ordering::Release);
        self.trigger_tick();
    }

    /// Triggers an immediate sampling tick and blocks until a full tick that started after
    /// this call has completed (or the sampler stops).
    pub fn sample_now(&self) {
        if !self.control.running.load(Ordering::Acquire) {
            return;
        }
        let mut seq = self
            .control
            .tick_seq
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // Require 2 completions so that even if a tick was already mid-flight when
        // `sample_now` was called, one full tick starts and finishes after this point.
        let target = seq.saturating_add(2);
        // Every completed tick and the loop's exit notify `tick_cvar`, so this wait ends on
        // the sampler's own progress; without a wake pipe the next timerfd tick serves.
        while *seq < target && self.control.running.load(Ordering::Acquire) {
            self.control.wake_pipe();
            seq = self
                .control
                .tick_cvar
                .wait(seq)
                .unwrap_or_else(|p| p.into_inner());
        }
    }
}

/// Spawns the sampler thread plus a [`SamplerWaker`] for prompt shutdown and on-demand ticks.
pub fn spawn_sampler_with_waker(
    config: SamplerConfig,
    appender: AppenderHandle,
    stop_flag: Arc<AtomicBool>,
) -> io::Result<(std::thread::JoinHandle<()>, SamplerWaker)> {
    spawn_sampler_with_home(config, appender, stop_flag, None)
}

/// Spawns the sampler thread configured with `$TACHO_HOME` so sampler ticks also run autonomous `decide`.
pub fn spawn_sampler_with_home(
    config: SamplerConfig,
    appender: AppenderHandle,
    stop_flag: Arc<AtomicBool>,
    home_dir: Option<std::path::PathBuf>,
) -> io::Result<(std::thread::JoinHandle<()>, SamplerWaker)> {
    spawn_sampler_with_launcher(config, appender, stop_flag, home_dir, None)
}

/// Spawns the sampler with the daemon's project process launcher.
pub fn spawn_sampler_with_launcher(
    config: SamplerConfig,
    appender: AppenderHandle,
    stop_flag: Arc<AtomicBool>,
    home_dir: Option<std::path::PathBuf>,
    launcher: Option<Arc<dyn crate::exec::Launcher>>,
) -> io::Result<(std::thread::JoinHandle<()>, SamplerWaker)> {
    use std::os::fd::FromRawFd;
    let mut fds = [-1i32; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe2 succeeded; both fds are fresh and owned here.
    let (read, write) = unsafe {
        (
            std::os::fd::OwnedFd::from_raw_fd(fds[0]),
            std::os::fd::OwnedFd::from_raw_fd(fds[1]),
        )
    };
    let control = Arc::new(SamplerControl::new(Some(write)));
    let waker = SamplerWaker {
        control: control.clone(),
    };
    let handle = std::thread::Builder::new()
        .name("tacho-sampler".to_string())
        .spawn(move || {
            run_sampler_loop(
                config,
                appender,
                stop_flag,
                Some(read),
                control,
                home_dir,
                launcher,
            )
        })?;
    Ok((handle, waker))
}

fn run_sampler_loop(
    config: SamplerConfig,
    appender: AppenderHandle,
    stop_flag: Arc<AtomicBool>,
    wake: Option<std::os::fd::OwnedFd>,
    control: Arc<SamplerControl>,
    home_dir: Option<std::path::PathBuf>,
    launcher: Option<Arc<dyn crate::exec::Launcher>>,
) {
    let interval = Duration::from_millis(config.interval_ms.max(100));
    let mut proc_sampler = ProcSampler::new(&config.mounts);
    proc_sampler.set_cgroup_enabled(config.cgroup && !config.dry_run);
    proc_sampler.set_dry_run(config.dry_run);
    proc_sampler.set_launcher(launcher);
    proc_sampler.set_worktree_scan_secs(config.worktree_scan_secs);
    proc_sampler.set_decide_refresh_secs(config.decide_refresh_secs);
    proc_sampler.set_decide_request_flag(control.decide_requested.clone());
    if let Some(home) = home_dir {
        proc_sampler.set_home_dir(home);
    }
    let mut run_track_buf = String::with_capacity(64);
    let ticker = IntervalTimer::new(interval);

    while !stop_flag.load(Ordering::Acquire) {
        if let Some(home) = proc_sampler.home_dir() {
            if !home.exists() {
                stop_flag.store(true, Ordering::Release);
                unsafe { libc::kill(std::process::id() as i32, libc::SIGTERM) };
                break;
            }
        }
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        proc_sampler.sample_tick(&appender, now_ns, &mut run_track_buf);
        {
            let mut seq = control.tick_seq.lock().unwrap_or_else(|p| p.into_inner());
            *seq = seq.saturating_add(1);
            control.tick_cvar.notify_all();
        }
        if !ticker.wait_next(wake.as_ref()) {
            break;
        }
    }

    control.running.store(false, Ordering::Release);
    control.tick_cvar.notify_all();
}

/// Fixed-rate tick source backed by `timerfd_create(CLOCK_MONOTONIC)`.
pub(super) struct IntervalTimer {
    fd: Option<std::os::fd::OwnedFd>,
    interval: Duration,
}

impl IntervalTimer {
    pub(super) fn new(interval: Duration) -> Self {
        use std::os::fd::{AsRawFd, FromRawFd};
        let raw = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_CLOEXEC | libc::TFD_NONBLOCK,
            )
        };
        let fd = (raw >= 0).then(|| unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) });
        if let Some(fd) = &fd {
            let ts = libc::timespec {
                tv_sec: interval.as_secs() as libc::time_t,
                tv_nsec: interval.subsec_nanos() as libc::c_long,
            };
            let spec = libc::itimerspec {
                it_interval: ts,
                it_value: ts,
            };
            unsafe { libc::timerfd_settime(fd.as_raw_fd(), 0, &spec, std::ptr::null_mut()) };
        }
        Self { fd, interval }
    }

    /// Blocks until the next tick or on-demand wake (true), or until `wake` hangs up (false).
    /// Missed ticks are coalesced rather than replayed.
    pub(super) fn wait_next(&self, wake: Option<&std::os::fd::OwnedFd>) -> bool {
        use std::os::fd::AsRawFd;
        let mut pfds = [
            libc::pollfd {
                fd: self.fd.as_ref().map_or(-1, |f| f.as_raw_fd()),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake.map_or(-1, |f| f.as_raw_fd()),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // Without a timerfd, fall back to a poll timeout of one interval.
        let timeout_ms = if self.fd.is_some() {
            -1
        } else {
            self.interval.as_millis().min(i32::MAX as u128) as i32
        };
        loop {
            let rc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, timeout_ms) };
            if rc < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return true;
            }
            if pfds[1].revents != 0 {
                if let Some(wfd) = wake {
                    let mut drain = [0u8; 64];
                    let n = unsafe {
                        libc::read(wfd.as_raw_fd(), drain.as_mut_ptr().cast(), drain.len())
                    };
                    if n == 0 || (pfds[1].revents & libc::POLLHUP) != 0 && n <= 0 {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            if pfds[0].revents != 0 {
                if let Some(fd) = &self.fd {
                    // Reading resets the expiration count, coalescing missed ticks.
                    let mut expirations = 0u64;
                    // SAFETY: reading 8 bytes into a local u64 from a timerfd we own.
                    let n = unsafe {
                        libc::read(
                            fd.as_raw_fd(),
                            &mut expirations as *mut u64 as *mut libc::c_void,
                            std::mem::size_of::<u64>(),
                        )
                    };
                    if n < 0 {
                        let err = io::Error::last_os_error();
                        if err.kind() != io::ErrorKind::WouldBlock {
                            eprintln!("[tacho-daemon] sampler timerfd read failed: {err}");
                        }
                    }
                }
            }
            return true;
        }
    }
}
