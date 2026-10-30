//! UNIX domain socket servers for `api.sock` and `trace.sock`.
//!
//! Enforces single-instance locking via flock, handles stale socket cleanup,
//! serves `api.sock` using a bounded worker pool (8 threads), and serves
//! `trace.sock` via a dedicated single thread streaming directly from SQLite WAL.
//!
//! Nothing here waits on a clock. Acceptors block in `poll(-1)` on their listener plus a
//! shutdown pipe; workers block on the connection channel, which disconnects when the
//! acceptor exits; and every live client stream is registered so that stopping the
//! server can `shutdown(2)` it, unblocking any worker parked in a read or write.

use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tacho_core::Input;

use crate::appender::AppenderHandle;
use crate::paths::DaemonPaths;
use crate::recorder::{render_report, render_status};
use crate::sampler::SamplerWaker;
use crate::snapshot::write_snapshot;

/// Maximum allowed NDJSON line length on `api.sock` (16 MiB) to prevent OOM from unbounded lines.
const MAX_API_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Number of `api.sock` worker threads.
const API_WORKERS: usize = 8;

/// A `trace.sock` client that stops reading must not wedge the single trace thread forever.
const TRACE_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(60);

/// Single-instance advisory lock on `$TACHO_RUNTIME/daemon.lock` (and `$TACHO_HOME/daemon.lock`).
pub struct DaemonLock {
    _files: Vec<fs::File>,
}

impl DaemonLock {
    /// Attempts to acquire an exclusive, non-blocking advisory flock on `lock_path`.
    pub fn acquire(lock_path: &Path) -> io::Result<Self> {
        let file = Self::flock_exclusive(lock_path)?;
        Ok(Self { _files: vec![file] })
    }

    /// Acquires exclusive, non-blocking advisory flocks on both `$TACHO_RUNTIME/daemon.lock`
    /// and `$TACHO_HOME/daemon.lock` so a second daemon cannot share either runtime sockets
    /// or the SQLite WAL event log directory.
    pub fn acquire_paths(paths: &DaemonPaths) -> io::Result<Self> {
        let runtime_lock_path = paths.lock_file();
        let home_lock_path = paths.home.join("daemon.lock");
        let mut files = Vec::with_capacity(2);
        files.push(Self::flock_exclusive(&runtime_lock_path)?);
        if home_lock_path != runtime_lock_path {
            files.push(Self::flock_exclusive(&home_lock_path)?);
        }
        Ok(Self { _files: files })
    }

    fn flock_exclusive(lock_path: &Path) -> io::Result<fs::File> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;

        let res = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if res != 0 {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("another daemon instance holds lock {:?}", lock_path),
            ));
        }
        Ok(file)
    }
}

/// A pipe whose read end reports `POLLHUP` once [`ShutdownPipe::trigger`] closes the write end.
///
/// Every acceptor polls the same read end, so one trigger wakes all of them, and the
/// `OwnedFd`s guarantee neither end leaks or is double-closed.
struct ShutdownPipe {
    read: OwnedFd,
    write: Mutex<Option<OwnedFd>>,
}

impl ShutdownPipe {
    fn new() -> io::Result<Self> {
        let mut fds = [-1i32; 2];
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: pipe2 succeeded, so both fds are open and exclusively ours.
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        Ok(Self {
            read,
            write: Mutex::new(Some(write)),
        })
    }

    fn read_fd(&self) -> RawFd {
        self.read.as_raw_fd()
    }

    fn trigger(&self) {
        let mut guard = self.write.lock().unwrap_or_else(|p| p.into_inner());
        drop(guard.take());
    }
}

/// Tracks every live client stream so shutdown can unblock workers stuck in I/O.
#[derive(Default)]
struct ConnRegistry {
    next_id: AtomicU64,
    closed: AtomicBool,
    conns: Mutex<HashMap<u64, UnixStream>>,
}

/// Removes a stream from the [`ConnRegistry`] when the connection is done.
struct ConnGuard<'a> {
    registry: &'a ConnRegistry,
    id: u64,
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        self.registry
            .conns
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
    }
}

impl ConnRegistry {
    /// Registers `stream`; returns `None` (and shuts the stream down) once the server is stopping.
    fn register(&self, stream: &UnixStream) -> Option<ConnGuard<'_>> {
        let clone = stream.try_clone().ok()?;
        let mut conns = self.conns.lock().unwrap_or_else(|p| p.into_inner());
        // Checked under the lock so a concurrent `shutdown_all` cannot miss this stream.
        if self.closed.load(Ordering::Acquire) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        conns.insert(id, clone);
        Some(ConnGuard { registry: self, id })
    }

    fn shutdown_all(&self) {
        let conns = self.conns.lock().unwrap_or_else(|p| p.into_inner());
        self.closed.store(true, Ordering::Release);
        for stream in conns.values() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

/// State shared by the acceptors, the trace thread and the API workers.
struct ServerShared {
    dry_run: bool,
    launcher: Option<Arc<dyn crate::exec::Launcher>>,
    stop: Arc<AtomicBool>,
    pipe: ShutdownPipe,
    conns: ConnRegistry,
    appender: AppenderHandle,
    sampler_waker: Mutex<Option<SamplerWaker>>,
    paths: DaemonPaths,
}

impl ServerShared {
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    /// Flags shutdown and wakes every thread that might be blocked on it.
    fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.pipe.trigger();
        self.conns.shutdown_all();
        self.appender.wake_state_waiters();
    }
}

/// Running server handle managing background socket listener threads.
pub struct DaemonServer {
    paths: DaemonPaths,
    _lock: DaemonLock,
    shared: Arc<ServerShared>,
    api_thread: Option<std::thread::JoinHandle<()>>,
    trace_thread: Option<std::thread::JoinHandle<()>>,
    worker_threads: Vec<std::thread::JoinHandle<()>>,
}

impl DaemonServer {
    /// Starts the servers on `api.sock` and `trace.sock`, acquiring the daemon lock first.
    pub fn start(
        paths: DaemonPaths,
        appender: AppenderHandle,
        shutdown_flag: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let lock = DaemonLock::acquire_paths(&paths)?;
        Self::start_with_lock(paths, lock, appender, shutdown_flag, false)
    }

    /// Starts the servers on `api.sock` and `trace.sock` using an already-acquired [`DaemonLock`].
    pub fn start_with_lock(
        paths: DaemonPaths,
        lock: DaemonLock,
        appender: AppenderHandle,
        shutdown_flag: Arc<AtomicBool>,
        dry_run: bool,
    ) -> io::Result<Self> {
        Self::start_with_launcher(paths, lock, appender, shutdown_flag, dry_run, None)
    }

    /// Starts API workers with the daemon's project process launcher.
    pub fn start_with_launcher(
        paths: DaemonPaths,
        lock: DaemonLock,
        appender: AppenderHandle,
        shutdown_flag: Arc<AtomicBool>,
        dry_run: bool,
        launcher: Option<Arc<dyn crate::exec::Launcher>>,
    ) -> io::Result<Self> {
        // We hold the lock, so any socket file present is a leftover from an unclean exit.
        for sock in [paths.api_sock(), paths.trace_sock()] {
            match fs::remove_file(&sock) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        let api_listener = UnixListener::bind(paths.api_sock())?;
        let trace_listener = UnixListener::bind(paths.trace_sock())?;
        fs::set_permissions(paths.api_sock(), fs::Permissions::from_mode(0o700))?;
        fs::set_permissions(paths.trace_sock(), fs::Permissions::from_mode(0o700))?;
        api_listener.set_nonblocking(true)?;
        trace_listener.set_nonblocking(true)?;

        let shared = Arc::new(ServerShared {
            dry_run,
            launcher,
            stop: shutdown_flag,
            pipe: ShutdownPipe::new()?,
            conns: ConnRegistry::default(),
            appender,
            sampler_waker: Mutex::new(None),
            paths: paths.clone(),
        });

        // The acceptor owns the only sender; when it exits the channel disconnects and
        // idle workers return from `recv` without any timeout.
        let (conn_tx, conn_rx) = crossbeam_channel::bounded::<UnixStream>(128);
        let mut worker_threads = Vec::with_capacity(API_WORKERS);
        for i in 0..API_WORKERS {
            let rx = conn_rx.clone();
            let sh = shared.clone();
            worker_threads.push(
                std::thread::Builder::new()
                    .name(format!("tacho-api-worker-{}", i + 1))
                    .spawn(move || run_api_worker(rx, &sh))?,
            );
        }
        drop(conn_rx);

        let sh = shared.clone();
        let api_thread = std::thread::Builder::new()
            .name("tacho-api-acceptor".to_string())
            .spawn(move || {
                accept_loop(&api_listener, &sh, |stream| conn_tx.send(stream).is_ok());
            })?;

        let sh = shared.clone();
        let trace_thread = std::thread::Builder::new()
            .name("tacho-trace-server".to_string())
            .spawn(move || {
                accept_loop(&trace_listener, &sh, |stream| {
                    serve_trace_client(stream, &sh);
                    true
                });
            })?;

        Ok(Self {
            paths,
            _lock: lock,
            shared,
            api_thread: Some(api_thread),
            trace_thread: Some(trace_thread),
            worker_threads,
        })
    }

    /// Attaches a [`SamplerWaker`] so `{"cmd":"flush"}` triggers a synchronous sampler tick
    /// before flushing the appender.
    pub fn set_sampler_waker(&self, waker: SamplerWaker) {
        *self
            .shared
            .sampler_waker
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(waker);
    }

    /// Stops servers, joins threads, and removes socket files.
    pub fn stop(&mut self) {
        self.shared.request_stop();

        if let Some(t) = self.api_thread.take() {
            let _ = t.join();
        }
        if let Some(t) = self.trace_thread.take() {
            let _ = t.join();
        }
        for w in self.worker_threads.drain(..) {
            let _ = w.join();
        }

        let _ = fs::remove_file(self.paths.api_sock());
        let _ = fs::remove_file(self.paths.trace_sock());
        self._lock._files.clear();
    }
}

impl Drop for DaemonServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Accepts connections on a non-blocking `listener` until shutdown, handing each to `on_conn`.
///
/// Blocks in `poll(-1)` on the listener and the shutdown pipe, so it neither spins nor
/// wakes up on a timer. Returns when `on_conn` returns false, on shutdown, or on a fatal
/// listener error.
fn accept_loop(
    listener: &UnixListener,
    shared: &ServerShared,
    mut on_conn: impl FnMut(UnixStream) -> bool,
) {
    while !shared.stopping() {
        match listener.accept() {
            Ok((stream, _)) => {
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                if !on_conn(stream) {
                    return;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !wait_readable_or_shutdown(listener.as_raw_fd(), shared.pipe.read_fd()) {
                    return;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            // Per-connection failures (peer gone before accept, fd exhaustion) are transient.
            Err(e) if is_transient_accept_error(&e) => {
                if !wait_readable_or_shutdown(listener.as_raw_fd(), shared.pipe.read_fd()) {
                    return;
                }
            }
            Err(e) => {
                eprintln!("[tacho-daemon] accept failed, listener exiting: {}", e);
                return;
            }
        }
    }
}

fn is_transient_accept_error(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ECONNABORTED | libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    )
}

/// Blocks until `fd` is readable (true) or the shutdown pipe fires or poll fails (false).
fn wait_readable_or_shutdown(fd: RawFd, shutdown_fd: RawFd) -> bool {
    loop {
        let mut pfds = [
            libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: shutdown_fd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
        if ret < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return false;
        }
        if pfds[1].revents != 0 {
            return false;
        }
        return true;
    }
}

/// Streams one snapshot to a `trace.sock` client.
///
/// CRITICAL: snapshots use their own read-only WAL connection and never touch the appender
/// channel or locks, so they succeed even if the appender is wedged.
fn serve_trace_client(stream: UnixStream, shared: &ServerShared) {
    let Some(_guard) = shared.conns.register(&stream) else {
        return;
    };
    let _ = stream.set_write_timeout(Some(TRACE_CLIENT_WRITE_TIMEOUT));
    let mut writer = &stream;
    if let Err(e) = write_snapshot(&shared.paths.events_db(), None, None, &mut writer) {
        if !is_disconnect(&e) {
            eprintln!("[tacho-daemon] trace.sock snapshot failed: {}", e);
        }
    }
    let _ = writer.flush();
}

fn is_disconnect(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
    )
}

/// Worker thread servicing api.sock connections until the acceptor hangs up.
fn run_api_worker(rx: Receiver<UnixStream>, shared: &ServerShared) {
    while let Ok(stream) = rx.recv() {
        if shared.stopping() {
            continue; // Drain and drop queued connections.
        }
        let Some(_guard) = shared.conns.register(&stream) else {
            continue;
        };
        handle_api_connection(stream, shared);
    }
}

/// Reads a single newline-delimited UTF-8 line up to `max_bytes`.
/// Returns `Err(InvalidData)` if the line exceeds `max_bytes` without a newline or contains invalid UTF-8.
fn read_bounded_line(
    reader: &mut impl BufRead,
    line_buf: &mut String,
    max_bytes: usize,
) -> io::Result<usize> {
    let mut raw = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            break;
        }
        let (take_len, done) = match available.iter().position(|&b| b == b'\n') {
            Some(pos) => (pos + 1, true),
            None => (available.len(), false),
        };
        if raw.len() + take_len > max_bytes {
            reader.consume(take_len);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "API request line exceeds maximum allowed byte length",
            ));
        }
        raw.extend_from_slice(&available[..take_len]);
        reader.consume(take_len);
        if done {
            break;
        }
    }
    if raw.is_empty() {
        return Ok(0);
    }
    let s = std::str::from_utf8(&raw).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    line_buf.push_str(s);
    Ok(raw.len())
}

/// What to do after a request has been handled.
enum Reply {
    /// Write one NDJSON response line and keep serving.
    Line(Vec<u8>),
    /// The handler streamed its own output and the connection is finished.
    Closed,
    /// Write the response line, then stop the whole server.
    LineThenShutdown(Vec<u8>),
}

/// Builds a JSON error reply; serde escapes the message, so arbitrary text cannot break framing.
fn error_reply(msg: impl std::fmt::Display) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "status": "error", "error": msg.to_string() }))
        .unwrap_or_default()
}

const OK_REPLY: &[u8] = b"{\"status\":\"ok\"}";

/// Processes NDJSON requests on a client stream until disconnection or shutdown.
fn handle_api_connection(stream: UnixStream, shared: &ServerShared) {
    let Ok(reader_stream) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::with_capacity(65536, reader_stream);
    let mut writer = io::BufWriter::with_capacity(65536, stream);
    let mut line_buf = String::with_capacity(1024);

    while !shared.stopping() {
        line_buf.clear();
        match read_bounded_line(&mut reader, &mut line_buf, MAX_API_LINE_BYTES) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // Tell the client why before hanging up; the stream position is unreliable now.
                let _ = writer.write_all(&error_reply(e));
                let _ = writer.write_all(b"\n");
                break;
            }
            Err(_) => break,
        }

        let trimmed = line_buf.trim();
        if trimmed.is_empty() {
            continue;
        }

        let reply = process_ndjson_line(trimmed, shared, writer.get_mut());
        let (bytes, shutdown_after) = match reply {
            Reply::Line(b) => (b, false),
            Reply::LineThenShutdown(b) => (b, true),
            Reply::Closed => return,
        };
        if writer.write_all(&bytes).is_err() || writer.write_all(b"\n").is_err() {
            break;
        }
        // Batch acks for pipelined requests: only flush once the client's input is drained.
        if (shutdown_after || reader.buffer().is_empty()) && writer.flush().is_err() {
            break;
        }
        if shutdown_after {
            shared.request_stop();
            break;
        }
    }
    let _ = writer.flush();
}

/// `{"input": ...}` or `{"inputs": [...]}` decoded without building a `Value` tree.
#[derive(serde::Deserialize)]
struct IngestLine {
    #[serde(default)]
    input: Option<Input>,
    #[serde(default)]
    inputs: Option<Vec<Input>>,
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Queues inputs on the appender and returns the client-facing ack.
fn ingest(appender: &AppenderHandle, inputs: Vec<Input>) -> Vec<u8> {
    let ts = now_ns();
    let batch: Vec<(Input, u64)> = inputs.into_iter().map(|i| (i, ts)).collect();
    match appender.send_input_batch(batch) {
        Ok(()) => OK_REPLY.to_vec(),
        Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => {
            eprintln!("[tacho-daemon] appender queue full; rejecting ingest");
            error_reply("queue full")
        }
        Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
            error_reply("appender stopped")
        }
    }
}

/// Handles a single line of NDJSON input.
fn process_ndjson_line(line: &str, shared: &ServerShared, stream: &mut UnixStream) -> Reply {
    // Fast path: hot ingest traffic skips the generic `Value` parse.
    if line.contains("\"input") {
        if let Ok(req) = serde_json::from_str::<IngestLine>(line) {
            match (req.input, req.inputs) {
                (Some(one), None) => return Reply::Line(ingest(&shared.appender, vec![one])),
                (None, Some(many)) => return Reply::Line(ingest(&shared.appender, many)),
                (Some(one), Some(mut many)) => {
                    many.insert(0, one);
                    return Reply::Line(ingest(&shared.appender, many));
                }
                (None, None) => {} // e.g. a command whose payload mentions "input"
            }
        }
    }

    let val: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return Reply::Line(error_reply(format!("invalid JSON: {}", e))),
    };

    // The fast path failed to decode an ingest line: report which schema is wrong.
    for key in ["input", "inputs"] {
        if let Some(v) = val.get(key) {
            let err = if key == "input" {
                serde_json::from_value::<Input>(v.clone()).err()
            } else {
                serde_json::from_value::<Vec<Input>>(v.clone()).err()
            };
            return Reply::Line(error_reply(format!(
                "invalid Input schema: {}",
                err.map(|e| e.to_string()).unwrap_or_default()
            )));
        }
    }

    let Some(cmd) = val.get("cmd").and_then(|v| v.as_str()) else {
        return Reply::Line(error_reply("missing 'input' or 'cmd' in request"));
    };
    let appender = &shared.appender;
    match cmd {
        "ping" => Reply::Line(b"{\"pong\":true}".to_vec()),
        "flush" => {
            let waker = shared
                .sampler_waker
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if let Some(w) = waker {
                w.sample_now();
            }
            if appender.flush() {
                Reply::Line(b"{\"flushed\":true,\"status\":\"ok\"}".to_vec())
            } else {
                Reply::Line(error_reply(
                    "flush did not complete: appender busy or stopped",
                ))
            }
        }
        "tick" | "decide" => Reply::Line(handle_decide(&val, shared)),
        "status" => Reply::Line(handle_status(&val, shared)),
        "report" => Reply::Line(handle_report(&val, appender)),
        "watch" => {
            handle_watch(&val, shared, stream);
            Reply::Closed
        }
        "trace" => {
            handle_trace(&val, &shared.paths, stream);
            Reply::Closed
        }
        "shutdown" => {
            appender.shutdown();
            Reply::LineThenShutdown(b"{\"message\":\"shutting down\",\"status\":\"ok\"}".to_vec())
        }
        _ => Reply::Line(error_reply(format!("unknown command '{}'", cmd))),
    }
}

fn handle_status(val: &serde_json::Value, shared: &ServerShared) -> Vec<u8> {
    let json = val.get("json").and_then(|v| v.as_bool()).unwrap_or(false);
    let state = shared.appender.live_state();
    let mut text = render_status(&state, json, now_ns());
    let db_size = fs::metadata(shared.paths.events_db())
        .map(|m| m.len())
        .unwrap_or(0);
    if !json {
        text.push_str(&format!("\nevents.sqlite: {} bytes\n", db_size));
    }
    let resp = serde_json::json!({
        "status": "ok",
        "output": text,
        "events_sqlite_bytes": db_size,
        "events_sqlite_size_bytes": db_size,
    });
    serde_json::to_vec(&resp).unwrap_or_default()
}

fn handle_report(val: &serde_json::Value, appender: &AppenderHandle) -> Vec<u8> {
    let agent = val.get("agent").and_then(|v| v.as_str()).unwrap_or("");
    let since = val.get("since").and_then(|v| v.as_u64());
    let state = appender.live_state();
    let text = render_report(&state, agent, since, now_ns());
    let resp = serde_json::json!({
        "status": "ok",
        "output": text,
        "agent_state": state.agents.get(agent),
    });
    serde_json::to_vec(&resp).unwrap_or_default()
}

fn handle_trace(val: &serde_json::Value, paths: &DaemonPaths, stream: &mut UnixStream) {
    let since = val.get("since").and_then(|v| v.as_u64());
    let until = val.get("until").and_then(|v| v.as_u64());
    let from_log = val
        .get("from_log")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let res = if from_log {
        crate::snapshot::rebuild_from_log(&paths.events_db(), since, until, stream)
    } else {
        write_snapshot(&paths.events_db(), since, until, stream)
    };
    if let Err(e) = res {
        if !is_disconnect(&e) {
            eprintln!("[tacho-daemon] trace command failed: {}", e);
        }
    }
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

/// True when any agent is in the state named by `until` (`stalled` also matches flags).
fn watch_target_matched(state: &crate::recorder::LiveState, target: &str) -> bool {
    state.agents.values().any(|a| {
        a.state.eq_ignore_ascii_case(target)
            || (target.eq_ignore_ascii_case("stalled")
                && (!a.flags.is_empty() || a.timeline.iter().any(|t| t.kind == "Stall")))
    })
}

fn write_json_line(stream: &mut UnixStream, v: &serde_json::Value) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(v).unwrap_or_default();
    bytes.push(b'\n');
    stream.write_all(&bytes)?;
    stream.flush()
}

/// Streams rendered status whenever live state changes, until the client hangs up,
/// the `until` target matches, or the server stops.
///
/// A helper thread blocks in `poll(-1)` for the peer's hangup and the shutdown pipe, then
/// wakes the state condvar, so the loop below waits only on real events.
fn handle_watch(val: &serde_json::Value, shared: &ServerShared, stream: &mut UnixStream) {
    let until = val.get("until").and_then(|v| v.as_str());
    let appender = &shared.appender;
    let hung_up = Arc::new(AtomicBool::new(false));

    let hangup_watcher = {
        let fd = stream.as_raw_fd();
        let shutdown_fd = shared.pipe.read_fd();
        let hung_up = hung_up.clone();
        let appender = appender.clone();
        // The fds outlive the thread: it is joined below before `stream` can be dropped,
        // and the shutdown pipe lives as long as `shared`.
        std::thread::Builder::new()
            .name("tacho-watch-hup".to_string())
            .spawn(move || {
                wait_peer_hangup(fd, shutdown_fd);
                hung_up.store(true, Ordering::Release);
                appender.wake_state_waiters();
            })
    };

    let mut seq = appender.state_seq();
    let mut last_rendered = String::new();
    if write_json_line(stream, &serde_json::json!({"status": "watching"})).is_ok() {
        while !shared.stopping() && !hung_up.load(Ordering::Acquire) {
            let state = appender.live_state();
            let current = render_status(&state, false, now_ns());
            if current != last_rendered {
                let line = serde_json::json!({"event": "state_change", "state": current});
                if write_json_line(stream, &line).is_err() {
                    break;
                }
                last_rendered = current;
            }
            if let Some(target) = until {
                if watch_target_matched(&state, target) {
                    let _ = write_json_line(stream, &serde_json::json!({"matched": target}));
                    break;
                }
            }
            seq = appender.wait_state_change_until(seq, &hung_up);
        }
    }

    // Shutting the socket down raises POLLHUP for the helper, so the join cannot hang.
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
    if let Ok(t) = hangup_watcher {
        let _ = t.join();
    }
}

/// Blocks until the peer of `fd` hangs up (or the socket is shut down) or `shutdown_fd` fires.
fn wait_peer_hangup(fd: RawFd, shutdown_fd: RawFd) {
    let hup = libc::POLLHUP | libc::POLLRDHUP | libc::POLLERR | libc::POLLNVAL;
    loop {
        let mut pfds = [
            libc::pollfd {
                fd,
                events: libc::POLLRDHUP,
                revents: 0,
            },
            libc::pollfd {
                fd: shutdown_fd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
        if ret < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
        if pfds[1].revents != 0 || pfds[0].revents & hup != 0 {
            return;
        }
        // Only POLLRDHUP was requested, so bytes a watch client sends do not wake us.
    }
}

/// Runs one `decide` pass over the stored fleet state and, unless `dry_run`, records it.
///
/// A recorded tick must be replayable, so it needs the real main SHA: taken from the
/// request's `main_sha`, else resolved with git in the request's `repo`. Without one the
/// tick is refused instead of logging an empty SHA.
fn handle_decide(val: &serde_json::Value, shared: &ServerShared) -> Vec<u8> {
    let dry_run = shared.dry_run
        || val
            .get("dry_run")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

    let main_sha = val
        .get("main_sha")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            val.get("repo")
                .and_then(|v| v.as_str())
                .and_then(|repo| crate::fleet::git_head_sha(Path::new(repo)))
        })
        .unwrap_or_default();
    if main_sha.is_empty() && !dry_run {
        return error_reply(
            "refusing to record a decision tick without main_sha: pass \"main_sha\" or \"repo\"",
        );
    }

    let live_state = shared.appender.live_state();
    let explicit_sha = (!main_sha.is_empty()).then_some(main_sha);
    match crate::fleet::execute_decide_tick_with_launcher(
        &shared.paths.home,
        &live_state,
        explicit_sha,
        dry_run,
        shared.launcher.clone(),
        Some(shared.appender.clone()),
    ) {
        Ok((tick_id, _decision_state, _world_obs, plan)) => {
            let mut resp = serde_json::json!({ "status": "ok", "plan": plan });
            if let Some(id) = tick_id {
                resp["tick_id"] = serde_json::json!(id);
            }
            serde_json::to_vec(&resp).unwrap_or_default()
        }
        Err(e) => error_reply(format!("failed to execute decision tick: {}", e)),
    }
}

#[cfg(test)]
mod tests;
