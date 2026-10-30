use super::*;
use crate::appender::spawn_appender;
use crate::log::EventLog;
use std::io::Read;
use tacho_core::Recorder;

struct Fixture {
    _temp: tempfile::TempDir,
    shared: Arc<ServerShared>,
    appender_thread: Option<std::thread::JoinHandle<()>>,
}

impl Fixture {
    fn new(capacity: usize) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let paths = DaemonPaths::custom(temp.path().join("home"), temp.path().join("rt")).unwrap();
        let log = EventLog::open(&paths.events_db()).unwrap();
        let (appender, thread) = spawn_appender(Recorder::new(), log, capacity);
        let shared = Arc::new(ServerShared {
            launcher: None,
            dry_run: false,
            stop: Arc::new(AtomicBool::new(false)),
            pipe: ShutdownPipe::new().unwrap(),
            conns: ConnRegistry::default(),
            appender,
            sampler_waker: Mutex::new(None),
            paths,
        });
        Self {
            _temp: temp,
            shared,
            appender_thread: Some(thread),
        }
    }

    /// Serves one connection on a socketpair in a worker thread, like an api.sock worker.
    fn connect(&self) -> (UnixStream, std::thread::JoinHandle<()>) {
        let (client, server) = UnixStream::pair().unwrap();
        let shared = self.shared.clone();
        let worker = std::thread::spawn(move || {
            let Some(_guard) = shared.conns.register(&server) else {
                return;
            };
            handle_api_connection(server, &shared);
        });
        (client, worker)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.shared.appender.shutdown();
        if let Some(t) = self.appender_thread.take() {
            let _ = t.join();
        }
    }
}

fn request(
    client: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    line: &[u8],
) -> serde_json::Value {
    client.write_all(line).unwrap();
    client.write_all(b"\n").unwrap();
    let mut resp = String::new();
    reader.read_line(&mut resp).unwrap();
    serde_json::from_str(&resp).unwrap_or_else(|e| panic!("reply {:?} is not JSON: {}", resp, e))
}

fn sample_input() -> Input {
    Input::RunStart {
        source_ts_ns: Some(100),
        provider: "test".into(),
        session_id: "s1".into(),
        agent: Some("a1".into()),
        task: None,
        pid: None,
        cwd: None,
        model: None,
        parent_session: None,
        git_tree: None,
        role: None,
    }
}

#[test]
fn test_ingest_reports_stopped_appender_for_batch_and_single() {
    let mut fx = Fixture::new(1);
    let (mut client, worker) = fx.connect();
    let mut reader = BufReader::new(client.try_clone().unwrap());

    // Join the appender so its channel is disconnected.
    fx.shared.appender.shutdown();
    fx.appender_thread.take().unwrap().join().unwrap();

    let batch = serde_json::to_vec(&serde_json::json!({ "inputs": [sample_input()] })).unwrap();
    let v = request(&mut client, &mut reader, &batch);
    assert_eq!(v["status"], "error");
    assert_eq!(v["error"], "appender stopped");

    let single = serde_json::to_vec(&serde_json::json!({ "input": sample_input() })).unwrap();
    let v = request(&mut client, &mut reader, &single);
    assert_eq!(v["error"], "appender stopped");

    drop(reader);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn test_error_replies_are_valid_json_even_with_hostile_text() {
    let fx = Fixture::new(64);
    let (mut client, worker) = fx.connect();
    let mut reader = BufReader::new(client.try_clone().unwrap());

    // Quotes, backslashes and newlines in the command must not break the reply framing.
    let v = request(
        &mut client,
        &mut reader,
        br#"{"cmd":"x\"}],\n\\ \u2028 \ud83d\ude80"}"#,
    );
    assert_eq!(v["status"], "error");
    assert!(v["error"].as_str().unwrap().starts_with("unknown command"));

    let v = request(&mut client, &mut reader, br#"{"cmd": "ping", "#);
    assert!(v["error"].as_str().unwrap().starts_with("invalid JSON"));

    let v = request(&mut client, &mut reader, br#"{"input": {"type": "Nope"}}"#);
    assert!(v["error"]
        .as_str()
        .unwrap()
        .starts_with("invalid Input schema"));

    let v = request(&mut client, &mut reader, br#"{"inputs": 7}"#);
    assert!(v["error"]
        .as_str()
        .unwrap()
        .starts_with("invalid Input schema"));

    let v = request(&mut client, &mut reader, br#"[1,2,3]"#);
    assert!(v["error"].as_str().unwrap().contains("missing"));

    let v = request(
        &mut client,
        &mut reader,
        br#"{"cmd":"ping","note":"\"input\""}"#,
    );
    assert_eq!(
        v["pong"], true,
        "commands mentioning \"input\" must still run"
    );

    let v = request(&mut client, &mut reader, br#"{"inputs": []}"#);
    assert_eq!(v["status"], "ok");

    drop(reader);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn test_invalid_utf8_and_oversized_lines_get_an_error_then_close() {
    let fx = Fixture::new(64);
    let (mut client, worker) = fx.connect();
    client.write_all(b"{\"cmd\":\"\xff\xfe\"}\n").unwrap();
    let mut out = String::new();
    client.read_to_string(&mut out).unwrap();
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["status"], "error");
    worker.join().unwrap();

    let (client, worker) = fx.connect();
    let writer = {
        let mut c = client.try_clone().unwrap();
        std::thread::spawn(move || {
            // One line one byte over the limit; the server stops reading mid-way.
            let chunk = vec![b'a'; 1 << 20];
            let mut sent = 0usize;
            while sent <= MAX_API_LINE_BYTES {
                if c.write_all(&chunk).is_err() {
                    return;
                }
                sent += chunk.len();
            }
        })
    };
    // The server hangs up with our unread bytes queued, so after the reply line the
    // socket reports ECONNRESET; read just the line.
    let mut out = String::new();
    BufReader::new(client.try_clone().unwrap())
        .read_line(&mut out)
        .unwrap();
    assert!(out.contains("exceeds maximum"), "got {:?}", out);
    worker.join().unwrap();
    drop(client);
    writer.join().unwrap();
}

#[test]
fn test_pipelined_ingest_gets_one_ack_per_line_and_is_committed() {
    let fx = Fixture::new(1024);
    let (mut client, worker) = fx.connect();
    let line = serde_json::to_string(&serde_json::json!({ "input": sample_input() })).unwrap();
    let mut payload = String::new();
    for _ in 0..500 {
        payload.push_str(&line);
        payload.push('\n');
    }
    payload.push_str("{\"cmd\":\"flush\"}\n");
    client.write_all(payload.as_bytes()).unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();

    let mut out = String::new();
    client.read_to_string(&mut out).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 501);
    assert!(lines[..500].iter().all(|l| *l == "{\"status\":\"ok\"}"));
    assert!(lines[500].contains("\"flushed\":true"));
    worker.join().unwrap();
    assert!(fx.shared.appender.live_state().agents.contains_key("a1"));
}

#[test]
fn test_watch_ends_when_client_hangs_up() {
    let fx = Fixture::new(64);
    let (client, worker) = fx.connect();
    let mut c = client.try_clone().unwrap();
    c.write_all(b"{\"cmd\":\"watch\"}\n").unwrap();
    let mut reader = BufReader::new(client);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("watching"));
    line.clear();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("state_change"));

    // No state changes will ever happen; only the hangup can end the watch.
    drop(reader);
    drop(c);
    worker.join().unwrap();
}

#[test]
fn test_watch_until_matches_on_state_change() {
    let fx = Fixture::new(64);
    let (client, worker) = fx.connect();
    let mut c = client.try_clone().unwrap();
    c.write_all(b"{\"cmd\":\"watch\",\"until\":\"exited\"}\n")
        .unwrap();
    let mut reader = BufReader::new(client);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("watching"));

    let appender = &fx.shared.appender;
    appender.send_input(sample_input(), 100).unwrap();
    appender
        .send_input(
            Input::RunEnd {
                source_ts_ns: Some(200),
                session: tacho_core::SessionKey::new("test", "s1"),
                exit_code: Some(0),
                reason: None,
            },
            200,
        )
        .unwrap();
    assert!(appender.flush());

    let mut rest = String::new();
    reader.read_to_string(&mut rest).unwrap();
    assert!(rest.contains("{\"matched\":\"exited\"}"), "got {:?}", rest);
    worker.join().unwrap();
}

#[test]
fn test_request_stop_unblocks_idle_and_watching_connections() {
    let fx = Fixture::new(64);
    let (idle, idle_worker) = fx.connect();
    let (mut watcher, watch_worker) = fx.connect();
    watcher.write_all(b"{\"cmd\":\"watch\"}\n").unwrap();
    let mut reader = BufReader::new(watcher.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();

    fx.shared.request_stop();
    idle_worker.join().unwrap();
    watch_worker.join().unwrap();
    drop(idle);

    // Registering after stop refuses the connection.
    let (a, _b) = UnixStream::pair().unwrap();
    assert!(fx.shared.conns.register(&a).is_none());
}

#[test]
fn test_tick_refuses_to_record_without_main_sha_but_allows_dry_run() {
    let fx = Fixture::new(64);
    let (mut client, worker) = fx.connect();
    let mut reader = BufReader::new(client.try_clone().unwrap());

    let v = request(&mut client, &mut reader, br#"{"cmd":"tick"}"#);
    assert_eq!(v["status"], "error", "{v}");
    assert!(v["error"].as_str().unwrap().contains("main_sha"));

    let v = request(
        &mut client,
        &mut reader,
        br#"{"cmd":"tick","dry_run":true}"#,
    );
    assert_eq!(v["status"], "ok", "{v}");
    assert!(v.get("tick_id").is_none());

    let v = request(
        &mut client,
        &mut reader,
        br#"{"cmd":"decide","main_sha":"0123456789abcdef0123456789abcdef01234567"}"#,
    );
    assert_eq!(v["status"], "ok", "{v}");
    assert!(v["tick_id"].is_i64());

    drop(reader);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn test_server_stop_with_connected_clients_and_shutdown_command() {
    let temp = tempfile::tempdir().unwrap();
    let paths = DaemonPaths::custom(temp.path().join("home"), temp.path().join("rt")).unwrap();
    let log = EventLog::open(&paths.events_db()).unwrap();
    let (appender, appender_thread) = spawn_appender(Recorder::new(), log, 64);
    let stop = Arc::new(AtomicBool::new(false));
    let mut server = DaemonServer::start(paths.clone(), appender.clone(), stop.clone()).unwrap();

    // A second server on the same paths is refused by the lock.
    assert!(DaemonServer::start(paths.clone(), appender.clone(), stop.clone()).is_err());

    // An idle client that never sends anything.
    let _idle = UnixStream::connect(paths.api_sock()).unwrap();

    let mut c = UnixStream::connect(paths.api_sock()).unwrap();
    c.write_all(b"{\"cmd\":\"ping\"}\n{\"cmd\":\"shutdown\"}\n")
        .unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).unwrap();
    assert!(out.contains("pong"), "{out}");
    assert!(out.contains("shutting down"), "{out}");
    assert!(stop.load(Ordering::Acquire));

    server.stop();
    assert!(!paths.api_sock().exists());
    assert!(!paths.trace_sock().exists());
    appender_thread.join().unwrap();
}

#[test]
fn test_trace_sock_serves_snapshot_and_survives_a_client_that_hangs_up() {
    let temp = tempfile::tempdir().unwrap();
    let paths = DaemonPaths::custom(temp.path().join("home"), temp.path().join("rt")).unwrap();
    let log = EventLog::open(&paths.events_db()).unwrap();
    let (appender, appender_thread) = spawn_appender(Recorder::new(), log, 64);
    appender.send_input(sample_input(), 100).unwrap();
    assert!(appender.flush());
    let mut server = DaemonServer::start(
        paths.clone(),
        appender.clone(),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();

    drop(UnixStream::connect(paths.trace_sock()).unwrap());

    let mut c = UnixStream::connect(paths.trace_sock()).unwrap();
    let mut bytes = Vec::new();
    c.read_to_end(&mut bytes).unwrap();
    let trace = tacho_proto::perfetto::protos::Trace::decode(&bytes[..]).unwrap();
    assert!(!trace.packet.is_empty());

    server.stop();
    appender.shutdown();
    appender_thread.join().unwrap();
}

use prost::Message;
