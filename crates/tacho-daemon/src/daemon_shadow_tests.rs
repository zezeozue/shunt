use super::*;
use crate::fleet::{FleetState, FleetStore};
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use tacho_core::decide::dispatch::TodoItem;

#[test]
fn shadow_mode_covers_autonomous_ticks_and_api_requests() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let paths = DaemonPaths::custom(directory.path().join("home"), directory.path().join("run"))?;
    paths.ensure_dirs()?;
    let state = FleetState {
        max_workers: 1,
        main_sha: Some("main".into()),
        todo: vec![TodoItem {
            key: "queued-task".into(),
            brief: "Observe this task".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let store = FleetStore::open(&paths.events_db())?;
    store.save(&state)?;
    let mut config = Config::default();
    config.sampler.dry_run = true;
    let mut daemon = Daemon::start(paths.clone(), config)?;
    daemon.sampler_waker.request_decide();
    daemon.sampler_waker.sample_now();
    let mut connection = UnixStream::connect(paths.api_sock())?;
    connection.write_all(b"{\"cmd\":\"tick\",\"main_sha\":\"main\"}\n")?;
    let mut response = String::new();
    BufReader::new(connection).read_line(&mut response)?;
    let response: serde_json::Value = serde_json::from_str(&response)?;
    assert_eq!(response.get("status").and_then(|s| s.as_str()), Some("ok"));
    assert_eq!(store.load()?, state);
    assert!(store.recent_decisions(100, None, None)?.is_empty());
    assert!(store.load_latest_decision_tick()?.is_none());
    assert!(daemon.retention_thread.is_none());
    daemon.stop();
    Ok(())
}
