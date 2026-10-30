use super::*;
use crate::fleet::{FleetState, FleetStore};
use tacho_core::decide::dispatch::PullFacts;
use tacho_core::decide::plugin::MockForge;
use tacho_core::decide::verify::BatchRun;

#[derive(Default)]
struct RecordingHands {
    effects: usize,
    delivered: bool,
}

impl Hands for RecordingHands {
    fn spawn_worker(&mut self, _: &mut FleetStore, _: &TodoItem) -> io::Result<Option<u32>> {
        self.effects += 1;
        Ok(None)
    }
    fn deliver_tell(&mut self, _: &mut FleetStore, _: &str, _: &str) -> io::Result<()> {
        self.effects += 1;
        Ok(())
    }
    fn emit(&mut self, _: &str, _: &str, _: &str) {
        self.effects += 1;
    }
    fn notify_owner(&mut self, _: &str, _: &str) -> Result<bool, String> {
        self.effects += 1;
        Ok(self.delivered)
    }
}

#[test]
fn forge_observation_uses_the_configured_workflow_file() -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let mut store = FleetStore::open(&home.path().join("events.sqlite"))?;
    store.save(&FleetState {
        max_workers: 0,
        workflow_path: Some(".github/workflows/ci.yaml".into()),
        ..Default::default()
    })?;
    let forge = MockForge::default();
    let live = tacho_core::LiveState::default();
    let mut hands = RecordingHands::default();
    let mut options = TickOptions::new(&live);
    options.dry_run = true;
    options.explicit_main_sha = Some("main".into());
    options.procs = Some(Vec::new());
    options.forge = Some(&forge);
    run_tick(home.path(), &mut store, &mut hands, options)?;
    let requests = forge
        .ci_requests
        .lock()
        .map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(*requests, vec![("main".into(), "ci.yaml".into())]);
    assert_eq!(hands.effects, 0);
    Ok(())
}

#[test]
fn dry_run_uses_fresh_forge_heads_without_persisting_or_executing(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let mut store = FleetStore::open(&home.path().join("events.sqlite"))?;
    let state = FleetState {
        max_workers: 0,
        open_prs_cache: vec![PullFacts {
            number: 7,
            head: "old".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    store.save(&state)?;
    let forge = MockForge {
        open_prs: Ok(vec![PullFacts {
            number: 7,
            head: "new".into(),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let live = tacho_core::LiveState::default();
    let mut hands = RecordingHands::default();
    let mut options = TickOptions::new(&live);
    options.dry_run = true;
    options.explicit_main_sha = Some("main".into());
    options.procs = Some(Vec::new());
    options.now_override = Some(1000);
    options.forge = Some(&forge);
    let result = run_tick(home.path(), &mut store, &mut hands, options)?;
    assert!(result.obs.open_prs.iter().any(|pr| pr.head == "new"));
    assert_eq!(store.load()?, state);
    assert!(store.recent_decisions(100, None, None)?.is_empty());
    assert!(store.load_latest_decision_tick()?.is_none());
    assert_eq!(hands.effects, 0);
    assert!(forge
        .issues
        .lock()
        .map_err(|e| io::Error::other(e.to_string()))?
        .is_empty());
    Ok(())
}

#[test]
fn a_dry_run_refuses_failed_observations_without_recording_them(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let mut store = FleetStore::open(&home.path().join("events.sqlite"))?;
    let forge = MockForge {
        open_prs: Err("unavailable".into()),
        ..Default::default()
    };
    let live = tacho_core::LiveState::default();
    let mut hands = RecordingHands::default();
    let mut options = TickOptions::new(&live);
    options.dry_run = true;
    options.explicit_main_sha = Some("main".into());
    options.procs = Some(Vec::new());
    options.now_override = Some(1000);
    options.forge = Some(&forge);
    assert!(run_tick(home.path(), &mut store, &mut hands, options).is_err());
    assert!(store.recent_decisions(100, None, None)?.is_empty());
    assert_eq!(hands.effects, 0);
    Ok(())
}

#[test]
fn undelivered_notifications_remain_pending_until_a_channel_succeeds(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let mut store = FleetStore::open(&home.path().join("events.sqlite"))?;
    store.save(&FleetState {
        max_workers: 0,
        batches: vec![BatchRun {
            id: "red".into(),
            status: "failed".into(),
            finished_at: Some(900),
            ..Default::default()
        }],
        ..Default::default()
    })?;
    let live = tacho_core::LiveState::default();
    let mut hands = RecordingHands::default();
    for (now, delivered) in [(1000, false), (1001, true)] {
        hands.delivered = delivered;
        let mut options = TickOptions::new(&live);
        options.explicit_main_sha = Some("main".into());
        options.procs = Some(Vec::new());
        options.now_override = Some(now);
        let result = run_tick(home.path(), &mut store, &mut hands, options)?;
        assert!(!result.plan.owner_notifications.is_empty());
        for notification in &result.plan.owner_notifications {
            assert_eq!(
                store.load()?.notified_failures.contains(&notification.key),
                delivered
            );
        }
    }
    Ok(())
}

#[test]
fn the_shadow_record_is_complete_json_with_secrets_removed(
) -> Result<(), Box<dyn std::error::Error>> {
    let state = DecisionState {
        todo: vec![TodoItem {
            key: "task".into(),
            brief: "private-credential\nsecond line".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let observation = WorldObservation::default();
    let plan = tacho_core::decide::decide(&state, &observation);
    let record = dry_run_record(&state, &observation, &plan, &["private-credential".into()])?;
    let line = serde_json::to_string(&record)?;
    assert!(!line.contains("private-credential"));
    assert!(!line.contains('\n'));
    let parsed: serde_json::Value = serde_json::from_str(&line)?;
    assert!(parsed.get("state").is_some());
    assert!(parsed.get("observation").is_some());
    assert!(parsed
        .get("plan")
        .is_some_and(|p| p.get("batch_plans").is_some()));
    assert_eq!(
        state.todo.first().map(|t| t.brief.as_str()),
        Some("private-credential\nsecond line")
    );
    Ok(())
}
