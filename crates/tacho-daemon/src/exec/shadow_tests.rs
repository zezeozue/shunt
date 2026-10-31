use super::*;
use crate::fleet::{FleetState, FleetStore};
use tacho_core::decide::dispatch::PullFacts;
use tacho_core::decide::plugin::MockForge;
use tacho_core::decide::verify::BatchRun;

#[derive(Default)]
struct RecordingHands {
    effects: usize,
    delivered: bool,
    fail_notify: bool,
    sent_mails: Vec<(String, String)>,
}

impl Hands for RecordingHands {
    fn spawn_worker(&mut self, _: &mut FleetStore, _: &TodoItem) -> io::Result<Option<u32>> {
        self.effects += 1;
        Ok(None)
    }
    fn spawn_batch(
        &mut self,
        _: &mut FleetStore,
        _: &tacho_core::decide::verify::BatchPlan,
    ) -> io::Result<Option<u32>> {
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
    fn notify_owner(&mut self, subject: &str, body: &str) -> Result<bool, String> {
        self.effects += 1;
        if self.fail_notify {
            return Err("mail transport unavailable".into());
        }
        if self.delivered {
            self.sent_mails
                .push((subject.to_string(), body.to_string()));
        }
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

#[test]
fn ready_set_subset_and_daily_digest_notifications_follow_daemon_lifecycle(
) -> Result<(), Box<dyn std::error::Error>> {
    use tacho_core::decide::land::TrainRecord;
    use tacho_core::decide::verify::{AreaCheck, AreaRule, BatchMode, PassedRecord};

    let home = tempfile::tempdir()?;
    let mut store = FleetStore::open(&home.path().join("events.sqlite"))?;
    let main_sha = "m".repeat(40);
    let sha_101 = "a".repeat(40);
    let sha_102 = "b".repeat(40);
    let sha_103 = "c".repeat(40);
    let sha_102_v2 = "d".repeat(40);

    let make_pr = |number: u64, head: &str| PullFacts {
        number,
        head: head.to_string(),
        branch: format!("feat-{number}"),
        title: format!("PR {number}"),
        state: "OPEN".into(),
        mergeable: "MERGEABLE".into(),
        files: vec!["src/lib.rs".into()],
        ..Default::default()
    };

    store.save(&FleetState {
        max_workers: 0,
        area_rules: vec![AreaRule {
            area: "core".into(),
            prefixes: vec!["src/".into()],
            checks: vec![("test".into(), "true".into())],
            unit_prefixes: BTreeMap::new(),
            isolate_batch: false,
            needs_screenshot: false,
        }],
        open_prs_cache: vec![make_pr(101, &sha_101), make_pr(102, &sha_102)],
        batches: vec![BatchRun {
            id: "b001".into(),
            group: "core".into(),
            prs: vec![101, 102],
            shas: BTreeMap::from([(101, sha_101.clone()), (102, sha_102.clone())]),
            main_sha: main_sha.clone(),
            areas: vec!["core".into()],
            checks: vec![AreaCheck {
                area: "core".into(),
                units: vec!["all".into()],
            }],
            mode: BatchMode::Presubmit,
            status: "passed".into(),
            started_at: 900,
            finished_at: Some(950),
            tree: Some("tree-101-102".into()),
            ..Default::default()
        }],
        passed: BTreeMap::from([
            (
                101,
                PassedRecord {
                    pr: 101,
                    sha: sha_101.clone(),
                    main_sha: main_sha.clone(),
                    batch_id: "b001".into(),
                    passed_at: 950,
                    tree: "tree-101-102".into(),
                    areas: vec!["core".into()],
                },
            ),
            (
                102,
                PassedRecord {
                    pr: 102,
                    sha: sha_102.clone(),
                    main_sha: main_sha.clone(),
                    batch_id: "b001".into(),
                    passed_at: 950,
                    tree: "tree-101-102".into(),
                    areas: vec!["core".into()],
                },
            ),
        ]),
        trains: vec![TrainRecord {
            id: "t001".into(),
            prs: vec![101, 102],
            shas: BTreeMap::from([(101, sha_101.clone()), (102, sha_102.clone())]),
            base: main_sha.clone(),
            tree: "tree-101-102".into(),
            status: "open".into(),
            created_at: 960,
            batch_id: "b001".into(),
            landed_prs: Vec::new(),
            reason: None,
        }],
        ..Default::default()
    })?;

    let live = tacho_core::LiveState::default();
    let mut hands = RecordingHands::default();
    let run_at =
        |store: &mut FleetStore, hands: &mut RecordingHands, now: i64| -> io::Result<TickOutcome> {
            let mut options = TickOptions::new(&live);
            options.explicit_main_sha = Some(main_sha.clone());
            options.procs = Some(Vec::new());
            options.now_override = Some(now);
            run_tick(home.path(), store, hands, options)
        };

    // 1. Unconfigured channel (Ok(false)) and failing channel (Err) keep ready notice pending.
    hands.delivered = false;
    let _ = run_at(&mut store, &mut hands, 1_000)?;
    assert!(store.load()?.notified_failures.is_empty());
    assert!(hands.sent_mails.is_empty());

    hands.fail_notify = true;
    assert!(run_at(&mut store, &mut hands, 1_005).is_err());
    assert!(store.load()?.notified_failures.is_empty());

    // 2. Working channel sends the [101, 102] ready notice once.
    hands.fail_notify = false;
    hands.delivered = true;
    let _ = run_at(&mut store, &mut hands, 1_010)?;
    assert_eq!(hands.sent_mails.len(), 1);
    assert!(hands.sent_mails[0]
        .0
        .contains("train t001 ready to merge (#101 #102)"));
    hands.sent_mails.clear();

    // 3. After #101 merges and the ready set shrinks to [102], the daemon does not re-notify.
    store.mutate(None, |st| {
        st.merged_prs.insert(101);
        st.open_prs_cache.retain(|p| p.number != 101);
    })?;
    let out_subset = run_at(&mut store, &mut hands, 1_020)?;
    assert!(
        !out_subset
            .plan
            .owner_notifications
            .iter()
            .any(|n| n.kind == "ready-train"),
        "already-announced subset [102] must not re-notify"
    );
    assert!(hands.sent_mails.is_empty());

    // 4. Adding #103 to the ready set notifies once.
    store.mutate(None, |st| {
        st.open_prs_cache.push(make_pr(103, &sha_103));
        st.batches.push(BatchRun {
            id: "b002".into(),
            group: "core".into(),
            prs: vec![102, 103],
            shas: BTreeMap::from([(102, sha_102.clone()), (103, sha_103.clone())]),
            main_sha: main_sha.clone(),
            areas: vec!["core".into()],
            checks: vec![AreaCheck {
                area: "core".into(),
                units: vec!["all".into()],
            }],
            mode: BatchMode::Presubmit,
            status: "passed".into(),
            started_at: 1_025,
            finished_at: Some(1_028),
            tree: Some("tree-102-103".into()),
            ..Default::default()
        });
        st.passed.insert(
            103,
            PassedRecord {
                pr: 103,
                sha: sha_103.clone(),
                main_sha: main_sha.clone(),
                batch_id: "b002".into(),
                passed_at: 1_028,
                tree: "tree-102-103".into(),
                areas: vec!["core".into()],
            },
        );
        st.trains[0].prs = vec![102, 103];
        st.trains[0].shas.insert(103, sha_103.clone());
    })?;
    let _ = run_at(&mut store, &mut hands, 1_030)?;
    assert_eq!(hands.sent_mails.len(), 1);
    assert!(hands.sent_mails[0].0.contains("(#102 #103)"));
    hands.sent_mails.clear();

    // 5. Updating #102 to a new verified head notifies once.
    store.mutate(None, |st| {
        if let Some(p) = st.open_prs_cache.iter_mut().find(|p| p.number == 102) {
            p.head = sha_102_v2.clone();
        }
        st.batches.push(BatchRun {
            id: "b003".into(),
            group: "core".into(),
            prs: vec![102, 103],
            shas: BTreeMap::from([(102, sha_102_v2.clone()), (103, sha_103.clone())]),
            main_sha: main_sha.clone(),
            areas: vec!["core".into()],
            checks: vec![AreaCheck {
                area: "core".into(),
                units: vec!["all".into()],
            }],
            mode: BatchMode::Presubmit,
            status: "passed".into(),
            started_at: 1_035,
            finished_at: Some(1_038),
            tree: Some("tree-102v2-103".into()),
            ..Default::default()
        });
        st.passed.insert(
            102,
            PassedRecord {
                pr: 102,
                sha: sha_102_v2.clone(),
                main_sha: main_sha.clone(),
                batch_id: "b003".into(),
                passed_at: 1_038,
                tree: "tree-102v2-103".into(),
                areas: vec!["core".into()],
            },
        );
        st.trains[0].shas.insert(102, sha_102_v2.clone());
    })?;
    let _ = run_at(&mut store, &mut hands, 1_040)?;
    assert_eq!(hands.sent_mails.len(), 1);
    hands.sent_mails.clear();

    // 6. Advancing across the 24h daily window: an unconfigured send keeps the digest pending,
    // and a working send delivers the accumulated digest once and clears it.
    hands.delivered = false;
    let _ = run_at(&mut store, &mut hands, 87_400)?;
    assert!(hands.sent_mails.is_empty());
    assert!(!store.load()?.digest_entries.is_empty());

    hands.delivered = true;
    let _ = run_at(&mut store, &mut hands, 87_405)?;
    assert_eq!(hands.sent_mails.len(), 1);
    assert_eq!(hands.sent_mails[0].0, "[tacho] daily digest");
    assert!(hands.sent_mails[0].1.contains("EVENTS"));
    assert!(hands.sent_mails[0].1.contains("train t001"));
    assert!(hands.sent_mails[0].1.contains("PROGRESS"));
    assert!(store.load()?.digest_entries.is_empty());
    assert_eq!(store.load()?.digest_sent_at, Some(87_405));
    hands.sent_mails.clear();

    let _ = run_at(&mut store, &mut hands, 87_410)?;
    assert!(hands.sent_mails.is_empty());
    Ok(())
}
