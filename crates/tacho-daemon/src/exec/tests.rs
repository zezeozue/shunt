use super::*;
use crate::fleet::DaemonHands;
use tacho_core::decide::dispatch::{AgentRun, PullFacts};
use tacho_core::decide::ops::snapshot::MainCiRun;
use tacho_core::decide::plugin::MockForge;
use tacho_core::decide::verify::{BatchMode, BatchRun};
use tacho_core::decide::watch::{HUNG_SECS, STALLED_SECS};

#[test]
fn a_tick_cannot_overwrite_a_batch_that_finished_during_observation() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = FleetStore::open(&directory.path().join("events.sqlite")).unwrap();
    let observed = store.load().unwrap();
    assert!(observed.batches.is_empty());
    let decision_state = observe::decision_state_of(&observed, 10);
    let world = WorldObservation::default();
    let plan = tacho_core::decide::decide(&decision_state, &world);
    store
        .mutate(None, |state| {
            state.batches.push(BatchRun {
                id: "completed-during-observation".to_string(),
                status: "passed".to_string(),
                tree: Some("verified-tree".to_string()),
                ..Default::default()
            });
        })
        .unwrap();
    let result = persist_plan_state(&mut store, &observed, &decision_state, &world, &plan, 10);
    assert!(
        result.is_err(),
        "a superseded tick must refuse its state update"
    );
    let after = store.load().unwrap();
    assert!(after
        .batches
        .iter()
        .any(|batch| batch.id == "completed-during-observation" && batch.status == "passed"));
}

#[test]
fn a_rejected_tick_journal_write_preserves_the_observed_fleet_state() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("events.sqlite");
    let mut store = FleetStore::open(&database).unwrap();
    let before = store.load().unwrap();
    rusqlite::Connection::open(&database).unwrap().execute_batch(
        "CREATE TRIGGER reject_tick BEFORE INSERT ON decision_ticks BEGIN SELECT RAISE(ABORT, 'journal unavailable'); END;"
    ).unwrap();
    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: directory.path().to_path_buf(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut options = TickOptions::new(&live);
    options.now_override = Some(10);
    options.explicit_main_sha = Some("main".to_string());
    options.procs = Some(Vec::new());
    assert!(run_tick(directory.path(), &mut store, &mut hands, options).is_err());
    assert_eq!(
        store.load().unwrap(),
        before,
        "a tick without a journal must not commit its state"
    );
}

/// RAII guard that kills a test child process (and its process group) and reaps it on drop.
struct ChildGuard {
    child: std::process::Child,
    kill_group: bool,
}

impl ChildGuard {
    fn new(child: std::process::Child, kill_group: bool) -> Self {
        Self { child, kill_group }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.stdin.take();
        let pid = self.child.id() as libc::pid_t;
        if self.kill_group && pid > 1 {
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn test_run_tick_persists_real_progress_samples_and_replays_identically() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();

    let my_pid = std::process::id();
    let my_st = identity::proc_pid_starttime(my_pid).expect("own starttime");
    store
        .mutate(None, |st| {
            st.agents.insert(
                "worker-1".to_string(),
                AgentRun {
                    key: "worker-1".to_string(),
                    pid: Some(my_pid),
                    starttime: Some(my_st),
                    started_at: 1_000,
                    ..Default::default()
                },
            );
        })
        .unwrap();

    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.to_path_buf(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };

    // Tick 1 at t = 1000: first sample is recorded; agent is Moving (0 supervision actions).
    let mut opts1 = TickOptions::new(&live);
    opts1.now_override = Some(1_000);
    opts1.explicit_main_sha = Some("a".repeat(40));
    opts1.procs = Some(vec![ProcEntry {
        pid: my_pid,
        ppid: 1,
        pgid: my_pid,
        state: "S".to_string(),
        cpu_ticks: 100,
        ..Default::default()
    }]);
    let out1 = run_tick(home, &mut store, &mut hands, opts1).unwrap();
    assert!(out1.plan.supervision_actions.is_empty());
    assert_eq!(store.load().unwrap().samples["worker-1"].len(), 1);

    // Tick 2 at t = 1000 + STALLED_SECS + 100 with a busy child process (10 ticks/s >= 5 ticks/s):
    // must NOT be marked stalled!
    let mut opts2 = TickOptions::new(&live);
    let t2 = 1_000 + STALLED_SECS + 100;
    opts2.now_override = Some(t2);
    opts2.explicit_main_sha = Some("a".repeat(40));
    opts2.procs = Some(vec![
        ProcEntry {
            pid: my_pid,
            ppid: 1,
            pgid: my_pid,
            state: "S".to_string(),
            cpu_ticks: 100,
            ..Default::default()
        },
        ProcEntry {
            pid: my_pid.saturating_add(99_999),
            ppid: my_pid,
            pgid: my_pid,
            state: "R".to_string(),
            cpu_ticks: ((t2 - 1_000) as u64) * 10,
            ..Default::default()
        },
    ]);
    let out2 = run_tick(home, &mut store, &mut hands, opts2).unwrap();
    assert!(
        out2.plan.supervision_actions.is_empty(),
        "busy child process must prevent stall verdict, got: {:?}",
        out2.plan.supervision_actions
    );

    // Tick 3 at t2 + STALLED_SECS + 100 where child is gone and root CPU only grew by 1 tick:
    // now idle for >= STALLED_SECS -> Stalled -> Nudge!
    let t3 = t2 + STALLED_SECS + 100;
    let mut opts3 = TickOptions::new(&live);
    opts3.now_override = Some(t3);
    opts3.explicit_main_sha = Some("a".repeat(40));
    opts3.procs = Some(vec![ProcEntry {
        pid: my_pid,
        ppid: 1,
        pgid: my_pid,
        state: "S".to_string(),
        cpu_ticks: 100 + ((t2 - 1_000) as u64) * 10 + 1,
        ..Default::default()
    }]);
    let out3 = run_tick(home, &mut store, &mut hands, opts3).unwrap();
    assert!(
        out3.plan
            .supervision_actions
            .iter()
            .any(|s| matches!(s, SuperviseAction::Nudge { key, .. } if key == "worker-1")),
        "expected Nudge after STALLED_SECS of idle CPU, got: {:?}",
        out3.plan.supervision_actions
    );

    // Deterministic replay of the recorded decision tick from SQLite must produce the exact plan!
    let tick_id = out3.tick_id.expect("recorded tick_id");
    let (replayed_state, replayed_obs, logged_plan) = store.load_decision_tick(tick_id).unwrap();
    let replayed_plan = tacho_core::decide::decide(&replayed_state, &replayed_obs);
    assert_eq!(replayed_plan, logged_plan);
    let _ = HUNG_SECS;
}

#[test]
fn test_run_tick_with_mock_forge_refreshes_world_and_clears_main_broken() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let repo = home.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let run_git_cmd = |args: &[&str]| {
        let st = std::process::Command::new("git")
            .args(args)
            .current_dir(&repo)
            .status()
            .unwrap();
        assert!(st.success(), "git {args:?} failed");
    };
    run_git_cmd(&["init", "-q", "-b", "main"]);
    run_git_cmd(&["config", "user.email", "t@example.com"]);
    run_git_cmd(&["config", "user.name", "Tester"]);
    std::fs::write(repo.join("f.txt"), "v1\n").unwrap();
    run_git_cmd(&["add", "f.txt"]);
    run_git_cmd(&["commit", "-q", "-m", "c1"]);
    let sha1 = observe::local_main_sha(&repo).unwrap();

    std::fs::write(repo.join("f.txt"), "v2\n").unwrap();
    run_git_cmd(&["add", "f.txt"]);
    run_git_cmd(&["commit", "-q", "-m", "c2"]);
    let sha2 = observe::local_main_sha(&repo).unwrap();

    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |st| {
            st.batches.push(BatchRun {
                id: "b-main-broken".to_string(),
                group: "core".to_string(),
                prs: vec![55],
                shas: BTreeMap::from([(55, sha1.clone())]),
                main_sha: sha1.clone(),
                areas: vec!["core".to_string()],
                checks: Vec::new(),
                mode: BatchMode::Presubmit,
                status: "main-broken".to_string(),
                started_at: 100,
                finished_at: Some(120),
                tree: None,
                pid: None,
                log: String::new(),
                culprit: None,
                skipped: Vec::new(),
                flakes: Vec::new(),
            });
        })
        .unwrap();

    let forge = MockForge {
        main_ci: Ok(Some(MainCiRun {
            id: 501,
            status: "completed".to_string(),
            conclusion: "success".to_string(),
            sha: sha2.clone(),
            title: "CI".to_string(),
            age_seconds: None,
            completed_at: "2026-10-25T13:00:00Z".to_string(),
        })),
        open_prs: Ok(vec![PullFacts {
            number: 55,
            title: "Fix core".to_string(),
            branch: "feat/55".to_string(),
            base: "main".to_string(),
            head: sha1,
            state: "OPEN".to_string(),
            mergeable: "MERGEABLE".to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    };

    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.to_path_buf(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut opts = TickOptions::new(&live);
    opts.repo = Some(repo);
    opts.forge = Some(&forge);
    opts.procs = Some(Vec::new());

    let out = run_tick(home, &mut store, &mut hands, opts).unwrap();
    assert_eq!(out.plan.cleared_main_broken.len(), 1);
    assert_eq!(out.plan.cleared_main_broken[0].batch_id, "b-main-broken");
    let after = store.load().unwrap();
    assert!(after.batches.is_empty());
    assert_eq!(after.open_prs_cache.len(), 1);
    assert_eq!(after.main_ci.as_ref().unwrap().sha, sha2);
}

#[test]
fn test_run_tick_forwards_owner_notifications_and_watchdog_alerts_to_forge_issues() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |st| {
            st.batches.push(BatchRun {
                id: "b-red".to_string(),
                group: "core".to_string(),
                prs: vec![7],
                shas: BTreeMap::from([(7, "a".repeat(40))]),
                main_sha: "b".repeat(40),
                areas: vec!["core".to_string()],
                checks: Vec::new(),
                mode: BatchMode::Presubmit,
                status: "failed".to_string(),
                started_at: 100,
                finished_at: Some(120),
                tree: None,
                pid: None,
                log: "test failure".to_string(),
                culprit: Some(7),
                skipped: Vec::new(),
                flakes: Vec::new(),
            });
            st.max_workers = 1;
            st.todo.push(TodoItem {
                key: "pending-work-1".to_string(),
                brief: "Work item 1".to_string(),
                area: "core".to_string(),
                ..Default::default()
            });
            st.todo.push(TodoItem {
                key: "pending-work-2".to_string(),
                brief: "Work item 2".to_string(),
                area: "core".to_string(),
                ..Default::default()
            });
            st.last_tick_ts = Some(1_000);
        })
        .unwrap();

    let forge = MockForge::default();
    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.to_path_buf(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut opts = TickOptions::new(&live);
    opts.explicit_main_sha = Some("b".repeat(40));
    opts.forge = Some(&forge);
    opts.procs = Some(Vec::new());
    opts.now_override = Some(2_000); // > 600s since last_tick_ts=1_000 with pending todo -> watchdog_alert

    let out = run_tick(home, &mut store, &mut hands, opts).unwrap();
    assert!(!out.plan.owner_notifications.is_empty());
    assert!(out.plan.watchdog_alert.is_some());

    let issues = forge.issues.lock().unwrap();
    assert!(
        issues.iter().any(|(title, _)| title == "tacho is down"),
        "expected 'tacho is down' issue on MockForge, got: {:?}",
        *issues
    );
    assert!(
        issues
            .iter()
            .any(|(title, _)| title == "tacho alert: batch-red:b-red"),
        "expected batch-red alert issue on MockForge, got: {:?}",
        *issues
    );
}

#[test]
fn test_run_tick_retries_failed_owner_notifications_and_watchdog_alerts_on_next_tick() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |st| {
            st.batches.push(BatchRun {
                id: "b-red".to_string(),
                group: "core".to_string(),
                prs: vec![7],
                shas: BTreeMap::from([(7, "a".repeat(40))]),
                main_sha: "b".repeat(40),
                areas: vec!["core".to_string()],
                checks: Vec::new(),
                mode: BatchMode::Presubmit,
                status: "failed".to_string(),
                started_at: 100,
                finished_at: Some(120),
                tree: None,
                pid: None,
                log: "test failure".to_string(),
                culprit: Some(7),
                skipped: Vec::new(),
                flakes: Vec::new(),
            });
            st.max_workers = 1;
            let my_pid = std::process::id();
            let my_st = identity::proc_pid_starttime(my_pid).expect("own starttime");
            st.agents.insert(
                "busy-worker".to_string(),
                AgentRun {
                    key: "busy-worker".to_string(),
                    pid: Some(my_pid),
                    starttime: Some(my_st),
                    started_at: 1_000,
                    ..Default::default()
                },
            );
            st.todo.push(TodoItem {
                key: "pending-work-1".to_string(),
                brief: "Work item 1".to_string(),
                area: "core".to_string(),
                ..Default::default()
            });
            st.last_tick_ts = Some(1_000);
        })
        .unwrap();

    let broken_forge = tacho_core::decide::plugin::CliForgePlugin {
        name: "broken-gh".to_string(),
        cli_bin: "/nonexistent/gh-binary".to_string(),
        repo: Some("owner/repo".to_string()),
        allow_merge: false,
    };
    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.to_path_buf(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut opts1 = TickOptions::new(&live);
    opts1.explicit_main_sha = Some("b".repeat(40));
    opts1.forge = Some(&broken_forge);
    opts1.procs = Some(Vec::new());
    opts1.now_override = Some(2_000);

    let err1 = run_tick(home, &mut store, &mut hands, opts1).unwrap_err();
    assert!(
        err1.to_string().contains("set_issue_failed")
            && err1.to_string().contains("watchdog_alert_failed"),
        "expected set_issue_failed and watchdog_alert_failed in tick error, got: {err1}"
    );
    let decisions1 = store.recent_decisions(50, None, None).unwrap();
    assert!(
        decisions1.iter().any(|d| d.action == "set_issue_failed"),
        "expected set_issue_failed on broken forge, got: {:?}",
        decisions1
    );
    assert!(
        decisions1
            .iter()
            .any(|d| d.action == "watchdog_alert_failed"),
        "expected watchdog_alert_failed on broken forge, got: {:?}",
        decisions1
    );

    let st_after_fail = store.load().unwrap();
    assert!(
        !st_after_fail.notified_failures.contains("batch-red:b-red"),
        "failed owner notification must be reverted from notified_failures so next tick retries"
    );
    assert_eq!(
        st_after_fail.last_watchdog_alert_ts, None,
        "failed watchdog alert must revert last_watchdog_alert_ts so next tick retries"
    );

    // Next tick with a working forge retries and succeeds.
    store
        .mutate(None, |st| {
            st.last_tick_ts = Some(1_000);
        })
        .unwrap();
    let working_forge = MockForge::default();
    let mut opts2 = TickOptions::new(&live);
    opts2.explicit_main_sha = Some("b".repeat(40));
    opts2.forge = Some(&working_forge);
    opts2.procs = Some(Vec::new());
    opts2.now_override = Some(2_005);

    let _out2 = run_tick(home, &mut store, &mut hands, opts2).unwrap();
    let st_after_ok = store.load().unwrap();
    assert!(st_after_ok.notified_failures.contains("batch-red:b-red"));
    assert_eq!(st_after_ok.last_watchdog_alert_ts, Some(2_005));
}

#[test]
fn test_tidy_refuses_plain_subdir_inside_enclosing_git_repo() {
    let dir = tempfile::tempdir().unwrap();
    let parent_repo = dir.path().join("srv-fleet");
    std::fs::create_dir_all(&parent_repo).unwrap();

    let run_git = |args: &[&str]| {
        let st = std::process::Command::new("git")
            .args(args)
            .current_dir(&parent_repo)
            .status()
            .unwrap();
        assert!(st.success(), "git {args:?} failed");
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.email", "t@example.com"]);
    run_git(&["config", "user.name", "Tester"]);
    std::fs::write(parent_repo.join("root.txt"), "root\n").unwrap();
    run_git(&["add", "root.txt"]);
    run_git(&["commit", "-q", "-m", "initial"]);

    std::fs::write(parent_repo.join("untracked_parent.txt"), "do not stage\n").unwrap();
    let plain_subdir = parent_repo.join("trees").join("plain-dir");
    std::fs::create_dir_all(&plain_subdir).unwrap();
    std::fs::write(plain_subdir.join("work.txt"), "precious\n").unwrap();

    let mut store = FleetStore::open(&parent_repo.join("events.sqlite")).unwrap();
    store
        .mutate(None, |st| {
            st.worktrees.insert(
                "plain-dir".to_string(),
                crate::fleet::WorktreeEntry {
                    slug: "plain-dir".to_string(),
                    path: plain_subdir.to_string_lossy().to_string(),
                    branch: "feat/plain".to_string(),
                    base_sha: String::new(),
                    base_pr: None,
                    port: 3100,
                    port_last: 3119,
                    created_at: 100,
                },
            );
        })
        .unwrap();

    let decisions = vec![
        tacho_core::decide::watch::TidyDecision::RemoveCheckout {
            path: plain_subdir.to_string_lossy().to_string(),
            rescue: true,
            because: "test remove checkout on plain subdir".to_string(),
        },
        tacho_core::decide::watch::TidyDecision::RemoveTree {
            path: plain_subdir.to_string_lossy().to_string(),
            rescue: false,
            because: "test remove tree on plain subdir".to_string(),
        },
        tacho_core::decide::watch::TidyDecision::RemoveTree {
            path: parent_repo.to_string_lossy().to_string(),
            rescue: true,
            because: "test remove tree on main checkout".to_string(),
        },
        tacho_core::decide::watch::TidyDecision::RemoveCheckout {
            path: parent_repo.to_string_lossy().to_string(),
            rescue: false,
            because: "test remove checkout on main checkout".to_string(),
        },
    ];
    assert!(
        tidy::rescue_worktree(&mut store, &plain_subdir, 1_000).is_err(),
        "rescue_worktree must refuse a plain subdir inside an enclosing repo"
    );
    let removed = tidy::apply_tidy_decisions(&mut store, &decisions, 1_000, false).unwrap();
    assert!(
        removed.is_empty(),
        "neither plain subdir nor main checkout may ever be removed"
    );
    assert!(
        plain_subdir.join("work.txt").exists(),
        "plain subdir contents must remain intact"
    );
    assert!(
        parent_repo.join("root.txt").exists(),
        "main checkout must remain intact"
    );

    // Verify the enclosing parent git repo has zero staged changes (git add -A did NOT run in parent_repo).
    let cached = std::process::Command::new("git")
        .args(["diff", "--cached", "--no-renames", "--name-only"])
        .current_dir(&parent_repo)
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&cached.stdout).trim(),
        "",
        "enclosing parent repo must not have staged files"
    );
}

#[test]
fn test_retired_main_checkout_survives_tidy_with_index_and_untracked_files_intact() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let holder = dir.path().join("holder");
    let main_checkout = holder.join("repo");
    std::fs::create_dir_all(&main_checkout).unwrap();

    let run_git = |args: &[&str]| {
        let st = std::process::Command::new("git")
            .args(args)
            .current_dir(&main_checkout)
            .status()
            .unwrap();
        assert!(st.success(), "git {args:?} failed");
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.email", "t@example.com"]);
    run_git(&["config", "user.name", "Tester"]);
    std::fs::write(main_checkout.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"]);
    run_git(&["commit", "-q", "-m", "initial"]);

    // Stage a file in the main checkout's index and create an untracked file.
    std::fs::write(main_checkout.join("staged.txt"), "staged work\n").unwrap();
    run_git(&["add", "staged.txt"]);
    std::fs::write(main_checkout.join("untracked.txt"), "untracked work\n").unwrap();

    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    let main_path_str = main_checkout.to_string_lossy().to_string();
    let holder_path_str = holder.to_string_lossy().to_string();
    store
        .mutate(None, |st| {
            st.retired.insert("retired-main".to_string());
            st.retired.insert("retired-holder".to_string());
            st.worktrees.insert(
                "retired-main".to_string(),
                crate::fleet::WorktreeEntry {
                    slug: "retired-main".to_string(),
                    path: main_path_str.clone(),
                    branch: "main".to_string(),
                    base_sha: String::new(),
                    base_pr: None,
                    port: 3200,
                    port_last: 3219,
                    created_at: 100,
                },
            );
            st.worktrees.insert(
                "retired-holder".to_string(),
                crate::fleet::WorktreeEntry {
                    slug: "retired-holder".to_string(),
                    path: holder_path_str.clone(),
                    branch: "main".to_string(),
                    base_sha: String::new(),
                    base_pr: None,
                    port: 3220,
                    port_last: 3239,
                    created_at: 100,
                },
            );
            st.agents.insert(
                "retired-main".to_string(),
                AgentRun {
                    key: "retired-main".to_string(),
                    worktree: main_path_str.clone(),
                    started_at: 100,
                    finished_at: Some(200),
                    exit_code: Some(0),
                    ..Default::default()
                },
            );
        })
        .unwrap();

    // 1. Run a full autonomous tick: tidy must not plan or execute removal of the main checkout.
    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.clone(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut opts = TickOptions::new(&live);
    opts.explicit_main_sha = Some("a".repeat(40));
    opts.procs = Some(Vec::new());
    opts.now_override = Some(100_000);
    let out = run_tick(&home, &mut store, &mut hands, opts).unwrap();
    assert!(
        out.plan.tidy_decisions.is_empty(),
        "decide must never plan tidy removal for the main checkout or its holder: {:?}",
        out.plan.tidy_decisions
    );

    // 2. Also invoke apply_tidy_decisions directly with RemoveTree and RemoveCheckout (with and without rescue).
    let explicit = vec![
        tacho_core::decide::watch::TidyDecision::RemoveTree {
            path: main_path_str.clone(),
            rescue: true,
            because: "retired worktree".to_string(),
        },
        tacho_core::decide::watch::TidyDecision::RemoveCheckout {
            path: main_path_str.clone(),
            rescue: false,
            because: "stale checkout".to_string(),
        },
        tacho_core::decide::watch::TidyDecision::RemoveTree {
            path: holder_path_str.clone(),
            rescue: false,
            because: "holder of main checkout".to_string(),
        },
        tacho_core::decide::watch::TidyDecision::RemoveCheckout {
            path: holder_path_str.clone(),
            rescue: true,
            because: "holder checkout".to_string(),
        },
    ];
    let removed = tidy::apply_tidy_decisions(&mut store, &explicit, 100_000, false).unwrap();
    assert!(removed.is_empty());

    // Main checkout, its staged index, and its untracked files are all intact.
    assert!(main_checkout.join(".git").is_dir());
    assert_eq!(
        std::fs::read_to_string(main_checkout.join("staged.txt")).unwrap(),
        "staged work\n"
    );
    assert_eq!(
        std::fs::read_to_string(main_checkout.join("untracked.txt")).unwrap(),
        "untracked work\n"
    );
    let cached = std::process::Command::new("git")
        .args(["diff", "--cached", "--no-renames", "--name-only"])
        .current_dir(&main_checkout)
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&cached.stdout).trim(),
        "staged.txt",
        "main checkout git index must remain intact"
    );
}

#[test]
fn test_worker_running_in_main_checkout_is_stopped_with_page() {
    use std::io::BufRead;
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let main_checkout = dir.path().join("repo");
    std::fs::create_dir_all(&main_checkout).unwrap();

    let st = std::process::Command::new("git")
        .args(["init", "-q", "-b", "main"])
        .current_dir(&main_checkout)
        .status()
        .unwrap();
    assert!(st.success());

    // Spawn a background worker process group leader that also spawns a child in its group,
    // whose recorded worktree is the main checkout.
    let mut raw_child = unsafe {
        use std::os::unix::process::CommandExt;
        std::process::Command::new("bash")
            .args([
                "-c",
                "{ cat >/dev/null; } <&0 & sub=$!; echo $sub; wait $sub",
            ])
            .current_dir(&main_checkout)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .pre_exec(|| {
                libc::setpgid(0, 0);
                Ok(())
            })
            .spawn()
            .unwrap()
    };
    let _child_stdin = raw_child.stdin.take();
    let mut child_stdout = std::io::BufReader::new(raw_child.stdout.take().unwrap());
    let mut child = ChildGuard::new(raw_child, true);
    let mut sub_pid_line = String::new();
    child_stdout.read_line(&mut sub_pid_line).unwrap();
    let sub_pid: u32 = sub_pid_line.trim().parse().expect("sub-child pid");
    assert!(identity::pid_alive(sub_pid), "sub-child must be alive");

    let pid = child.id();
    let starttime = identity::proc_pid_starttime(pid).expect("child starttime");

    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    let main_str = main_checkout.to_string_lossy().to_string();
    store
        .mutate(None, |s| {
            s.repo_root = Some(main_str.clone());
            s.agents.insert(
                "rogue-worker".to_string(),
                AgentRun {
                    key: "rogue-worker".to_string(),
                    pid: Some(pid),
                    starttime: Some(starttime),
                    worktree: main_str.clone(),
                    started_at: 1_000,
                    ..Default::default()
                },
            );
        })
        .unwrap();

    let forge = MockForge::default();
    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.clone(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut opts = TickOptions::new(&live);
    opts.explicit_main_sha = Some("a".repeat(40));
    opts.forge = Some(&forge);
    opts.procs = Some(vec![ProcEntry {
        pid,
        ppid: 1,
        pgid: pid,
        state: "S".to_string(),
        ..Default::default()
    }]);
    opts.now_override = Some(1_010);

    let out = run_tick(&home, &mut store, &mut hands, opts).unwrap();
    let wait_status = child.wait().unwrap();
    assert!(
        !wait_status.success(),
        "worker in main checkout must be terminated by supervision"
    );
    identity::wait_for_pid_exit_event(sub_pid).unwrap();
    assert!(
        !identity::pid_alive(sub_pid),
        "child process in main-checkout worker's process group must also be terminated"
    );
    assert!(
        out.plan
            .supervision_actions
            .iter()
            .any(|a| matches!(a, SuperviseAction::Escalate { key, .. } if key == "rogue-worker")),
        "expected Escalate supervision action for rogue-worker, got: {:?}",
        out.plan.supervision_actions
    );
    assert!(
        !out.plan.owner_notifications.is_empty(),
        "expected owner notification/page when stopping worker in main checkout"
    );
    let final_state = store.load().unwrap();
    assert!(
        final_state.holds.contains_key("rogue-worker"),
        "rogue-worker must be placed on hold"
    );
    assert_eq!(final_state.agents["rogue-worker"].finished_at, Some(1_010));
}

#[test]
fn test_holder_directory_worker_is_not_mistaken_for_main_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let fleet_main = dir.path().join("fleet-repo");
    std::fs::create_dir_all(&fleet_main).unwrap();
    let holder = dir.path().join("holder");
    let child_repo = holder.join("sub-repo");
    std::fs::create_dir_all(&child_repo).unwrap();

    // Even when the enclosing temp directory itself sits inside an unrelated git repository
    // and holder contains a child repo, holder must never resolve to the main checkout.
    for repo in [dir.path(), fleet_main.as_path(), child_repo.as_path()] {
        let st = std::process::Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(st.success());
    }
    assert!(
        !observe::resolves_to_main_checkout(&holder),
        "a holder directory containing a child repo or sitting inside an unconfigured enclosing repo must not resolve to main checkout"
    );
    assert!(
        !observe::resolves_to_fleet_main_checkout(&holder, Some(&fleet_main)),
        "holder directory outside fleet_main must not resolve to fleet_main"
    );
    assert!(
        observe::resolves_to_fleet_main_checkout(&fleet_main, Some(&fleet_main)),
        "fleet_main itself must resolve to fleet_main"
    );

    let mut raw_child = std::process::Command::new("cat")
        .current_dir(&holder)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _stdin = raw_child.stdin.take();
    let mut child = ChildGuard::new(raw_child, false);
    let pid = child.id();
    let starttime = identity::proc_pid_starttime(pid).expect("child starttime");

    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |s| {
            s.repo_root = Some(fleet_main.to_string_lossy().to_string());
            s.agents.insert(
                "holder-worker".to_string(),
                AgentRun {
                    key: "holder-worker".to_string(),
                    pid: Some(pid),
                    starttime: Some(starttime),
                    worktree: holder.to_string_lossy().to_string(),
                    started_at: 1_000,
                    ..Default::default()
                },
            );
        })
        .unwrap();

    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.clone(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let mut opts = TickOptions::new(&live);
    opts.repo = Some(fleet_main);
    opts.explicit_main_sha = Some("a".repeat(40));
    opts.procs = Some(vec![ProcEntry {
        pid,
        ppid: 1,
        pgid: pid,
        state: "S".to_string(),
        ..Default::default()
    }]);
    opts.now_override = Some(1_010);

    let out = run_tick(&home, &mut store, &mut hands, opts).unwrap();
    assert!(
        out.obs.main_checkout_workers.is_empty(),
        "holder directory must not be flagged as fleet main checkout"
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "worker in holder directory must not be killed"
    );
}

#[test]
fn test_ordinary_stall_escalation_notifies_owner_without_killing_worker() {
    let dir = tempfile::tempdir().unwrap();
    // Initialize the enclosing temp root as an unrelated git repo to prove the fallback
    // never trusts an enclosing repository it was not told about.
    let st = std::process::Command::new("git")
        .args(["init", "-q", "-b", "main"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    assert!(st.success());

    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let fleet_main = dir.path().join("fleet-repo");
    std::fs::create_dir_all(&fleet_main).unwrap();
    let st = std::process::Command::new("git")
        .args(["init", "-q", "-b", "main"])
        .current_dir(&fleet_main)
        .status()
        .unwrap();
    assert!(st.success());

    let wt = dir.path().join("wt-1");
    std::fs::create_dir_all(&wt).unwrap();

    let mut raw_child = std::process::Command::new("cat")
        .current_dir(&wt)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _stdin = raw_child.stdin.take();
    let mut child = ChildGuard::new(raw_child, false);
    let pid = child.id();
    let starttime = identity::proc_pid_starttime(pid).expect("child starttime");

    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |s| {
            s.repo_root = Some(fleet_main.to_string_lossy().to_string());
            s.agents.insert(
                "stalled-worker".to_string(),
                AgentRun {
                    key: "stalled-worker".to_string(),
                    pid: Some(pid),
                    starttime: Some(starttime),
                    worktree: wt.to_string_lossy().to_string(),
                    started_at: 1_000,
                    ..Default::default()
                },
            );
            s.nudged.insert("stalled-worker".to_string());
            s.samples.insert(
                "stalled-worker".to_string(),
                vec![tacho_core::decide::watch::ProgressSample {
                    ts: 1_000,
                    log_bytes: 0,
                    head_sha: String::new(),
                    dirty_files: 0,
                    cpu_ticks: 100,
                    newest_mtime: None,
                }],
            );
        })
        .unwrap();

    let live = tacho_core::LiveState::default();
    let mut hands = DaemonHands {
        home: home.clone(),
        live_state: live.clone(),
        appender: None,
        launcher: None,
    };
    let t_escalate = 1_000 + STALLED_SECS + 120;
    let mut opts = TickOptions::new(&live);
    opts.repo = Some(fleet_main);
    opts.explicit_main_sha = Some("a".repeat(40));
    opts.now_override = Some(t_escalate);
    opts.procs = Some(vec![ProcEntry {
        pid,
        ppid: 1,
        pgid: pid,
        state: "S".to_string(),
        cpu_ticks: 105, // alive (>100) but idle (< 5 ticks/s)
        ..Default::default()
    }]);

    let out = run_tick(&home, &mut store, &mut hands, opts).unwrap();
    assert!(
        out.plan
            .supervision_actions
            .iter()
            .any(|a| matches!(a, SuperviseAction::Escalate { key, .. } if key == "stalled-worker")),
        "expected Escalate for already-nudged stalled worker, got: {:?}",
        out.plan.supervision_actions
    );
    assert!(
        out.plan
            .owner_notifications
            .iter()
            .any(|n| n.kind == "stalled" && n.subject.contains("stalled-worker")),
        "expected owner notification for escalated stall, got: {:?}",
        out.plan.owner_notifications
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "ordinary stall escalation must NOT kill the worker"
    );
    let st = store.load().unwrap();
    assert!(
        !st.holds.contains_key("stalled-worker"),
        "ordinary stall escalation must NOT hold the task"
    );
    assert!(
        st.agents["stalled-worker"].finished_at.is_none(),
        "ordinary stall escalation must leave the worker running"
    );
}

#[test]
fn test_root_disk_check_in_tick_warns_and_prunes_volumes_and_clears_on_recovery() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let mut store = FleetStore::open(&home.join("events.sqlite")).unwrap();
    let live = tacho_core::LiveState::default();

    let prune_marker = dir.path().join("prune-ran.marker");
    let fake_prune = dir.path().join("fake-runner-prune.sh");
    std::fs::write(
        &fake_prune,
        format!(
            "#!/bin/sh\necho \"$1\" > '{}'\necho \"pruned 3 unused runner volumes\"\n",
            prune_marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake_prune, std::fs::Permissions::from_mode(0o755)).unwrap();

    struct PruneHands {
        inner: DaemonHands,
        prune_script: std::path::PathBuf,
    }
    impl Hands for PruneHands {
        fn spawn_worker(
            &mut self,
            store: &mut FleetStore,
            item: &TodoItem,
        ) -> io::Result<Option<u32>> {
            self.inner.spawn_worker(store, item)
        }
        fn deliver_tell(
            &mut self,
            store: &mut FleetStore,
            agent_key: &str,
            msg: &str,
        ) -> io::Result<()> {
            self.inner.deliver_tell(store, agent_key, msg)
        }
        fn emit(&mut self, track: &str, name: &str, because: &str) {
            self.inner.emit(track, name, because);
        }
        fn prune_root_disk_volumes(&mut self) -> Result<String, String> {
            super::prune_runner_volumes_command(Some(&self.prune_script))
        }
    }

    let mut hands = PruneHands {
        inner: DaemonHands {
            home: home.clone(),
            live_state: live.clone(),
            appender: None,
            launcher: None,
        },
        prune_script: fake_prune,
    };
    let gib = 1024 * 1024 * 1024u64;

    // 1. Critical root disk (85% used, 15 GiB free): warns owner, executes runner prune, and records output.
    let mut opts1 = TickOptions::new(&live);
    opts1.explicit_main_sha = Some("a".repeat(40));
    opts1.now_override = Some(2_000);
    opts1.procs = Some(Vec::new());
    opts1.root_disk_bytes = Some((100 * gib, 15 * gib));
    let out1 = run_tick(&home, &mut store, &mut hands, opts1).unwrap();
    assert!(out1.plan.root_disk_action.as_ref().unwrap().warn);
    assert!(
        out1.plan
            .root_disk_action
            .as_ref()
            .unwrap()
            .prune_docker_volumes
    );
    assert_eq!(
        std::fs::read_to_string(&prune_marker).unwrap().trim(),
        "check",
        "root-disk prune helper must be invoked with 'check'"
    );
    assert!(
        out1.plan
            .owner_notifications
            .iter()
            .any(|n| n.kind == "root-disk" && n.body.contains("The root disk is filling")),
        "expected root-disk owner notification, got: {:?}",
        out1.plan.owner_notifications
    );
    let decisions = store.recent_decisions(10, None, None).unwrap();
    assert!(
        decisions
            .iter()
            .any(|d| d.action == "prune_root_disk_volumes"
                && d.target == "root-disk"
                && d.because.contains("pruned 3 unused runner volumes")),
        "expected prune_root_disk_volumes decision with prune output, got: {:?}",
        decisions
    );

    // 2. Recovery (50% used, 50 GiB free): clears root-disk attention and resets notification latch.
    let mut opts2 = TickOptions::new(&live);
    opts2.explicit_main_sha = Some("a".repeat(40));
    opts2.now_override = Some(2_060);
    opts2.procs = Some(Vec::new());
    opts2.root_disk_bytes = Some((100 * gib, 50 * gib));
    let out2 = run_tick(&home, &mut store, &mut hands, opts2).unwrap();
    assert!(!out2.plan.root_disk_action.as_ref().unwrap().warn);
    assert!(!out2
        .plan
        .attention_items
        .iter()
        .any(|a| a.kind == "root-disk"));
    let st2 = store.load().unwrap();
    assert!(
        !st2.notified_failures
            .iter()
            .any(|k| k.starts_with("attention:root-disk:")),
        "recovery must clear root-disk notification latch"
    );
}
