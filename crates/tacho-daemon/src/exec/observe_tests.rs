use super::*;
use crate::exec::Hands;
use crate::fleet::FleetStore;
use tacho_core::decide::dispatch::{PullFacts, TodoItem};
use tacho_core::decide::ops::snapshot::MainCiRun;
use tacho_core::decide::plugin::MockForge;
use tacho_core::decide::watch::{verdict, Verdict, HUNG_SECS, STALLED_SECS};

struct TestHands {
    pub attention: Vec<(String, String, String)>,
}

impl Hands for TestHands {
    fn spawn_worker(
        &mut self,
        _store: &mut FleetStore,
        _item: &TodoItem,
    ) -> std::io::Result<Option<u32>> {
        Ok(None)
    }

    fn deliver_tell(
        &mut self,
        _store: &mut FleetStore,
        _agent_key: &str,
        _msg: &str,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn emit(&mut self, track: &str, name: &str, because: &str) {
        self.attention
            .push((track.to_string(), name.to_string(), because.to_string()));
    }
}

#[test]
fn test_agent_cpu_ticks_includes_busy_child_processes_and_prevents_false_stall() {
    let root_pid = 4000;
    let child_pid = 4001;
    let procs_t0 = vec![
        ProcEntry {
            pid: root_pid,
            ppid: 1,
            pgid: root_pid,
            state: "S".to_string(),
            cpu_ticks: 50,
            ..Default::default()
        },
        ProcEntry {
            pid: child_pid,
            ppid: root_pid,
            pgid: root_pid,
            state: "R".to_string(),
            cpu_ticks: 100,
            ..Default::default()
        },
    ];
    let s0 = progress_sample(1_000, Some(root_pid), "", "", &procs_t0).unwrap();
    assert_eq!(s0.cpu_ticks, 150);

    // After 2800s (> STALLED_SECS = 2700s), root process has 0 new ticks,
    // but child process burned 28_000 ticks (10 ticks/s >= 5 ticks/s IDLE_CPU_TICKS_PER_SEC).
    let procs_t1 = vec![
        ProcEntry {
            pid: root_pid,
            ppid: 1,
            pgid: root_pid,
            state: "S".to_string(),
            cpu_ticks: 50,
            ..Default::default()
        },
        ProcEntry {
            pid: child_pid,
            ppid: root_pid,
            pgid: root_pid,
            state: "R".to_string(),
            cpu_ticks: 100 + 28_000,
            ..Default::default()
        },
    ];
    let s1 = progress_sample(
        1_000 + STALLED_SECS + 100,
        Some(root_pid),
        "",
        "",
        &procs_t1,
    )
    .unwrap();
    assert_eq!(s1.cpu_ticks, 28_150);

    let (v, _) = verdict(&[s0, s1], 1_000 + STALLED_SECS + 100);
    assert_eq!(
        v,
        Verdict::Moving,
        "busy child process tree must keep agent Moving past STALLED_SECS"
    );
}

#[test]
fn test_worktree_head_or_dirty_change_prevents_stall_and_idle_stalls_only_after_window() {
    let pid = 5000;
    let procs_idle = vec![ProcEntry {
        pid,
        ppid: 1,
        pgid: pid,
        state: "S".to_string(),
        cpu_ticks: 100,
        ..Default::default()
    }];

    // (i) First tick alone is NEVER marked stalled or hung.
    let s0 = progress_sample(1_000, Some(pid), "", "", &procs_idle).unwrap();
    let (v_first, _) = verdict(std::slice::from_ref(&s0), 1_000);
    assert_eq!(v_first, Verdict::Moving);

    // (ii) If head_sha or dirty_files changes before STALLED_SECS, agent is NOT marked stalled.
    let mut s_work = s0.clone();
    s_work.ts = 1_000 + 2_000;
    s_work.dirty_files = 2;
    let mut s_later = s_work.clone();
    s_later.ts = 1_000 + STALLED_SECS + 100;
    s_later.cpu_ticks = 101;
    let (v_work, _) = verdict(&[s0.clone(), s_work, s_later], 1_000 + STALLED_SECS + 100);
    assert_eq!(v_work, Verdict::Moving);

    // (iii) Truly idle agent (0 CPU, unchanged head_sha and dirty_files) becomes Hung
    // only once real observed sample timestamps span >= HUNG_SECS, and Stalled once
    // low-CPU samples span >= STALLED_SECS.
    let mut s_before_hung = s0.clone();
    s_before_hung.ts = 1_000 + HUNG_SECS - 1;
    assert_eq!(
        verdict(&[s0.clone(), s_before_hung], 1_000 + HUNG_SECS - 1).0,
        Verdict::Moving
    );

    let mut s_at_hung = s0.clone();
    s_at_hung.ts = 1_000 + HUNG_SECS;
    assert_eq!(
        verdict(&[s0.clone(), s_at_hung], 1_000 + HUNG_SECS).0,
        Verdict::Hung
    );

    let mut s_at_stalled = s0.clone();
    s_at_stalled.ts = 1_000 + STALLED_SECS;
    s_at_stalled.cpu_ticks = 102; // > 0 ticks so alive, but < 5 ticks/s so idle
    assert_eq!(
        verdict(&[s0, s_at_stalled], 1_000 + STALLED_SECS).0,
        Verdict::Stalled
    );
}

#[test]
fn test_refresh_world_updates_state_and_retains_last_known_on_forge_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = FleetStore::open(&dir.path().join("events.sqlite")).unwrap();
    let mut hands = TestHands {
        attention: Vec::new(),
    };

    let forge_ok = MockForge {
        main_ci: Ok(Some(MainCiRun {
            id: 99,
            status: "completed".to_string(),
            conclusion: "success".to_string(),
            sha: "a".repeat(40),
            title: "CI".to_string(),
            age_seconds: None,
            completed_at: "2026-10-25T12:00:00Z".to_string(),
        })),
        open_prs: Ok(vec![PullFacts {
            number: 12,
            title: "Add feature".to_string(),
            branch: "feat/12".to_string(),
            base: "main".to_string(),
            head: "b".repeat(40),
            state: "OPEN".to_string(),
            mergeable: "MERGEABLE".to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    };

    let out1 = refresh_world(
        &mut store,
        &forge_ok,
        None,
        &mut hands,
        Duration::from_secs(2),
        true,
    )
    .unwrap();
    assert!(out1.changed);
    assert!(out1.errors.is_empty());
    let st1 = store.load().unwrap();
    assert_eq!(st1.open_prs_cache.len(), 1);
    assert_eq!(st1.open_prs_cache[0].number, 12);
    assert_eq!(st1.main_ci.as_ref().unwrap().id, 99);
    assert!(st1.world_refreshed_at.contains_key("prs"));
    assert!(st1.world_refreshed_at.contains_key("ci"));

    // Second refresh with identical data reports changed = false.
    let out2 = refresh_world(
        &mut store,
        &forge_ok,
        None,
        &mut hands,
        Duration::from_secs(2),
        true,
    )
    .unwrap();
    assert!(!out2.changed);

    // Forge failure emits attention and records decision, while preserving last-known PRs and CI!
    let forge_err = MockForge {
        main_ci: Err("502 Bad Gateway".to_string()),
        open_prs: Err("rate limit exceeded".to_string()),
        ..Default::default()
    };
    let out3 = refresh_world(
        &mut store,
        &forge_err,
        None,
        &mut hands,
        Duration::from_secs(2),
        true,
    )
    .unwrap();
    assert!(!out3.changed);
    assert_eq!(out3.errors.len(), 2);
    assert_eq!(hands.attention.len(), 2);
    let st3 = store.load().unwrap();
    assert_eq!(st3.open_prs_cache.len(), 1);
    assert_eq!(st3.open_prs_cache[0].number, 12);
    assert_eq!(st3.main_ci.as_ref().unwrap().id, 99);
}

#[test]
fn test_build_tidy_snapshot_uses_actual_idle_mtime_and_detects_untracked_dirty_files() {
    let dir = tempfile::tempdir().unwrap();
    let main_repo = dir.path().join("main-repo");
    std::fs::create_dir_all(&main_repo).unwrap();

    let run_git = |args: &[&str]| {
        let st = std::process::Command::new("git")
            .args(args)
            .current_dir(&main_repo)
            .status()
            .unwrap();
        assert!(st.success(), "git {args:?} failed");
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.email", "t@example.com"]);
    run_git(&["config", "user.name", "Tester"]);
    std::fs::write(main_repo.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"]);
    run_git(&["commit", "-q", "-m", "initial"]);
    let base_sha = crate::sampler::run_git(&main_repo, &["rev-parse", "HEAD"], GIT_CAP)
        .unwrap()
        .trim()
        .to_string();

    let wt_dir = dir.path().join("wt-active");
    run_git(&[
        "worktree",
        "add",
        "-q",
        "-b",
        "feat/active",
        wt_dir.to_str().unwrap(),
        "HEAD",
    ]);
    assert!(is_linked_worktree(&wt_dir));
    assert!(!is_linked_worktree(&main_repo));

    // Provisioned `.tachoenv` alone must NOT mark the worktree dirty.
    std::fs::write(
        wt_dir.join(".tachoenv"),
        "export TACHO_WORKTREE=wt-active\n",
    )
    .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut state = FleetState::default();
    // Even if the main checkout is recorded in `state.worktrees`, it must NEVER be a removal
    // candidate in `snap.worktrees` and must be protected in `snap.held_paths`.
    state.worktrees.insert(
        "main-checkout".to_string(),
        crate::fleet::WorktreeEntry {
            slug: "main-checkout".to_string(),
            path: main_repo.to_string_lossy().to_string(),
            branch: "main".to_string(),
            base_sha: base_sha.clone(),
            base_pr: None,
            port: 3000,
            port_last: 3019,
            created_at: now - 86_400,
        },
    );
    state.worktrees.insert(
        "wt-active".to_string(),
        crate::fleet::WorktreeEntry {
            slug: "wt-active".to_string(),
            path: wt_dir.to_string_lossy().to_string(),
            branch: "feat/active".to_string(),
            base_sha,
            base_pr: None,
            port: 3100,
            port_last: 3119,
            created_at: now - 7_200, // created 2 hours ago, but files modified just now!
        },
    );

    let snap_clean = build_tidy_snapshot(
        &state,
        &tacho_core::LiveState::default(),
        &BTreeSet::new(),
        now,
    );
    assert_eq!(snap_clean.worktrees.len(), 1);
    assert_eq!(snap_clean.worktrees[0].path, wt_dir.to_string_lossy());
    assert!(
        snap_clean
            .held_paths
            .contains(&main_repo.to_string_lossy().to_string()),
        "main checkout must be protected in held_paths"
    );
    assert!(
        snap_clean.worktrees[0].age_secs < 60,
        "expected idle age_secs < 60 for recently modified worktree, got {}",
        snap_clean.worktrees[0].age_secs
    );
    assert!(
        !snap_clean.worktrees[0].dirty,
        "worktree with only .tachoenv should be clean"
    );

    // Writing an untracked file makes dirty == true and keeps age_secs < 60.
    std::fs::write(wt_dir.join("untracked_wip.rs"), "pub fn wip() {}\n").unwrap();
    let snap_dirty = build_tidy_snapshot(
        &state,
        &tacho_core::LiveState::default(),
        &BTreeSet::new(),
        now,
    );
    assert_eq!(snap_dirty.worktrees.len(), 1);
    assert!(snap_dirty.worktrees[0].dirty);
    assert!(snap_dirty.worktrees[0].age_secs < 60);
}

#[test]
fn test_git_commands_refuse_plain_subdir_inside_enclosing_repo() {
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

    // Create an untracked file in the enclosing parent repo and a plain subdirectory
    // pretending to be a worktree (`--show-toplevel` would walk up to `parent_repo` if unconfined).
    std::fs::write(parent_repo.join("untracked_parent.txt"), "do not stage\n").unwrap();
    let plain_subdir = parent_repo.join("trees").join("not-a-worktree");
    std::fs::create_dir_all(&plain_subdir).unwrap();
    std::fs::write(plain_subdir.join("file.txt"), "hello\n").unwrap();

    assert!(
        !is_git_checkout(&plain_subdir),
        "plain subdir inside a parent git repo must not be treated as its own worktree"
    );
    let obs = observe_worktree(&plain_subdir);
    assert_eq!(obs, WorktreeObs::default());
    assert!(
        !parent_repo.join(".git").join("index.lock").exists(),
        "observe_worktree must never leave .git/index.lock in parent repo"
    );
}
