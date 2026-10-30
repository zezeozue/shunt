//! Comprehensive unit tests for the pure decision engine (`decide::dispatch`,
//! `decide::verify`, `decide::land`, and `decide::watch`).

use crate::decide::dispatch::*;
use crate::decide::land::*;
use crate::decide::verify::*;
use crate::decide::watch::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[test]
fn test_run_lineage_and_resumption() {
    assert_eq!(base_key("auth-fix"), "auth-fix");
    assert_eq!(base_key("auth-fix-r1"), "auth-fix");
    assert_eq!(base_key("auth-fix-r2"), "auth-fix");
    assert_eq!(attempt("auth-fix"), 0);
    assert_eq!(attempt("auth-fix-r1"), 1);
    assert_eq!(run_key("auth-fix-r1", 2), "auth-fix-r2");

    let mut agents = BTreeMap::new();
    agents.insert(
        "auth-fix".to_string(),
        AgentRun {
            key: "auth-fix".to_string(),
            area: "auth".to_string(),
            brief: "Fix login race".to_string(),
            worktree: "/tmp/wt/auth-fix".to_string(),
            started_at: 100,
            finished_at: Some(200),
            exit_code: Some(1),
            ..Default::default()
        },
    );

    let retired = BTreeSet::new();
    let holds = BTreeMap::new();
    let live = BTreeSet::new();
    let todo = Vec::new();

    assert_eq!(
        run_state(&agents, &todo, &retired, &live, "auth-fix"),
        RunState::Resuming
    );
    // Backoff not elapsed at t=250 (needs 200 + 120 = 320)
    assert!(!needs_resume(
        &agents, &todo, &retired, &holds, &live, "auth-fix", 250
    ));
    assert!(needs_resume(
        &agents, &todo, &retired, &holds, &live, "auth-fix", 320
    ));

    let resumed = resume_task(
        agents.get("auth-fix").unwrap(),
        "abc1234 wip commit",
        "M src/lib.rs",
        320,
    );
    assert_eq!(resumed.key, "auth-fix-r1");
    assert!(resumed.brief.contains("Stopped: previous run (auth-fix)"));
    assert!(resumed.brief.contains("abc1234 wip commit"));

    // After r1 also fails, state becomes Stuck
    agents.insert(
        "auth-fix-r1".to_string(),
        AgentRun {
            key: "auth-fix-r1".to_string(),
            area: "auth".to_string(),
            brief: resumed.brief.clone(),
            worktree: "/tmp/wt/auth-fix".to_string(),
            started_at: 325,
            finished_at: Some(400),
            exit_code: Some(1),
            ..Default::default()
        },
    );
    assert_eq!(
        run_state(&agents, &todo, &retired, &live, "auth-fix"),
        RunState::Stuck
    );
    assert_eq!(stuck_runs(&agents, &todo, &retired, &holds, &live).len(), 1);
}

#[test]
fn test_stated_and_guessed_pr_attribution() {
    assert_eq!(stated_pr("all done\nPR: #42\n"), Some(Some(42)));
    assert_eq!(stated_pr("nothing needed\nPR: none\n"), Some(None));
    assert_eq!(stated_pr("mentioned #42 inline"), None);

    let open = vec![PullFacts {
        number: 42,
        branch: "other-branch".to_string(),
        head: "deadbeef".to_string(),
        ..Default::default()
    }];
    // Mentioning #42 without matching branch or `PR: #42` line is only `guessed`
    let (pr, src) = find_pr("my-task", 100, &open, "I saw #42 in logs");
    assert_eq!(pr, Some(42));
    assert_eq!(src.as_deref(), Some("guessed"));

    let mut run = AgentRun {
        key: "my-task".to_string(),
        pr,
        pr_source: src,
        finished_at: Some(200),
        exit_code: Some(0),
        ..Default::default()
    };
    // Guessed PR is NOT trusted by recorded_pr and flags a pr_problem
    assert_eq!(recorded_pr(&run), None);
    assert!(pr_problem(&run, None).is_some());

    // Confirming via attach makes it trusted
    run.pr_source = Some("attached".to_string());
    assert_eq!(recorded_pr(&run), Some(42));
    assert!(pr_problem(&run, None).is_none());
}

#[test]
fn test_area_gate_and_next_work_stacking() {
    let mut agents = BTreeMap::new();
    agents.insert(
        "step-1".to_string(),
        AgentRun {
            key: "step-1".to_string(),
            area: "storage".to_string(),
            started_at: 100,
            finished_at: Some(200),
            exit_code: Some(0),
            pr: Some(10),
            pr_source: Some("stated".to_string()),
            ..Default::default()
        },
    );

    let todo = vec![
        TodoItem {
            key: "step-2".to_string(),
            brief: "Second storage change".to_string(),
            area: "storage".to_string(),
            priority: 0,
            created_at: 150,
            ..Default::default()
        },
        TodoItem {
            key: "step-3".to_string(),
            brief: "Third storage change".to_string(),
            area: "storage".to_string(),
            priority: 0,
            created_at: 160,
            ..Default::default()
        },
    ];

    let retired = BTreeSet::new();
    let holds = BTreeMap::new();
    let live = BTreeSet::new();
    let mut passed = BTreeSet::new();
    let failed = BTreeSet::new();
    let merged = BTreeSet::new();
    let closed = BTreeSet::new();

    // While PR #10 has not passed batch verification, storage area is gated
    let (chosen, _) = next_work(
        &todo, &agents, &retired, &holds, &live, &passed, &failed, &merged, &closed, 4,
    );
    assert!(chosen.is_empty());

    // Once PR #10 passes, at most ONE task in the `storage` area starts, stacked on PR #10
    passed.insert(10);
    let (chosen, rem) = next_work(
        &todo, &agents, &retired, &holds, &live, &passed, &failed, &merged, &closed, 4,
    );
    assert_eq!(chosen.len(), 1);
    assert_eq!(chosen[0].key, "step-2");
    assert_eq!(chosen[0].base.as_deref(), Some("10"));
    assert_eq!(rem.len(), 1);
    assert_eq!(rem[0].key, "step-3");
}

#[test]
fn test_compare_expansion() {
    let item = TodoItem {
        key: "bench".to_string(),
        brief: "Optimize parser".to_string(),
        area: "core".to_string(),
        compare: Some("opus,codex,gemini".to_string()),
        ..Default::default()
    };
    let expanded = compare_items(&item, Some("high")).unwrap();
    assert_eq!(expanded.len(), 3);
    assert_eq!(expanded[0].key, "bench-opus");
    assert_eq!(expanded[0].provider.as_deref(), Some("claude"));
    assert_eq!(expanded[0].area, ""); // Cleared so compare variants run in parallel
    assert_eq!(expanded[1].key, "bench-codex");
    assert_eq!(expanded[1].provider.as_deref(), Some("codex"));
    assert_eq!(expanded[2].key, "bench-gemini");
    assert_eq!(expanded[2].provider.as_deref(), Some("gemini"));
}

#[test]
fn test_stack_ordering_and_whole_stack_cap() {
    let open = vec![
        PullFacts {
            number: 1,
            branch: "feat-1".to_string(),
            base: "main".to_string(),
            ..Default::default()
        },
        PullFacts {
            number: 2,
            branch: "feat-2".to_string(),
            base: "feat-1".to_string(),
            ..Default::default()
        },
        PullFacts {
            number: 3,
            branch: "feat-3".to_string(),
            base: "feat-2".to_string(),
            ..Default::default()
        },
        PullFacts {
            number: 4,
            branch: "solo-4".to_string(),
            base: "main".to_string(),
            ..Default::default()
        },
    ];
    let pmap = parents(&open);
    assert_eq!(root(3, &pmap), 1);
    assert_eq!(ordered(&[3, 1, 2], &pmap), vec![1, 2, 3]);

    // Capping [4, 1, 2, 3] with limit 2:
    // Child 2 rides with parent 1, and child 3 rides with parent 2 so the whole stack [1, 2, 3] rides together!
    assert_eq!(cap(&[3, 2, 1], &pmap, 2), vec![1, 2, 3]);
    // With limit 3 on [3, 2, 1, 4], the stack [1, 2, 3] fills the cap and solo 4 waits
    assert_eq!(cap(&[3, 2, 1, 4], &pmap, 3), vec![1, 2, 3]);
}

#[test]
fn test_bisect_checks_main_before_blaming_first_member() {
    let members = vec![
        (10, "sha10".to_string()),
        (11, "sha11".to_string()),
        (12, "sha12".to_string()),
        (13, "sha13".to_string()),
    ];
    let mut b = Bisect {
        members: members.clone(),
        results: BTreeMap::from([(4, false)]),
    };
    // Midpoint probe is 2
    assert_eq!(next_probe(&b), ProbeStep::Probe(2));
    // Suppose prefix 2 also fails -> probes 1
    b.results.insert(2, false);
    assert_eq!(next_probe(&b), ProbeStep::Probe(1));
    // Prefix 1 fails -> MUST probe 0 (main alone) before blaming PR #10!
    b.results.insert(1, false);
    assert_eq!(next_probe(&b), ProbeStep::Probe(0));
    // If main passes, PR #10 is the culprit
    b.results.insert(0, true);
    assert_eq!(
        next_probe(&b),
        ProbeStep::Culprit((10, "sha10".to_string()))
    );

    // If main fails, report MainBroken instead of blaming PR #10
    b.results.insert(0, false);
    assert_eq!(next_probe(&b), ProbeStep::MainBroken);
}

#[test]
fn test_classify_code_beats_infra_noise() {
    let pure_infra = "curl: (7) Failed to connect: ECONNREFUSED\n503 Service Unavailable";
    assert_eq!(classify(pure_infra), "infrastructure");

    let mixed =
        "warning: ECONNREFUSED on optional telemetry\ntest decide::tests::my_test ... FAILED\n";
    assert_eq!(classify(mixed), "code");

    let proptest_log = "503 Service Unavailable\nminimal failing input: x = 0\n";
    assert_eq!(classify(proptest_log), "code");
    assert_eq!(
        counterexample(proptest_log).as_deref(),
        Some("minimal failing input: x = 0")
    );
}

#[test]
fn test_flake_ledger_auto_queues_task_at_threshold() {
    let ledger = BTreeMap::new();
    let tests = vec!["recorder::tests::flaky_wal".to_string()];
    let l1 = record_flakes(
        &ledger,
        &tests,
        "core",
        "b001",
        "test recorder::tests::flaky_wal ... FAILED",
        100,
    );
    let todo = Vec::new();
    let agents = BTreeMap::new();
    let retired = BTreeSet::new();

    // 1 flake is below threshold (2)
    let (_, tasks1) = flake_tasks(&l1, &todo, &agents, &retired, 100);
    assert!(tasks1.is_empty());

    // 2nd flake crosses threshold and queues a fix task
    let l2 = record_flakes(
        &l1,
        &tests,
        "core",
        "b002",
        "test recorder::tests::flaky_wal ... FAILED",
        200,
    );
    let (l2_updated, tasks2) = flake_tasks(&l2, &todo, &agents, &retired, 200);
    assert_eq!(tasks2.len(), 1);
    assert_eq!(tasks2[0].area, "core");
    assert!(tasks2[0].brief.contains("tacho starve"));

    // Calling again does not duplicate the task once queued
    let (_, tasks3) = flake_tasks(&l2_updated, &tasks2, &agents, &retired, 210);
    assert!(tasks3.is_empty());
}

#[test]
fn test_pr_preflight_refusals_and_proof_validation() {
    let proof = ProofSummary {
        head: "abc123456789".to_string(),
        tree: "tree11111111".to_string(),
        base: "base00000000".to_string(),
        ok: true,
        timestamp: 1000,
        changed_files: vec!["crates/tacho-core/src/lib.rs".to_string()],
        checks: vec![ProofCheck {
            name: "test".to_string(),
            cmd: "cargo test".to_string(),
            ok: true,
            duration_ms: 42,
            output_tail: "ok".to_string(),
            area: Some("workspace".to_string()),
            units: vec!["all".to_string()],
        }],
        screenshots: Vec::new(),
    };

    // Valid when tree matches and no forbidden trailers exist
    let ok_refusals = pr_refusals(
        "Add pure decision engine",
        "Implements dispatch, verify, land, and watch modules.",
        &["abc1234 Add pure decision engine".to_string()],
        &[],
        &["crates/tacho-core/src/lib.rs".to_string()],
        &[],
        "abc123456789",
        "tree11111111",
        Some(&proof),
        &[],
    );
    assert!(
        ok_refusals.is_empty(),
        "expected no refusals, got {ok_refusals:?}"
    );

    // Refuses stale tree, dirty files, merge commits, and Co-Authored-By
    let bad_refusals = pr_refusals(
        "Add feature",
        "Summary\n\nCo-Authored-By: Someone <a@b.com>",
        &["abc1234 Add feature".to_string()],
        &["def5678 Merge branch 'main'".to_string()],
        &["crates/tacho-core/src/lib.rs".to_string()],
        &["M Cargo.toml".to_string()],
        "abc123456789",
        "different_tree_hash",
        Some(&proof),
        &[],
    );
    assert!(bad_refusals.iter().any(|r| r.contains("merge commit")));
    assert!(bad_refusals
        .iter()
        .any(|r| r.contains("uncommitted changes")));
    assert!(bad_refusals.iter().any(|r| r.contains("proof is stale")));
    assert!(bad_refusals.iter().any(|r| r.contains("Co-Authored-By")));
}

#[test]
fn test_land_still_ready_and_train_tree_verification() {
    let pr = PullFacts {
        number: 11,
        state: "OPEN".to_string(),
        mergeable: "MERGEABLE".to_string(),
        head: "sha11".to_string(),
        ..Default::default()
    };
    let rec = PassedRecord {
        pr: 11,
        sha: "sha11".to_string(),
        main_sha: "main0".to_string(),
        batch_id: "b001".to_string(),
        passed_at: 100,
        tree: "tree_b001".to_string(),
        areas: vec!["core".to_string()],
    };
    let batch = BatchRun {
        id: "b001".to_string(),
        prs: vec![10, 11],
        shas: BTreeMap::from([(10, "sha10".to_string()), (11, "sha11".to_string())]),
        main_sha: "main0".to_string(),
        status: "passed".to_string(),
        tree: Some("tree_b001".to_string()),
        ..Default::default()
    };
    let holds = BTreeMap::new();

    // Still ready if main moved only because earlier batch member #10 merged
    assert!(still_ready(
        &pr,
        &rec,
        Some(&batch),
        &holds,
        "main_after_10",
        &[10]
    ));
    // Not ready if an unrelated PR #99 merged onto main
    assert!(!still_ready(
        &pr,
        &rec,
        Some(&batch),
        &holds,
        "main_after_99",
        &[10, 99]
    ));

    let train = make_train(&[], &batch, "main0", "tree_b001", 200).unwrap();
    let after_10 = after_outside_merge(&train, 10, "main_after_10");
    assert_eq!(after_10.status, "open");
    assert_eq!(after_10.landed_prs, vec![10]);
    assert_eq!(mismatch(&after_10, "tree_b001"), None);
    assert!(mismatch(&after_10, "wrong_tree").is_some());
}

#[test]
fn test_drop_breakers_bisects_compile_culprit_and_descendants() {
    let parent_map = BTreeMap::from([(2, 1)]);
    // PR #2 fails to compile; PR #1 and #3 compile cleanly
    let (surviving, dropped) = drop_breakers(&[1, 2, 3], &parent_map, |slice| {
        if slice.contains(&2) {
            Err("error[E0425]: cannot find value `broken` in PR 2".to_string())
        } else {
            Ok(())
        }
    });
    assert_eq!(surviving, vec![1, 3]);
    assert!(dropped.contains_key(&2));
}

#[test]
fn test_watch_verdicts_supervision_starve_and_tidy() {
    let s0 = ProgressSample {
        ts: 0,
        log_bytes: 1000,
        head_sha: "a".to_string(),
        dirty_files: 0,
        cpu_ticks: 500,
        ..Default::default()
    };
    let s1 = ProgressSample {
        ts: 2000,
        log_bytes: 1000,
        head_sha: "a".to_string(),
        dirty_files: 0,
        cpu_ticks: 500,
        ..Default::default()
    };
    // 2000s (>= 1800s) with 0 log growth and 0 CPU delta -> HUNG -> Resume
    let (v_hung, idle) = verdict(&[s0.clone(), s1], 2000);
    assert_eq!(v_hung, Verdict::Hung);
    assert!(matches!(
        supervise_step("w1", v_hung, idle, false),
        SuperviseAction::Resume { .. }
    ));

    // 3000s (>= 2700s) with CPU delta > 0 but 0 work -> STALLED -> Nudge then Escalate
    let s2 = ProgressSample {
        ts: 3000,
        log_bytes: 1000,
        head_sha: "a".to_string(),
        dirty_files: 0,
        cpu_ticks: 900,
        ..Default::default()
    };
    let (v_stalled, idle2) = verdict(&[s0, s2], 3000);
    assert_eq!(v_stalled, Verdict::Stalled);
    assert!(matches!(
        supervise_step("w1", v_stalled, idle2, false),
        SuperviseAction::Nudge { .. }
    ));
    assert!(matches!(
        supervise_step("w1", v_stalled, idle2, true),
        SuperviseAction::Escalate { .. }
    ));

    // Starve planning refuses when a batch is running and caps to 1/8th of CPUs
    assert!(plan_starve(16, None, None, true, 60).is_err());
    let plan = plan_starve(16, None, None, false, 60).unwrap();
    assert_eq!(plan.cpus, vec![14, 15]);
    assert_eq!(plan.burners, 8);

    // Tidy respects live worktrees and sets rescue=true on dirty/unpushed trees
    let snap = TidySnapshot {
        now: 10_000,
        worktrees: vec![
            WorktreeCandidate {
                path: "/wt/live".to_string(),
                branch: "live".to_string(),
                age_secs: 9_000,
                dirty: false,
                unpushed_commits: 0,
                pr_merged_or_closed: false,
            },
            WorktreeCandidate {
                path: "/wt/dirty-old".to_string(),
                branch: "dirty-old".to_string(),
                age_secs: 9_000,
                dirty: true,
                unpushed_commits: 1,
                pr_merged_or_closed: false,
            },
        ],
        live_agent_worktrees: BTreeSet::from(["/wt/live".to_string()]),
        ..Default::default()
    };
    let decisions = decide_tidy(&snap, 3_600);
    assert_eq!(decisions.len(), 1);
    assert_eq!(
        decisions[0],
        TidyDecision::RemoveTree {
            path: "/wt/dirty-old".to_string(),
            rescue: true,
            because: "worktree idle for 9000s >= 3600s".to_string(),
        }
    );
    assert!(owns(
        Path::new("/wt/live/src/lib.rs"),
        Path::new("/wt/live")
    ));
    assert!(!owns(
        Path::new("/wt/live2/src/lib.rs"),
        Path::new("/wt/live")
    ));
}

#[test]
fn test_parity_edge_cases_cap_flake_key_retire_env_and_guardrails() {
    // 1. cap: child rides with parent past limit, and child whose parent was left out waits
    let up = BTreeMap::from([(7, 6)]);
    assert_eq!(
        cap(&[1, 2, 3, 4, 5, 6, 7], &up, 6),
        vec![1, 2, 3, 4, 5, 6, 7]
    );
    let up2 = BTreeMap::from([(8, 7)]);
    assert_eq!(
        cap(&[1, 2, 3, 4, 5, 6, 7, 8], &up2, 6),
        vec![1, 2, 3, 4, 5, 6]
    );

    // 2. flake_key: long names sharing a 40+ char prefix get distinct hash suffixes
    let k_alpha =
        flake_key("pool::tests::rate_locking_is_kept_under_load_when_the_redis_pool_is_full_alpha");
    let k_beta =
        flake_key("pool::tests::rate_locking_is_kept_under_load_when_the_redis_pool_is_full_beta");
    assert_ne!(k_alpha, k_beta);

    // 3. record_flakes: idempotent when called twice for the same batch_id
    let ledger = BTreeMap::new();
    let l1 = record_flakes(
        &ledger,
        &["pool::tests::lock".to_string()],
        "core",
        "b001",
        "test pool::tests::lock ... FAILED",
        100,
    );
    let l2 = record_flakes(
        &l1,
        &["pool::tests::lock".to_string()],
        "core",
        "b001",
        "test pool::tests::lock ... FAILED",
        200,
    );
    assert_eq!(l2["pool::tests::lock"].count, 1);
    assert_eq!(l2["pool::tests::lock"].last_seen, 100);

    // 4. failed_tests: parses Rust, Gradle/JUnit, and Playwright
    let multi_log = "\
test billing::tests::charges_once ... ok
test billing::tests::refunds_twice ... FAILED
NumbersFlowTest > a_number_is_registered FAILED
  ✘   40 [chromium] › e2e/console.spec.ts:1013:3 › the audit log (7.2s)
";
    assert_eq!(
        failed_tests(multi_log),
        vec![
            "NumbersFlowTest > a_number_is_registered".to_string(),
            "billing::tests::refunds_twice".to_string(),
            "e2e/console.spec.ts:1013:3 › the audit log".to_string(),
        ]
    );

    // 5. retire: strips retired key from downstream todo.after lists
    let todo = vec![
        TodoItem {
            key: "step-1".to_string(),
            brief: "First".to_string(),
            area: "core".to_string(),
            ..Default::default()
        },
        TodoItem {
            key: "step-2".to_string(),
            brief: "Second".to_string(),
            area: "core".to_string(),
            after: vec!["step-1".to_string()],
            ..Default::default()
        },
    ];
    let (new_todo, new_retired, _) = retire(
        "step-1",
        &todo,
        &BTreeMap::new(),
        &BTreeSet::new(),
        &BTreeMap::new(),
    );
    assert!(new_retired.contains("step-1"));
    assert_eq!(new_todo.len(), 1);
    assert!(new_todo[0].after.is_empty());

    // 6. env_file / parse_env: roundtrips embedded single quotes
    let env_txt = env_file(
        "my-slug",
        "deadbeef",
        Some(42),
        20000,
        20019,
        Some("/tmp/it's/target"),
    );
    let parsed = parse_env(&env_txt);
    assert_eq!(parsed.get("SLUG").map(String::as_str), Some("my-slug"));
    assert_eq!(
        parsed.get("CARGO_TARGET_DIR").map(String::as_str),
        Some("/tmp/it's/target")
    );

    // 7. proof_plan: scopes leaf module edits to `--lib -- <module>::` and test suites to `--test <suite>`
    let plan = proof_plan(
        &[
            "crates/tacho-core/src/decide/verify.rs".to_string(),
            "crates/tacho-core/tests/oracle.rs".to_string(),
        ],
        &[],
    );
    assert!(plan
        .iter()
        .any(|(_, cmd)| cmd == "cargo fmt --all -- --check"));
    assert!(plan
        .iter()
        .any(|(_, cmd)| cmd == "cargo test -p tacho-core --lib -- decide::verify::"));
    assert!(plan
        .iter()
        .any(|(_, cmd)| cmd == "cargo test -p tacho-core --test oracle"));

    // 8. burner_command & guard_bash_tool & unknown model pricing
    assert_eq!(
        burner_command("timeout", "timeout -k 5 30 /usr/bin/yes").as_deref(),
        Some("yes")
    );
    assert!(guard_bash_tool(true, "cargo test", false).is_err());
    assert!(guard_bash_tool(false, "yes > /dev/null", false).is_err());
    assert!(guard_bash_tool(false, "yes > /dev/null", true).is_ok());
    assert!(guard_bash_tool(false, "git stash", false).is_err());
    assert!(guard_bash_tool(false, "git commit --no-verify -m 'wip'", false).is_err());
    assert_eq!(price("unknown-custom-model", 100_000, 50_000, 0, 0), 0.0);
    assert!(queue_grew(&[10, 11], &[10, 11, 12]));
    assert!(!queue_grew(&[10, 11], &[10]));
}

#[test]
fn test_child_cpu_counts_toward_stalled_verdict() {
    // Agent log/head/dirty_files have been unchanged for 48m (> 45m STALLED_SECS),
    // BUT child processes in the process tree are burning CPU (`>= IDLE_CPU_TICKS_PER_SEC` = 5 ticks/s).
    // Must be Verdict::Moving, not Verdict::Stalled!
    let samples_busy_child = vec![
        ProgressSample {
            ts: 1000,
            log_bytes: 500,
            head_sha: "abc".to_string(),
            dirty_files: 1,
            cpu_ticks: 100,
            ..Default::default()
        },
        ProgressSample {
            ts: 1000 + 2800,
            log_bytes: 500,
            head_sha: "abc".to_string(),
            dirty_files: 1,
            // 2800s * 10 ticks/s = 28_000 ticks (>= 5 ticks/s)
            cpu_ticks: 100 + 28_000,
            ..Default::default()
        },
    ];
    let (v, _idle) = verdict(&samples_busy_child, 1000 + 2900);
    assert_eq!(v, Verdict::Moving);

    // Conversely, if CPU rate is only 1 tick over 2800s (< 5 ticks/s), it is alive (not Hung),
    // but process tree is idle for >= 45m, so it IS Verdict::Stalled.
    let samples_idle_child = vec![
        ProgressSample {
            ts: 1000,
            log_bytes: 500,
            head_sha: "abc".to_string(),
            dirty_files: 1,
            cpu_ticks: 100,
            ..Default::default()
        },
        ProgressSample {
            ts: 1000 + 2800,
            log_bytes: 500,
            head_sha: "abc".to_string(),
            dirty_files: 1,
            cpu_ticks: 101,
            ..Default::default()
        },
    ];
    let (v_idle, _) = verdict(&samples_idle_child, 1000 + 2900);
    assert_eq!(v_idle, Verdict::Stalled);
}

#[test]
fn test_provider_interface_start_resume_tell_alive_stop_cost() {
    use super::dispatch::{
        builtin_providers, migration_order, resolve_with_providers, Provider, ProviderSpec,
        TellPlan,
    };

    let providers = builtin_providers();
    let claude = providers.get("claude").unwrap();

    // 1. start
    let start_cmd = claude.start(
        "w1",
        "/tmp/wt1",
        "Fix the bug",
        Some("sonnet"),
        Some("high"),
    );
    assert_eq!(start_cmd.program, "claude");
    assert!(start_cmd.args.contains(&"--model".to_string()));
    assert!(start_cmd.args.contains(&"sonnet".to_string()));
    assert!(start_cmd.args.contains(&"--effort".to_string()));
    assert!(start_cmd.args.contains(&"high".to_string()));
    assert!(start_cmd.args.contains(&"Fix the bug".to_string()));

    // 2. resume with session_id
    let resume_cmd = claude.resume(
        "w1",
        "/tmp/wt1",
        Some("sess-abc"),
        "Address review comments",
        Some("opus"),
        None,
    );
    assert!(resume_cmd.args.contains(&"--resume".to_string()));
    assert!(resume_cmd.args.contains(&"sess-abc".to_string()));
    assert!(resume_cmd
        .args
        .contains(&"Address review comments".to_string()));

    // 3. tell on a LIVE worker -> LiveInbox (no cold restart!)
    let live_tell = claude.tell(
        "w1",
        "/tmp/wt1",
        Some("sess-abc"),
        "New instruction while running",
        true,
        Some("opus"),
        None,
    );
    assert_eq!(
        live_tell,
        TellPlan::LiveInbox {
            key: "w1".to_string(),
            session_id: Some("sess-abc".to_string()),
            message: "New instruction while running".to_string(),
        }
    );

    // 4. tell on a STOPPED worker -> ResumeWithMessage
    let stopped_tell = claude.tell(
        "w1",
        "/tmp/wt1",
        Some("sess-abc"),
        "Wake up and rebase",
        false,
        Some("opus"),
        None,
    );
    assert!(matches!(stopped_tell, TellPlan::ResumeWithMessage { .. }));

    // 5. alive & stop & cost
    let mut live_pids = BTreeSet::new();
    live_pids.insert(4242);
    assert!(claude.alive(Some(4242), &live_pids));
    assert!(!claude.alive(Some(9999), &live_pids));
    let stop_plan = claude.stop("w1", "/tmp/wt1", Some("sess-abc"), Some(4242));
    assert_eq!(stop_plan.pid, Some(4242));
    let cost = claude.cost(
        &[r#"{"type":"result","total_cost_usd":0.25,"usage":{"input_tokens":1000,"output_tokens":500}}"#],
        Some("opus"),
    );
    assert!((cost - 0.25).abs() < 1e-6);

    // 6. Custom configured provider with custom tell_program
    let mut custom_map = builtin_providers();
    custom_map.insert(
        "custom-agent".to_string(),
        ProviderSpec {
            name: "custom-agent".to_string(),
            binary: "/usr/local/bin/custom-agent".to_string(),
            default_model: Some("custom-v1".to_string()),
            default_effort: None,
            start_args: vec![
                "run".to_string(),
                "--prompt".to_string(),
                "{prompt}".to_string(),
            ],
            resume_args: vec![
                "resume".to_string(),
                "--session".to_string(),
                "{session}".to_string(),
                "{message}".to_string(),
            ],
            resume_without_session_args: Vec::new(),
            model_flag: Some("--model".to_string()),
            effort_flag: None,
            tell_program: Some("/usr/local/bin/custom-tell".to_string()),
            tell_args: vec![
                "--session".to_string(),
                "{session}".to_string(),
                "{message}".to_string(),
            ],
            stop_program: None,
            stop_args: Vec::new(),
            env: Vec::new(),
            supports_hook_inbox: false,
            model_aliases: BTreeMap::new(),
        },
    );
    let choice = resolve_with_providers(
        Some("custom-agent"),
        None,
        None,
        "claude",
        None,
        None,
        &custom_map,
    )
    .unwrap();
    assert_eq!(choice.provider, "custom-agent");
    assert_eq!(choice.model.as_deref(), Some("custom-v1"));
    let custom_live_tell =
        custom_map["custom-agent"].tell("w2", "/tmp/wt2", Some("s-99"), "ping", true, None, None);
    assert_eq!(
        custom_live_tell,
        TellPlan::LiveCommand {
            program: "/usr/local/bin/custom-tell".to_string(),
            args: vec![
                "--session".to_string(),
                "s-99".to_string(),
                "ping".to_string()
            ],
        }
    );

    // 7. Provider with no tell_args and no hook inbox (or idle session) resumes with message
    let codex = providers.get("codex").unwrap();
    let codex_tell = codex.tell(
        "w3",
        "/tmp/wt3",
        Some("codex-s1"),
        "rebase now",
        true,
        None,
        None,
    );
    assert!(matches!(codex_tell, TellPlan::ResumeWithMessage { .. }));
    let idle_claude_tell = claude.tell_session(
        "w1",
        "/tmp/wt1",
        Some("sess-abc"),
        "wake up",
        true,
        true,
        Some("opus"),
        None,
    );
    assert!(matches!(
        idle_claude_tell,
        TellPlan::ResumeWithMessage { .. }
    ));

    // 8. migration_order
    let mig_prs = vec![(105, vec![14, 15]), (102, vec![12]), (109, vec![])];
    assert_eq!(migration_order(&mig_prs), vec![102, 105, 109]);
}

#[test]
fn test_workflow_job_parser_and_batch_helpers() {
    use super::verify::{
        affected_units, area_hang_report, batch_heads_moved, batch_refs_moved, env_file_with_vars,
        job_argv, job_for, job_for_yaml, parse_workflow_yaml, service_argv,
    };

    let wf_yaml = r#"
# Real GitHub Actions YAML workflow
name: CI
on: [push, pull_request]
jobs:
  backend:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/org/ci:${{ env.CI_TAG }}
    env:
      RUST_BACKTRACE: "1"
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: postgres
        ports:
          - "5432:5432"
        options: --health-cmd pg_isready
    steps:
      - uses: actions/checkout@v4
      - uses: Swatinem/rust-cache@v2
      - name: Run backend tests
        working-directory: crates/backend
        env:
          DATABASE_URL: postgres://postgres:postgres@127.0.0.1:5432/test
        run: |
          cargo fmt --check
          cargo test --locked
"#;

    let mut exprs = BTreeMap::new();
    exprs.insert("env.CI_TAG".to_string(), "v1.2.3".to_string());

    let wf = parse_workflow_yaml(wf_yaml).unwrap();
    let job = job_for(&wf, "backend", "backend", &exprs).unwrap();
    assert_eq!(job.image.as_deref(), Some("ghcr.io/org/ci:v1.2.3"));
    assert_eq!(job.steps.len(), 1);
    assert_eq!(job.steps[0].name, "Run backend tests");
    assert_eq!(
        job.steps[0].working_directory.as_deref(),
        Some("crates/backend")
    );
    assert_eq!(job.steps[0].run, "cargo fmt --check\ncargo test --locked");
    let svc = job.service.as_ref().unwrap();
    assert_eq!(svc.image, "postgres:16");

    // Malformed YAML or unknown expression is strictly refused
    assert!(parse_workflow_yaml("jobs:\n  bad_line_without_colon\n").is_err());
    assert!(job_for_yaml(wf_yaml, "backend", "backend", &BTreeMap::new()).is_err());

    // batch_refs_moved catches moved main SHA as well as moved PR heads
    let mut expected_prs = BTreeMap::new();
    expected_prs.insert(11, "aaaa1111".to_string());
    assert!(batch_refs_moved("main0001", "main0001", &expected_prs, &expected_prs).is_none());
    let main_moved =
        batch_refs_moved("main0001", "main0002", &expected_prs, &expected_prs).unwrap();
    assert!(main_moved.contains("main moved from main0001 to main0002"));

    let svc_cmd = service_argv(svc, "tacho-svc-b1");
    assert!(svc_cmd.contains(&"postgres:16".to_string()));
    let run_cmd = job_argv(
        &job,
        "/tmp/batch-wt",
        Some("tacho-svc-b1"),
        &[("/tmp/cargo-cache".to_string(), "/root/.cargo".to_string())],
    );
    assert!(run_cmd.contains(&"container:tacho-svc-b1".to_string()));
    assert!(run_cmd.contains(&"ghcr.io/org/ci:v1.2.3".to_string()));

    // Unknown expression is strictly refused
    let empty_exprs = BTreeMap::new();
    assert!(job_for(&wf, "backend", "backend", &empty_exprs).is_err());

    // Head movement refusal during batch verification
    let mut expected = BTreeMap::new();
    expected.insert(11, "aaaa1111".to_string());
    expected.insert(12, "bbbb2222".to_string());
    let mut current = expected.clone();
    assert!(batch_heads_moved(&expected, &current).is_none());
    current.insert(12, "cccc3333".to_string());
    let moved = batch_heads_moved(&expected, &current).unwrap();
    assert!(
        moved.contains("PR #12 moved from aaaa1111")
            || moved.contains("PR #12 moved from bbbb2222")
    );

    // Affected units
    let mut unit_prefixes = BTreeMap::new();
    unit_prefixes.insert("api".to_string(), vec!["crates/api/".to_string()]);
    unit_prefixes.insert("db".to_string(), vec!["crates/db/".to_string()]);
    assert_eq!(
        affected_units(&["crates/api/src/lib.rs".to_string()], &unit_prefixes),
        vec!["api".to_string()]
    );
    assert_eq!(
        affected_units(&["Cargo.lock".to_string()], &unit_prefixes),
        vec!["all".to_string()]
    );

    // Area timeout hang report
    let log = "=== step: Build ===\nok\n=== step: Integration Suite ===\nrunning test_slow...\n";
    let hang = area_hang_report("batch-7", "backend", 600, log);
    assert_eq!(hang.active_step.as_deref(), Some("Integration Suite"));
    assert!(hang
        .summary
        .contains("timed out after 600s in step 'Integration Suite'"));

    // env_file_with_vars
    let env_txt = env_file_with_vars(
        "w1",
        "deadbeef",
        None,
        20000,
        20019,
        None,
        &[("DATABASE_URL", "postgres://postgres@127.0.0.1:20001/w1")],
    );
    let parsed = parse_env(&env_txt);
    assert_eq!(
        parsed.get("DATABASE_URL").map(String::as_str),
        Some("postgres://postgres@127.0.0.1:20001/w1")
    );
}

#[test]
fn test_unified_replayable_decide_function() {
    use super::watch::{BehindCommit, DeployRunObs};
    use super::{decide, BatchOutcomeObs, BatchResolution, DecisionState, WorldObservation};

    let mut state = DecisionState {
        max_parallel: 2,
        todo: vec![TodoItem {
            key: "feat-a".to_string(),
            brief: "Implement feature A".to_string(),
            area: "core".to_string(),
            priority: 5,
            created_at: 100,
            ..Default::default()
        }],
        ..Default::default()
    };
    state.batches.push(super::verify::BatchRun {
        id: "b-1".to_string(),
        group: "default".to_string(),
        prs: vec![50],
        shas: BTreeMap::from([(50, "sha50".to_string())]),
        main_sha: "main-sha-1".to_string(),
        areas: vec!["tacho-core".to_string()],
        status: "running".to_string(),
        started_at: 900,
        ..Default::default()
    });

    let obs = WorldObservation {
        now: 1000,
        main_sha: "main-sha-1".to_string(),
        open_prs: vec![PullFacts {
            number: 50,
            title: "Add feature 50".to_string(),
            branch: "agent/prior".to_string(),
            head: "sha50".to_string(),
            base: "main".to_string(),
            state: "OPEN".to_string(),
            mergeable: "MERGEABLE".to_string(),
            files: vec!["crates/tacho-core/src/lib.rs".to_string()],
            additions: 10,
            deletions: 2,
            ..Default::default()
        }],
        batch_outcomes: vec![BatchOutcomeObs {
            batch_id: "b-1".to_string(),
            passed: true,
            failed_area: None,
            log_output: "all green".to_string(),
            tree_sha: "tree-50".to_string(),
        }],
        deploy_runs: vec![DeployRunObs {
            run_id: 9001,
            head_sha: "main-sha-1".to_string(),
            status: "in_progress".to_string(),
            gate_conclusion: Some("success".to_string()),
            on_main: true,
        }],
        behind_commits: vec![BehindCommit {
            sha: "112233445566".to_string(),
            ts: 950,
            ci_green: true,
        }],
        detached_checkout: true,
        ..Default::default()
    };

    let plan1 = decide(&state, &obs);
    let plan2 = decide(&state, &obs);
    // Replayable determinism: identical inputs produce identical plans.
    assert_eq!(plan1, plan2);
    assert_eq!(plan1.dispatched_tasks.len(), 1);
    assert_eq!(plan1.dispatched_tasks[0].key, "feat-a");
    assert_eq!(
        plan1.batch_resolutions,
        vec![BatchResolution::Passed {
            batch_id: "b-1".to_string(),
            prs: vec![50],
        }]
    );
    assert!(plan1.next_state.passed.contains_key(&50));
    assert_eq!(plan1.ready_prs.len(), 1);
    assert_eq!(plan1.ready_prs[0].pr, 50);
    assert_eq!(
        plan1.self_update.target_sha.as_deref(),
        Some("112233445566")
    );
    assert!(plan1
        .owner_notifications
        .iter()
        .any(|n| n.key == "deploy:9001"));
    // Second tick with plan1.next_state does NOT re-send the tell-once deploy notification!
    let plan_second_tick = decide(&plan1.next_state, &obs);
    assert!(!plan_second_tick
        .owner_notifications
        .iter()
        .any(|n| n.key == "deploy:9001"));
}

fn running_batch(id: &str, prs: Vec<u64>) -> super::verify::BatchRun {
    super::verify::BatchRun {
        id: id.to_string(),
        group: "default".to_string(),
        shas: prs.iter().map(|&p| (p, format!("sha{p}"))).collect(),
        prs,
        main_sha: "main".to_string(),
        status: "running".to_string(),
        started_at: 900,
        ..Default::default()
    }
}

fn open_pr(number: u64) -> PullFacts {
    PullFacts {
        number,
        head: format!("sha{number}"),
        base: "main".to_string(),
        state: "OPEN".to_string(),
        mergeable: "MERGEABLE".to_string(),
        ..Default::default()
    }
}

fn outcome(id: &str, passed: bool, log: &str) -> super::BatchOutcomeObs {
    super::BatchOutcomeObs {
        batch_id: id.to_string(),
        passed,
        failed_area: Some("core".to_string()),
        log_output: log.to_string(),
        tree_sha: "tree".to_string(),
    }
}

#[test]
fn test_batch_outcomes_are_idempotent_across_ticks_and_duplicates() {
    use super::{decide, BatchResolution, DecisionState, WorldObservation};
    let state = DecisionState {
        batches: vec![running_batch("b-1", vec![1, 2])],
        ..Default::default()
    };
    let infra_log = "fetch: connection reset by peer\nCould not resolve host";
    // The same failing outcome sent twice in one observation is resolved once.
    let obs = WorldObservation {
        now: 1000,
        main_sha: "main".to_string(),
        open_prs: vec![open_pr(1), open_pr(2)],
        batch_outcomes: vec![
            outcome("b-1", false, infra_log),
            outcome("b-1", false, infra_log),
        ],
        ..Default::default()
    };
    let plan = decide(&state, &obs);
    assert_eq!(
        plan.batch_resolutions.len(),
        1,
        "{:?}",
        plan.batch_resolutions
    );
    let flakes_after_first = plan.next_state.flakes.clone();

    // Callers re-feeding the finished batch on the next tick must not re-resolve it.
    let obs2 = WorldObservation {
        now: 2000,
        batch_outcomes: vec![outcome("b-1", false, infra_log)],
        ..obs.clone()
    };
    let plan2 = decide(&plan.next_state, &obs2);
    assert!(
        plan2.batch_resolutions.is_empty(),
        "{:?}",
        plan2.batch_resolutions
    );
    assert_eq!(plan2.next_state.flakes, flakes_after_first);
    assert!(!plan2
        .batch_resolutions
        .iter()
        .any(|r| matches!(r, BatchResolution::Bisect { .. })));
}

#[test]
fn test_infra_failure_marks_batch_flake_and_never_passes_prs() {
    use super::{decide, BatchResolution, DecisionState, WorldObservation};
    let state = DecisionState {
        batches: vec![running_batch("b-9", vec![7])],
        ..Default::default()
    };
    let obs = WorldObservation {
        now: 1000,
        main_sha: "main".to_string(),
        open_prs: vec![open_pr(7)],
        batch_outcomes: vec![outcome(
            "b-9",
            false,
            "fetch: connection reset by peer\nCould not resolve host",
        )],
        ..Default::default()
    };
    let plan = decide(&state, &obs);
    assert!(matches!(
        plan.batch_resolutions.as_slice(),
        [BatchResolution::InfraRetry { .. }]
    ));
    assert_eq!(plan.next_state.batches[0].status, "flake");
    assert!(!plan.next_state.passed.contains_key(&7));
    assert!(!plan.next_state.failed.contains_key(&7));
    assert!(plan.ready_prs.is_empty());
}

#[test]
fn test_batch_refs_moved_with_non_ascii_shas_does_not_panic() {
    let expected = BTreeMap::from([(1, "ééééééééé".to_string())]);
    let current = BTreeMap::from([(1, "🦀🦀🦀🦀🦀🦀🦀🦀🦀".to_string())]);
    let reason = super::verify::batch_refs_moved("mainé€€€€€€€", "x", &expected, &current)
        .expect("refs moved");
    assert!(reason.contains("PR #1"), "{reason}");
}

// ============================================================================
// Brutal Cross-Concern Torture, Pluggable Trait, & Property Tests
// ============================================================================

#[test]
fn test_pluggable_traits_and_project_policy_unification() {
    use super::plugin::{
        ensure_no_shell_argv, CliForgePlugin, CodeHostPlugin, ConfigNotifier, DefaultProvisioner,
        DefaultWatcherPlugin, DeployGateInput, ForgeCheckObs, ForgeMergeableState, GateDecision,
        NotifierAction, NotifierPlugin, ProjectPolicy, ProviderPlugin, ProvisionerPlugin,
        ScrubbedNotifier, WatcherPlugin, WorkflowGateInput,
    };

    let policy = ProjectPolicy::default();
    assert!(policy.validate().is_ok());
    assert!(!policy.area_globs().is_empty());
    let (req_ci, main_ci) = policy.ci_job_rules();
    assert!(!req_ci.is_empty());
    assert!(!main_ci.is_empty());
    let (locks, gens, codegen) = policy.conflict_generator_rules();
    assert!(!locks.is_empty());
    assert!(!gens.is_empty());
    assert!(!codegen.is_empty());
    let (ui_roots, shot_dirs, pixel_files) = policy.screenshot_scope();
    assert!(!ui_roots.is_empty());
    assert!(!shot_dirs.is_empty());
    assert!(!pixel_files.is_empty());
    assert!(!policy.burner_names().is_empty());
    let quotas = policy.quotas();
    assert_eq!(quotas.agent_slots, policy.dispatch.slots);

    // 1. ProviderPlugin: argv template expansion without shell
    let claude = policy.providers.get("claude").expect("claude spec");
    let p_plugin: &dyn ProviderPlugin = claude;
    assert_eq!(p_plugin.name(), "claude");
    let start_cmd = p_plugin
        .start(
            "t1",
            "/work/t1",
            "fix $(rm -rf /); `id`",
            Some("opus"),
            Some("high"),
        )
        .expect("start argv");
    assert!(ensure_no_shell_argv(&start_cmd.program, &start_cmd.args).is_ok());
    assert!(start_cmd.args.iter().any(|a| a.contains("$(rm -rf /)")));
    assert!(ensure_no_shell_argv("sh", &["-c".to_string(), "echo".to_string()]).is_err());
    assert!(ensure_no_shell_argv("", &[]).is_err());
    let resume_cmd = p_plugin
        .resume(
            "t1",
            "/work/t1",
            Some("sess-42"),
            "continue",
            Some("opus"),
            Some("high"),
        )
        .expect("resume argv");
    assert!(resume_cmd.args.iter().any(|a| a.contains("sess-42")));
    let live_pids = BTreeSet::from([1234u32]);
    assert!(p_plugin.alive(Some(1234), &live_pids));
    assert!(!p_plugin.alive(Some(9999), &live_pids));
    let stop_plan = p_plugin.stop("t1", "/work/t1", Some("sess-42"), Some(1234));
    assert_eq!(stop_plan.pid, Some(1234));
    assert!(
        p_plugin.cost(
            &[r#"{"usage":{"input_tokens":100000,"output_tokens":50000}}"#],
            Some("opus")
        ) > 0.0
    );

    // 2. ForgePlugin / CodeHostPlugin: PR facts, mergeable state, checks, and scrubbed comments
    let forge = CliForgePlugin {
        name: "github".to_string(),
        cli_bin: "gh".to_string(),
        repo: Some("org/repo".to_string()),
        ..Default::default()
    };
    assert!(
        forge.merge_pr(1).is_err(),
        "PR merging must be off by default on CliForgePlugin"
    );
    let host: &dyn CodeHostPlugin = &forge;
    assert_eq!(host.forge_name(), "github");
    assert_eq!(
        host.resolve_mergeable("UNKNOWN", true),
        ForgeMergeableState::Conflicting
    );
    assert_eq!(
        host.resolve_mergeable("MERGEABLE", true),
        ForgeMergeableState::Mergeable
    );
    let secret = format!("ghp_{}", "A".repeat(36));
    let comment_argv = host.plan_issue_comment_cmd(99, &format!("token is {secret}"));
    assert!(!comment_argv.iter().any(|arg| arg.contains(&secret)));
    assert!(comment_argv.iter().any(|arg| arg.contains("<redacted>")));

    let checks = vec![
        ForgeCheckObs {
            name: "ci".to_string(),
            status: "completed".to_string(),
            conclusion: Some("success".to_string()),
            details_url: None,
        },
        ForgeCheckObs {
            name: "lint".to_string(),
            status: "in_progress".to_string(),
            conclusion: None,
            details_url: None,
        },
    ];
    let verdict = host.evaluate_checks(&checks, &["ci".to_string(), "e2e".to_string()]);
    assert!(!verdict.all_passed);
    assert_eq!(verdict.pending_jobs, vec!["lint"]);
    assert_eq!(verdict.missing_required, vec!["e2e"]);

    // 3. NotifierPlugin: owner issue, comment, and webhook sink scrubbed by default
    let notifier = ScrubbedNotifier::new(ConfigNotifier::default());
    let sk_secret = format!("sk-ant-api03-{}", "x".repeat(32));
    let NotifierAction::Sink {
        subject,
        body,
        payload,
        ..
    } = notifier.notify_sink(
        &format!("Alert {sk_secret}"),
        "DATABASE_URL=postgres://user:supersecretpw@db.internal:5432/prod",
        &serde_json::json!({
            "nested": {
                "auth": format!("Authorization: Bearer {sk_secret}"),
                "continued": format!("API_KEY=\\\n{sk_secret}")
            }
        }),
    )
    else {
        panic!("expected Sink action");
    };
    assert!(!subject.contains(&sk_secret));
    assert!(!body.contains("supersecretpw"));
    let payload_str = serde_json::to_string(&payload).unwrap();
    assert!(!payload_str.contains(&sk_secret));
    assert!(payload_str.contains("<redacted>"));

    // 4. ProvisionerPlugin: port block, DB recipe, deps cache, .tachoenv
    let provisioner = DefaultProvisioner::for_fleet_home("fleet-home");
    let ports0 = provisioner.allocate_port_block(0).unwrap();
    let ports1 = provisioner.allocate_port_block(1).unwrap();
    assert_ne!(ports0.http_port, ports1.http_port);
    let db = provisioner.db_recipe("wt-alpha", &ports0);
    assert_eq!(db.host_port, ports0.db_port);
    let env_text = provisioner
        .render_tachoenv("wt-alpha", 0, "", &BTreeMap::new())
        .unwrap();
    assert!(env_text.contains("TACHO_PORT_BASE="));
    assert!(env_text.contains("CARGO_TARGET_DIR='fleet-home/cache/warm/slot-0/target'"));
    assert!(env_text.contains("DATABASE_URL='postgres://"));

    // 5. WatcherPlugin: custom workflow and deploy gates
    let watcher = DefaultWatcherPlugin::default();
    assert_eq!(
        watcher.evaluate_workflow_gate(&WorkflowGateInput {
            workflow_name: "ci".to_string(),
            branch: "main".to_string(),
            status: "completed".to_string(),
            conclusion: Some("success".to_string()),
            failed_jobs: Vec::new(),
        }),
        GateDecision::Allow
    );
    assert!(matches!(
        watcher.evaluate_deploy_gate(&DeployGateInput {
            environment: "prod".to_string(),
            head_sha: "abc12345".to_string(),
            main_ci_green: true,
            active_incidents: vec!["INC-1".to_string()],
            freeze_active: false,
        }),
        GateDecision::Hold { .. }
    ));
}

#[test]
fn test_multi_tick_adversarial_simulation_across_all_concerns() {
    use super::plugin::ProjectPolicy;
    use super::verify::PassedRecord;
    use super::watch::{
        BehindCommit, ContainerCandidate, DeployRunObs, ProgressSample, TidySnapshot,
        WorktreeCandidate,
    };
    use super::{
        decide, decide_with_policy, BatchOutcomeObs, BatchResolution, BurnerProcess, DecisionState,
        WorldObservation,
    };

    let make_todo = |key: &str, area: &str, priority: i32, after: &[&str]| TodoItem {
        key: key.to_string(),
        brief: format!("implement {key}"),
        area: area.to_string(),
        priority,
        after: after.iter().map(|s| (*s).to_string()).collect(),
        ..Default::default()
    };

    let policy = ProjectPolicy::default();
    let mut state = DecisionState {
        max_parallel: 3,
        todo: vec![
            make_todo("auth-1", "api", 10, &[]),
            make_todo("auth-2", "api", 5, &["auth-1"]),
            make_todo("ui-1", "web", 8, &[]),
        ],
        ..Default::default()
    };
    state
        .inbox
        .insert("live-worker".to_string(), vec!["check logs".to_string()]);
    state.agents.insert(
        "live-worker".to_string(),
        AgentRun {
            key: "live-worker".to_string(),
            area: "ops".to_string(),
            started_at: 100,
            ..Default::default()
        },
    );

    // Tick 1: dispatch tasks across areas, sample stalled agent, kill runaway burner, deliver inbox tell.
    let obs1 = WorldObservation {
        now: 1_000,
        main_sha: "main-001".to_string(),
        live_keys: BTreeSet::from(["live-worker".to_string()]),
        agent_samples: BTreeMap::from([(
            "live-worker".to_string(),
            ProgressSample {
                ts: 1_000,
                cpu_ticks: 50,
                log_bytes: 1024,
                head_sha: "c1".to_string(),
                dirty_files: 1,
                ..Default::default()
            },
        )]),
        burners: vec![
            BurnerProcess {
                pid: 9001,
                comm: "stress-ng".to_string(),
                elapsed_secs: 750,
                starttime: Some(11),
            },
            BurnerProcess {
                pid: 9002,
                comm: "stress-ng".to_string(),
                elapsed_secs: 30,
                starttime: Some(12),
            },
        ],
        ..Default::default()
    };

    let plan1 = decide_with_policy(&state, &obs1, &policy);
    let plan1_replay = decide_with_policy(&state, &obs1, &policy);
    assert_eq!(plan1, plan1_replay);
    assert_eq!(plan1.burner_kills.len(), 1);
    assert_eq!(plan1.burner_kills[0].pid, 9001);
    assert_eq!(
        plan1.tell_actions,
        vec![("live-worker".to_string(), "check logs".to_string())]
    );
    // Both auth-1 (area api) and ui-1 (area web) dispatch; auth-2 waits on auth-1.
    let dispatched_keys: Vec<&str> = plan1
        .dispatched_tasks
        .iter()
        .map(|t| t.key.as_str())
        .collect();
    assert_eq!(dispatched_keys, vec!["auth-1", "ui-1"]);

    // Tick 2: auth-1 and ui-1 open PRs #101 and #102; batch b-1 runs on [101, 102] and fails with an infra flake!
    let mut state2 = plan1.next_state.clone();
    let mut b1 = running_batch("b-1", vec![101, 102]);
    b1.main_sha = "main-001".to_string();
    b1.shas = BTreeMap::from([(101, "sha-101".to_string()), (102, "sha-102".to_string())]);
    state2.batches.push(b1);

    let mut pr101 = open_pr(101);
    pr101.head = "sha-101".to_string();
    pr101.files = vec!["crates/api/src/lib.rs".to_string()];
    let mut pr102 = open_pr(102);
    pr102.head = "sha-102".to_string();
    pr102.files = vec!["crates/web/src/lib.rs".to_string()];

    let obs2 = WorldObservation {
        now: 1_100,
        main_sha: "main-001".to_string(),
        open_prs: vec![pr101.clone(), pr102.clone()],
        batch_outcomes: vec![BatchOutcomeObs {
            batch_id: "b-1".to_string(),
            passed: false,
            failed_area: Some("api".to_string()),
            log_output: "docker: Error response from daemon: TLS handshake timeout".to_string(),
            tree_sha: "tree-b1".to_string(),
        }],
        ..Default::default()
    };

    let plan2 = decide(&state2, &obs2);
    assert_eq!(plan2, decide(&state2, &obs2));
    // Flake batch must be marked "flake", never pass PRs 101/102, and never enter ready_prs!
    assert!(matches!(
        plan2.batch_resolutions.as_slice(),
        [BatchResolution::InfraRetry { .. }]
    ));
    assert_eq!(plan2.next_state.batches[0].status, "flake");
    assert!(!plan2.next_state.batches[0].flakes.is_empty());
    assert!(plan2.ready_prs.is_empty());
    assert!(!plan2.next_state.passed.contains_key(&101));

    // Tick 3: even if a bogus caller tries to inject a PassedRecord pointing at the flaky batch b-1,
    // `ready_prs` / `still_ready` / `make_train` MUST reject it because `!b.flakes.is_empty()`!
    let mut state3 = plan2.next_state.clone();
    state3.passed.insert(
        101,
        PassedRecord {
            pr: 101,
            sha: "sha-101".to_string(),
            main_sha: "main-001".to_string(),
            batch_id: "b-1".to_string(),
            passed_at: 1_100,
            tree: "tree-b1".to_string(),
            areas: vec!["api".to_string()],
        },
    );
    let obs3 = WorldObservation {
        now: 1_200,
        main_sha: "main-001".to_string(),
        open_prs: vec![pr101.clone(), pr102.clone()],
        ..Default::default()
    };
    let plan3 = decide(&state3, &obs3);
    assert!(
        plan3.ready_prs.is_empty(),
        "Flaky batch b-1 must NEVER be reused as passing verification for ready_prs"
    );
    assert!(
        super::land::make_train(&[], &state3.batches[0], "main-001", "tree-b1", 1_200).is_err(),
        "make_train must reject a batch with recorded flakes"
    );

    // Tick 4: fresh batch b-2 fails with a proptest counterexample + ECONNREFUSED noise in the same log.
    // Proptest counterexample MUST be classified as "code" (never flake), triggering Bisect on [101, 102]!
    let mut state4 = plan3.next_state.clone();
    state4.passed.clear();
    let mut b2 = running_batch("b-2", vec![101, 102]);
    b2.main_sha = "main-001".to_string();
    b2.shas = BTreeMap::from([(101, "sha-101".to_string()), (102, "sha-102".to_string())]);
    state4.batches.push(b2);

    let obs4 = WorldObservation {
        now: 1_300,
        main_sha: "main-001".to_string(),
        open_prs: vec![pr101.clone(), pr102.clone()],
        batch_outcomes: vec![BatchOutcomeObs {
            batch_id: "b-2".to_string(),
            passed: false,
            failed_area: Some("api".to_string()),
            log_output: " ECONNREFUSED 127.0.0.1:5432\nminimal failing input: x = -1\n".to_string(),
            tree_sha: "tree-b2".to_string(),
        }],
        failed_main_jobs: BTreeMap::from([(
            "nightly".to_string(),
            format!("failed with token ghp_{}", "Z".repeat(36)),
        )]),
        tidy_snapshot: TidySnapshot {
            now: 1_300,
            worktrees: vec![WorktreeCandidate {
                path: "/work/old-wt".to_string(),
                branch: "feat/old".to_string(),
                age_secs: 7_200,
                dirty: true,
                unpushed_commits: 1,
                pr_merged_or_closed: true,
            }],
            checkouts: Vec::new(),
            containers: vec![ContainerCandidate {
                id: "c-dead".to_string(),
                name: "tacho-pg-dead".to_string(),
                age_secs: 9_000,
                ports: vec![15432],
            }],
            networks: vec!["tacho-net-orphan".to_string()],
            live_agent_worktrees: BTreeSet::new(),
            live_batch_worktrees: BTreeSet::new(),
            held_paths: BTreeSet::new(),
            held_ports: BTreeSet::new(),
        },
        behind_commits: vec![BehindCommit {
            sha: "green-main-999".to_string(),
            ts: 100,
            ci_green: true,
        }],
        detached_checkout: true,
        ..Default::default()
    };

    let plan4 = decide(&state4, &obs4);
    assert_eq!(plan4, decide(&state4, &obs4));
    assert!(matches!(
        plan4.batch_resolutions.as_slice(),
        [BatchResolution::Bisect {
            left_prs,
            right_prs,
            ..
        }] if left_prs == &vec![101] && right_prs == &vec![102]
    ));
    assert!(!plan4.tidy_decisions.is_empty());
    assert_eq!(
        plan4.self_update.target_sha.as_deref(),
        Some("green-main-999")
    );
    // Verify owner notifications scrubbed the ghp_ secret from failed_main_jobs!
    for notif in &plan4.owner_notifications {
        assert!(!notif.subject.contains("ghp_"));
        assert!(!notif.body.contains("ghp_"));
        assert!(!notif.because.contains("ghp_"));
    }

    // Tick 5: batch b-3 on [101] cleanly passes with 0 flakes -> PR #101 becomes ready!
    let mut state5 = plan4.next_state.clone();
    let mut b3 = running_batch("b-3", vec![101]);
    b3.main_sha = "main-001".to_string();
    b3.shas = BTreeMap::from([(101, "sha-101".to_string())]);
    b3.areas = vec!["core".to_string()];
    state5.batches.push(b3);

    let obs5 = WorldObservation {
        now: 5_500,
        main_sha: "main-001".to_string(),
        open_prs: vec![pr101],
        live_keys: BTreeSet::from(["live-worker".to_string()]),
        batch_outcomes: vec![BatchOutcomeObs {
            batch_id: "b-3".to_string(),
            passed: true,
            failed_area: None,
            log_output: "all checks green".to_string(),
            tree_sha: "tree-b3".to_string(),
        }],
        deploy_runs: vec![DeployRunObs {
            run_id: 555,
            head_sha: "main-001".to_string(),
            status: "in_progress".to_string(),
            on_main: true,
            gate_conclusion: Some("success".to_string()),
        }],
        ..Default::default()
    };

    let plan5 = decide(&state5, &obs5);
    assert_eq!(plan5, decide(&state5, &obs5));
    assert_eq!(plan5.ready_prs.len(), 1);
    assert_eq!(plan5.ready_prs[0].pr, 101);
    assert!(plan5.watchdog_alert.is_some());
}

mod torture_proptests {
    use super::*;
    use crate::decide::land;
    use crate::decide::ops::render::{html, issue_text};
    use crate::decide::ops::snapshot::{redact_json, snapshot, AttentionRules, SnapshotObs};
    use crate::decide::ops::OpsConfig;
    use crate::decide::verify::{classify, counterexample, reusable_batch, BatchRun, PassedRecord};
    use crate::decide::{decide, BatchOutcomeObs, BurnerProcess, DecisionState, WorldObservation};
    use proptest::prelude::*;
    use std::collections::HashMap;

    fn arb_pr(num: u64) -> impl Strategy<Value = PullFacts> {
        (
            prop_oneof![Just("OPEN".to_string()), Just("CLOSED".to_string())],
            prop_oneof![
                Just("MERGEABLE".to_string()),
                Just("CONFLICTING".to_string()),
                Just("UNKNOWN".to_string()),
            ],
            "[a-f0-9]{8}",
            prop::bool::ANY,
        )
            .prop_map(move |(state, mergeable, head, draft)| PullFacts {
                number: num,
                title: format!("PR {num}"),
                branch: format!("work/task-{num}"),
                base: "main".to_string(),
                head,
                state,
                mergeable,
                draft,
                files: vec![format!("crates/area{}/lib.rs", num % 3)],
                tree: format!("tree-{num}"),
                additions: 10,
                deletions: 2,
                labels: Vec::new(),
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// 1. Deterministic replay: re-running `decide(state, obs)` on any generated
        /// `(DecisionState, WorldObservation)` across multiple ticks produces bit-for-bit
        /// identical `DecisionPlan` structs and canonical JSON serializations.
        #[test]
        fn prop_decide_is_bit_for_bit_deterministic_across_multi_tick_trajectories(
            max_parallel in 1usize..6,
            now_base in 1_000i64..50_000,
            prs in prop::collection::vec(arb_pr(1), 0..4),
            batch_passed in prop::bool::ANY,
            has_flake_in_batch in prop::bool::ANY,
            elapsed_burner in 0i64..1_200,
        ) {
            let mut open_prs = prs;
            for (idx, pr) in open_prs.iter_mut().enumerate() {
                pr.number = (idx as u64) + 1;
                pr.branch = format!("work/task-{}", pr.number);
            }

            let mut shas = BTreeMap::new();
            let mut pr_nums = Vec::new();
            for pr in &open_prs {
                shas.insert(pr.number, pr.head.clone());
                pr_nums.push(pr.number);
            }

            let mut batch = running_batch("b-sim", pr_nums.clone());
            batch.main_sha = "main-sha".to_string();
            batch.shas = shas;
            if has_flake_in_batch {
                batch.flakes = vec!["flaky_test_case".to_string()];
            }

            let state = DecisionState {
                max_parallel,
                todo: vec![
                    TodoItem {
                        key: "task-1".to_string(),
                        brief: "do task 1".to_string(),
                        area: "a0".to_string(),
                        priority: 5,
                        ..Default::default()
                    },
                    TodoItem {
                        key: "task-2".to_string(),
                        brief: "do task 2".to_string(),
                        area: "a1".to_string(),
                        priority: 3,
                        after: vec!["task-1".to_string()],
                        ..Default::default()
                    },
                ],
                batches: if pr_nums.is_empty() { Vec::new() } else { vec![batch] },
                ..Default::default()
            };

            let obs = WorldObservation {
                now: now_base,
                main_sha: "main-sha".to_string(),
                open_prs: open_prs.clone(),
                batch_outcomes: if pr_nums.is_empty() {
                    Vec::new()
                } else {
                    vec![BatchOutcomeObs {
                        batch_id: "b-sim".to_string(),
                        passed: batch_passed,
                        failed_area: Some("a0".to_string()),
                        log_output: if batch_passed {
                            "ok".to_string()
                        } else {
                            "test api::tests::it_works ... FAILED".to_string()
                        },
                        tree_sha: "tree-sim".to_string(),
                    }]
                },
                burners: vec![BurnerProcess {
                    pid: 4242,
                    comm: "stress".to_string(),
                    elapsed_secs: elapsed_burner,
                    starttime: Some(99),
                }],
                ..Default::default()
            };

            let plan_a = decide(&state, &obs);
            let plan_b = decide(&state, &obs);
            prop_assert_eq!(&plan_a, &plan_b);
            prop_assert_eq!(
                serde_json::to_vec(&plan_a).unwrap(),
                serde_json::to_vec(&plan_b).unwrap()
            );

            // Second tick replay from next_state
            let obs_tick2 = WorldObservation {
                now: now_base + 700,
                main_sha: "main-sha".to_string(),
                open_prs,
                ..Default::default()
            };
            let plan2_a = decide(&plan_a.next_state, &obs_tick2);
            let plan2_b = decide(&plan_b.next_state, &obs_tick2);
            prop_assert_eq!(&plan2_a, &plan2_b);
            prop_assert_eq!(
                serde_json::to_vec(&plan2_a).unwrap(),
                serde_json::to_vec(&plan2_b).unwrap()
            );
        }

        /// 2. Flake batch & proptest counterexample invariants:
        /// - Any batch with `status != "passed" || !flakes.is_empty()` is NEVER reused as
        ///   passing verification for `still_ready`, `queue`, `reusable_batch`, `make_train`, or `decide`.
        /// - Any log containing a proptest counterexample is NEVER classified as `"infrastructure"` (flake).
        #[test]
        fn prop_flake_batches_never_pass_and_counterexamples_never_flake(
            status in prop_oneof![
                Just("passed".to_string()),
                Just("flake".to_string()),
                Just("failed".to_string()),
                Just("running".to_string()),
                Just("head-moved".to_string()),
            ],
            flakes in prop::collection::vec("[a-z_]{3,12}", 0..4),
            ce_val in -10_000i64..10_000i64,
            infra_noise in prop_oneof![
                Just("ECONNREFUSED 127.0.0.1:5432"),
                Just("503 Service Unavailable"),
                Just("TLS handshake timeout"),
                Just("connection reset by peer"),
            ],
        ) {
            prop_assume!(status != "passed" || !flakes.is_empty());

            let mut pr = open_pr(10);
            pr.head = "sha-10".to_string();
            let shas = BTreeMap::from([(10u64, "sha-10".to_string())]);
            let batch = BatchRun {
                id: "b-flaky".to_string(),
                prs: vec![10],
                shas: shas.clone(),
                main_sha: "main-1".to_string(),
                status,
                started_at: 100,
                finished_at: Some(200),
                areas: vec!["core".to_string()],
                log: String::new(),
                tree: Some("tree-10".to_string()),
                culprit: None,
                skipped: Vec::new(),
                flakes,
                ..Default::default()
            };
            let rec = PassedRecord {
                pr: 10,
                sha: "sha-10".to_string(),
                main_sha: "main-1".to_string(),
                batch_id: "b-flaky".to_string(),
                passed_at: 200,
                tree: "tree-10".to_string(),
                areas: vec!["core".to_string()],
            };
            let holds = BTreeMap::new();
            let passed_map = BTreeMap::from([(10u64, rec.clone())]);

            prop_assert!(!land::still_ready(&pr, &rec, Some(&batch), &holds, "main-1", &[]));
            prop_assert!(
                land::queue(
                    std::slice::from_ref(&pr),
                    &passed_map,
                    std::slice::from_ref(&batch),
                    &holds,
                    "main-1",
                    &HashMap::new(),
                    &[]
                )
                .is_empty()
            );
            prop_assert!(
                reusable_batch(
                    std::slice::from_ref(&batch),
                    &[10],
                    &shas,
                    "main-1",
                    &["core".to_string()]
                )
                .is_none()
            );
            prop_assert!(land::make_train(&[], &batch, "main-1", "tree-10", 300).is_err());

            // Counterexample invariant: even when surrounded by infra errors, proptest counterexamples are ALWAYS "code".
            let mixed_log = format!("{infra_noise}\nminimal failing input: x = {ce_val}\n{infra_noise}");
            prop_assert!(counterexample(&mixed_log).is_some());
            prop_assert_eq!(classify(&mixed_log), "code");
        }

        /// 3. Scrubber default-on invariants across owner notifications, snapshot, render,
        /// line continuations (`\\\n`), and nested JSON structures.
        #[test]
        fn prop_scrubber_redacts_line_continuations_nested_json_and_all_outputs(
            suffix in "[A-Za-z0-9]{36}",
            prefix_word in "[a-z]{3,8}",
        ) {
            let ghp_token = format!("ghp_{suffix}");
            let sk_token = format!("sk-ant-api03-{suffix}");
            // Secret split across a backslash-newline continuation:
            let split_secret = format!("OPENAI_API_KEY=\\\n{sk_token}");
            let bearer_secret = format!("Authorization: Bearer {ghp_token}");

            let scrubber = crate::scrub::Scrubber::default();
            let scrubbed_split = scrubber.scrub(&split_secret);
            prop_assert!(!scrubbed_split.contains(&sk_token));
            prop_assert!(!scrubbed_split.contains(&suffix));

            // Nested JSON redaction:
            let nested = serde_json::json!({
                "outer": {
                    "list": [
                        {"env": split_secret.clone()},
                        {"hdr": bearer_secret},
                        {"normal": prefix_word.clone()}
                    ]
                }
            });
            let nested_redacted = redact_json(nested, &[]);
            let nested_str = serde_json::to_string(&nested_redacted).unwrap();
            prop_assert!(!nested_str.contains(&ghp_token));
            prop_assert!(!nested_str.contains(&sk_token));
            prop_assert!(nested_str.contains(&prefix_word));

            // Snapshot + HTML + Issue text renderers scrub by default even when default configs are used:
            let snap_obs = SnapshotObs {
                now: 5_000,
                flakes: vec![format!("leaked {ghp_token} and {split_secret}")],
                ..Default::default()
            };
            let snap = snapshot(&snap_obs, &AttentionRules::default(), &OpsConfig::default());
            let snap_json = serde_json::to_string(&snap).unwrap();
            let rendered_html = html(&snap, &[], "tacho");
            let rendered_issue = issue_text(&snap, "https://status.example.invalid", &[]);

            prop_assert!(!snap_json.contains(&ghp_token));
            prop_assert!(!snap_json.contains(&sk_token));
            prop_assert!(!rendered_html.contains(&ghp_token));
            prop_assert!(!rendered_html.contains(&sk_token));
            prop_assert!(!rendered_issue.contains(&ghp_token));
            prop_assert!(!rendered_issue.contains(&sk_token));
        }
    }
}

#[test]
fn test_decide_populates_tell_actions_and_worktree_removal() {
    use crate::decide::{decide, DecisionState, WorldObservation};

    let mut state = DecisionState::default();
    state.agents.insert(
        "live-stalled".to_string(),
        AgentRun {
            key: "live-stalled".to_string(),
            worktree: "/tmp/wt/live-stalled".to_string(),
            started_at: 100,
            ..Default::default()
        },
    );
    state.samples.insert(
        "live-stalled".to_string(),
        vec![
            ProgressSample {
                ts: 100,
                log_bytes: 0,
                head_sha: String::new(),
                dirty_files: 0,
                cpu_ticks: 0,
                ..Default::default()
            },
            ProgressSample {
                ts: 2_000,
                log_bytes: 0,
                head_sha: String::new(),
                dirty_files: 0,
                cpu_ticks: 1,
                ..Default::default()
            },
        ],
    );
    state.agents.insert(
        "retired-agent".to_string(),
        AgentRun {
            key: "retired-agent".to_string(),
            worktree: "/tmp/wt/retired-agent".to_string(),
            started_at: 100,
            finished_at: Some(200),
            exit_code: Some(0),
            ..Default::default()
        },
    );
    state.agents.insert(
        "merged-agent".to_string(),
        AgentRun {
            key: "merged-agent".to_string(),
            worktree: "/tmp/wt/merged-agent".to_string(),
            started_at: 100,
            finished_at: Some(200),
            exit_code: Some(0),
            pr: Some(77),
            pr_source: Some("stated".to_string()),
            ..Default::default()
        },
    );
    state.retired.insert("retired-agent".to_string());
    state.merged_prs.insert(77);
    state.inbox.insert(
        "live-stalled".to_string(),
        vec!["please focus on auth".to_string()],
    );

    let mut obs = WorldObservation {
        now: 3_000,
        main_sha: "1111111111111111111111111111111111111111".to_string(),
        ..Default::default()
    };
    obs.live_keys.insert("live-stalled".to_string());
    obs.agent_samples.insert(
        "live-stalled".to_string(),
        ProgressSample {
            ts: 3_000,
            log_bytes: 0,
            head_sha: String::new(),
            dirty_files: 0,
            cpu_ticks: 2,
            ..Default::default()
        },
    );

    let plan = decide(&state, &obs);

    assert!(plan
        .tell_actions
        .iter()
        .any(|(k, m)| k == "live-stalled" && m == "please focus on auth"));
    assert!(plan
        .tell_actions
        .iter()
        .any(|(k, m)| k == "live-stalled" && m.contains("Stall nudge")));
    assert!(!plan.next_state.inbox.contains_key("live-stalled"));

    assert!(plan.tidy_decisions.iter().any(|d| matches!(
        d,
        TidyDecision::RemoveTree { path, rescue: true, .. } if path == "/tmp/wt/retired-agent"
    )));
    assert!(plan.tidy_decisions.iter().any(|d| matches!(
        d,
        TidyDecision::RemoveTree { path, rescue: true, .. } if path == "/tmp/wt/merged-agent"
    )));
    assert!(!plan.tidy_decisions.iter().any(|d| matches!(
        d,
        TidyDecision::RemoveTree { path, .. } if path == "/tmp/wt/live-stalled"
    )));
}
