use super::dispatch::compute_next_instruction;
use std::collections::{BTreeMap, BTreeSet};
use tacho_core::decide::dispatch::PullFacts;
use tacho_core::decide::verify::{AreaRule, BatchRun, PassedRecord};
use tacho_daemon::FleetState;

pub(super) fn verified_state() -> FleetState {
    FleetState {
        main_sha: Some("main".into()),
        open_prs_cache: vec![PullFacts {
            number: 20,
            head: "head".into(),
            branch: "change".into(),
            base: "main".into(),
            state: "OPEN".into(),
            mergeable: "MERGEABLE".into(),
            files: vec!["src/lib.rs".into()],
            ..Default::default()
        }],
        passed: BTreeMap::from([(
            20,
            PassedRecord {
                pr: 20,
                sha: "head".into(),
                main_sha: "main".into(),
                batch_id: "batch".into(),
                tree: "tree".into(),
                areas: vec!["core".into()],
                ..Default::default()
            },
        )]),
        batches: vec![BatchRun {
            id: "batch".into(),
            prs: vec![20],
            shas: BTreeMap::from([(20, "head".into())]),
            main_sha: "main".into(),
            tree: Some("tree".into()),
            areas: vec!["core".into()],
            status: "passed".into(),
            finished_at: Some(100),
            ..Default::default()
        }],
        area_rules: vec![AreaRule {
            area: "core".into(),
            prefixes: vec!["src/".into()],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn next_recommends_only_a_current_complete_batch_proof() {
    let valid = verified_state();
    assert_eq!(
        compute_next_instruction(&valid, &BTreeSet::new(), 100),
        "ask the owner to land PR #20"
    );
    type Mutation = (&'static str, fn(&mut FleetState));
    let mutations: &[Mutation] = &[
        ("head moved", |s| {
            for p in &mut s.open_prs_cache {
                p.head = "new".into();
            }
        }),
        ("main moved", |s| s.main_sha = Some("unrelated-main".into())),
        ("main unknown", |s| s.main_sha = None),
        ("draft", |s| {
            for p in &mut s.open_prs_cache {
                p.draft = true;
            }
        }),
        ("conflict", |s| {
            for p in &mut s.open_prs_cache {
                p.mergeable = "CONFLICTING".into();
            }
        }),
        ("closed", |s| {
            for p in &mut s.open_prs_cache {
                p.state = "CLOSED".into();
            }
        }),
        ("not observed", |s| s.open_prs_cache.clear()),
        ("files unknown", |s| {
            for p in &mut s.open_prs_cache {
                p.files.clear();
            }
        }),
        ("missing area", |s| {
            for p in s.passed.values_mut() {
                p.areas.clear();
            }
        }),
        ("batch missing", |s| s.batches.clear()),
        ("batch omitted PR", |s| {
            for b in &mut s.batches {
                b.prs.clear();
            }
        }),
        ("batch head differs", |s| {
            for b in &mut s.batches {
                b.shas.insert(20, "old".into());
            }
        }),
        ("batch tree differs", |s| {
            for b in &mut s.batches {
                b.tree = Some("other".into());
            }
        }),
        ("flaky batch", |s| {
            for b in &mut s.batches {
                b.flakes.push("case".into());
            }
        }),
    ];
    for (reason, mutate) in mutations {
        let mut state = valid.clone();
        mutate(&mut state);
        let instruction = compute_next_instruction(&state, &BTreeSet::new(), 100);
        assert!(
            !instruction.starts_with("ask the owner to land"),
            "{reason}: {instruction}"
        );
    }
}

#[test]
fn next_rebases_a_conflicting_draft_before_verification() {
    let mut state = verified_state();
    state.passed.clear();
    for pr in &mut state.open_prs_cache {
        pr.draft = true;
        pr.mergeable = "CONFLICTING".into();
    }
    assert!(compute_next_instruction(&state, &BTreeSet::new(), 100)
        .starts_with("tacho session rebase-20"));
}
