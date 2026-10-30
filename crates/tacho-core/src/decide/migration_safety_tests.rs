use super::*;

#[test]
fn project_defaults_do_not_reopen_an_explicit_zero_worker_limit() {
    let mut state = DecisionState {
        max_parallel: 0,
        ..Default::default()
    };
    ProjectPolicy::default().apply_to_state(&mut state);
    assert_eq!(state.max_parallel, 0);
}

#[test]
fn zero_worker_slots_never_dispatch_queued_work() {
    let state = DecisionState {
        max_parallel: 0,
        todo: vec![TodoItem {
            key: "queued-task".into(),
            brief: "A task held by admission".into(),
            area: "core".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let plan = decide(
        &state,
        &WorldObservation {
            now: 100,
            ..Default::default()
        },
    );
    assert!(plan.dispatched_tasks.is_empty());
    assert_eq!(plan.next_state.max_parallel, 0);
}

#[test]
fn an_observed_train_never_mails_without_current_member_proofs() {
    let observation = WorldObservation {
        now: 100,
        ready_trains: vec![(
            "train".into(),
            vec![7],
            BTreeMap::from([(7, "head".into())]),
        )],
        ..Default::default()
    };
    let plan = decide(&DecisionState::default(), &observation);
    assert!(!plan
        .owner_notifications
        .iter()
        .any(|notification| notification.kind == "ready-train"));
}
