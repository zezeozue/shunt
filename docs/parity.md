# Fleet parity

The live reference is nzfleet. A matching pure rule does not establish parity
unless the CLI and daemon use that rule with equivalent observations. Keep the
existing fleet authoritative until a real train has been compared in shadow
mode and every required project hook is configured.

| Behaviour | nzfleet location | Shunt location | Test or remaining evidence |
| --- | --- | --- | --- |
| Progress at the hung/stalled thresholds | `progress.py:verdict` | `decide/watch/verdict.rs` | `watch/tests/progress.rs`; external comparison of 352 verdict/action scenarios |
| CPU activity includes busy descendants | `supervise.py:tree_cpu` | `exec/observe.rs:agent_cpu_ticks` | `test_agent_cpu_ticks_includes_busy_child_processes_and_prevents_false_stall` |
| First stall asks the worker for its blocker | `supervise.py:step`, `supervise_tick.py:watch_agents` | `decide/watch/supervise.rs`, `exec/mod.rs` | `case_31_first_stall_stops_group_and_schedules_immediate_resume_with_question`; complete intervention comparison remains required |
| Second stall escalates without killing | `supervise_tick.py:watch_agents` | `exec/mod.rs` | `test_ordinary_stall_escalation_notifies_owner_without_killing_worker` |
| Recovery clears the stale escalation | `supervise_tick.py` | `decide/watch/tick.rs` | `case_33_agent_recovery_clears_stalled_need` |
| Process identities fence signals | `supervise_tick.py:_stop` | `exec/identity.rs` | `verify_agent_pid_refuses_mismatched_starttime_and_dead_pids` |
| Duplicate batches keep the oldest live run | `supervise.py:duplicate_batches` | `decide/watch/procs.rs` | `case_47_duplicate_batches_keeps_oldest_alive_and_returns_newer_duplicates` |
| Only owned runaway burners are stopped | `supervise.py:burners` | `exec/observe.rs`, `exec/identity.rs` | `case_41_unrelated_host_burner_outside_agent_trees_work_root_and_cgroups_is_ignored`; executor attribution is also required |
| Worker slots can explicitly be zero | `dispatch.py`, `plan.py` | `decide/mod.rs` | `zero_worker_slots_never_dispatch_queued_work` |
| Invalid persisted state preserves limits and holds | state reader | `fleet.rs` | `corrupt_state_is_refused_without_reopening_worker_slots`, `a_mutation_never_replaces_corrupt_state_or_runs_its_action` |
| A tick preserves concurrent results and journals its state atomically | state transaction | `fleet.rs:commit_tick`, `exec/mod.rs` | `a_tick_cannot_overwrite_a_batch_that_finished_during_observation`, `a_rejected_tick_journal_write_preserves_the_observed_fleet_state` |
| Current proof identity gates readiness | `plan.py:still_ready` | `decide/land/legacy.rs`, `fleet/dispatch.rs` | `next_recommends_only_a_current_complete_batch_proof` |
| Batch checks use the proposed tree | `premerge.py`, `trainrun.py` | `fleet/batch/checkout.rs` | `a_batch_checks_the_proposed_tree_and_preserves_the_main_checkout`, `a_green_main_checkout_cannot_certify_a_broken_proposed_tree`, `a_check_cannot_certify_a_tree_it_replaces_after_running` |
| Conflicting drafts need rebasing | next-step planner | `fleet/dispatch.rs` | `next_rebases_a_conflicting_draft_before_verification` |
| Train notifications require member proofs | `trainrun.py:mail_released_trains` | `decide/mod.rs` | `an_observed_train_never_mails_without_current_member_proofs` |
| Failed notification channels remain pending | `mail.py`, `owner_tick.py` | `exec/mod.rs` | `undelivered_notifications_remain_pending_until_a_channel_succeeds` |
| Notification values are scrubbed | `redact.py`, `mail.py` | `decide/plugin.rs`, `Scrubber` | Scrubber property tests; real project secret-pattern configuration still required |
| An already mailed ready subset stays quiet | `owner.py:is_new` | `decide/watch/legacy_notify.rs` | Missing whole-executor comparison: Shunt keys a ready notification by the entire ordered set |
| Daily digest is wired into the daemon | `owner_tick.py:digest` | `decide/watch/tick.rs:decide_owner_mail` | Missing executor parity: a pure digest helper test alone is insufficient |
| Runner trust is checked before work starts | `runner/`, runner trust scripts | Project runner hooks | Missing project configuration and real runner comparison |
| Autonomous worker dispatch | `tick.py`, `agents.py` | `exec/mod.rs`, `DaemonHands` | The CLI daemon installs a process launcher for both sampler and API ticks. `the_daemon_starts_a_queued_worker_in_its_allocated_worktree` starts a real process and checks its directory and subsequent trace persistence; a daemon dry run leaves the task queued. |
| Autonomous verification launches | `tick.py`, batch executor | `decide/mod.rs`, `exec/mod.rs` | Missing execution wiring: `batch_plans` are computed but the shared tick executor does not launch them; manual `tacho batch` is a separate path |
| Warm caches and private build state | `areas.py`, `runner/` | `decide/verify/ci_runner.rs`, `decide/watch/budget.rs` | Pure planner coverage; real project mounts, hooks and admission still need comparison |
| Root prune respects running work | `supervise_tick.py:watch_root_disk` | `exec/mod.rs`, configured prune helper | `test_root_disk_check_in_tick_warns_and_prunes_volumes_and_clears_on_recovery`; install and verify the project helper |
| Screenshot owners and golden lists | `shots.py` | `examples/nzela-shots.json`, `decide/land/shots.rs` | `the_project_example_preserves_screen_and_shared_code_ownership`; external comparison over 1,461 tracked PNGs in 40 source/golden scenarios |
| Fresh observation without execution | world adapters and pure plan | `exec/mod.rs`, `exec/observe.rs` | `dry_run_uses_fresh_forge_heads_without_persisting_or_executing`, `a_dry_run_refuses_failed_observations_without_recording_them` |
| Main CI uses the configured workflow | workflow configuration | `exec/observe.rs` | `forge_observation_uses_the_configured_workflow_file`; set `workflow_path` to the project's workflow file |
| Fresh pull observations preserve check selection | PR files from the forge | `decide/plugin.rs` | `an_open_pull_observation_preserves_the_files_that_select_its_checks`, `a_partial_forge_file_list_cannot_select_an_incomplete_proof` |

The screenshot example is project configuration. Copy it to `tacho-shots.json`
in the project worktree. The no-pixel filename pattern is currently built into
the screenshot implementation, so changes to that pattern need a separate
comparison with the reference.

A state import must preserve retry lineage, holds, batch membership and order,
proof trees, and current forge heads. An isolated seven-PR snapshot tests CLI
behaviour; it does not prove that the complete state has been migrated.
