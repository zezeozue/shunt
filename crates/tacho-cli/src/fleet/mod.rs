//! Fleet orchestrator, work queue, batch verification, merge trains, and machine care commands.
//!
//! All decisions are computed purely via `tacho_core::decide::{dispatch, verify, land, watch}`
//! and persisted with their `because` reason into `$TACHO_HOME/events.sqlite` via [`FleetStore`].
//!
//! Organized into focused submodules:
//! - [`store`]: persistent `FleetStore` and `FleetMeta` access, atomic file I/O, `/proc` PID
//!   starttime verification, git plumbing helpers, and `EnvironmentFile` loading with process-env
//!   precedence.
//! - [`dispatch`]: work queue (`todo`), holds (`hold`, `release`), retirement (`retire`),
//!   provider sessions (`session`), PR attachment (`attach`), process stop (`stop`), live
//!   operator guidance (`tell`), and priority next-step instruction (`next`).
//! - [`batch`]: speculative batch verification (`batch`), origin-aware retry/bisect (`Daemon`,
//!   `Manual`, `Train`), detached background mode, flake ledger (`flakes`), and area hang reporting.
//! - [`train`]: merge train assembly (`train`), candidate ordering, conflict settlement,
//!   compile-breaker bisection, `release-train` 4-state notice machine, and `queue`.
//! - [`land`]: landing verified merge trains and ready PRs (`land`) with base and tree verification.
//! - [`shape`]: PR branch shape/replay (`replay <pr>`) and decision/batch replay (`tick:<id>`, `batch-<id>`).
//! - [`watch`]: process tree sampling, progress verdicts (`progress`), orchestrator `tick`, and
//!   `watchdog` `OPEN`/`UPDATE`/`CLOSE` issue lifecycle.
//! - [`tidy`]: worktree/container janitor (`tidy`), rescue commits (`refs/heads/rescue/<key>-<ts>`),
//!   live-process `cwd`/`argv`/`fd` keep checks, and `--stale-runs DAYS`.
//! - [`ops`]: decision log query (`events` with `--since`/`-n`/`-f`), `top`, `serve`, `setup`,
//!   `version`, self-update with test & rollback, and owner `Notifier` with `Scrubber`.

pub mod batch;
pub mod dispatch;
pub mod land;
pub mod ops;
pub mod session;
pub mod shape;
pub mod store;
pub mod tidy;
pub mod train;
pub mod watch;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod readiness_tests;

pub use batch::{
    execute_batch, execute_batch_with, execute_flakes, has_proptest_counterexample,
    launch_detached_batch, select_batch_checks, BatchOptions, BatchOrigin,
};
pub use dispatch::{
    build_session_argv, compute_next_instruction, deliver_tell_action, detect_brief_source,
    execute_attach, execute_hold, execute_next, execute_release, execute_retire,
    execute_retire_with, execute_session, execute_session_with, execute_stop, execute_tell,
    execute_tell_with, execute_todo, execute_todo_with, format_retired_lines,
    infer_provider_from_model, spawn_worker_for_item, spawn_worker_in_dir, validate_retire_reason,
    ResolvedBrief, RetireOptions, SessionOptions, TodoOptions,
};
pub use land::{execute_land, execute_land_with, verify_train_land_integrity, LandOptions};
pub use ops::{
    apply_link_action, build_serve_payload, execute_events, execute_events_with, execute_serve,
    execute_setup, execute_top, execute_version, filter_decision_events, observe_link_state,
    parse_since_cutoff, run_self_update, run_setup_with, write_scrubbed_snapshot, EventsOptions,
    LinkAction, LinkState, Notifier, SelfUpdateOutcome, SetupReport,
};
pub use shape::{execute_replay, replay_decision, replay_pr_branch, PrReplayOutcome, ReplayTarget};
#[cfg(test)]
pub(crate) use store::verify_agent_pid;
pub use store::{
    emit_decision_event, fleet_meta_path, git_stdout, live_agent_keys, load_fleet_env,
    load_fleet_meta, mutate_fleet_meta, now_ns, now_secs, open_fleet_store, parse_systemd_env_file,
    pid_alive, pid_alive_matching, proc_pid_cgroup, proc_pid_starttime, record_batch_pid_identity,
    resolve_live_main_sha, resolve_live_pr_heads, save_fleet_meta, wait_for_pid_exit, write_atomic,
    BatchPidIdentity, CodeMoveRecord, FleetMeta, NoticeState, RetiredMeta, WatchdogIssueState,
};
pub use tidy::{
    apply_tidy_decisions, build_tidy_snapshot, execute_tidy, execute_tidy_with, rescue_worktree,
    retire_stale_runs, scan_live_process_holds, LiveProcessHolds, TidyOptions,
};
pub use train::{
    assemble_train, candidate_screens, classify_train_conflict, execute_queue,
    execute_release_train, execute_release_train_with, execute_shots, execute_shots_accept_in,
    execute_train, execute_train_with, is_frontend_path, order_train_candidates,
    persist_shot_report_to_run_dir, select_default_train_prs, transition_release_notice,
    ReleaseTrainOptions, TrainConflictResolution, TrainOptions,
};
pub use watch::{
    execute_progress, execute_tick, execute_tick_with, execute_watchdog, run_watchdog_lifecycle,
    scan_procs, WatchdogStep,
};
