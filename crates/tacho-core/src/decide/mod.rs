//! Pure decision engine for fleet orchestration, verification, landing, and supervision.
//!
//! Every decision in this module is a pure function of plain data structures:
//! no IO, no threads, no system clocks, and no mutable globals.
//! Every decision carries its explicit `because` reason so it can be recorded
//! into the SQLite WAL event log (`$TACHO_HOME/events.sqlite`) and replayed
//! deterministically.
//!
//! Organized by concern:
//! - [`dispatch`]: work queue selection, area serialization, prerequisites, holds,
//!   retirement, run lineage/resumption, PR attachment, rebase handoffs, and `--compare` expansion.
//! - [`verify`]: PR stack ordering, batch planning, prefix bisection, log failure classification
//!   (code vs. infrastructure), flake ledger, single-retry fix state machine, and proof preflight.
//! - [`land`]: ready queue, verified merge trains, combined train PR assembly, generator settling,
//!   and compile-breaker bisection.
//! - [`watch`]: agent & chain progress verdicts (`MOVING`, `STALLED`, `HUNG`), phase notes,
//!   supervision actions, CPU starvation planning, machine janitor (`tidy`), watchdog alerts,
//!   cost accounting, and human attention items.

pub mod dispatch;
pub mod land;
pub mod ops;
pub mod plugin;
pub mod verify;
pub mod watch;

pub use plugin::*;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use dispatch::{
    base_key, base_keys, find_pr, latest, needs_resume, next_hands, next_work, resume_task,
    AgentRun, Hold, PullFacts, TodoItem,
};
use land::{queue, ReadyItem};
use verify::{
    batch_refs_moved, batches, classify, counterexample, decide_fixes, failed_tests, flake_tasks,
    record_flakes, summary_line, AreaRule, BatchPlan, BatchRun, FailedRecord, FixDecisions,
    FlakeEntry, PassedRecord, MAX_BATCH_SIZE,
};
use watch::{
    decide_owner_notifications, decide_self_update, decide_tidy, down_alert, down_reason,
    needs_you, supervise_step, take_sample, verdict, AttentionItem, BehindCommit, DeployRunObs,
    OwnerNotification, ProgressSample, SelfUpdateDecision, SuperviseAction, TidyDecision,
    TidySnapshot, Verdict,
};

/// Complete pure fleet state input to [`decide`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DecisionState {
    pub max_parallel: usize,
    pub todo: Vec<TodoItem>,
    pub agents: BTreeMap<String, AgentRun>,
    pub retired: BTreeSet<String>,
    pub holds: BTreeMap<String, Hold>,
    pub passed: BTreeMap<u64, PassedRecord>,
    pub failed: BTreeMap<u64, FailedRecord>,
    pub merged_prs: BTreeSet<u64>,
    pub closed_prs: BTreeSet<u64>,
    pub batches: Vec<BatchRun>,
    pub flakes: BTreeMap<String, FlakeEntry>,
    pub samples: BTreeMap<String, Vec<ProgressSample>>,
    pub nudged: BTreeSet<String>,
    pub rebase_attempts: HashMap<u64, String>,
    #[serde(default)]
    pub fix_notified: HashMap<u64, String>,
    pub notified_keys: BTreeSet<String>,
    pub area_rules: Vec<AreaRule>,
    pub refused_self_update_sha: Option<String>,
    pub last_tick_ts: Option<i64>,
    pub last_watchdog_alert_ts: Option<i64>,
    #[serde(default)]
    pub tidy_min_age_secs: i64,
    #[serde(default)]
    pub inbox: BTreeMap<String, Vec<String>>,
}

/// Observation of a finished batch check on the current tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchOutcomeObs {
    pub batch_id: String,
    pub passed: bool,
    pub failed_area: Option<String>,
    pub log_output: String,
    pub tree_sha: String,
}

/// Snapshot of the external world passed into [`decide`].
///
/// Captures all side-effecting reads (clock, GitHub PRs, process liveness/CPU, batch logs,
/// deploy runs, git upstream commits, tidy snapshot) as plain data so any tick can be replayed deterministically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WorldObservation {
    pub now: i64,
    pub main_sha: String,
    pub open_prs: Vec<PullFacts>,
    pub merged_prs: BTreeSet<u64>,
    pub closed_prs: BTreeSet<u64>,
    pub live_keys: BTreeSet<String>,
    pub agent_samples: BTreeMap<String, ProgressSample>,
    pub agent_log_tails: BTreeMap<String, String>,
    pub batch_outcomes: Vec<BatchOutcomeObs>,
    pub deploy_runs: Vec<DeployRunObs>,
    pub behind_commits: Vec<BehindCommit>,
    pub detached_checkout: bool,
    #[serde(default)]
    pub failed_main_jobs: BTreeMap<String, String>,
    #[serde(default)]
    pub ready_trains: Vec<(String, Vec<u64>, BTreeMap<u64, String>)>,
    #[serde(default)]
    pub tidy_snapshot: TidySnapshot,
    #[serde(default)]
    pub burners: Vec<BurnerProcess>,
    /// Main's last completed CI run; `None` when unknown (main-broken batches are then kept
    /// until all their members are merged or closed).
    #[serde(default)]
    pub main_ci: Option<crate::decide::verify::MainCiObs>,
    /// Live workers whose directory resolves to the repository's main checkout (`key -> path`).
    #[serde(default)]
    pub main_checkout_workers: BTreeMap<String, String>,
    /// Root filesystem (`/`) `(total_bytes, avail_bytes)` capacity snapshot, when observed.
    #[serde(default)]
    pub root_disk_bytes: Option<(u64, u64)>,
}

/// An observed runaway burner process (e.g. stress, yes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BurnerProcess {
    pub pid: u32,
    pub comm: String,
    pub elapsed_secs: i64,
    #[serde(default)]
    pub starttime: Option<u64>,
}

/// Pure resolution of a completed batch run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchResolution {
    /// All PRs in the batch passed at their expected heads.
    Passed { batch_id: String, prs: Vec<u64> },
    /// One or more PRs (or `main`) moved their head SHA while the batch was running; results are discarded.
    HeadMoved { batch_id: String, reason: String },
    /// Infrastructure/flake failure; retry the batch without blaming the PRs.
    InfraRetry { batch_id: String, reason: String },
    /// Multi-PR code failure; bisect the batch.
    Bisect {
        batch_id: String,
        left_prs: Vec<u64>,
        right_prs: Vec<u64>,
    },
    /// Single-PR code failure; mark the PR as failed.
    FailedSingle {
        batch_id: String,
        pr: u64,
        reason: String,
    },
}

/// Complete deterministic plan produced by a single [`decide`] tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionPlan {
    pub reaped_agents: Vec<(String, Option<u64>, bool)>,
    pub supervision_actions: Vec<SuperviseAction>,
    pub resumed_tasks: Vec<TodoItem>,
    pub dispatched_tasks: Vec<TodoItem>,
    pub rebase_handoffs: Vec<(u64, String)>,
    pub barred_rebases: Vec<(u64, String)>,
    pub fix_decisions: FixDecisions,
    pub batch_resolutions: Vec<BatchResolution>,
    pub batch_plans: Vec<BatchPlan>,
    pub ready_prs: Vec<ReadyItem>,
    pub tidy_decisions: Vec<TidyDecision>,
    pub attention_items: Vec<AttentionItem>,
    pub owner_notifications: Vec<OwnerNotification>,
    pub self_update: SelfUpdateDecision,
    pub watchdog_alert: Option<String>,
    pub next_state: DecisionState,
    #[serde(default)]
    pub burner_kills: Vec<BurnerProcess>,
    #[serde(default)]
    pub tell_actions: Vec<(String, String)>,
    /// `main-broken` batches forgotten this tick, each with its reason.
    #[serde(default)]
    pub cleared_main_broken: Vec<crate::decide::verify::ClearedBatch>,
    /// Running batches that duplicate an older one (same group / PR set) and must be aborted.
    #[serde(default)]
    pub duplicate_batch_aborts: Vec<String>,
    /// Root-disk capacity verdict (`warn` / `prune_docker_volumes`) evaluated by [`ops::config::check_root_disk`].
    #[serde(default)]
    pub root_disk_action: Option<crate::decide::ops::config::RootDiskAction>,
}

/// Replayable pure decision function `(DecisionState, WorldObservation) -> DecisionPlan`.
///
/// Given the current fleet state and a world observation snapshot, computes every orchestration,
/// supervision, batch verification, landing, notification, and self-update decision without IO.
pub fn decide(state: &DecisionState, obs: &WorldObservation) -> DecisionPlan {
    let mut next = state.clone();
    // 1. Update merged and closed PR sets from world observation.
    for &m in &obs.merged_prs {
        next.merged_prs.insert(m);
        next.closed_prs.remove(&m);
    }
    for &c in &obs.closed_prs {
        if !next.merged_prs.contains(&c) {
            next.closed_prs.insert(c);
        }
    }

    let mut current_pr_shas = BTreeMap::new();
    for pr in &obs.open_prs {
        if !pr.head.is_empty() {
            current_pr_shas.insert(pr.number, pr.head.clone());
        }
    }

    // 2. Reap exited agents and attribute PRs (`find_pr`).
    let mut reaped_agents = Vec::new();
    let agent_keys: Vec<String> = next.agents.keys().cloned().collect();
    for k in agent_keys {
        let should_reap = next
            .agents
            .get(&k)
            .is_some_and(|a| a.finished_at.is_none() && !obs.live_keys.contains(&k));
        if should_reap {
            let started_at = next.agents[&k].started_at;
            let tail = obs
                .agent_log_tails
                .get(&k)
                .map(String::as_str)
                .unwrap_or("");
            let (found_pr, found_src) = find_pr(&k, started_at, &obs.open_prs, tail);
            let is_none = found_src.as_deref() == Some("none");
            if let Some(a) = next.agents.get_mut(&k) {
                a.finished_at = Some(obs.now);
                if a.exit_code.is_none() {
                    a.exit_code = Some(if found_pr.is_some() || is_none { 0 } else { 1 });
                }
                if found_pr.is_some() {
                    a.pr = found_pr;
                    a.pr_source = found_src;
                } else if is_none {
                    a.pr_none = true;
                    a.pr_source = Some("none".to_string());
                }
            }
            reaped_agents.push((k, found_pr, is_none));
        }
    }

    // 3. Sample progress and supervise live agents (`MOVING`, `STALLED`, `HUNG`), and refuse any
    // worker whose directory resolves to the main checkout.
    let mut supervision_actions = Vec::new();
    for (key, wt_path) in &obs.main_checkout_workers {
        let because = format!(
            "worker {key} directory {wt_path} resolves to the main checkout; stopping with a page"
        );
        supervision_actions.push(SuperviseAction::Escalate {
            key: key.clone(),
            because: because.clone(),
        });
        next.holds =
            crate::decide::dispatch::with_hold(&next.holds, base_key(key), &because, obs.now);
        if let Some(a) = next.agents.get_mut(key) {
            a.finished_at = Some(obs.now);
            a.exit_code = Some(143);
            a.status_note = "stopped: directory resolves to main checkout".to_string();
        }
    }
    for key in &obs.live_keys {
        if obs.main_checkout_workers.contains_key(key) {
            continue;
        }
        if let Some(sample) = obs.agent_samples.get(key) {
            let existing = next.samples.get(key).map(Vec::as_slice).unwrap_or(&[]);
            let updated = take_sample(existing, sample.clone(), 60);
            let (v, idle) = verdict(&updated, obs.now);
            let base = base_key(key);
            let was_nudged = next.nudged.contains(key) || next.nudged.contains(base);
            let step = supervise_step(key, v, idle, was_nudged);
            next.samples.insert(key.clone(), updated);
            if matches!(v, Verdict::Moving) {
                next.notified_keys.retain(|k| {
                    !k.starts_with(&format!("attention:stalled:{key}:"))
                        && !k.starts_with(&format!("attention:stalled:{base}:"))
                });
            }
            if matches!(step, SuperviseAction::Nudge { .. }) {
                next.nudged.insert(key.clone());
                next.nudged.insert(base.to_string());
            }
            if !matches!(step, SuperviseAction::Ok) {
                supervision_actions.push(step);
            }
        }
    }

    // 4. Resolve completed batch outcomes (including head-movement refusal and bisection).
    let mut batch_resolutions = Vec::new();
    let mut resolved_this_tick = BTreeSet::new();
    for outcome in &obs.batch_outcomes {
        let Some(b_idx) = next.batches.iter().position(|b| b.id == outcome.batch_id) else {
            continue;
        };
        // Outcomes are idempotent: callers may re-send finished batches (or the same one twice),
        // and re-applying would double-count flakes or re-bisect. A batch that finished at or
        // before the previous tick, or was already resolved in this tick, has been handled.
        let already_resolved = next.batches[b_idx]
            .finished_at
            .zip(state.last_tick_ts)
            .is_some_and(|(finished, last_tick)| finished <= last_tick);
        if already_resolved || !resolved_this_tick.insert(outcome.batch_id.clone()) {
            continue;
        }
        let batch_prs = next.batches[b_idx].prs.clone();
        let expected_shas = next.batches[b_idx].shas.clone();
        let batch_areas = next.batches[b_idx].areas.clone();
        let batch_main = next.batches[b_idx].main_sha.clone();

        if let Some(moved_reason) =
            batch_refs_moved(&batch_main, &obs.main_sha, &expected_shas, &current_pr_shas)
        {
            next.batches[b_idx].status = "head-moved".to_string();
            next.batches[b_idx].finished_at = Some(obs.now);
            next.batches[b_idx].log = moved_reason.clone();
            batch_resolutions.push(BatchResolution::HeadMoved {
                batch_id: outcome.batch_id.clone(),
                reason: moved_reason,
            });
            continue;
        }

        if outcome.passed && next.batches[b_idx].flakes.is_empty() {
            next.batches[b_idx].status = "passed".to_string();
            next.batches[b_idx].finished_at = Some(obs.now);
            next.batches[b_idx].tree = Some(outcome.tree_sha.clone());
            for &pr in &batch_prs {
                let sha = expected_shas
                    .get(&pr)
                    .cloned()
                    .unwrap_or_else(|| batch_main.clone());
                next.failed.remove(&pr);
                next.passed.insert(
                    pr,
                    PassedRecord {
                        pr,
                        sha,
                        main_sha: batch_main.clone(),
                        batch_id: outcome.batch_id.clone(),
                        passed_at: obs.now,
                        tree: outcome.tree_sha.clone(),
                        areas: batch_areas.clone(),
                    },
                );
            }
            batch_resolutions.push(BatchResolution::Passed {
                batch_id: outcome.batch_id.clone(),
                prs: batch_prs,
            });
        } else if outcome.passed {
            // A batch that recorded flakes is never accepted as a clean pass.
            let reason = format!("flake batch ({})", next.batches[b_idx].flakes.join(", "));
            next.batches[b_idx].status = "flake".to_string();
            next.batches[b_idx].finished_at = Some(obs.now);
            for &pr in &batch_prs {
                if next
                    .passed
                    .get(&pr)
                    .is_some_and(|rec| rec.batch_id == outcome.batch_id)
                {
                    next.passed.remove(&pr);
                }
            }
            batch_resolutions.push(BatchResolution::InfraRetry {
                batch_id: outcome.batch_id.clone(),
                reason,
            });
        } else {
            let kind = classify(&outcome.log_output).to_string();
            let sum = summary_line(&outcome.log_output);
            let f_tests = failed_tests(&outcome.log_output);
            let ce = counterexample(&outcome.log_output);
            next.batches[b_idx].status = "failed".to_string();
            next.batches[b_idx].finished_at = Some(obs.now);
            next.batches[b_idx].log = outcome.log_output.clone();

            if kind == "infrastructure" {
                // Never leave an infra failure as plain "failed": a `flake` batch proves nothing
                // and must not be reused as a pass or as a red verdict against its PRs.
                next.batches[b_idx].status = "flake".to_string();
                let area_hint = outcome
                    .failed_area
                    .as_deref()
                    .or_else(|| batch_areas.first().map(String::as_str))
                    .unwrap_or("");
                let flake_names = if f_tests.is_empty() {
                    vec![sum.clone()]
                } else {
                    f_tests
                };
                next.batches[b_idx].flakes = flake_names.clone();
                let updated = record_flakes(
                    &next.flakes,
                    &flake_names,
                    &outcome.batch_id,
                    area_hint,
                    &outcome.log_output,
                    obs.now,
                );
                let (with_tasks, new_items) =
                    flake_tasks(&updated, &next.todo, &next.agents, &next.retired, obs.now);
                next.flakes = with_tasks;
                next.todo.extend(new_items);
                batch_resolutions.push(BatchResolution::InfraRetry {
                    batch_id: outcome.batch_id.clone(),
                    reason: sum,
                });
            } else if batch_prs.len() > 1 {
                let mid = batch_prs.len() / 2;
                batch_resolutions.push(BatchResolution::Bisect {
                    batch_id: outcome.batch_id.clone(),
                    left_prs: batch_prs[..mid].to_vec(),
                    right_prs: batch_prs[mid..].to_vec(),
                });
            } else if let Some(&single_pr) = batch_prs.first() {
                let sha = expected_shas.get(&single_pr).cloned().unwrap_or_default();
                next.passed.remove(&single_pr);
                next.failed.insert(
                    single_pr,
                    FailedRecord {
                        pr: single_pr,
                        sha,
                        batch_id: outcome.batch_id.clone(),
                        failed_at: obs.now,
                        kind,
                        summary: sum.clone(),
                        failed_tests: f_tests,
                        counterexample: ce,
                        log: outcome.log_output.clone(),
                    },
                );
                batch_resolutions.push(BatchResolution::FailedSingle {
                    batch_id: outcome.batch_id.clone(),
                    pr: single_pr,
                    reason: sum,
                });
            }
        }
    }

    // 4b. Forget finished `main-broken` batches (`unbreak`): members all gone, or main green
    // again on a newer descendant; still-open members are re-batched below on the new main.
    let open_numbers: BTreeSet<u64> = obs
        .open_prs
        .iter()
        .map(|p| p.number)
        .filter(|n| !next.merged_prs.contains(n) && !next.closed_prs.contains(n))
        .collect();
    let cleared_main_broken =
        crate::decide::verify::unbreak::clear(&next.batches, &open_numbers, obs.main_ci.as_ref());
    next.batches.retain(|b| {
        !cleared_main_broken
            .iter()
            .any(|c| c.batch_id == b.id && b.status == "main-broken")
    });

    // 5. Queue automatic resume candidates (after backoff) and select next work up to free slots.
    let mut resumed_tasks = Vec::new();
    for root in base_keys(&next.agents, &next.retired) {
        if needs_resume(
            &next.agents,
            &next.todo,
            &next.retired,
            &next.holds,
            &obs.live_keys,
            &root,
            obs.now,
        ) {
            if let Some(last) = latest(&next.agents, &next.retired, &root).cloned() {
                let item = resume_task(&last, "", "", obs.now);
                if !next.todo.iter().any(|t| t.key == item.key) {
                    next.todo.push(item.clone());
                }
                resumed_tasks.push(item);
            }
        }
    }

    let passed_prs: BTreeSet<u64> = next.passed.keys().copied().collect();
    let failed_prs: BTreeSet<u64> = next.failed.keys().copied().collect();
    let free_slots = next.max_parallel.saturating_sub(obs.live_keys.len());
    let (dispatched_tasks, remaining_todo) = next_work(
        &next.todo,
        &next.agents,
        &next.retired,
        &next.holds,
        &obs.live_keys,
        &passed_prs,
        &failed_prs,
        &next.merged_prs,
        &next.closed_prs,
        free_slots,
    );
    next.todo = remaining_todo;

    // 6. Conflicting PR rebase handoffs (`next_hands`) and single-retry red PR fixes (`decide_fixes`).
    let remaining_slots = free_slots.saturating_sub(dispatched_tasks.len());
    let (to_rebase, barred) = next_hands(
        &obs.open_prs,
        &next.agents,
        &next.holds,
        &obs.live_keys,
        &next.rebase_attempts,
        remaining_slots,
    );
    let rebase_handoffs: Vec<(u64, String)> = to_rebase
        .into_iter()
        .map(|p| {
            next.rebase_attempts.insert(p.number, p.head.clone());
            (p.number, p.head.clone())
        })
        .collect();
    let barred_rebases: Vec<(u64, String)> = barred
        .into_iter()
        .map(|(p, reason)| (p.number, reason))
        .collect();

    let fix_slots = remaining_slots.saturating_sub(rebase_handoffs.len());
    let fix_decisions = decide_fixes(
        &next.failed,
        &obs.open_prs,
        &next.agents,
        &next.holds,
        &obs.live_keys,
        &next.fix_notified,
        fix_slots,
    );
    for (pr_num, _) in &fix_decisions.notify {
        if let Some(rec) = next.failed.get(pr_num) {
            next.fix_notified.insert(*pr_num, rec.sha.clone());
        }
    }

    // 7. Speculative verification batches (`batches`), ready queue (`queue`), and janitor (`decide_tidy`).
    let batch_plans = batches(
        &obs.open_prs,
        &next.holds,
        &next.passed,
        &next.failed,
        &next.batches,
        &next.area_rules,
        MAX_BATCH_SIZE,
    );
    let ready_prs = queue(
        &obs.open_prs,
        &next.passed,
        &next.batches,
        &next.holds,
        &obs.main_sha,
        &HashMap::new(),
        &next.area_rules,
    );
    let tidy_min_age = if next.tidy_min_age_secs > 0 {
        next.tidy_min_age_secs
    } else {
        3_600
    };
    let mut tidy_decisions = decide_tidy(&obs.tidy_snapshot, tidy_min_age);
    for a in next.agents.values() {
        if a.worktree.is_empty()
            || obs.live_keys.contains(&a.key)
            || obs.tidy_snapshot.live_agent_worktrees.contains(&a.worktree)
            || obs.tidy_snapshot.live_batch_worktrees.contains(&a.worktree)
            || obs.tidy_snapshot.held_paths.contains(&a.worktree)
            || next
                .agents
                .values()
                .any(|other| obs.live_keys.contains(&other.key) && other.worktree == a.worktree)
        {
            continue;
        }
        let reason = if next.retired.contains(&a.key) || next.retired.contains(base_key(&a.key)) {
            Some(format!("retired agent {}", a.key))
        } else if let Some(pr) = a.pr {
            if next.merged_prs.contains(&pr) {
                Some(format!("merged PR #{pr} for {}", a.key))
            } else if next.closed_prs.contains(&pr) {
                Some(format!("closed PR #{pr} for {}", a.key))
            } else {
                None
            }
        } else {
            None
        };
        if let Some(because) = reason {
            let already_queued = tidy_decisions
                .iter()
                .any(|d| matches!(d, TidyDecision::RemoveTree { path, .. } if path == &a.worktree));
            if !already_queued {
                let rescue = obs
                    .tidy_snapshot
                    .worktrees
                    .iter()
                    .find(|w| w.path == a.worktree)
                    .map(|w| w.dirty || w.unpushed_commits > 0)
                    .unwrap_or(true);
                tidy_decisions.push(TidyDecision::RemoveTree {
                    path: a.worktree.clone(),
                    rescue,
                    because,
                });
            }
        }
    }

    // 8. Human attention items (`needs_you`), tell-once owner notifications, self-update, and watchdog.
    let mut attention_items = needs_you(
        &next.agents,
        &next.todo,
        &next.retired,
        &next.holds,
        &obs.live_keys,
        &obs.open_prs,
        &next.passed,
        &next.failed,
        obs.now,
    );
    for (pr_num, reason) in &barred_rebases {
        attention_items.push(AttentionItem {
            kind: "rebase-conflict".to_string(),
            target: format!("pr:{pr_num}"),
            summary: reason.clone(),
            command_hint: format!("tacho hand {pr_num}"),
        });
    }
    for (key, wt_path) in &obs.main_checkout_workers {
        attention_items.push(AttentionItem {
            kind: "main-checkout-worker".to_string(),
            target: key.clone(),
            summary: format!(
                "worker {key} was started with the main checkout ({wt_path}) as its directory and was stopped"
            ),
            command_hint: format!("tacho start {}", base_key(key)),
        });
    }
    for act in &supervision_actions {
        if let SuperviseAction::Escalate { key, because } = act {
            if !obs.main_checkout_workers.contains_key(key) {
                attention_items.push(AttentionItem {
                    kind: "stalled".to_string(),
                    target: key.clone(),
                    summary: because.clone(),
                    command_hint: format!("tacho tell {key} \"...\""),
                });
            }
        }
    }
    let root_disk_action = obs
        .root_disk_bytes
        .map(|(total, avail)| crate::decide::ops::config::check_root_disk(total, avail));
    if let Some(ref disk) = root_disk_action {
        if disk.warn {
            attention_items.push(AttentionItem {
                kind: "root-disk".to_string(),
                target: "root-disk".to_string(),
                summary: format!(
                    "The root disk is filling: {}% used, {} GiB free",
                    disk.used_percent, disk.avail_gib
                ),
                command_hint: "tacho tidy".to_string(),
            });
        } else {
            next.notified_keys
                .retain(|k| !k.starts_with("attention:root-disk:"));
        }
    }
    let self_update = decide_self_update(
        obs.detached_checkout,
        &obs.behind_commits,
        next.refused_self_update_sha.as_deref(),
        obs.now,
    );
    if let Some(ref att) = self_update.attention {
        attention_items.push(att.clone());
    }

    let mut failed_main_jobs = obs.failed_main_jobs.clone();
    for b in &next.batches {
        if b.status == "main-broken" {
            failed_main_jobs
                .entry(format!("batch-{}", b.id))
                .or_insert_with(|| b.log.lines().next().unwrap_or("main broken").to_string());
        }
    }

    let verified_trains: Vec<_> = obs
        .ready_trains
        .iter()
        .filter(|(_, order, heads)| {
            !order.is_empty()
                && order.iter().all(|pr| {
                    ready_prs
                        .iter()
                        .any(|item| item.pr == *pr && heads.get(pr) == Some(&item.sha))
                })
        })
        .cloned()
        .collect();
    let owner_notifications = decide_owner_notifications(
        &attention_items,
        &next.batches,
        &verified_trains,
        &failed_main_jobs,
        &obs.deploy_runs,
        &next.notified_keys,
    );
    for n in &owner_notifications {
        next.notified_keys.insert(n.key.clone());
    }

    let has_active_work = !obs.live_keys.is_empty() || !next.todo.is_empty();
    let down = down_reason(next.last_tick_ts, obs.now, 600, has_active_work);
    let watchdog_alert = down_alert(down.as_deref(), next.last_watchdog_alert_ts, obs.now, 3_600);
    next.last_tick_ts = Some(obs.now);
    if watchdog_alert.is_some() {
        next.last_watchdog_alert_ts = Some(obs.now);
    }

    let mut burner_kills = Vec::new();
    for b in &obs.burners {
        if b.elapsed_secs >= 600 {
            burner_kills.push(b.clone());
        }
    }

    let mut tell_actions = Vec::new();
    for (agent_key, msgs) in &state.inbox {
        if obs.live_keys.contains(agent_key) || next.agents.contains_key(agent_key) {
            for msg in msgs {
                tell_actions.push((agent_key.clone(), msg.clone()));
            }
        }
    }
    for (k, _) in &tell_actions {
        next.inbox.remove(k);
    }
    for act in &supervision_actions {
        if let SuperviseAction::Nudge { key, because } = act {
            let nudge_msg =
                format!("Stall nudge ({because}): please commit progress or explain status.");
            if !tell_actions
                .iter()
                .any(|(k, m)| k == key && m == &nudge_msg)
            {
                tell_actions.push((key.clone(), nudge_msg));
            }
        }
    }

    DecisionPlan {
        reaped_agents,
        supervision_actions,
        resumed_tasks,
        dispatched_tasks,
        rebase_handoffs,
        barred_rebases,
        fix_decisions,
        batch_resolutions,
        batch_plans,
        ready_prs,
        tidy_decisions,
        attention_items,
        owner_notifications,
        self_update,
        watchdog_alert,
        next_state: next,
        cleared_main_broken,
        burner_kills,
        tell_actions,
        duplicate_batch_aborts: crate::decide::watch::duplicate_batches(&state.batches),
        root_disk_action,
    }
}

/// Evaluate [`decide`] after applying a unified [`ProjectPolicy`] to the state.
/// Existing worker limits, including zero, take precedence over policy defaults.
pub fn decide_with_policy(
    state: &DecisionState,
    obs: &WorldObservation,
    policy: &ProjectPolicy,
) -> DecisionPlan {
    let mut configured = state.clone();
    policy.apply_to_state(&mut configured);
    decide(&configured, obs)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod migration_safety_tests;
