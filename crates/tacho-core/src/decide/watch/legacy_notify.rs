//! Legacy human attention (`needs_you`), self-update (`decide_self_update`), and owner notification
//! (`decide_owner_notifications`) helpers preserved for backward compatibility.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::decide::dispatch::{
    attempt, base_key, pr_problem, stuck_runs, AgentRun, Hold, PullFacts, TodoItem, STALE_HOLD_SECS,
};
use crate::decide::verify::{BatchRun, FailedRecord, PassedRecord};

use super::legacy::{AttentionItem, SELF_UPDATE_STALE_SECS};

/// Compute the list of human-actionable `NEEDS YOU` items (`tacho status` / `tacho top`).
#[allow(clippy::too_many_arguments)]
pub fn needs_you(
    agents: &BTreeMap<String, AgentRun>,
    todo: &[TodoItem],
    retired: &BTreeSet<String>,
    holds: &BTreeMap<String, Hold>,
    live_keys: &BTreeSet<String>,
    open_prs: &[PullFacts],
    passed: &BTreeMap<u64, PassedRecord>,
    failed: &BTreeMap<u64, FailedRecord>,
    now: i64,
) -> Vec<AttentionItem> {
    let mut items = Vec::new();
    let _ = passed;

    for a in agents.values() {
        if retired.contains(&a.key) || live_keys.contains(&a.key) {
            continue;
        }
        if let Some(prob) = pr_problem(a, None) {
            items.push(AttentionItem {
                kind: "attach".to_string(),
                target: a.key.clone(),
                summary: prob,
                command_hint: format!("tacho attach {} <pr>|none", a.key),
            });
        }
    }

    for stuck in stuck_runs(agents, todo, retired, holds, live_keys) {
        items.push(AttentionItem {
            kind: "stuck".to_string(),
            target: stuck.key.clone(),
            summary: format!(
                "{} stopped after {} attempt(s) without a PR",
                stuck.key,
                attempt(&stuck.key) + 1
            ),
            command_hint: format!(
                "tacho session {} --resume  # or: tacho retire {}",
                stuck.key,
                base_key(&stuck.key)
            ),
        });
    }

    let open_by_num: HashMap<u64, &PullFacts> = open_prs.iter().map(|p| (p.number, p)).collect();
    for (&pr_num, rec) in failed {
        if holds.contains_key(&format!("pr:{pr_num}")) {
            continue;
        }
        if open_by_num
            .get(&pr_num)
            .is_some_and(|pr| pr.head == rec.sha)
            && crate::decide::dispatch::already_ran("fix", pr_num, &rec.sha, agents, live_keys)
        {
            items.push(AttentionItem {
                kind: "red-pr".to_string(),
                target: format!("pr:{pr_num}"),
                summary: format!("PR #{pr_num} still red after auto-fix: {}", rec.summary),
                command_hint: format!("tacho replay {}", rec.batch_id),
            });
        }
    }

    for (target, reason, age) in crate::decide::dispatch::stale_holds(holds, now, STALE_HOLD_SECS) {
        items.push(AttentionItem {
            kind: "stale-hold".to_string(),
            target: target.clone(),
            summary: format!("held for {}h: {}", age / 3600, reason),
            command_hint: format!("tacho release {target}"),
        });
    }

    items
}

/// A commit on `main` ahead of the currently running orchestrator binary/checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehindCommit {
    pub sha: String,
    pub ts: i64,
    pub ci_green: bool,
}

/// Pure decision for whether the running orchestrator should self-update to a newer green `main` commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelfUpdateDecision {
    pub target_sha: Option<String>,
    pub stale: bool,
    pub distance_summary: String,
    pub attention: Option<AttentionItem>,
    pub because: String,
}

/// Observed CI / deploy run on `main`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployRunObs {
    pub run_id: u64,
    pub head_sha: String,
    pub status: String,
    pub gate_conclusion: Option<String>,
    pub on_main: bool,
}

/// Tell-once owner notification item (email / attention line).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerNotification {
    pub key: String,
    pub kind: String,
    pub subject: String,
    pub body: String,
    pub command_hint: String,
    pub because: String,
}

/// Decide whether the running orchestrator should advance to a newer `main` commit.
pub fn decide_self_update(
    detached: bool,
    behind: &[BehindCommit],
    refused_sha: Option<&str>,
    now: i64,
) -> SelfUpdateDecision {
    if behind.is_empty() {
        return SelfUpdateDecision {
            target_sha: None,
            stale: false,
            distance_summary: "up to date with main".to_string(),
            attention: None,
            because: "up to date with main".to_string(),
        };
    }

    let oldest_ts = behind.iter().map(|c| c.ts).min().unwrap_or(now);
    let oldest_min = ((now - oldest_ts).max(0)) / 60;
    let plural = if behind.len() == 1 { "" } else { "s" };
    let distance_summary = format!(
        "behind main by {} commit{plural}, the oldest {oldest_min} min old",
        behind.len()
    );
    let stale = (now - oldest_ts) > SELF_UPDATE_STALE_SECS;
    let attention = if stale {
        Some(AttentionItem {
            kind: "self-update".to_string(),
            target: "tacho".to_string(),
            summary: format!(
                "orchestrator code is {distance_summary} (over {} min)",
                SELF_UPDATE_STALE_SECS / 60
            ),
            command_hint: "tacho tick".to_string(),
        })
    } else {
        None
    };

    if !detached {
        return SelfUpdateDecision {
            target_sha: None,
            stale,
            distance_summary,
            attention,
            because: "checkout is not detached; skipping self-update".to_string(),
        };
    }

    let candidate = behind
        .iter()
        .take(5)
        .find(|c| c.ci_green)
        .map(|c| c.sha.clone());

    match candidate {
        Some(ref sha) if refused_sha == Some(sha.as_str()) => SelfUpdateDecision {
            target_sha: None,
            stale,
            distance_summary,
            attention,
            because: format!(
                "refusing self-update to {}: self-test previously failed on that commit",
                crate::char_prefix(sha, 8)
            ),
        },
        Some(sha) => {
            let short = crate::char_prefix(&sha, 8);
            SelfUpdateDecision {
                because: format!("moving orchestrator to green main commit {short}"),
                target_sha: Some(sha),
                stale,
                distance_summary,
                attention,
            }
        }
        None => SelfUpdateDecision {
            target_sha: None,
            stale,
            distance_summary,
            attention,
            because: "waiting for main CI to turn green before self-updating".to_string(),
        },
    }
}

/// Compute new tell-once owner notifications.
#[allow(clippy::too_many_arguments)]
pub fn decide_owner_notifications(
    attention_items: &[AttentionItem],
    batches: &[BatchRun],
    ready_trains: &[(String, Vec<u64>, BTreeMap<u64, String>)],
    failed_main_jobs: &BTreeMap<String, String>,
    deploy_runs: &[DeployRunObs],
    already_notified: &BTreeSet<String>,
) -> Vec<OwnerNotification> {
    let mut out = Vec::new();

    for item in attention_items {
        let key = format!("attention:{}:{}:{}", item.kind, item.target, item.summary);
        if already_notified.contains(&key) {
            continue;
        }
        out.push(OwnerNotification {
            key,
            kind: item.kind.clone(),
            subject: format!("[tacho] {} needs attention ({})", item.target, item.kind),
            body: format!("{}\nFix: {}", item.summary, item.command_hint),
            command_hint: item.command_hint.clone(),
            because: format!(
                "{} needs attention: {} (run `{}`)",
                item.target, item.summary, item.command_hint
            ),
        });
    }

    let mut announced_trains = BTreeSet::new();
    let mut announced_members = BTreeSet::new();
    for key in already_notified {
        if let Some(rest) = key.strip_prefix("ready:") {
            if let Some((prev_train, members_csv)) = rest.split_once(':') {
                if !prev_train.is_empty() {
                    announced_trains.insert(prev_train);
                }
                for member in members_csv.split(',').filter(|m| !m.is_empty()) {
                    announced_members.insert(member);
                }
            }
        } else if let Some(prev_train) = key.strip_prefix("ready-train:") {
            if !prev_train.is_empty() {
                announced_trains.insert(prev_train);
            }
        } else if let Some(member) = key.strip_prefix("ready-pr:") {
            if !member.is_empty() {
                announced_members.insert(member);
            }
        }
    }

    for (train_id, order, heads) in ready_trains {
        if train_id.is_empty() || order.is_empty() {
            continue;
        }
        let mut head_parts = Vec::new();
        for pr in order {
            let sha = heads.get(pr).map(String::as_str).unwrap_or("");
            head_parts.push(format!("{pr}:{}", crate::char_prefix(sha, 8)));
        }
        let key = format!("ready:{train_id}:{}", head_parts.join(","));
        let all_members_already_announced = announced_trains.contains(train_id.as_str())
            && head_parts
                .iter()
                .all(|part| announced_members.contains(part.as_str()));
        if already_notified.contains(&key) || all_members_already_announced {
            continue;
        }
        let pr_list = order
            .iter()
            .map(|n| format!("#{n}"))
            .collect::<Vec<_>>()
            .join(" ");
        let cmd = format!("tacho land --train {train_id}");
        out.push(OwnerNotification {
            key,
            kind: "ready-train".to_string(),
            subject: format!("[tacho] train {train_id} ready to merge ({pr_list})"),
            body: format!("Verified train {train_id} ({pr_list}) is ready.\nMerge: {cmd}"),
            command_hint: cmd.clone(),
            because: format!("train {train_id} ({pr_list}) is ready to merge (run `{cmd}`)"),
        });
    }

    for b in batches {
        if b.status != "failed" && b.status != "red" && b.status != "red-once" {
            continue;
        }
        let key = format!("batch-red:{}", b.id);
        if already_notified.contains(&key) {
            continue;
        }
        let cmd = format!("tacho replay {}", b.id);
        let culprit_str = b
            .culprit
            .map(|c| format!(" (blamed PR #{c})"))
            .unwrap_or_default();
        out.push(OwnerNotification {
            key,
            kind: "red-batch".to_string(),
            subject: format!("[tacho] batch {} failed{}", b.id, culprit_str),
            body: format!(
                "Batch {} on PRs {:?} ended red{}.\nInspect: {}",
                b.id, b.prs, culprit_str, cmd
            ),
            command_hint: cmd.clone(),
            because: format!("batch {} ended red{} (run `{cmd}`)", b.id, culprit_str),
        });
    }

    for (job_key, line) in failed_main_jobs {
        let key = format!("main-job:{job_key}");
        if already_notified.contains(&key) {
            continue;
        }
        let cmd = "tacho status".to_string();
        out.push(OwnerNotification {
            key,
            kind: "main-red".to_string(),
            subject: format!("[tacho] main job failed: {job_key}"),
            body: format!("Main CI job {job_key} failed: {line}\nCheck: {cmd}"),
            command_hint: cmd,
            because: format!("main CI job {job_key} failed: {line}"),
        });
    }

    for d in deploy_runs {
        if !d.on_main || !matches!(d.status.as_str(), "in_progress" | "queued" | "waiting") {
            continue;
        }
        if matches!(
            d.gate_conclusion.as_deref(),
            Some("skipped" | "failure" | "cancelled")
        ) {
            continue;
        }
        let key = format!("deploy:{}", d.run_id);
        if already_notified.contains(&key) {
            continue;
        }
        let short = &d.head_sha[..d.head_sha.len().min(8)];
        let cmd = "tacho top".to_string();
        out.push(OwnerNotification {
            key,
            kind: "deploy-started".to_string(),
            subject: format!("[tacho] deploy of {short} started (run #{})", d.run_id),
            body: format!("Deploy of main commit {short} started (run #{})", d.run_id),
            command_hint: cmd,
            because: format!("deploy of {short} started (run #{})", d.run_id),
        });
    }

    let scrubber = crate::scrub::Scrubber::enabled();
    for notif in &mut out {
        notif.subject = scrubber.scrub(&notif.subject);
        notif.body = scrubber.scrub(&notif.body);
        notif.because = scrubber.scrub(&notif.because);
    }

    out
}
