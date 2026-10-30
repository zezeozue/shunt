//! Ready queue, verified merge trains, combined train PR assembly,
//! generator settling, and compile-breaker bisection.

use crate::decide::dispatch::{Hold, PullFacts};
use crate::decide::verify::{
    areas_for_files, is_isolated_ci, ordered, parents, AreaRule, BatchRun, FailedRecord,
    PassedRecord,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Entry in the ready-to-land queue (`tacho queue` / `tacho land`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyItem {
    pub pr: u64,
    pub sha: String,
    pub batch_id: String,
    pub batch_started_at: i64,
    pub position_in_batch: usize,
    pub title: String,
    pub branch: String,
}

/// A verified merge train record (`tacho train`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TrainRecord {
    pub id: String,
    pub prs: Vec<u64>,
    #[serde(default)]
    pub shas: BTreeMap<u64, String>,
    pub base: String,
    pub tree: String,
    /// `"open"`, `"landed"`, `"superseded"`, `"aborted"`
    pub status: String,
    pub created_at: i64,
    #[serde(default)]
    pub batch_id: String,
    #[serde(default)]
    pub landed_prs: Vec<u64>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Combined release-train pull request state (`tacho release-train`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TrainPrState {
    pub number: Option<u64>,
    pub branch: String,
    pub base: String,
    pub head: String,
    pub tree: String,
    pub members: Vec<u64>,
    #[serde(default)]
    pub member_shas: BTreeMap<u64, String>,
    #[serde(default)]
    pub dropped: BTreeMap<u64, String>,
    #[serde(default)]
    pub reviewed: Option<String>,
    #[serde(default)]
    pub proved_batch: Option<String>,
    #[serde(default)]
    pub proved_tree: Option<String>,
}

/// Check whether a PR's prior batch pass (`PassedRecord`) still holds against `current_main`.
///
/// A pass holds when:
/// 1. The PR is still `OPEN`, not `CONFLICTING`, not held, and its `head == record.sha`.
/// 2. Either `current_main == record.main_sha`, OR `commits_since_pass` on `main` only
///    merged earlier PRs from the *same* batch (`batch.prs[..pos]`).
pub fn still_ready(
    pr: &PullFacts,
    record: &PassedRecord,
    batch: Option<&BatchRun>,
    holds: &BTreeMap<String, Hold>,
    current_main: &str,
    merged_prs_since: &[u64],
) -> bool {
    if !pr.state.eq_ignore_ascii_case("OPEN")
        || pr.draft
        || pr.head.is_empty()
        || current_main.is_empty()
    {
        return false;
    }
    if pr.mergeable.eq_ignore_ascii_case("CONFLICTING") {
        return false;
    }
    if holds.contains_key(&format!("pr:{}", pr.number)) {
        return false;
    }
    if pr.number != record.pr || pr.head != record.sha {
        return false;
    }
    let Some(b) = batch else {
        return false;
    };
    if b.status != "passed"
        || !b.flakes.is_empty()
        || b.id != record.batch_id
        || b.main_sha != record.main_sha
        || !b.prs.contains(&pr.number)
        || b.shas.get(&pr.number) != Some(&record.sha)
        || record.tree.is_empty()
        || b.tree.as_ref() != Some(&record.tree)
    {
        return false;
    }
    if current_main == record.main_sha {
        return true;
    }
    let Some(pos) = b.prs.iter().position(|&n| n == pr.number) else {
        return false;
    };
    if merged_prs_since.is_empty() {
        return false;
    }
    let earlier: BTreeSet<u64> = b.prs[..pos].iter().copied().collect();
    merged_prs_since.iter().all(|m| earlier.contains(m))
}

/// Build the ordered ready-to-land queue across all open PRs.
/// Ordered by `(batch_started_at, position_in_batch, pr_number)`.
pub fn queue(
    open_prs: &[PullFacts],
    passed: &BTreeMap<u64, PassedRecord>,
    batches: &[BatchRun],
    holds: &BTreeMap<String, Hold>,
    current_main: &str,
    merged_prs_since: &HashMap<String, Vec<u64>>,
    rules: &[AreaRule],
) -> Vec<ReadyItem> {
    let batch_by_id: HashMap<&str, &BatchRun> =
        batches.iter().map(|b| (b.id.as_str(), b)).collect();
    let mut items = Vec::new();

    for pr in open_prs {
        let Some(rec) = passed.get(&pr.number) else {
            continue;
        };
        let b = batch_by_id.get(rec.batch_id.as_str()).copied();
        let required = areas_for_files(&pr.files, rules);
        if pr.files.is_empty()
            || required.iter().any(|area| !rec.areas.contains(area))
            || b.is_none_or(|batch| required.iter().any(|area| !batch.areas.contains(area)))
        {
            continue;
        }
        let since = merged_prs_since
            .get(&rec.main_sha)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if !still_ready(pr, rec, b, holds, current_main, since) {
            continue;
        }
        let (started_at, pos) = match b {
            Some(batch) => (
                batch.started_at,
                batch.prs.iter().position(|&n| n == pr.number).unwrap_or(0),
            ),
            None => (rec.passed_at, 0),
        };
        items.push(ReadyItem {
            pr: pr.number,
            sha: rec.sha.clone(),
            batch_id: rec.batch_id.clone(),
            batch_started_at: started_at,
            position_in_batch: pos,
            title: pr.title.clone(),
            branch: pr.branch.clone(),
        });
    }

    items.sort_by(|a, b| {
        a.batch_started_at
            .cmp(&b.batch_started_at)
            .then_with(|| a.position_in_batch.cmp(&b.position_in_batch))
            .then_with(|| a.pr.cmp(&b.pr))
    });
    items
}

/// Check whether the ready-to-land list `now` gained any PR number that was not in `before`.
pub fn queue_grew(before: &[u64], now: &[u64]) -> bool {
    super::ready::grew(before, now)
}

/// Check whether a PR's diff (`additions + deletions`) grew suspiciously (> 3x + 200 lines)
/// after an automated rebase or fix.
pub fn grew(before_lines: u64, after_lines: u64) -> bool {
    after_lines > before_lines.saturating_mul(3).saturating_add(200)
}

/// Return a human-readable standing label for an open PR (`"ready"`, `"verifying"`, `"failed"`, `"conflicting"`, `"held"`, `"pending"`).
pub fn pr_standing(
    pr: &PullFacts,
    holds: &BTreeMap<String, Hold>,
    passed: &BTreeMap<u64, PassedRecord>,
    failed: &BTreeMap<u64, FailedRecord>,
    batches: &[BatchRun],
) -> String {
    if holds.contains_key(&format!("pr:{}", pr.number)) {
        return "held".to_string();
    }
    if pr.mergeable.eq_ignore_ascii_case("CONFLICTING") {
        return "conflicting".to_string();
    }
    if batches
        .iter()
        .any(|b| (b.status == "running" || b.status == "bisecting") && b.prs.contains(&pr.number))
    {
        return "verifying".to_string();
    }
    if let Some(p) = passed.get(&pr.number) {
        if p.sha == pr.head {
            let batch_clean = batches
                .iter()
                .find(|b| b.id == p.batch_id)
                .is_none_or(|b| b.status == "passed" && b.flakes.is_empty());
            if batch_clean {
                return "ready".to_string();
            }
        }
    }
    if let Some(f) = failed.get(&pr.number) {
        if f.sha == pr.head {
            return "failed".to_string();
        }
    }
    "pending".to_string()
}

/// Create a new [`TrainRecord`] for a verified batch.
pub fn make_train(
    existing: &[TrainRecord],
    batch: &BatchRun,
    base_sha: &str,
    tree_sha: &str,
    now: i64,
) -> Result<TrainRecord, String> {
    if batch.status != "passed" || !batch.flakes.is_empty() {
        return Err(format!(
            "batch {} has status '{}' and {} flake(s) (must be 'passed' with 0 flakes)",
            batch.id,
            batch.status,
            batch.flakes.len()
        ));
    }
    if tree_sha.trim().is_empty() {
        return Err("train tree SHA cannot be empty".to_string());
    }
    let next_n = existing
        .iter()
        .filter_map(|t| t.id.strip_prefix('t').and_then(|s| s.parse::<u64>().ok()))
        .max()
        .unwrap_or(0)
        + 1;
    Ok(TrainRecord {
        id: format!("t{next_n:03}"),
        prs: batch.prs.clone(),
        shas: batch.shas.clone(),
        base: base_sha.to_string(),
        tree: tree_sha.to_string(),
        status: "open".to_string(),
        created_at: now,
        batch_id: batch.id.clone(),
        landed_prs: Vec::new(),
        reason: None,
    })
}

/// Find the currently open [`TrainRecord`], if any.
pub fn open_for(trains: &[TrainRecord]) -> Option<&TrainRecord> {
    trains.iter().rev().find(|t| t.status == "open")
}

/// Append a new train record, marking any previously `"open"` trains as `"superseded"`.
pub fn with_train(trains: &[TrainRecord], new_train: TrainRecord) -> Vec<TrainRecord> {
    let mut out: Vec<TrainRecord> = trains
        .iter()
        .cloned()
        .map(|mut t| {
            if t.status == "open" && t.id != new_train.id {
                t.status = "superseded".to_string();
                t.reason = Some(format!("superseded by {}", new_train.id));
            }
            t
        })
        .collect();
    out.push(new_train);
    out
}

/// Check whether a train is safe to land right now against `current_main` and `open_prs`.
pub fn train_refusal(
    train: &TrainRecord,
    current_main: &str,
    open_prs: &[PullFacts],
    holds: &BTreeMap<String, Hold>,
) -> Option<String> {
    if train.status != "open" {
        return Some(format!(
            "train {} is '{}' (not 'open')",
            train.id, train.status
        ));
    }
    if train.base != current_main {
        return Some(format!(
            "main moved from {} to {} since train {} was verified",
            &train.base[..train.base.len().min(8)],
            crate::char_prefix(current_main, 8),
            train.id
        ));
    }
    let by_num: HashMap<u64, &PullFacts> = open_prs.iter().map(|p| (p.number, p)).collect();
    for &pr_num in &train.prs {
        if train.landed_prs.contains(&pr_num) {
            continue;
        }
        if let Some(h) = holds.get(&format!("pr:{pr_num}")) {
            return Some(format!("PR #{pr_num} is held: {}", h.reason));
        }
        let Some(pr) = by_num.get(&pr_num) else {
            return Some(format!("PR #{pr_num} is no longer open"));
        };
        let expected = train.shas.get(&pr_num).map(String::as_str).unwrap_or("");
        if pr.head != expected {
            return Some(format!(
                "PR #{pr_num} head moved from {} to {}",
                crate::char_prefix(expected, 8),
                &pr.head[..pr.head.len().min(8)]
            ));
        }
    }
    None
}

/// Update a train after an outside merge advances `main`: if the merged PR was the next
/// expected member of the train, record it in `landed_prs` and advance `base` to `new_main`;
/// otherwise mark the train `"superseded"`.
pub fn after_outside_merge(train: &TrainRecord, merged_pr: u64, new_main: &str) -> TrainRecord {
    let mut updated = train.clone();
    let remaining: Vec<u64> = updated
        .prs
        .iter()
        .copied()
        .filter(|p| !updated.landed_prs.contains(p))
        .collect();
    if remaining.first() == Some(&merged_pr) {
        updated.landed_prs.push(merged_pr);
        updated.base = new_main.to_string();
        if updated.landed_prs.len() == updated.prs.len() {
            updated.status = "landed".to_string();
        }
    } else {
        updated.status = "superseded".to_string();
        updated.reason = Some(format!("outside merge of PR #{merged_pr} moved main"));
    }
    updated
}

/// Mark a train as `"landed"` once all its PRs have merged.
pub fn finished(train: &TrainRecord) -> TrainRecord {
    let mut updated = train.clone();
    updated.landed_prs = updated.prs.clone();
    updated.status = "landed".to_string();
    updated
}

/// Verify that `main`'s post-merge `git_tree` equals the verified `train.tree`.
/// Returns an error string if they diverge.
pub fn mismatch(train: &TrainRecord, main_tree: &str) -> Option<String> {
    if train.tree != main_tree {
        Some(format!(
            "train {} tree mismatch after merge: expected {}, got {}",
            train.id,
            &train.tree[..train.tree.len().min(12)],
            crate::char_prefix(main_tree, 12)
        ))
    } else {
        None
    }
}

/// Return any open PRs that touch `ci` and therefore must land solo before a combined release-train PR.
pub fn solo_ci<'a>(open_prs: &'a [PullFacts], rules: &[AreaRule]) -> Vec<&'a PullFacts> {
    open_prs
        .iter()
        .filter(|p| p.state.eq_ignore_ascii_case("OPEN") && is_isolated_ci(&p.files, rules))
        .collect()
}

/// Check whether any solo CI PR is still awaiting verification/landing.
pub fn ci_proof_pending(
    open_prs: &[PullFacts],
    passed: &BTreeMap<u64, PassedRecord>,
    rules: &[AreaRule],
) -> Vec<u64> {
    solo_ci(open_prs, rules)
        .into_iter()
        .filter(|p| passed.get(&p.number).map(|r| r.sha.as_str()) != Some(p.head.as_str()))
        .map(|p| p.number)
        .collect()
}

/// Filter open PRs eligible to ride in a combined release-train PR.
/// Excludes draft PRs, conflicting PRs, held PRs, failed PRs at current head, and isolated CI PRs.
pub fn eligible<'a>(
    open_prs: &'a [PullFacts],
    holds: &BTreeMap<String, Hold>,
    failed: &BTreeMap<u64, FailedRecord>,
    rules: &[AreaRule],
) -> Vec<&'a PullFacts> {
    open_prs
        .iter()
        .filter(|p| {
            if !p.state.eq_ignore_ascii_case("OPEN") || p.draft {
                return false;
            }
            if p.mergeable.eq_ignore_ascii_case("CONFLICTING") {
                return false;
            }
            if holds.contains_key(&format!("pr:{}", p.number)) {
                return false;
            }
            if is_isolated_ci(&p.files, rules) {
                return false;
            }
            if let Some(f) = failed.get(&p.number) {
                if f.sha == p.head {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Order eligible PRs topologically (parents before children, then ascending PR number).
pub fn order_train_prs(prs: &[&PullFacts], all_open: &[PullFacts]) -> Vec<u64> {
    let parent_map = parents(all_open);
    let nums: Vec<u64> = prs.iter().map(|p| p.number).collect();
    ordered(&nums, &parent_map)
}

/// Determine which configured generator commands need to run to settle generated files
/// across the merged train tree.
/// `generators` maps a file prefix/pattern to a generator command.
pub fn settle(changed_files: &[String], generators: &[(String, String)]) -> Vec<String> {
    let mut cmds = Vec::new();
    for (prefix, cmd) in generators {
        if changed_files.iter().any(|f| f.starts_with(prefix)) && !cmds.contains(cmd) {
            cmds.push(cmd.clone());
        }
    }
    cmds
}

/// Bisect compile breakers in a combined train using a caller-supplied `compiles(prefix)` oracle.
/// Returns `(surviving_prs, dropped_prs_with_reason)`.
pub fn drop_breakers<F>(
    candidates: &[u64],
    parent_map: &BTreeMap<u64, u64>,
    mut compiles: F,
) -> (Vec<u64>, BTreeMap<u64, String>)
where
    F: FnMut(&[u64]) -> Result<(), String>,
{
    let mut current: Vec<u64> = candidates.to_vec();
    let mut dropped: BTreeMap<u64, String> = BTreeMap::new();

    while !current.is_empty() {
        let Err(full_err) = compiles(&current) else {
            break;
        };
        if current.len() == 1 {
            let culprit = current.remove(0);
            dropped.insert(culprit, full_err);
            break;
        }
        // Binary search the lowest prefix `current[..k]` that fails to compile
        let mut lo = 0usize;
        let mut hi = current.len();
        let mut last_err = full_err;
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            match compiles(&current[..mid]) {
                Ok(()) => lo = mid,
                Err(e) => {
                    hi = mid;
                    last_err = e;
                }
            }
        }
        let culprit = current[hi - 1];
        dropped.insert(culprit, last_err);
        // Remove culprit and any stack descendants
        current.retain(|&pr| {
            if pr == culprit {
                return false;
            }
            let mut cur = pr;
            let mut seen = BTreeSet::new();
            while let Some(&p) = parent_map.get(&cur) {
                if !seen.insert(cur) {
                    break;
                }
                if p == culprit {
                    return false;
                }
                cur = p;
            }
            true
        });
    }

    (current, dropped)
}

/// List all reasons why a combined release-train PR (`TrainPrState`) cannot land yet.
///
/// Requires:
/// 1. Non-empty `members` and non-empty `head`/`tree`.
/// 2. A passed batch (`proved_batch`) whose `tree` matches `train_pr.tree` (`proved.tree == tree`).
/// 3. The proving batch had no skipped checks (`skipped.is_empty()`) and no uninvestigated flakes.
/// 4. All required areas touched by the member PRs were covered by the proving batch.
/// 5. Explicit review sign-off at the exact `head` SHA (`reviewed == Some(head)`).
pub fn waiting_for(
    train_pr: &TrainPrState,
    batches: &[BatchRun],
    open_prs: &[PullFacts],
    rules: &[AreaRule],
    require_review: bool,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if train_pr.members.is_empty() {
        reasons.push("train PR has no member PRs".to_string());
        return reasons;
    }
    if train_pr.head.is_empty() || train_pr.tree.is_empty() {
        reasons.push("train PR branch has not been built/pushed yet".to_string());
        return reasons;
    }

    let by_num: HashMap<u64, &PullFacts> = open_prs.iter().map(|p| (p.number, p)).collect();
    let mut needed_areas = BTreeSet::new();
    for &m in &train_pr.members {
        if let Some(p) = by_num.get(&m) {
            for a in areas_for_files(&p.files, rules) {
                needed_areas.insert(a);
            }
        }
    }

    let matching_batch = batches
        .iter()
        .rev()
        .find(|b| b.status == "passed" && b.tree.as_deref() == Some(train_pr.tree.as_str()));

    match matching_batch {
        None => {
            if train_pr.proved_tree.as_deref() != Some(train_pr.tree.as_str()) {
                reasons.push(format!(
                    "exact tree {} has not passed batch verification yet",
                    &train_pr.tree[..train_pr.tree.len().min(12)]
                ));
            }
        }
        Some(b) => {
            if !b.skipped.is_empty() {
                reasons.push(format!("batch {} skipped checks {:?}", b.id, b.skipped));
            }
            if !b.flakes.is_empty() {
                reasons.push(format!("batch {} had flaky tests {:?}", b.id, b.flakes));
            }
            let covered: BTreeSet<&str> = b.areas.iter().map(String::as_str).collect();
            let missing: Vec<String> = needed_areas
                .into_iter()
                .filter(|a| !covered.contains(a.as_str()))
                .collect();
            if !missing.is_empty() {
                reasons.push(format!(
                    "batch {} did not cover required areas {:?}",
                    b.id, missing
                ));
            }
        }
    }

    if require_review && train_pr.reviewed.as_deref() != Some(train_pr.head.as_str()) {
        reasons.push(format!(
            "train head {} has not been reviewed/approved yet",
            &train_pr.head[..train_pr.head.len().min(8)]
        ));
    }

    reasons
}

/// Like [`waiting_for`], additionally enforcing [`super::assemble::ci_proof_areas`] when an
/// unproven pipeline change (`ci_sha`) has not yet been passed by `main_ci`.
#[allow(clippy::too_many_arguments)]
pub fn waiting_for_with_ci_proof(
    train_pr: &TrainPrState,
    batches: &[BatchRun],
    open_prs: &[PullFacts],
    rules: &[AreaRule],
    require_review: bool,
    ci_sha: Option<&str>,
    main_ci: &super::assemble::MainCi,
    is_ancestor: impl Fn(&str, &str) -> bool,
) -> Vec<String> {
    let mut reasons = waiting_for(train_pr, batches, open_prs, rules, require_review);
    let all_areas: BTreeSet<String> = rules.iter().map(|r| r.area.clone()).collect();
    let proof_areas =
        super::assemble::ci_proof_areas(ci_sha, main_ci, is_ancestor, &all_areas, "gate");
    if !proof_areas.is_empty() {
        let matching_batch = batches
            .iter()
            .rev()
            .find(|b| b.status == "passed" && b.tree.as_deref() == Some(train_pr.tree.as_str()));
        match matching_batch {
            Some(b) => {
                let covered: BTreeSet<&str> = b.areas.iter().map(String::as_str).collect();
                let missing: Vec<String> = proof_areas
                    .into_iter()
                    .filter(|a| !covered.contains(a.as_str()))
                    .collect();
                if !missing.is_empty() {
                    reasons.push(format!(
                        "batch {} did not cover CI-proof areas {:?}",
                        b.id, missing
                    ));
                }
            }
            None => {
                reasons.push(format!(
                    "CI-proof areas {:?} require a passed batch on tree {}",
                    proof_areas,
                    &train_pr.tree[..train_pr.tree.len().min(12)]
                ));
            }
        }
    }
    reasons
}
