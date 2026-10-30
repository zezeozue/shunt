//! Unified pluggable architecture and [`ProjectPolicy`] configuration for `tacho-core`.
//!
//! Any engineering team can adapt `tachograph` to their repository, agent CLI, code host,
//! notification channels, worktree provisioning, and deploy gates via configuration or custom
//! trait implementations without modifying core decision logic:
//!
//! - [`ProviderPlugin`]: Agent lifecycle (`start`, `resume`, `tell`, `alive`, `stop`, `cost`)
//!   backed by argv templates with zero shell interpolation.
//! - [`ForgePlugin`] / [`CodeHostPlugin`]: Code-host and pull-request adapter (PR facts,
//!   mergeable state, CI checks, and scrubbed issue body/comment updates).
//! - [`NotifierPlugin`]: Owner issue, PR/issue comment, and mail/webhook sink notifications,
//!   unconditionally scrubbed through [`Scrubber::enabled()`](crate::scrub::Scrubber::enabled).
//! - [`ProvisionerPlugin`]: Worktree environment provisioning (deterministic port block,
//!   ephemeral DB container recipe, dependency cache mounts, and `.tachoenv` rendering).
//! - [`WatcherPlugin`]: Custom workflow and deploy gates plus project-specific burner rules.
//! - [`ProjectPolicy`]: Unified project configuration aggregating [`DispatchConfig`],
//!   [`VerifyConfig`], [`LandConfig`], [`WatchConfig`], [`OpsConfig`], [`AreaRule`], and
//!   [`ProviderSpec`] definitions.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::decide::dispatch::model::Mergeable as DispatchMergeable;
use crate::decide::dispatch::PullFacts;
use crate::decide::dispatch::{
    builtin_providers, DispatchConfig, Provider, ProviderCommand, ProviderSpec, StopPlan, TellPlan,
};
use crate::decide::land::LandConfig;
use crate::decide::ops::snapshot::redact_json;
use crate::decide::ops::snapshot::MainCiRun;
use crate::decide::ops::OpsConfig;
use crate::decide::verify::{
    allocate_ports, env_file, AreaRule, PathAreaRule, VerifyConfig, PORT_BLOCK_SIZE,
};
use crate::decide::watch::WatchConfig;
use crate::decide::BurnerProcess;
use crate::scrub::Scrubber;

// ============================================================================
// 1. ProviderPlugin
// ============================================================================

/// Rejects any `program` or `argv` slice that attempts to invoke an interactive or command shell.
pub fn ensure_no_shell_argv(program: &str, args: &[String]) -> Result<(), String> {
    let prog_trim = program.trim();
    if prog_trim.is_empty() {
        return Err("provider program cannot be empty".to_string());
    }
    let bin = prog_trim.rsplit('/').next().unwrap_or(prog_trim);
    if matches!(bin, "sh" | "bash" | "zsh" | "dash" | "ksh") {
        return Err(format!(
            "shell binary '{bin}' is forbidden in ProviderPlugin program"
        ));
    }
    if matches!(bin, "env" | "nice" | "timeout") {
        if let Some(first) = args.first() {
            let first_bin = first.rsplit('/').next().unwrap_or(first.as_str());
            if matches!(first_bin, "sh" | "bash" | "zsh" | "dash" | "ksh") {
                return Err(format!(
                    "shell wrapper '{bin} {first_bin}' is forbidden in ProviderPlugin argv"
                ));
            }
        }
    }
    Ok(())
}

/// Pluggable agent provider interface (`start`, `resume`, `tell`, `alive`, `stop`, `cost`).
///
/// Backed by config argv templates without shell invocation so untrusted task briefs,
/// branch names, or PR titles can never cause shell injection.
pub trait ProviderPlugin: Send + Sync {
    /// Canonical provider identifier (e.g. `"claude"`, `"codex"`, `"gemini"`).
    fn name(&self) -> &str;

    /// Plan the direct `argv` command to launch a fresh agent session in `worktree`.
    fn start(
        &self,
        key: &str,
        worktree: &str,
        prompt: &str,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> Result<ProviderCommand, String>;

    /// Plan the direct `argv` command to resume an existing agent session.
    fn resume(
        &self,
        key: &str,
        worktree: &str,
        session_id: Option<&str>,
        message: &str,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> Result<ProviderCommand, String>;

    /// Plan how to deliver `message` to a running or stopped agent without cold-restarting a live turn.
    #[allow(clippy::too_many_arguments)]
    fn tell(
        &self,
        key: &str,
        worktree: &str,
        session_id: Option<&str>,
        message: &str,
        is_live: bool,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> TellPlan;

    /// Check whether a worker is alive given its recorded `pid` and the set of live host PIDs.
    fn alive(&self, pid: Option<u32>, live_pids: &BTreeSet<u32>) -> bool;

    /// Plan how to stop a worker cleanly.
    fn stop(
        &self,
        key: &str,
        worktree: &str,
        session_id: Option<&str>,
        pid: Option<u32>,
    ) -> StopPlan;

    /// Compute the USD cost of a provider run from JSONL transcript lines.
    fn cost(&self, jsonl_lines: &[&str], model: Option<&str>) -> f64;
}

impl ProviderPlugin for ProviderSpec {
    fn name(&self) -> &str {
        <ProviderSpec as Provider>::name(self)
    }

    fn start(
        &self,
        key: &str,
        worktree: &str,
        prompt: &str,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> Result<ProviderCommand, String> {
        let cmd = <ProviderSpec as Provider>::start(self, key, worktree, prompt, model, effort);
        ensure_no_shell_argv(&cmd.program, &cmd.args)?;
        Ok(cmd)
    }

    fn resume(
        &self,
        key: &str,
        worktree: &str,
        session_id: Option<&str>,
        message: &str,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> Result<ProviderCommand, String> {
        let cmd = <ProviderSpec as Provider>::resume(
            self, key, worktree, session_id, message, model, effort,
        );
        ensure_no_shell_argv(&cmd.program, &cmd.args)?;
        Ok(cmd)
    }

    fn tell(
        &self,
        key: &str,
        worktree: &str,
        session_id: Option<&str>,
        message: &str,
        is_live: bool,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> TellPlan {
        <ProviderSpec as Provider>::tell(
            self, key, worktree, session_id, message, is_live, model, effort,
        )
    }

    fn alive(&self, pid: Option<u32>, live_pids: &BTreeSet<u32>) -> bool {
        <ProviderSpec as Provider>::alive(self, pid, live_pids)
    }

    fn stop(
        &self,
        key: &str,
        worktree: &str,
        session_id: Option<&str>,
        pid: Option<u32>,
    ) -> StopPlan {
        <ProviderSpec as Provider>::stop(self, key, worktree, session_id, pid)
    }

    fn cost(&self, jsonl_lines: &[&str], model: Option<&str>) -> f64 {
        <ProviderSpec as Provider>::cost(self, jsonl_lines, model)
    }
}

// ============================================================================
// 2. ForgePlugin / CodeHostPlugin
// ============================================================================

/// Normalized pull-request metadata returned by a [`ForgePlugin`] / [`CodeHostPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ForgePrFacts {
    pub number: u64,
    pub title: String,
    pub body: String,
    pub author: String,
    pub head_sha: String,
    pub base_ref: String,
    pub is_draft: bool,
    pub mergeable: ForgeMergeableState,
    pub labels: Vec<String>,
    pub changed_files: Vec<String>,
    pub review_approved: bool,
}

/// Normalized mergeability state across code hosts (GitHub, GitLab, Gitea).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ForgeMergeableState {
    Mergeable,
    Conflicting,
    #[default]
    Unknown,
}

impl ForgeMergeableState {
    /// Convert into [`dispatch::model::Mergeable`](DispatchMergeable).
    pub fn to_dispatch(self) -> DispatchMergeable {
        match self {
            Self::Mergeable => DispatchMergeable::Mergeable,
            Self::Conflicting => DispatchMergeable::Conflicting,
            Self::Unknown => DispatchMergeable::Unknown,
        }
    }
}

/// Single check-run / status context observation on a PR or commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ForgeCheckObs {
    pub name: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub details_url: Option<String>,
}

/// Aggregated check evaluation result for a PR or commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ForgeChecksVerdict {
    pub all_passed: bool,
    pub pending_jobs: Vec<String>,
    pub failed_jobs: Vec<String>,
    pub missing_required: Vec<String>,
}

/// Pluggable code-host / forge interface (PR facts, mergeable state, CI checks, and
/// issue/PR body & comment updates).
///
/// Also exported as [`CodeHostPlugin`].
pub trait ForgePlugin: Send + Sync {
    /// Identifier of the forge backend (e.g. `"github"`, `"gitlab"`).
    fn forge_name(&self) -> &str;

    /// Build the direct `argv` command to query PR facts (head SHA, mergeable state, labels, files).
    fn plan_pr_facts_cmd(&self, pr_number: u64) -> Vec<String>;

    /// Build the direct `argv` command to query a PR's mergeable state.
    fn plan_mergeable_cmd(&self, pr_number: u64) -> Vec<String>;

    /// Build the direct `argv` command to query check runs for a PR or git ref.
    fn plan_checks_cmd(&self, pr_or_ref: &str) -> Vec<String>;

    /// Build the direct `argv` command to update an issue or PR body (scrubbed by default).
    fn plan_update_issue_body_cmd(&self, issue_number: u64, body: &str) -> Vec<String>;

    /// Build the direct `argv` command to post an issue or PR comment (scrubbed by default).
    fn plan_issue_comment_cmd(&self, issue_number: u64, body: &str) -> Vec<String>;

    /// Build the direct `argv` command listing the last completed CI run of `workflow` on
    /// `branch` (JSON array with `headSha`, `conclusion`, `updatedAt`, ...).
    fn plan_main_ci_cmd(&self, branch: &str, workflow: &str) -> Vec<String>;

    /// Build the direct `argv` command listing open PRs (number, head, mergeability, labels).
    fn plan_open_prs_cmd(&self) -> Vec<String>;

    /// Parse the output of [`ForgePlugin::plan_main_ci_cmd`]. `Ok(None)` means the forge has no
    /// completed run yet; `Err` means the output is not what the forge should return.
    fn parse_main_ci(&self, stdout: &str) -> Result<Option<MainCiRun>, String> {
        parse_gh_run_list(stdout)
    }

    /// Parse the output of [`ForgePlugin::plan_open_prs_cmd`].
    fn parse_open_prs(&self, stdout: &str) -> Result<Vec<PullFacts>, String> {
        parse_gh_pr_list(stdout)
    }

    /// Last completed CI run on `branch` for `workflow`.
    ///
    /// `run` executes one planned argv and returns its stdout; the caller owns the IO and must
    /// bound it (the daemon caps each call). Core stays free of process spawning.
    fn main_ci_run(
        &self,
        branch: &str,
        workflow: &str,
        run: &mut dyn FnMut(&[String]) -> Result<String, String>,
    ) -> Result<Option<MainCiRun>, String> {
        let out = run(&self.plan_main_ci_cmd(branch, workflow))?;
        self.parse_main_ci(&out)
    }

    /// Open PRs with heads, mergeability, and labels. See [`ForgePlugin::main_ci_run`] for `run`.
    fn open_prs(
        &self,
        run: &mut dyn FnMut(&[String]) -> Result<String, String>,
    ) -> Result<Vec<PullFacts>, String> {
        let out = run(&self.plan_open_prs_cmd())?;
        self.parse_open_prs(&out)
    }

    /// Creates or updates an open issue with `title` and `body` (scrubbed via [`Scrubber`]).
    fn set_issue(&self, title: &str, body: &str) -> Result<Option<u64>, String> {
        let _ = (title, body);
        Ok(None)
    }

    /// Closes an open issue matching `title` after posting `comment` (scrubbed via [`Scrubber`]).
    fn close_issue(&self, title: &str, comment: &str) -> Result<bool, String> {
        let _ = (title, comment);
        Ok(false)
    }

    /// Posts a comment on issue `number` (scrubbed via [`Scrubber`]).
    fn comment_issue(&self, number: u64, body: &str) -> Result<(), String> {
        let _ = (number, body);
        Ok(())
    }

    /// Marks draft PR `pr` ready for review.
    fn mark_pr_ready(&self, pr: u64) -> Result<(), String> {
        let _ = pr;
        Ok(())
    }

    /// Merges PR `pr` on the forge.
    fn merge_pr(&self, pr: u64) -> Result<(), String> {
        let _ = pr;
        Ok(())
    }

    /// Lists GitHub Actions caches for `plan_actions_tidy`.
    fn list_actions_caches(&self) -> Result<Vec<crate::decide::ops::tidy::ActionsCache>, String> {
        Ok(Vec::new())
    }

    /// Lists GitHub Actions artifacts for `plan_actions_tidy`.
    fn list_actions_artifacts(
        &self,
    ) -> Result<Vec<crate::decide::ops::tidy::ActionsArtifact>, String> {
        Ok(Vec::new())
    }

    /// Deletes a GitHub Actions cache by ID.
    fn delete_actions_cache(&self, id: u64) -> Result<(), String> {
        let _ = id;
        Ok(())
    }

    /// Deletes a GitHub Actions artifact by ID.
    fn delete_actions_artifact(&self, id: u64) -> Result<(), String> {
        let _ = id;
        Ok(())
    }

    /// Resolve a raw mergeable string from the forge API, preserving `was_conflicting` when
    /// the forge transiently reports `"UNKNOWN"` right after a push.
    fn resolve_mergeable(&self, raw: &str, was_conflicting: bool) -> ForgeMergeableState {
        if crate::decide::dispatch::select::is_conflicting_str(raw, was_conflicting) {
            ForgeMergeableState::Conflicting
        } else if raw.trim().eq_ignore_ascii_case("MERGEABLE") {
            ForgeMergeableState::Mergeable
        } else {
            ForgeMergeableState::Unknown
        }
    }

    /// Evaluate observed check runs against the project's required check names.
    fn evaluate_checks(
        &self,
        checks: &[ForgeCheckObs],
        required_checks: &[String],
    ) -> ForgeChecksVerdict {
        let mut pending_jobs = Vec::new();
        let mut failed_jobs = Vec::new();
        for check in checks {
            let status = check.status.to_ascii_lowercase();
            let conclusion = check
                .conclusion
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            if status != "completed" && status != "success" {
                pending_jobs.push(check.name.clone());
            } else if matches!(
                conclusion.as_str(),
                "failure" | "timed_out" | "cancelled" | "action_required" | "startup_failure"
            ) || status == "failure"
                || status == "error"
            {
                failed_jobs.push(check.name.clone());
            }
        }
        let mut missing_required = Vec::new();
        for req in required_checks {
            let found = checks.iter().any(|c| &c.name == req);
            if !found {
                missing_required.push(req.clone());
            }
        }
        pending_jobs.sort();
        pending_jobs.dedup();
        failed_jobs.sort();
        failed_jobs.dedup();
        missing_required.sort();
        missing_required.dedup();

        let all_passed =
            pending_jobs.is_empty() && failed_jobs.is_empty() && missing_required.is_empty();
        ForgeChecksVerdict {
            all_passed,
            pending_jobs,
            failed_jobs,
            missing_required,
        }
    }
}

/// Alias for [`ForgePlugin`] so callers can refer to either `ForgePlugin` or `CodeHostPlugin`.
pub use ForgePlugin as CodeHostPlugin;

/// Parses `gh run list --json headSha,conclusion,updatedAt,status,databaseId,displayTitle`
/// output (a JSON array, newest first) into the newest completed run.
pub fn parse_gh_run_list(stdout: &str) -> Result<Option<MainCiRun>, String> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Run {
        #[serde(default)]
        database_id: u64,
        #[serde(default)]
        head_sha: String,
        #[serde(default)]
        conclusion: String,
        #[serde(default)]
        status: String,
        #[serde(default)]
        updated_at: String,
        #[serde(default)]
        display_title: String,
    }
    let runs: Vec<Run> = serde_json::from_str(stdout.trim())
        .map_err(|e| format!("unexpected run list output: {e}"))?;
    Ok(runs
        .into_iter()
        .find(|r| !r.head_sha.is_empty() && !r.conclusion.is_empty())
        .map(|r| MainCiRun {
            id: r.database_id,
            status: if r.status.is_empty() {
                "completed".to_string()
            } else {
                r.status
            },
            conclusion: r.conclusion.to_ascii_lowercase(),
            sha: r.head_sha,
            title: r.display_title,
            age_seconds: None,
            completed_at: r.updated_at,
        }))
}

/// Parses open pull heads, labels and complete file lists from the forge.
pub fn parse_gh_pr_list(stdout: &str) -> Result<Vec<PullFacts>, String> {
    #[derive(Deserialize)]
    struct Label {
        #[serde(default)]
        name: String,
    }
    #[derive(Deserialize)]
    struct File {
        path: String,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Pr {
        number: u64,
        #[serde(default)]
        title: String,
        #[serde(default)]
        head_ref_name: String,
        #[serde(default)]
        base_ref_name: String,
        #[serde(default)]
        head_ref_oid: String,
        #[serde(default)]
        mergeable: String,
        #[serde(default)]
        is_draft: bool,
        #[serde(default)]
        labels: Vec<Label>,
        #[serde(default)]
        files: Vec<File>,
        #[serde(default)]
        changed_files: Option<usize>,
    }
    let prs: Vec<Pr> = serde_json::from_str(stdout.trim())
        .map_err(|e| format!("unexpected PR list output: {e}"))?;
    if prs.iter().any(|pr| {
        pr.changed_files
            .is_some_and(|count| count != pr.files.len())
    }) {
        return Err("forge returned an incomplete PR file list".into());
    }
    let mut out: Vec<PullFacts> = prs
        .into_iter()
        .map(|p| {
            let mut labels: Vec<String> = p.labels.into_iter().map(|l| l.name).collect();
            labels.sort();
            PullFacts {
                number: p.number,
                title: p.title,
                branch: p.head_ref_name,
                base: p.base_ref_name,
                head: p.head_ref_oid,
                state: "OPEN".to_string(),
                mergeable: p.mergeable,
                draft: p.is_draft,
                labels,
                files: p.files.into_iter().map(|file| file.path).collect(),
                ..Default::default()
            }
        })
        .collect();
    out.sort_by_key(|p| p.number);
    Ok(out)
}

/// Offline / test forge: returns configured values and never runs a command.
#[derive(Debug, Clone)]
pub struct MockForge {
    /// Value returned by [`ForgePlugin::main_ci_run`]; `Err` simulates a forge failure.
    pub main_ci: Result<Option<MainCiRun>, String>,
    /// Recorded `main_ci_run(branch, workflow)` calls.
    pub ci_requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    /// Value returned by [`ForgePlugin::open_prs`]; `Err` simulates a forge failure.
    pub open_prs: Result<Vec<PullFacts>, String>,
    /// Recorded `set_issue(title, body)` calls.
    pub issues: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    /// Recorded `close_issue(title, comment)` calls.
    pub closed_issues: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    /// Recorded `comment_issue(number, body)` calls.
    pub issue_comments: std::sync::Arc<std::sync::Mutex<Vec<(u64, String)>>>,
    /// Recorded `mark_pr_ready(pr)` calls.
    pub ready_prs: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    /// Recorded `merge_pr(pr)` calls.
    pub merged_prs: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    /// Configured actions caches returned by `list_actions_caches`.
    pub actions_caches: Vec<crate::decide::ops::tidy::ActionsCache>,
    /// Configured actions artifacts returned by `list_actions_artifacts`.
    pub actions_artifacts: Vec<crate::decide::ops::tidy::ActionsArtifact>,
    /// Recorded `delete_actions_cache(id)` calls.
    pub deleted_caches: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    /// Recorded `delete_actions_artifact(id)` calls.
    pub deleted_artifacts: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
}

impl Default for MockForge {
    fn default() -> Self {
        Self {
            main_ci: Ok(None),
            ci_requests: std::sync::Arc::default(),
            open_prs: Ok(Vec::new()),
            issues: std::sync::Arc::default(),
            closed_issues: std::sync::Arc::default(),
            issue_comments: std::sync::Arc::default(),
            ready_prs: std::sync::Arc::default(),
            merged_prs: std::sync::Arc::default(),
            actions_caches: Vec::new(),
            actions_artifacts: Vec::new(),
            deleted_caches: std::sync::Arc::default(),
            deleted_artifacts: std::sync::Arc::default(),
        }
    }
}

impl ForgePlugin for MockForge {
    fn forge_name(&self) -> &str {
        "mock"
    }
    fn plan_pr_facts_cmd(&self, _pr_number: u64) -> Vec<String> {
        Vec::new()
    }
    fn plan_mergeable_cmd(&self, _pr_number: u64) -> Vec<String> {
        Vec::new()
    }
    fn plan_checks_cmd(&self, _pr_or_ref: &str) -> Vec<String> {
        Vec::new()
    }
    fn plan_update_issue_body_cmd(&self, _issue_number: u64, _body: &str) -> Vec<String> {
        Vec::new()
    }
    fn plan_issue_comment_cmd(&self, _issue_number: u64, _body: &str) -> Vec<String> {
        Vec::new()
    }
    fn plan_main_ci_cmd(&self, _branch: &str, _workflow: &str) -> Vec<String> {
        Vec::new()
    }
    fn plan_open_prs_cmd(&self) -> Vec<String> {
        Vec::new()
    }
    fn main_ci_run(
        &self,
        branch: &str,
        workflow: &str,
        _run: &mut dyn FnMut(&[String]) -> Result<String, String>,
    ) -> Result<Option<MainCiRun>, String> {
        self.ci_requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((branch.to_string(), workflow.to_string()));
        self.main_ci.clone()
    }
    fn open_prs(
        &self,
        _run: &mut dyn FnMut(&[String]) -> Result<String, String>,
    ) -> Result<Vec<PullFacts>, String> {
        self.open_prs.clone()
    }
    fn set_issue(&self, title: &str, body: &str) -> Result<Option<u64>, String> {
        let scrubber = Scrubber::enabled();
        let clean_title = scrubber.scrub(title);
        let clean_body = scrubber.scrub(body);
        let mut guard = self.issues.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((idx, existing)) = guard
            .iter_mut()
            .enumerate()
            .find(|(_, (t, _))| t == &clean_title)
        {
            existing.1 = clean_body;
            Ok(Some((idx + 1) as u64))
        } else {
            guard.push((clean_title, clean_body));
            Ok(Some(guard.len() as u64))
        }
    }
    fn close_issue(&self, title: &str, comment: &str) -> Result<bool, String> {
        let scrubber = Scrubber::enabled();
        let mut guard = self.closed_issues.lock().unwrap_or_else(|p| p.into_inner());
        guard.push((scrubber.scrub(title), scrubber.scrub(comment)));
        Ok(true)
    }
    fn comment_issue(&self, number: u64, body: &str) -> Result<(), String> {
        let scrubber = Scrubber::enabled();
        let mut guard = self
            .issue_comments
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        guard.push((number, scrubber.scrub(body)));
        Ok(())
    }
    fn mark_pr_ready(&self, pr: u64) -> Result<(), String> {
        let mut guard = self.ready_prs.lock().unwrap_or_else(|p| p.into_inner());
        guard.push(pr);
        Ok(())
    }
    fn merge_pr(&self, pr: u64) -> Result<(), String> {
        let mut guard = self.merged_prs.lock().unwrap_or_else(|p| p.into_inner());
        guard.push(pr);
        Ok(())
    }
    fn list_actions_caches(&self) -> Result<Vec<crate::decide::ops::tidy::ActionsCache>, String> {
        Ok(self.actions_caches.clone())
    }
    fn list_actions_artifacts(
        &self,
    ) -> Result<Vec<crate::decide::ops::tidy::ActionsArtifact>, String> {
        Ok(self.actions_artifacts.clone())
    }
    fn delete_actions_cache(&self, id: u64) -> Result<(), String> {
        let mut guard = self
            .deleted_caches
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        guard.push(id);
        Ok(())
    }
    fn delete_actions_artifact(&self, id: u64) -> Result<(), String> {
        let mut guard = self
            .deleted_artifacts
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        guard.push(id);
        Ok(())
    }
}

/// Configurable CLI-backed forge plugin (defaults to GitHub CLI `gh` with explicit argv, no shell).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliForgePlugin {
    pub name: String,
    pub cli_bin: String,
    pub repo: Option<String>,
    /// Whether `merge_pr` is permitted to run `gh pr merge`. Defaults to `false` so PR merging
    /// is off by config unless explicitly enabled.
    #[serde(default)]
    pub allow_merge: bool,
}

/// Alias for [`CliForgePlugin`] configured for GitHub (`gh`).
pub type GitHubForge = CliForgePlugin;

impl Default for CliForgePlugin {
    fn default() -> Self {
        Self {
            name: "github".to_string(),
            cli_bin: "gh".to_string(),
            repo: None,
            allow_merge: false,
        }
    }
}

impl CliForgePlugin {
    /// Creates a GitHub CLI forge plugin bound to `repo` (`owner/name`) with PR merging off by default.
    pub fn new(repo: impl Into<String>) -> Self {
        let r = repo.into();
        Self {
            name: "github".to_string(),
            cli_bin: "gh".to_string(),
            repo: (!r.trim().is_empty()).then_some(r),
            allow_merge: false,
        }
    }

    /// Sets whether `merge_pr` is allowed to execute `gh pr merge`.
    pub fn with_allow_merge(mut self, allow_merge: bool) -> Self {
        self.allow_merge = allow_merge;
        self
    }

    fn repo_args(&self) -> Vec<String> {
        match &self.repo {
            Some(r) if !r.is_empty() => vec!["--repo".to_string(), r.clone()],
            _ => Vec::new(),
        }
    }

    fn run_cli(&self, args: &[String]) -> Result<String, String> {
        let mut attempts = 0usize;
        let out = loop {
            match std::process::Command::new(&self.cli_bin)
                .args(args)
                .stdin(std::process::Stdio::null())
                .output()
            {
                Ok(out) => break out,
                // On Linux, concurrent test threads calling fork+exec while a freshly created
                // executable script's write fd is being closed can briefly yield ETXTBSY (26).
                Err(e) if e.raw_os_error() == Some(26) && attempts < 200 => {
                    attempts += 1;
                    std::thread::yield_now();
                }
                Err(e) => return Err(format!("failed to spawn {}: {e}", self.cli_bin)),
            }
        };
        if !out.status.success() {
            return Err(format!(
                "{} {:?} failed: {}",
                self.cli_bin,
                args,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn find_open_issue_by_title(&self, title: &str) -> Result<Option<u64>, String> {
        #[derive(Deserialize)]
        struct IssueItem {
            number: u64,
            title: String,
        }
        let mut args = vec!["issue".to_string(), "list".to_string()];
        args.extend(self.repo_args());
        args.extend(
            [
                "--state",
                "open",
                "--limit",
                "100",
                "--json",
                "number,title",
            ]
            .map(String::from),
        );
        let raw = self.run_cli(&args)?;
        let items: Vec<IssueItem> =
            serde_json::from_str(&raw).map_err(|e| format!("invalid gh issue list JSON: {e}"))?;
        Ok(items
            .into_iter()
            .find(|i| i.title == title)
            .map(|i| i.number))
    }
}

impl ForgePlugin for CliForgePlugin {
    fn forge_name(&self) -> &str {
        &self.name
    }

    fn plan_pr_facts_cmd(&self, pr_number: u64) -> Vec<String> {
        let mut argv = vec![self.cli_bin.clone(), "pr".to_string(), "view".to_string()];
        argv.extend(self.repo_args());
        argv.push(pr_number.to_string());
        argv.push("--json".to_string());
        argv.push(
            "number,title,body,author,headRefOid,baseRefName,isDraft,mergeable,labels,files,reviewDecision"
                .to_string(),
        );
        argv
    }

    fn plan_mergeable_cmd(&self, pr_number: u64) -> Vec<String> {
        let mut argv = vec![self.cli_bin.clone(), "pr".to_string(), "view".to_string()];
        argv.extend(self.repo_args());
        argv.push(pr_number.to_string());
        argv.push("--json".to_string());
        argv.push("mergeable,mergeStateStatus".to_string());
        argv
    }

    fn plan_checks_cmd(&self, pr_or_ref: &str) -> Vec<String> {
        let mut argv = vec![self.cli_bin.clone(), "pr".to_string(), "checks".to_string()];
        argv.extend(self.repo_args());
        argv.push(pr_or_ref.to_string());
        argv.push("--json".to_string());
        argv.push("name,state,bucket,link".to_string());
        argv
    }

    fn plan_main_ci_cmd(&self, branch: &str, workflow: &str) -> Vec<String> {
        let mut argv = vec![self.cli_bin.clone(), "run".to_string(), "list".to_string()];
        argv.extend(self.repo_args());
        argv.extend(
            [
                "--branch",
                branch,
                "--workflow",
                workflow,
                "--status",
                "completed",
                "--limit",
                "1",
                "--json",
                "databaseId,headSha,conclusion,status,updatedAt,displayTitle",
            ]
            .map(String::from),
        );
        argv
    }

    fn plan_open_prs_cmd(&self) -> Vec<String> {
        let mut argv = vec![self.cli_bin.clone(), "pr".to_string(), "list".to_string()];
        argv.extend(self.repo_args());
        argv.extend(
            [
                "--state",
                "open",
                "--limit",
                "200",
                "--json",
                "number,title,headRefName,baseRefName,headRefOid,mergeable,isDraft,labels,files,changedFiles",
            ]
            .map(String::from),
        );
        argv
    }

    fn plan_update_issue_body_cmd(&self, issue_number: u64, body: &str) -> Vec<String> {
        let scrubbed = Scrubber::enabled().scrub(body);
        let mut argv = vec![
            self.cli_bin.clone(),
            "issue".to_string(),
            "edit".to_string(),
        ];
        argv.extend(self.repo_args());
        argv.push(issue_number.to_string());
        argv.push("--body".to_string());
        argv.push(scrubbed);
        argv
    }

    fn plan_issue_comment_cmd(&self, issue_number: u64, body: &str) -> Vec<String> {
        let scrubbed = Scrubber::enabled().scrub(body);
        let mut argv = vec![
            self.cli_bin.clone(),
            "issue".to_string(),
            "comment".to_string(),
        ];
        argv.extend(self.repo_args());
        argv.push(issue_number.to_string());
        argv.push("--body".to_string());
        argv.push(scrubbed);
        argv
    }

    fn set_issue(&self, title: &str, body: &str) -> Result<Option<u64>, String> {
        if self.repo.as_deref().is_none_or(|r| r.is_empty()) {
            return Ok(None);
        }
        let scrubber = Scrubber::enabled();
        let clean_title = scrubber.scrub(title);
        let clean_body = scrubber.scrub(body);
        if let Some(num) = self.find_open_issue_by_title(&clean_title)? {
            let mut args = vec!["issue".to_string(), "edit".to_string(), num.to_string()];
            args.extend(self.repo_args());
            args.push("--body".to_string());
            args.push(clean_body);
            self.run_cli(&args)?;
            return Ok(Some(num));
        }
        let mut args = vec!["issue".to_string(), "create".to_string()];
        args.extend(self.repo_args());
        args.extend([
            "--title".to_string(),
            clean_title,
            "--body".to_string(),
            clean_body,
        ]);
        let out = self.run_cli(&args)?;
        let parsed = out
            .rsplit('/')
            .next()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .or_else(|| out.trim().parse::<u64>().ok());
        Ok(parsed)
    }

    fn close_issue(&self, title: &str, comment: &str) -> Result<bool, String> {
        if self.repo.as_deref().is_none_or(|r| r.is_empty()) {
            return Ok(false);
        }
        let scrubber = Scrubber::enabled();
        let clean_title = scrubber.scrub(title);
        let clean_comment = scrubber.scrub(comment);
        let Some(num) = self.find_open_issue_by_title(&clean_title)? else {
            return Ok(false);
        };
        let mut args = vec!["issue".to_string(), "close".to_string(), num.to_string()];
        args.extend(self.repo_args());
        args.extend(["--comment".to_string(), clean_comment]);
        self.run_cli(&args)?;
        Ok(true)
    }

    fn comment_issue(&self, number: u64, body: &str) -> Result<(), String> {
        if self.repo.as_deref().is_none_or(|r| r.is_empty()) {
            return Ok(());
        }
        let clean_body = Scrubber::enabled().scrub(body);
        let mut args = vec![
            "issue".to_string(),
            "comment".to_string(),
            number.to_string(),
        ];
        args.extend(self.repo_args());
        args.extend(["--body".to_string(), clean_body]);
        self.run_cli(&args).map(|_| ())
    }

    fn mark_pr_ready(&self, pr: u64) -> Result<(), String> {
        let mut args = vec!["pr".to_string(), "ready".to_string(), pr.to_string()];
        args.extend(self.repo_args());
        self.run_cli(&args).map(|_| ())
    }

    fn merge_pr(&self, pr: u64) -> Result<(), String> {
        if !self.allow_merge {
            return Err(format!(
                "refusing to merge PR #{pr}: merging is off by config (merge_prs=false)"
            ));
        }
        let mut args = vec!["pr".to_string(), "merge".to_string(), pr.to_string()];
        args.extend(self.repo_args());
        args.push("--merge".to_string());
        self.run_cli(&args).map(|_| ())
    }

    fn list_actions_caches(&self) -> Result<Vec<crate::decide::ops::tidy::ActionsCache>, String> {
        let Some(repo) = self.repo.as_deref().filter(|r| !r.is_empty()) else {
            return Err("actions-tidy: forge repository is not configured".to_string());
        };
        let args = vec![
            "api".to_string(),
            "--paginate".to_string(),
            format!("repos/{repo}/actions/caches?per_page=100"),
        ];
        let raw = self.run_cli(&args)?;
        parse_gh_actions_caches(&raw)
    }

    fn list_actions_artifacts(
        &self,
    ) -> Result<Vec<crate::decide::ops::tidy::ActionsArtifact>, String> {
        let Some(repo) = self.repo.as_deref().filter(|r| !r.is_empty()) else {
            return Err("actions-tidy: forge repository is not configured".to_string());
        };
        let args = vec![
            "api".to_string(),
            "--paginate".to_string(),
            format!("repos/{repo}/actions/artifacts?per_page=100"),
        ];
        let raw = self.run_cli(&args)?;
        parse_gh_actions_artifacts(&raw)
    }

    fn delete_actions_cache(&self, id: u64) -> Result<(), String> {
        let Some(repo) = self.repo.as_deref().filter(|r| !r.is_empty()) else {
            return Err("actions-tidy: forge repository is not configured".to_string());
        };
        let args = vec![
            "api".to_string(),
            "-X".to_string(),
            "DELETE".to_string(),
            format!("repos/{repo}/actions/caches/{id}"),
        ];
        self.run_cli(&args).map(|_| ())
    }

    fn delete_actions_artifact(&self, id: u64) -> Result<(), String> {
        let Some(repo) = self.repo.as_deref().filter(|r| !r.is_empty()) else {
            return Err("actions-tidy: forge repository is not configured".to_string());
        };
        let args = vec![
            "api".to_string(),
            "-X".to_string(),
            "DELETE".to_string(),
            format!("repos/{repo}/actions/artifacts/{id}"),
        ];
        self.run_cli(&args).map(|_| ())
    }
}

/// Parses an integer unix timestamp or ISO-8601 UTC timestamp (`"YYYY-MM-DDTHH:MM:SSZ"`) into unix seconds.
pub fn parse_github_timestamp(val: &serde_json::Value) -> Option<i64> {
    if let Some(ts) = val.as_i64() {
        return Some(ts);
    }
    let s = val.as_str()?.trim();
    if let Ok(ts) = s.parse::<i64>() {
        return Some(ts);
    }
    let year = s.get(0..4)?.parse::<i64>().ok()?;
    let month = s.get(5..7)?.parse::<u32>().ok()?;
    let day = s.get(8..10)?.parse::<u32>().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let hour = s
        .get(11..13)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let min = s
        .get(14..16)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let sec = s
        .get(17..19)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let y = year - if month <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + (doe as i64) - 719468;
    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

/// Parses `gh api --paginate repos/<repo>/actions/caches?per_page=100` JSON output (including
/// concatenated multi-page objects, arrays, or NDJSON) into [`crate::decide::ops::tidy::ActionsCache`] items.
pub fn parse_gh_actions_caches(
    stdout: &str,
) -> Result<Vec<crate::decide::ops::tidy::ActionsCache>, String> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let push_item = |item: &serde_json::Value,
                     out: &mut Vec<crate::decide::ops::tidy::ActionsCache>| {
        if let (Some(id), Some(last_accessed_at)) = (
            item.get("id").and_then(|v| v.as_u64()),
            item.get("last_accessed_at")
                .or_else(|| item.get("created_at"))
                .and_then(parse_github_timestamp),
        ) {
            out.push(crate::decide::ops::tidy::ActionsCache {
                id,
                last_accessed_at,
            });
        }
    };
    let stream = serde_json::Deserializer::from_str(trimmed).into_iter::<serde_json::Value>();
    for res in stream {
        let val = res.map_err(|e| format!("invalid actions caches JSON: {e}"))?;
        if let Some(arr) = val.get("actions_caches").and_then(|v| v.as_array()) {
            for item in arr {
                push_item(item, &mut out);
            }
        } else if let Some(arr) = val.as_array() {
            for item in arr {
                if let Some(inner) = item.get("actions_caches").and_then(|v| v.as_array()) {
                    for sub in inner {
                        push_item(sub, &mut out);
                    }
                } else {
                    push_item(item, &mut out);
                }
            }
        } else {
            push_item(&val, &mut out);
        }
    }
    Ok(out)
}

/// Parses `gh api --paginate repos/<repo>/actions/artifacts?per_page=100` JSON output (including
/// concatenated multi-page objects, arrays, or NDJSON) into [`crate::decide::ops::tidy::ActionsArtifact`] items.
pub fn parse_gh_actions_artifacts(
    stdout: &str,
) -> Result<Vec<crate::decide::ops::tidy::ActionsArtifact>, String> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let push_item = |item: &serde_json::Value,
                     out: &mut Vec<crate::decide::ops::tidy::ActionsArtifact>| {
        if let (Some(id), Some(name), Some(created_at)) = (
            item.get("id").and_then(|v| v.as_u64()),
            item.get("name").and_then(|v| v.as_str()),
            item.get("created_at").and_then(parse_github_timestamp),
        ) {
            let expired = item
                .get("expired")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            out.push(crate::decide::ops::tidy::ActionsArtifact {
                id,
                name: name.to_string(),
                created_at,
                expired,
            });
        }
    };
    let stream = serde_json::Deserializer::from_str(trimmed).into_iter::<serde_json::Value>();
    for res in stream {
        let val = res.map_err(|e| format!("invalid actions artifacts JSON: {e}"))?;
        if let Some(arr) = val.get("artifacts").and_then(|v| v.as_array()) {
            for item in arr {
                push_item(item, &mut out);
            }
        } else if let Some(arr) = val.as_array() {
            for item in arr {
                if let Some(inner) = item.get("artifacts").and_then(|v| v.as_array()) {
                    for sub in inner {
                        push_item(sub, &mut out);
                    }
                } else {
                    push_item(item, &mut out);
                }
            }
        } else {
            push_item(&val, &mut out);
        }
    }
    Ok(out)
}

fn is_test_binary_path(exe: &std::path::Path) -> bool {
    let in_deps = exe
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        == Some("deps");
    let stem = exe.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
    let has_hash_suffix = stem.rsplit_once('-').is_some_and(|(_, suffix)| {
        suffix.len() == 16 && suffix.bytes().all(|b| b.is_ascii_hexdigit())
    });
    in_deps && has_hash_suffix
}

/// Returns `true` when executing inside a test harness (`cargo test`, `nextest`, or a child
/// process spawned by a test suite with `RUST_TEST_THREADS` / `TACHO_TEST_MODE`), so tests
/// never invoke an ambient production `TACHO_NOTIFY` hook unless `TACHO_TEST_NOTIFY_CMD` is set.
pub fn running_in_test_harness() -> bool {
    if cfg!(test)
        || std::env::var_os("TACHO_TEST_MODE").is_some()
        || std::env::var_os("RUST_TEST_THREADS").is_some()
        || std::env::var_os("NEXTEST").is_some()
        || std::env::var_os("NEXTEST_EXECUTION_MODE").is_some()
        || std::env::var_os("CARGO_BIN_EXE_tacho").is_some()
        || std::env::var_os("CARGO_TARGET_TMPDIR").is_some()
    {
        return true;
    }
    if std::env::current_exe().is_ok_and(|exe| is_test_binary_path(&exe)) {
        return true;
    }
    let mut cur_pid = std::process::id();
    for _ in 0..4 {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{cur_pid}/stat")) else {
            break;
        };
        let Some(rparen) = stat.rfind(')') else {
            break;
        };
        let Some(ppid) = stat[rparen + 1..]
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
        else {
            break;
        };
        if ppid <= 1 || ppid == cur_pid {
            break;
        }
        if std::fs::read_link(format!("/proc/{ppid}/exe"))
            .is_ok_and(|exe| is_test_binary_path(&exe))
        {
            return true;
        }
        cur_pid = ppid;
    }
    false
}

/// Scrubs `subject` and `body` via [`Scrubber::enabled`] (plus configured secret environment
/// variable values) and runs `cmd_override` (or `$TACHO_NOTIFY`, or `$TACHO_TEST_NOTIFY_CMD`
/// when [`running_in_test_harness`] is true) with `TACHO_NOTIFY_SUBJECT` and `TACHO_NOTIFY_BODY`
/// environment variables and stdin populated with the scrubbed body.
pub fn notify_owner_command_with(
    cmd_override: Option<&str>,
    subject: &str,
    body: &str,
) -> Result<bool, String> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let env_map: std::collections::BTreeMap<String, String> = std::env::vars().collect();
    let extra_secrets = crate::decide::ops::snapshot::configured_secrets(&env_map);
    let scrubber = Scrubber::enabled();
    let mut clean_subject = scrubber.scrub(subject);
    let mut clean_body = scrubber.scrub(body);
    for secret in &extra_secrets {
        if !secret.is_empty() {
            clean_subject = clean_subject.replace(secret, "<redacted>");
            clean_body = clean_body.replace(secret, "<redacted>");
        }
    }
    let in_test = running_in_test_harness();
    let resolved_cmd = match cmd_override {
        Some(cmd) => Some(cmd.trim().to_string()),
        None if in_test => std::env::var("TACHO_TEST_NOTIFY_CMD")
            .ok()
            .map(|s| s.trim().to_string()),
        None => std::env::var("TACHO_NOTIFY")
            .ok()
            .map(|s| s.trim().to_string()),
    };
    let Some(cmd_str) = resolved_cmd.filter(|s| !s.is_empty()) else {
        if !in_test {
            eprintln!("warning: no notifier is configured (set TACHO_NOTIFY)");
        }
        return Ok(false);
    };

    let mut child = Command::new("sh")
        .args(["-c", &cmd_str])
        .env("TACHO_NOTIFY_SUBJECT", &clean_subject)
        .env("TACHO_NOTIFY_BODY", &clean_body)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to spawn TACHO_NOTIFY: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(clean_body.as_bytes());
    }
    let out = child
        .wait_with_output()
        .map_err(|e| format!("failed to wait on TACHO_NOTIFY: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "TACHO_NOTIFY exited with {:?}: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(true)
}

/// Scrubs `subject` and `body` via [`Scrubber::enabled`] and, when `$TACHO_NOTIFY` is set to a
/// non-empty command, runs `sh -c "$TACHO_NOTIFY"` with `TACHO_NOTIFY_SUBJECT` and
/// `TACHO_NOTIFY_BODY` environment variables and stdin populated with the scrubbed body.
pub fn notify_owner_command(subject: &str, body: &str) -> Result<bool, String> {
    notify_owner_command_with(None, subject, body)
}

// ============================================================================
// 3. NotifierPlugin
// ============================================================================

/// Delivery channel and scrubbed payload planned by a [`NotifierPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotifierAction {
    /// Create or update the rolling owner status issue.
    OwnerIssue {
        title: String,
        body: String,
        labels: Vec<String>,
    },
    /// Post a comment on an issue or pull request.
    Comment { issue_or_pr: u64, body: String },
    /// Deliver an alert to an external mail or webhook sink.
    Sink {
        sink_kind: String,
        destination: String,
        subject: String,
        body: String,
        payload: serde_json::Value,
    },
}

/// Pluggable notification sink (owner issue, PR/issue comment, mail/webhook sink).
///
/// Every helper on `NotifierPlugin` scrubs titles, subjects, bodies, and nested JSON payloads
/// through [`Scrubber::enabled()`] by default so secrets never leak to external sinks.
pub trait NotifierPlugin: Send + Sync {
    /// Scrub arbitrary text using the default enabled [`Scrubber`].
    fn scrub_text(&self, text: &str) -> String {
        Scrubber::enabled().scrub(text)
    }

    /// Recursively scrub a JSON payload using [`redact_json`].
    fn scrub_json(&self, value: &serde_json::Value) -> serde_json::Value {
        redact_json(value.clone(), &[])
    }

    /// Plan an owner issue update with mandatory secret scrubbing.
    fn notify_owner_issue(&self, title: &str, body: &str, labels: &[String]) -> NotifierAction {
        NotifierAction::OwnerIssue {
            title: self.scrub_text(title),
            body: self.scrub_text(body),
            labels: labels.iter().map(|l| self.scrub_text(l)).collect(),
        }
    }

    /// Plan an issue or PR comment with mandatory secret scrubbing.
    fn notify_comment(&self, issue_or_pr: u64, body: &str) -> NotifierAction {
        NotifierAction::Comment {
            issue_or_pr,
            body: self.scrub_text(body),
        }
    }

    /// Plan a mail or webhook sink notification with mandatory secret scrubbing.
    fn notify_sink(&self, subject: &str, body: &str, payload: &serde_json::Value)
        -> NotifierAction;
}

/// Configurable notifier supporting owner issues, comments, and a mail or webhook sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigNotifier {
    pub sink_kind: String,
    pub destination: String,
    pub owner_issue_label: String,
}

impl Default for ConfigNotifier {
    fn default() -> Self {
        Self {
            sink_kind: "webhook".to_string(),
            destination: "https://hooks.example.invalid/tacho".to_string(),
            owner_issue_label: "tacho-owner".to_string(),
        }
    }
}

impl NotifierPlugin for ConfigNotifier {
    fn notify_sink(
        &self,
        subject: &str,
        body: &str,
        payload: &serde_json::Value,
    ) -> NotifierAction {
        NotifierAction::Sink {
            sink_kind: self.sink_kind.clone(),
            destination: self.scrub_text(&self.destination),
            subject: self.scrub_text(subject),
            body: self.scrub_text(body),
            payload: self.scrub_json(payload),
        }
    }
}

/// Decorator that wraps any [`NotifierPlugin`] and enforces a second pass of [`Scrubber::enabled()`]
/// on every outgoing [`NotifierAction`], guaranteeing zero unredacted leaks even if a custom
/// implementation overrides default trait methods.
#[derive(Debug, Clone, Default)]
pub struct ScrubbedNotifier<N> {
    pub inner: N,
}

impl<N> ScrubbedNotifier<N> {
    pub fn new(inner: N) -> Self {
        Self { inner }
    }
}

impl<N: NotifierPlugin> NotifierPlugin for ScrubbedNotifier<N> {
    fn notify_owner_issue(&self, title: &str, body: &str, labels: &[String]) -> NotifierAction {
        let scrubber = Scrubber::enabled();
        match self.inner.notify_owner_issue(title, body, labels) {
            NotifierAction::OwnerIssue {
                title,
                body,
                labels,
            } => NotifierAction::OwnerIssue {
                title: scrubber.scrub(&title),
                body: scrubber.scrub(&body),
                labels: labels.iter().map(|l| scrubber.scrub(l)).collect(),
            },
            other => other,
        }
    }

    fn notify_comment(&self, issue_or_pr: u64, body: &str) -> NotifierAction {
        let scrubber = Scrubber::enabled();
        match self.inner.notify_comment(issue_or_pr, body) {
            NotifierAction::Comment { issue_or_pr, body } => NotifierAction::Comment {
                issue_or_pr,
                body: scrubber.scrub(&body),
            },
            other => other,
        }
    }

    fn notify_sink(
        &self,
        subject: &str,
        body: &str,
        payload: &serde_json::Value,
    ) -> NotifierAction {
        let scrubber = Scrubber::enabled();
        match self.inner.notify_sink(subject, body, payload) {
            NotifierAction::Sink {
                sink_kind,
                destination,
                subject,
                body,
                payload,
            } => NotifierAction::Sink {
                sink_kind,
                destination: scrubber.scrub(&destination),
                subject: scrubber.scrub(&subject),
                body: scrubber.scrub(&body),
                payload: redact_json(payload, &[]),
            },
            other => other,
        }
    }
}

// ============================================================================
// 4. ProvisionerPlugin
// ============================================================================

/// Deterministic port block allocated to an isolated worktree slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortBlock {
    pub slot: u32,
    pub start_port: u16,
    pub last_port: u16,
    pub http_port: u16,
    pub db_port: u16,
}

/// Ephemeral database container recipe for a worktree slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionDbRecipe {
    pub container_name: String,
    pub image: String,
    pub host_port: u16,
    pub container_port: u16,
    pub database_url: String,
    pub docker_run_argv: Vec<String>,
}

/// Pluggable worktree environment provisioner (port block, DB recipe, deps cache, `.tachoenv`).
pub trait ProvisionerPlugin: Send + Sync {
    /// Allocate a deterministic, non-overlapping port block for `slot`.
    fn allocate_port_block(&self, slot: u32) -> Result<PortBlock, String>;

    /// Build the ephemeral database recipe and `DATABASE_URL` for `worktree_name` at `ports`.
    fn db_recipe(&self, worktree_name: &str, ports: &PortBlock) -> ProvisionDbRecipe;

    /// Compute dependency and compiler cache environment variables for `slot`.
    ///
    /// An empty `cache_root` means "use the provisioner's configured root".
    fn deps_cache_env(&self, slot: u32, cache_root: &str) -> BTreeMap<String, String>;

    /// Render the deterministic `.tachoenv` file contents for `worktree_name` and `slot`.
    fn render_tachoenv(
        &self,
        worktree_name: &str,
        slot: u32,
        cache_root: &str,
        extra_vars: &BTreeMap<String, String>,
    ) -> Result<String, String> {
        let ports = self.allocate_port_block(slot)?;
        let db = self.db_recipe(worktree_name, &ports);
        let cache_env = self.deps_cache_env(slot, cache_root);
        let target_dir = cache_env.get("CARGO_TARGET_DIR").map(String::as_str);
        let mut rendered = env_file(
            worktree_name,
            "HEAD",
            None,
            ports.start_port,
            ports.last_port,
            target_dir,
        );
        rendered.push_str(&format!(
            "export TACHO_PORT_BASE='{}'\nexport DATABASE_URL='{}'\n",
            ports.start_port, db.database_url
        ));
        for (k, v) in &cache_env {
            if k != "CARGO_TARGET_DIR" {
                rendered.push_str(&format!("export {k}='{v}'\n"));
            }
        }
        for (k, v) in extra_vars {
            rendered.push_str(&format!("export {k}='{v}'\n"));
        }
        Ok(rendered)
    }
}

/// Layout of the warm dependency and compiler caches handed to worktrees.
///
/// Each entry maps an environment variable to a path relative to the cache root; `{slot}` in a
/// path is replaced by the worktree slot so per-slot build dirs never collide.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WarmCacheLayout {
    /// Cache root. Empty means "not configured": use [`WarmCacheLayout::for_fleet_home`] to derive
    /// `<fleet home>/cache`, or pass an explicit root to [`ProvisionerPlugin::deps_cache_env`].
    pub root: String,
    /// Environment variable -> path under the root (may contain `{slot}`).
    pub env: BTreeMap<String, String>,
}

impl Default for WarmCacheLayout {
    fn default() -> Self {
        let env = [
            ("CARGO_TARGET_DIR", "warm/slot-{slot}/target"),
            ("CARGO_HOME", "cargo-home"),
            ("SCCACHE_DIR", "sccache"),
            ("NPM_CONFIG_CACHE", "npm"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        Self {
            root: String::new(),
            env,
        }
    }
}

impl WarmCacheLayout {
    /// Default layout rooted at `<fleet_home>/cache`.
    pub fn for_fleet_home(fleet_home: &str) -> Self {
        Self {
            root: format!("{}/cache", fleet_home.trim_end_matches('/')),
            ..Self::default()
        }
    }

    /// Resolves the layout for `slot` under `root` (or under [`Self::root`] when `root` is
    /// empty). Returns no variables when neither root is configured.
    pub fn env_for_slot(&self, slot: u32, root: &str) -> BTreeMap<String, String> {
        let root = if root.is_empty() { &self.root } else { root };
        let root = root.trim_end_matches('/');
        if root.is_empty() {
            return BTreeMap::new();
        }
        let slot = slot.to_string();
        self.env
            .iter()
            .map(|(k, rel)| {
                (
                    k.clone(),
                    format!("{root}/{}", rel.replace("{slot}", &slot)),
                )
            })
            .collect()
    }
}

/// Default configurable worktree provisioner backed by [`VerifyConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultProvisioner {
    pub verify: VerifyConfig,
    /// Warm cache layout; project config may override paths and variables.
    #[serde(default)]
    pub warm_cache: WarmCacheLayout,
    pub db_image: String,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
}

impl Default for DefaultProvisioner {
    fn default() -> Self {
        Self {
            verify: VerifyConfig::default(),
            db_image: "postgres:16-alpine".to_string(),
            db_user: "postgres".to_string(),
            db_password: "postgres".to_string(),
            db_name: "app_test".to_string(),
            warm_cache: WarmCacheLayout::default(),
        }
    }
}

impl DefaultProvisioner {
    /// Default provisioner whose warm caches live under `<fleet_home>/cache`.
    pub fn for_fleet_home(fleet_home: &str) -> Self {
        Self {
            warm_cache: WarmCacheLayout::for_fleet_home(fleet_home),
            ..Self::default()
        }
    }
}

impl ProvisionerPlugin for DefaultProvisioner {
    fn allocate_port_block(&self, slot: u32) -> Result<PortBlock, String> {
        let mut used = BTreeSet::new();
        for s in 0..slot {
            let base = 20_000u32
                .saturating_add(s.saturating_mul(u32::from(PORT_BLOCK_SIZE)))
                .min(u32::from(u16::MAX)) as u16;
            used.insert(base);
        }
        let (start_port, last_port) = allocate_ports(&used, |_| true)?;
        Ok(PortBlock {
            slot,
            start_port,
            last_port,
            http_port: start_port,
            db_port: start_port + 1,
        })
    }

    fn db_recipe(&self, worktree_name: &str, ports: &PortBlock) -> ProvisionDbRecipe {
        let container_name = format!("tacho-pg-{worktree_name}-{}", ports.slot);
        let host_port = ports.db_port;
        let container_port = 5432u16;
        let database_url = format!(
            "postgres://{}:{}@127.0.0.1:{}/{}",
            self.db_user, self.db_password, host_port, self.db_name
        );
        let docker_run_argv = vec![
            "docker".to_string(),
            "run".to_string(),
            "-d".to_string(),
            "--rm".to_string(),
            "--init".to_string(),
            "--name".to_string(),
            container_name.clone(),
            "-e".to_string(),
            format!("POSTGRES_USER={}", self.db_user),
            "-e".to_string(),
            format!("POSTGRES_PASSWORD={}", self.db_password),
            "-e".to_string(),
            format!("POSTGRES_DB={}", self.db_name),
            "-p".to_string(),
            format!("127.0.0.1:{host_port}:{container_port}"),
            "--tmpfs".to_string(),
            "/var/lib/postgresql/data:rw,noexec,nosuid,size=512m".to_string(),
            self.db_image.clone(),
        ];
        ProvisionDbRecipe {
            container_name,
            image: self.db_image.clone(),
            host_port,
            container_port,
            database_url,
            docker_run_argv,
        }
    }

    fn deps_cache_env(&self, slot: u32, cache_root: &str) -> BTreeMap<String, String> {
        self.warm_cache.env_for_slot(slot, cache_root)
    }
}

// ============================================================================
// 5. WatcherPlugin
// ============================================================================

/// Input observation for evaluating a custom workflow gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowGateInput {
    pub workflow_name: String,
    pub branch: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub failed_jobs: Vec<String>,
}

/// Input observation for evaluating a custom deploy gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeployGateInput {
    pub environment: String,
    pub head_sha: String,
    pub main_ci_green: bool,
    pub active_incidents: Vec<String>,
    pub freeze_active: bool,
}

/// Decision returned by a [`WatcherPlugin`] workflow or deploy gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateDecision {
    Allow,
    Hold { reason: String },
    Reject { reason: String },
}

impl GateDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// Pluggable workflow, deploy, and runaway-process watcher interface.
pub trait WatcherPlugin: Send + Sync {
    /// Evaluate whether a workflow run passes project-specific gating rules.
    fn evaluate_workflow_gate(&self, input: &WorkflowGateInput) -> GateDecision;

    /// Evaluate whether a deployment is allowed to proceed.
    fn evaluate_deploy_gate(&self, input: &DeployGateInput) -> GateDecision;

    /// Inspect a host process and return a termination reason if it matches custom burner rules.
    fn classify_burner(&self, proc: &BurnerProcess) -> Option<String>;
}

/// Default configurable [`WatcherPlugin`] driven by [`WatchConfig`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DefaultWatcherPlugin {
    pub config: WatchConfig,
    pub required_workflows: Vec<String>,
}

impl WatcherPlugin for DefaultWatcherPlugin {
    fn evaluate_workflow_gate(&self, input: &WorkflowGateInput) -> GateDecision {
        if !input.failed_jobs.is_empty() {
            return GateDecision::Reject {
                reason: format!(
                    "workflow {} failed jobs: {}",
                    input.workflow_name,
                    input.failed_jobs.join(", ")
                ),
            };
        }
        let status = input.status.to_ascii_lowercase();
        if status != "completed" && status != "success" {
            return GateDecision::Hold {
                reason: format!("workflow {} is {}", input.workflow_name, input.status),
            };
        }
        if let Some(conclusion) = &input.conclusion {
            if !conclusion.eq_ignore_ascii_case("success")
                && !conclusion.eq_ignore_ascii_case("skipped")
            {
                return GateDecision::Reject {
                    reason: format!(
                        "workflow {} concluded with {}",
                        input.workflow_name, conclusion
                    ),
                };
            }
        }
        GateDecision::Allow
    }

    fn evaluate_deploy_gate(&self, input: &DeployGateInput) -> GateDecision {
        if input.freeze_active {
            return GateDecision::Hold {
                reason: format!("deploy freeze active for {}", input.environment),
            };
        }
        if !input.active_incidents.is_empty() {
            return GateDecision::Hold {
                reason: format!(
                    "active incidents blocking deploy: {}",
                    input.active_incidents.join(", ")
                ),
            };
        }
        if !input.main_ci_green {
            return GateDecision::Reject {
                reason: format!("main CI is not green for {}", input.head_sha),
            };
        }
        GateDecision::Allow
    }

    fn classify_burner(&self, proc: &BurnerProcess) -> Option<String> {
        let is_named_burner = self
            .config
            .burner_names
            .iter()
            .any(|name| name == &proc.comm);
        if !is_named_burner {
            return None;
        }
        if proc.elapsed_secs >= self.config.burner_limit {
            return Some(format!("burner {} ({}s)", proc.comm, proc.elapsed_secs));
        }
        None
    }
}

// ============================================================================
// 6. ProjectPolicy (Unified Repository Policy & Quotas)
// ============================================================================

/// Summary of concurrency, retry, batch, and janitor quotas across all decision domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyQuotas {
    pub agent_slots: i32,
    pub max_cpu_percent: u32,
    pub resume_limit: u32,
    pub batch_size: usize,
    pub max_reruns: u32,
    pub max_runs: u32,
    pub build_idle_secs: i64,
}

/// Unified repository policy aggregating [`DispatchConfig`], [`VerifyConfig`], [`LandConfig`],
/// [`WatchConfig`], [`OpsConfig`], [`AreaRule`], and [`ProviderSpec`] definitions.
///
/// Teams can deserialize `ProjectPolicy` directly from `tacho.toml` (or JSON) to configure:
/// - Area globs and path rules (`area_rules`, `verify.areas.path_rules`)
/// - CI job rules (`verify.ci_runner.job_of_area`, `verify.ci_files`, `land.ci_paths`)
/// - Conflict/generator rules (`land.generated`, `land.generators`, `verify.areas.generated`)
/// - Screenshot scope (`land.ui_roots`, `land.shot_dirs`, `verify.areas.pixel_files`)
/// - Burner process names (`watch.burner_names`)
/// - Pool, batch, retry, and janitor quotas
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectPolicy {
    #[serde(default)]
    pub dispatch: DispatchConfig,
    #[serde(default)]
    pub verify: VerifyConfig,
    #[serde(default)]
    pub land: LandConfig,
    #[serde(default)]
    pub watch: WatchConfig,
    #[serde(default)]
    pub ops: OpsConfig,
    #[serde(default = "default_area_rules")]
    pub area_rules: Vec<AreaRule>,
    #[serde(default = "builtin_providers")]
    pub providers: BTreeMap<String, ProviderSpec>,
}

fn default_area_rules() -> Vec<AreaRule> {
    vec![
        AreaRule {
            area: "core".to_string(),
            prefixes: vec!["crates/".to_string(), "src/".to_string()],
            unit_prefixes: BTreeMap::new(),
            checks: Vec::new(),
            isolate_batch: false,
            needs_screenshot: false,
        },
        AreaRule {
            area: "ci".to_string(),
            prefixes: vec![".github/".to_string(), "scripts/".to_string()],
            unit_prefixes: BTreeMap::new(),
            checks: Vec::new(),
            isolate_batch: true,
            needs_screenshot: false,
        },
    ]
}

impl Default for ProjectPolicy {
    fn default() -> Self {
        let mut verify = VerifyConfig::default();
        if verify.areas.path_rules.is_empty() {
            verify.areas.path_rules.push(PathAreaRule {
                pattern: r"^crates/".to_string(),
                areas: BTreeSet::from(["core".to_string()]),
                train_areas: None,
            });
        }
        if verify.areas.pixel_files.is_empty() {
            verify.areas.pixel_files.push(r"\.png$".to_string());
        }
        if verify.areas.generated.is_empty() {
            verify
                .areas
                .generated
                .insert(r"Cargo\.lock$".to_string(), "core".to_string());
        }
        if verify.ci_runner.job_of_area.is_empty() {
            verify
                .ci_runner
                .job_of_area
                .insert("core".to_string(), "ci".to_string());
        }

        let mut land = LandConfig::default();
        if land.generated.is_empty() {
            land.generated
                .push(("core".to_string(), r"Cargo\.lock$".to_string()));
        }
        if land.generators.is_empty() {
            land.generators.insert(
                "Cargo.lock".to_string(),
                "cargo generate-lockfile".to_string(),
            );
        }
        if land.ui_roots.is_empty() {
            land.ui_roots.push("web/".to_string());
        }
        if land.shot_dirs.is_empty() {
            land.shot_dirs
                .push(("docs/screenshots/".to_string(), "ui".to_string()));
        }

        Self {
            dispatch: DispatchConfig::default(),
            verify,
            land,
            watch: WatchConfig::default(),
            ops: OpsConfig::default(),
            area_rules: default_area_rules(),
            providers: builtin_providers(),
        }
    }
}

impl ProjectPolicy {
    /// Configured area rules mapping file path prefixes/globs to verification areas.
    pub fn area_globs(&self) -> &[AreaRule] {
        &self.area_rules
    }

    /// Configured tier path rules (`verify.areas.path_rules`).
    pub fn path_area_rules(&self) -> &[PathAreaRule] {
        &self.verify.areas.path_rules
    }

    /// Required CI job mapping (`verify.ci_runner.job_of_area`) and CI pipeline file regexes (`verify.ci_files`).
    pub fn ci_job_rules(&self) -> (&BTreeMap<String, String>, &[String]) {
        (&self.verify.ci_runner.job_of_area, &self.verify.ci_files)
    }

    /// Conflict generated-file patterns, generator commands, and verify generated area map.
    #[allow(clippy::type_complexity)]
    pub fn conflict_generator_rules(
        &self,
    ) -> (
        &[(String, String)],
        &BTreeMap<String, String>,
        &BTreeMap<String, String>,
    ) {
        (
            &self.land.generated,
            &self.land.generators,
            &self.verify.areas.generated,
        )
    }

    /// UI roots, screenshot directories, and pixel file patterns.
    pub fn screenshot_scope(&self) -> (&[String], &[(String, String)], &[String]) {
        (
            &self.land.ui_roots,
            &self.land.shot_dirs,
            &self.verify.areas.pixel_files,
        )
    }

    /// Process `comm` names monitored for orphan/runaway CPU burner reaping.
    pub fn burner_names(&self) -> &[String] {
        &self.watch.burner_names
    }

    /// Consolidated pool, batch, retry, and janitor quotas.
    pub fn quotas(&self) -> PolicyQuotas {
        PolicyQuotas {
            agent_slots: self.dispatch.slots,
            max_cpu_percent: self.dispatch.max_cpu_percent,
            resume_limit: self.dispatch.resume_limit,
            batch_size: self.verify.batch_size,
            max_reruns: self.verify.reruns,
            max_runs: self.watch.max_runs,
            build_idle_secs: self.ops.tidy.build_idle,
        }
    }

    /// Validate policy invariants (positive slots, non-empty batch size, shell-free provider specs).
    pub fn validate(&self) -> Result<(), String> {
        if self.dispatch.slots <= 0 {
            return Err("dispatch.slots must be > 0".to_string());
        }
        if self.verify.batch_size == 0 {
            return Err("verify.batch_size must be > 0".to_string());
        }
        for (name, spec) in &self.providers {
            ensure_no_shell_argv(&spec.binary, &spec.start_args)
                .map_err(|e| format!("provider '{name}' start_args invalid: {e}"))?;
            if !spec.resume_args.is_empty() {
                ensure_no_shell_argv(&spec.binary, &spec.resume_args)
                    .map_err(|e| format!("provider '{name}' resume_args invalid: {e}"))?;
            }
        }
        Ok(())
    }

    /// Apply this `ProjectPolicy` onto a [`DecisionState`](crate::decide::DecisionState).
    pub fn apply_to_state(&self, state: &mut crate::decide::DecisionState) {
        if state.area_rules.is_empty() {
            state.area_rules = self.area_rules.clone();
        }
        if state.tidy_min_age_secs <= 0 {
            state.tidy_min_age_secs = self.ops.tidy.build_idle;
        }
    }
}
