//! World observation for the plan executor: turns fleet state plus what is really on the host
//! (process table, cgroups, git worktrees, persisted forge facts) into the pure
//! `(DecisionState, WorldObservation)` input of [`tacho_core::decide::decide`].
//!
//! Nothing here invents a value. A signal that cannot be read is left out (`None`, empty, or
//! the sample is skipped) so `decide` treats it conservatively; progress samples carry the
//! real timestamp they were taken at and `decide` appends them to the persisted history, so a
//! replay of a recorded tick sees exactly what happened.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use tacho_core::decide::verify::{flake_tasks, MainCiObs};
use tacho_core::decide::watch::{
    BehindCommit, CheckoutCandidate, DeployRunObs, ProcEntry, ProgressSample, TidySnapshot,
    WorktreeCandidate,
};
use tacho_core::decide::{BatchOutcomeObs, BurnerProcess, DecisionState, WorldObservation};

use super::identity::{pid_alive_matching, proc_pid_starttime};
use crate::fleet::FleetState;

/// Per-git-call cap for local observations (rev-parse, status, ls-files).
const GIT_CAP: Duration = Duration::from_secs(5);

/// Collects host `/proc` entries (ppid, cpu ticks, real age from starttime, cwd, argv).
pub fn scan_procs() -> Vec<ProcEntry> {
    use std::os::unix::fs::MetadataExt;
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return out;
    };
    let my_uid = unsafe { libc::geteuid() };
    let uptime_secs = fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse::<f64>().ok());
    let clk_tck = crate::sampler::get_clock_ticks_per_sec().max(1) as f64;
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(String::from) else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        if entry.metadata().is_ok_and(|m| m.uid() != my_uid) {
            continue;
        }
        let p_dir = entry.path();
        let Ok(stat) = fs::read_to_string(p_dir.join("stat")) else {
            continue;
        };
        let (Some(lparen), Some(rparen)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let comm = stat[lparen + 1..rparen].to_string();
        let fields: Vec<&str> = stat[rparen + 1..].split_whitespace().collect();
        if fields.len() < 20 {
            continue;
        }
        let parse = |i: usize| fields[i].parse::<u64>().ok();
        let (Some(ppid), Some(pgid), Some(utime), Some(stime), Some(start)) =
            (parse(1), parse(2), parse(11), parse(12), parse(19))
        else {
            continue;
        };
        if pid == 2 || ppid == 2 {
            continue;
        }
        // Age from the kernel's start time; unknown uptime means unknown age (0 = never
        // old enough for an age-based rule to fire).
        let elapsed_secs = uptime_secs
            .map(|up| (up - start as f64 / clk_tck).max(0.0) as i64)
            .unwrap_or(0);
        let args = fs::read(p_dir.join("cmdline"))
            .map(|b| {
                b.split(|&c| c == 0)
                    .filter(|s| !s.is_empty())
                    .map(|s| String::from_utf8_lossy(s).to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_else(|_| comm.clone());
        let cwd = fs::read_link(p_dir.join("cwd"))
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let env_starve = fs::read(p_dir.join("environ"))
            .map(|b| b.split(|&c| c == 0).any(|kv| kv == b"TACHO_STARVE=1"))
            .unwrap_or(false);
        out.push(ProcEntry {
            pid,
            ppid: ppid as u32,
            pgid: pgid as u32,
            state: fields[0].to_string(),
            cpu_ticks: utime.saturating_add(stime),
            elapsed_secs,
            comm,
            args,
            cwd,
            env_starve,
        });
    }
    out
}

/// CPU ticks of the agent's whole process tree: the `/proc` tree under `pid` (children
/// included) or, when the agent runs in its own tacho cgroup scope, that scope's
/// `usage_usec` (which also counts reparented and exited children), whichever is larger.
pub fn agent_cpu_ticks(pid: u32, procs: &[ProcEntry]) -> Option<u64> {
    if !procs.iter().any(|p| p.pid == pid) {
        return None;
    }
    let tree = tacho_core::decide::watch::tree_cpu(pid, procs);
    let scope = fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .ok()
        .and_then(|c| {
            c.lines()
                .find_map(crate::cgroup::parse_cgroup_line)
                .map(str::to_string)
        })
        .and_then(|path| {
            let stat = fs::read_to_string(
                Path::new("/sys/fs/cgroup")
                    .join(path.trim_start_matches('/'))
                    .join("cpu.stat"),
            )
            .ok()?;
            stat.lines()
                .find_map(|l| l.strip_prefix("usage_usec "))
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
        .map(|usec| usec.saturating_mul(crate::sampler::get_clock_ticks_per_sec()) / 1_000_000);
    Some(tree.max(scope.unwrap_or(0)))
}

/// Real git state of a worktree: HEAD sha, dirty-file count, newest tracked/untracked mtime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorktreeObs {
    pub head_sha: Option<String>,
    pub dirty_files: Option<u32>,
    pub newest_mtime: Option<i64>,
}

fn is_internal_kit_porcelain_line(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return true;
    }
    let rel = line.get(3..).unwrap_or(trimmed).trim();
    matches!(
        rel,
        ".tachoenv" | ".tachoenv.tmp" | ".tachoproof" | ".tachorun" | ".tacho-proof" | ".tacho-run"
    ) || rel.starts_with(".tachoproof/")
        || rel.starts_with(".tachorun/")
        || rel.starts_with(".tacho-proof/")
        || rel.starts_with(".tacho-run/")
}

/// Observes `dir` with bounded git calls confined strictly to `dir` as its own worktree
/// toplevel; any part that cannot be read (or if `dir` is not its own toplevel) stays `None`.
pub fn observe_worktree(dir: &Path) -> WorktreeObs {
    if !is_git_checkout(dir) {
        return WorktreeObs::default();
    }
    let head_sha = crate::sampler::run_git_in_worktree(dir, &["rev-parse", "HEAD"], GIT_CAP)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let dirty_files = crate::sampler::run_git_in_worktree(
        dir,
        &["status", "--porcelain", "--untracked-files=all"],
        GIT_CAP,
    )
    .ok()
    .map(|s| {
        s.lines()
            .filter(|l| !is_internal_kit_porcelain_line(l))
            .count() as u32
    });
    let newest_mtime = crate::sampler::scan_worktree_files(dir, GIT_CAP)
        .ok()
        .map(|(ns, _, _)| (ns / 1_000_000_000) as i64)
        .filter(|&s| s > 0);
    WorktreeObs {
        head_sha,
        dirty_files,
        newest_mtime,
    }
}

/// One progress sample from real observations taken at `now`.
///
/// Returns `None` when the agent's process tree cannot be read: a sample without its CPU
/// signal would look like a silent agent. Unknown worktree signals stay empty / `None`
/// (an unchanged empty head or zero dirty count is never counted as progress).
pub fn progress_sample(
    now: i64,
    pid: Option<u32>,
    log: &str,
    worktree: &str,
    procs: &[ProcEntry],
) -> Option<ProgressSample> {
    let cpu_ticks = agent_cpu_ticks(pid?, procs)?;
    let log_bytes = if log.is_empty() {
        0
    } else {
        fs::metadata(log).map(|m| m.len()).unwrap_or(0)
    };
    let wt = observe_worktree(Path::new(worktree));
    Some(ProgressSample {
        ts: now,
        log_bytes,
        head_sha: wt.head_sha.unwrap_or_default(),
        dirty_files: wt.dirty_files.unwrap_or(0),
        cpu_ticks,
        newest_mtime: wt.newest_mtime,
    })
}

/// Agents whose process is alive and verified: identity-checked fleet agents plus running
/// agents the recorder knows about.
pub fn live_agent_keys(state: &FleetState, live_state: &tacho_core::LiveState) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = live_state
        .agents
        .iter()
        .filter(|(_, a)| a.state != "Exited")
        .map(|(k, _)| k.clone())
        .collect();
    for a in state.agents.values() {
        if a.finished_at.is_none()
            && a.pid
                .is_some_and(|pid| pid_alive_matching(pid, a.starttime))
        {
            out.insert(a.key.clone());
        }
    }
    out
}

/// Builds a [`TidySnapshot`] with real dirty / unpushed checks for every recorded worktree.
pub fn build_tidy_snapshot(
    state: &FleetState,
    live_state: &tacho_core::LiveState,
    live_keys: &BTreeSet<String>,
    now: i64,
) -> TidySnapshot {
    let mut live_wt = BTreeSet::new();
    let mut held_ports = BTreeSet::new();
    for a in state.agents.values() {
        if live_keys.contains(&a.key) && !a.worktree.is_empty() {
            live_wt.insert(a.worktree.clone());
        }
    }
    for a in live_state.agents.values() {
        if a.state != "Exited" {
            if let Some(cwd) = a.cwd.as_ref().filter(|c| !c.is_empty()) {
                live_wt.insert(cwd.clone());
            }
        }
    }
    for wt in state.worktrees.values() {
        if live_wt.contains(&wt.path) {
            for p in wt.port..=wt.port_last {
                held_ports.insert(p);
            }
        }
    }
    let mut held_paths = BTreeSet::new();
    for h_key in state.holds.keys() {
        held_paths.insert(h_key.clone());
        if let Some(wt) = state.worktrees.get(h_key) {
            held_paths.insert(wt.path.clone());
        }
    }
    for a in state.agents.values() {
        if !a.worktree.is_empty() && !is_linked_worktree(Path::new(&a.worktree)) {
            held_paths.insert(a.worktree.clone());
        }
    }

    let mut worktrees = Vec::new();
    let mut recorded_paths = BTreeSet::new();
    let mut parent_wt_dirs = BTreeSet::new();
    for wt in state.worktrees.values() {
        let p = Path::new(&wt.path);
        if wt.path.is_empty() || !p.exists() {
            continue;
        }
        recorded_paths.insert(wt.path.clone());
        // The main checkout (or any non-linked directory) can never be a removal target;
        // mark it held so `decide` never plans `RemoveTree` for it even if an agent retired.
        if !is_linked_worktree(p) {
            held_paths.insert(wt.path.clone());
            continue;
        }
        if let Some(parent) = p.parent() {
            if parent
                .file_name()
                .is_some_and(|n| n == "worktrees" || n == ".worktrees")
            {
                parent_wt_dirs.insert(parent.to_path_buf());
            }
        }
        let wt_obs = observe_worktree(p);
        let last_mod = match wt_obs.newest_mtime {
            Some(m) if m > 0 => m,
            _ => wt.created_at,
        };
        let age_secs = (now - last_mod).max(0);
        // Unknown dirtiness is treated as dirty, so the janitor rescues before removing.
        let dirty = wt_obs.dirty_files.map(|n| n > 0).unwrap_or(true);
        let mut unpushed_commits = if wt.base_sha.is_empty() {
            0
        } else {
            crate::sampler::run_git_in_worktree(
                p,
                &["rev-list", "--count", &format!("{}..HEAD", wt.base_sha)],
                GIT_CAP,
            )
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(0)
        };
        if wt.base_sha.is_empty() || wt_obs.head_sha.as_deref() != Some(wt.base_sha.as_str()) {
            if let Ok(remote_branches) = crate::sampler::run_git_in_worktree(
                p,
                &["branch", "-r", "--contains", "HEAD"],
                GIT_CAP,
            ) {
                if remote_branches.trim().is_empty()
                    && wt_obs
                        .head_sha
                        .as_deref()
                        .is_some_and(|h| h != wt.base_sha.as_str())
                {
                    unpushed_commits = unpushed_commits.max(1);
                }
            }
        }
        let pr = state.agents.get(&wt.slug).and_then(|a| a.pr).or(wt.base_pr);
        let is_retired = state.retired.contains(&wt.slug)
            || state
                .retired
                .contains(tacho_core::decide::dispatch::base_key(&wt.slug));
        let pr_merged_or_closed = is_retired
            || pr
                .is_some_and(|pr| state.merged_prs.contains(&pr) || state.closed_prs.contains(&pr));
        worktrees.push(WorktreeCandidate {
            path: wt.path.clone(),
            branch: wt.branch.clone(),
            age_secs,
            dirty,
            unpushed_commits,
            pr_merged_or_closed,
        });
    }

    if let Some(home) = std::env::var_os("TACHO_HOME") {
        let home_wt = Path::new(&home).join("worktrees");
        if home_wt.is_dir() {
            parent_wt_dirs.insert(home_wt);
        }
    }
    let mut checkouts = Vec::new();
    for wt_root in parent_wt_dirs {
        let Ok(entries) = fs::read_dir(&wt_root) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if !p.is_dir() {
                continue;
            }
            let path_str = p.to_string_lossy().to_string();
            if recorded_paths.contains(&path_str) || !is_linked_worktree(&p) {
                continue;
            }
            let wt_obs = observe_worktree(&p);
            let dir_mtime = fs::metadata(&p)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(now);
            let last_mod = wt_obs.newest_mtime.filter(|&m| m > 0).unwrap_or(dir_mtime);
            let dirty = wt_obs.dirty_files.map(|n| n > 0).unwrap_or(true);
            let unpushed_commits = crate::sampler::run_git_in_worktree(
                &p,
                &["branch", "-r", "--contains", "HEAD"],
                GIT_CAP,
            )
            .ok()
            .map(|out| {
                if out.trim().is_empty() && wt_obs.head_sha.is_some() {
                    1
                } else {
                    0
                }
            })
            .unwrap_or(0);
            checkouts.push(CheckoutCandidate {
                path: path_str,
                age_secs: (now - last_mod).max(0),
                dirty,
                unpushed_commits,
            });
        }
    }

    TidySnapshot {
        now,
        worktrees,
        checkouts,
        containers: Vec::new(),
        networks: Vec::new(),
        live_agent_worktrees: live_wt,
        live_batch_worktrees: BTreeSet::new(),
        held_paths,
        held_ports,
    }
}

/// Whether `dir` is itself a git checkout whose `--show-toplevel` is `dir`.
pub fn is_git_checkout(dir: &Path) -> bool {
    if dir.as_os_str().is_empty() || !dir.join(".git").exists() {
        return false;
    }
    crate::sampler::run_git_in_worktree(dir, &["rev-parse", "--show-toplevel"], GIT_CAP).is_ok()
}

/// Whether `dir` is a linked git worktree (`git worktree add`), not a main repository checkout
/// or a directory holding one (`dir/.git` must be a gitfile rather than a `.git/` directory,
/// `--git-dir` must differ from `--git-common-dir`, and `--git-common-dir` must not reside inside `dir`).
pub fn is_linked_worktree(dir: &Path) -> bool {
    if dir.as_os_str().is_empty() || !dir.join(".git").is_file() || !is_git_checkout(dir) {
        return false;
    }
    let Ok(canon) = dir.canonicalize() else {
        return false;
    };
    let Ok(out) = crate::sampler::run_git_in_worktree(
        dir,
        &["rev-parse", "--git-dir", "--git-common-dir"],
        GIT_CAP,
    ) else {
        return false;
    };
    let mut lines = out.lines().map(str::trim).filter(|s| !s.is_empty());
    let (Some(git_dir), Some(common_dir)) = (lines.next(), lines.next()) else {
        return false;
    };
    let gd = dir.join(git_dir).canonicalize().ok();
    let cd = dir.join(common_dir).canonicalize().ok();
    match (gd, cd) {
        (Some(g), Some(c)) => g != c && !c.starts_with(&canon),
        _ => false,
    }
}

/// Resolves the main repository checkout path that owns `dir` (either `dir` itself when
/// `dir/.git` is a directory, or the parent of `--git-common-dir` when `dir` is a linked worktree).
///
/// Never walks parent directories above `dir`, so a directory inside an unrelated enclosing
/// checkout is never mistaken for a repository root.
pub fn owning_main_checkout(dir: &Path) -> Option<std::path::PathBuf> {
    if dir.as_os_str().is_empty() || !dir.exists() {
        return None;
    }
    let canon = dir.canonicalize().ok()?;
    let dot_git = canon.join(".git");
    if dot_git.is_dir() {
        return Some(canon);
    }
    if dot_git.is_file() {
        let out = crate::sampler::run_git_in_worktree(
            &canon,
            &["rev-parse", "--git-common-dir"],
            GIT_CAP,
        )
        .ok()?;
        let common = canon.join(out.trim()).canonicalize().ok()?;
        return common.parent().map(Path::to_path_buf);
    }
    None
}

/// Resolves the fleet's recorded main checkout path from `state` (`repo_root`, `worktrees`,
/// or `agents`), `TACHO_REPO`, or `repo`.
pub fn fleet_main_checkout(state: &FleetState, repo: Option<&Path>) -> Option<std::path::PathBuf> {
    if let Some(root) = state.repo_root.as_deref().filter(|s| !s.is_empty()) {
        let p = Path::new(root);
        if let Some(m) = owning_main_checkout(p).or_else(|| p.canonicalize().ok()) {
            return Some(m);
        }
    }
    if let Some(env_repo) = std::env::var_os("TACHO_REPO").filter(|s| !s.is_empty()) {
        let p = Path::new(&env_repo);
        if let Some(m) = owning_main_checkout(p).or_else(|| p.canonicalize().ok()) {
            return Some(m);
        }
    }
    if let Some(r) = repo {
        if let Some(m) = owning_main_checkout(r).or_else(|| r.canonicalize().ok()) {
            return Some(m);
        }
    }
    for wt in state.worktrees.values() {
        if let Some(m) = owning_main_checkout(Path::new(&wt.path)) {
            return Some(m);
        }
    }
    for a in state.agents.values() {
        let p = Path::new(&a.worktree);
        if is_linked_worktree(p) {
            if let Some(m) = owning_main_checkout(p) {
                return Some(m);
            }
        }
    }
    for a in state.agents.values() {
        let p = Path::new(&a.worktree);
        if p.join(".git").is_dir() {
            if let Ok(c) = p.canonicalize() {
                return Some(c);
            }
        }
    }
    None
}

/// Whether `dir` resolves to the fleet's recorded main checkout (`fleet_main`), or, when
/// `fleet_main` is `None`, whether `dir` itself is a `.git` repository root.
///
/// Never scans child directories and never trusts an enclosing repository unless it matches
/// an explicit `fleet_main`.
pub fn resolves_to_fleet_main_checkout(dir: &Path, fleet_main: Option<&Path>) -> bool {
    if dir.as_os_str().is_empty() || !dir.exists() || is_linked_worktree(dir) {
        return false;
    }
    let Ok(canon) = dir.canonicalize() else {
        return false;
    };
    if let Some(main_root) = fleet_main {
        let main_canon = main_root
            .canonicalize()
            .unwrap_or_else(|_| main_root.to_path_buf());
        if !canon.starts_with(&main_canon) {
            return false;
        }
        if canon == main_canon {
            return true;
        }
        let Some(enclosing) = canon.ancestors().find(|anc| anc.join(".git").exists()) else {
            return true;
        };
        if !enclosing.join(".git").is_dir() && is_linked_worktree(enclosing) {
            return false;
        }
        return enclosing == main_canon;
    }
    canon.join(".git").is_dir()
}

/// Whether `dir` resolves to the repository's main checkout (or a subdirectory inside it).
pub fn resolves_to_main_checkout(dir: &Path) -> bool {
    let env_main = std::env::var_os("TACHO_REPO")
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from);
    resolves_to_fleet_main_checkout(dir, env_main.as_deref())
}

/// Validates that `dir` is an isolated linked git worktree on a named branch other than `main`/`master`.
///
/// Returns the checked-out branch name on success, or a refusal reason when `dir` resolves to the
/// main checkout, is not a linked git worktree, has a detached `HEAD`, or is on `main`/`master`.
pub fn verify_worker_worktree(dir: &Path) -> Result<String, String> {
    if dir.as_os_str().is_empty() || !dir.exists() {
        return Err(format!(
            "refusing to start worker in '{}': directory does not exist",
            dir.display()
        ));
    }
    if resolves_to_main_checkout(dir) {
        return Err(format!(
            "refusing to start worker in {}: directory resolves to the main checkout",
            dir.display()
        ));
    }
    if !is_linked_worktree(dir) {
        return Err(format!(
            "refusing to start worker in {}: directory is not an isolated linked git worktree",
            dir.display()
        ));
    }
    let branch = crate::sampler::run_git_in_worktree(
        dir,
        &["symbolic-ref", "--short", "-q", "HEAD"],
        GIT_CAP,
    )
    .map_err(|_| {
        format!(
            "refusing to start worker in {}: HEAD is detached (worker must run on a named branch)",
            dir.display()
        )
    })?;
    let branch = branch.trim().to_string();
    if branch.is_empty() || branch == "main" || branch == "master" {
        return Err(format!(
            "refusing to start worker in {}: branch '{}' is not an isolated worker branch",
            dir.display(),
            branch
        ));
    }
    Ok(branch)
}

/// Whether `start` or an ancestor has a `.git` entry.
pub fn inside_git_checkout(start: &Path) -> bool {
    start.ancestors().any(|d| d.join(".git").exists())
}

/// Git facts about the checkout the tick runs against.
#[derive(Debug, Clone, Default)]
pub struct RepoObs {
    pub detached: bool,
    pub behind_commits: Vec<BehindCommit>,
}

/// Local (no network) git observations of `repo`.
pub fn observe_repo(repo: &Path) -> RepoObs {
    if !inside_git_checkout(repo) {
        return RepoObs::default();
    }
    let detached = crate::sampler::run_git(repo, &["symbolic-ref", "-q", "HEAD"], GIT_CAP).is_err();
    let behind_commits = crate::sampler::run_git(
        repo,
        &["log", "--format=%H:%ct", "HEAD..origin/main"],
        GIT_CAP,
    )
    .map(|out| {
        out.lines()
            .filter_map(|l| {
                let (sha, ts) = l.trim().split_once(':')?;
                Some(BehindCommit {
                    sha: sha.to_string(),
                    ts: ts.parse().ok()?,
                    ci_green: true,
                })
            })
            .collect()
    })
    .unwrap_or_default();
    RepoObs {
        detached,
        behind_commits,
    }
}

/// Local main sha of `repo`: `refs/heads/main`, else `HEAD`. Network lookups belong to the
/// world refresher, which persists `FleetState::main_sha`.
pub fn local_main_sha(repo: &Path) -> Option<String> {
    if !inside_git_checkout(repo) {
        return None;
    }
    ["refs/heads/main", "HEAD"].iter().find_map(|r| {
        crate::sampler::run_git(repo, &["rev-parse", r], GIT_CAP)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
}

/// Main-CI observation: `TACHO_MAIN_CI_JSON` override or the persisted last completed run,
/// with each `main-broken` batch's main checked for ancestry in `repo`.
pub fn observe_main_ci(state: &FleetState, repo: Option<&Path>) -> Option<MainCiObs> {
    let env_ci = env_json::<tacho_core::decide::ops::snapshot::MainCiRun>("TACHO_MAIN_CI_JSON");
    let run = env_ci.as_ref().or(state.main_ci.as_ref());
    tacho_core::decide::verify::unbreak::observe_main_ci(run, &state.batches, |a, b| {
        repo.is_some_and(|repo| {
            crate::sampler::run_git(repo, &["merge-base", "--is-ancestor", a, b], GIT_CAP).is_ok()
        })
    })
}

/// Batches that finished as a definite pass or fail since the previous tick.
pub fn new_batch_outcomes(state: &FleetState) -> Vec<BatchOutcomeObs> {
    let prev_tick_ts = state.last_tick_ts.unwrap_or(0);
    state
        .batches
        .iter()
        .filter(|b| b.finished_at.is_some_and(|t| t > prev_tick_ts))
        .filter(|b| b.status == "passed" || b.status == "failed")
        .map(|b| BatchOutcomeObs {
            batch_id: b.id.clone(),
            passed: b.status == "passed",
            failed_area: b.areas.first().cloned(),
            log_output: b.log.clone(),
            tree_sha: b.tree.clone().unwrap_or_default(),
        })
        .collect()
}

/// Converts persistent [`FleetState`] into the pure [`DecisionState`], adding flake tasks.
pub fn decision_state_of(state: &FleetState, now: i64) -> DecisionState {
    let (flakes, new_flake_tasks) = flake_tasks(
        &state.flakes,
        &state.todo,
        &state.agents,
        &state.retired,
        now,
    );
    let mut todo = state.todo.clone();
    for item in new_flake_tasks {
        if !todo.iter().any(|t| t.key == item.key) {
            todo.push(item);
        }
    }
    DecisionState {
        max_parallel: state.max_workers,
        todo,
        agents: state.agents.clone(),
        retired: state.retired.clone(),
        holds: state.holds.clone(),
        passed: state.passed.clone(),
        failed: state.failed.clone(),
        merged_prs: state.merged_prs.clone(),
        closed_prs: state.closed_prs.clone(),
        batches: state.batches.clone(),
        flakes,
        samples: state.samples.clone(),
        nudged: state.nudged.clone(),
        rebase_attempts: state.rebase_attempts.clone(),
        fix_notified: state.fix_notified.clone(),
        notified_keys: state.notified_failures.clone(),
        area_rules: state.area_rules.clone(),
        refused_self_update_sha: state.refused_self_update_sha.clone(),
        last_tick_ts: state.last_tick_ts,
        inbox: state.inbox.clone(),
        last_watchdog_alert_ts: state.last_watchdog_alert_ts,
        tidy_min_age_secs: 3_600,
    }
}

/// Everything the host side contributes to one tick.
pub struct HostObs<'a> {
    pub now: i64,
    pub live_state: &'a tacho_core::LiveState,
    pub procs: &'a [ProcEntry],
    /// Root pid of each recorder-tracked run, by agent key (from the appender's active runs).
    pub run_pids: BTreeMap<String, u32>,
    /// Checkout the tick runs against (git observations, ancestry); `None` = unknown.
    pub repo: Option<&'a Path>,
    pub main_sha: String,
    /// Optional `(total_bytes, avail_bytes)` of `/` observed for this tick.
    pub root_disk_bytes: Option<(u64, u64)>,
}

/// Reads `(total_bytes, avail_bytes)` of the root filesystem (`/`) via `statvfs(3)`.
pub fn statvfs_root_disk() -> Option<(u64, u64)> {
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c"/".as_ptr(), &mut st) != 0 {
            return None;
        }
        let frsize = if st.f_frsize > 0 {
            st.f_frsize
        } else {
            st.f_bsize
        };
        let total = st.f_blocks.saturating_mul(frsize);
        let avail = st.f_bavail.saturating_mul(frsize);
        (total > 0).then_some((total, avail))
    }
}

/// Agent progress samples for every live agent, from real observations only.
pub fn agent_samples(
    state: &FleetState,
    host: &HostObs<'_>,
    live_keys: &BTreeSet<String>,
) -> BTreeMap<String, ProgressSample> {
    let (live_state, procs, now) = (host.live_state, host.procs, host.now);
    let mut out = BTreeMap::new();
    for key in live_keys {
        let fleet = state.agents.get(key);
        let live = live_state.agents.get(key);
        let pid = fleet
            .and_then(|a| a.pid)
            .or_else(|| host.run_pids.get(key).copied())
            .or_else(|| live.and_then(|a| a.pid));
        let log = fleet.map(|a| a.log.as_str()).unwrap_or("");
        let worktree = fleet
            .map(|a| a.worktree.as_str())
            .filter(|w| !w.is_empty())
            .or_else(|| live.and_then(|a| a.cwd.as_deref()))
            .unwrap_or("");
        if let Some(sample) = progress_sample(now, pid, log, worktree, procs) {
            out.insert(key.clone(), sample);
        }
    }
    out
}

/// Builds the full `decide` input from fleet state and host observations.
pub fn observe(state: &FleetState, host: &HostObs<'_>) -> (DecisionState, WorldObservation) {
    let now = host.now;
    let live_keys = live_agent_keys(state, host.live_state);
    let mut agent_log_tails = BTreeMap::new();
    for (k, a) in &state.agents {
        if a.finished_at.is_none() && !live_keys.contains(k) && !a.log.is_empty() {
            let tail = fs::read_to_string(&a.log)
                .map(|s| {
                    let lines: Vec<&str> = s.lines().rev().take(50).collect();
                    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
                })
                .unwrap_or_default();
            agent_log_tails.insert(k.clone(), tail);
        }
    }
    let agent_samples = agent_samples(state, host, &live_keys);
    let repo_obs = host.repo.map(observe_repo).unwrap_or_default();

    let mut ready_trains = Vec::new();
    if let Some(open) = tacho_core::decide::land::open_for(&state.trains) {
        let heads = open
            .prs
            .iter()
            .filter_map(|&pr| {
                state
                    .open_prs_cache
                    .iter()
                    .find(|p| p.number == pr)
                    .map(|p| (pr, p.head.clone()))
            })
            .collect();
        ready_trains.push((open.id.clone(), open.prs.clone(), heads));
    }

    let agent_roots: BTreeSet<u32> = live_keys
        .iter()
        .filter_map(|k| {
            state
                .agents
                .get(k)
                .and_then(|a| a.pid)
                .or_else(|| host.run_pids.get(k).copied())
                .or_else(|| host.live_state.agents.get(k).and_then(|a| a.pid))
        })
        .collect();
    let known_worktrees: BTreeSet<&str> = state
        .worktrees
        .values()
        .map(|w| w.path.as_str())
        .chain(state.agents.values().map(|a| a.worktree.as_str()))
        .filter(|p| !p.is_empty())
        .collect();
    let burners = tacho_core::decide::watch::burners(host.procs)
        .into_iter()
        .filter(|b| {
            let in_agent_tree = agent_roots
                .iter()
                .any(|&root| tacho_core::decide::watch::below(root, host.procs).contains(&b.pid));
            let in_worktree = known_worktrees.iter().any(|wt| {
                !b.cwd.is_empty()
                    && (b.cwd == *wt
                        || b.cwd.starts_with(&format!("{}/", wt.trim_end_matches('/'))))
            });
            let in_cgroup = fs::read_to_string(format!("/proc/{}/cgroup", b.pid))
                .ok()
                .is_some_and(|cg| {
                    tacho_core::decide::watch::procs::in_agent_cgroup(
                        &cg,
                        &tacho_core::decide::watch::WatchConfig::default(),
                    ) || cg.contains("tacho-")
                });
            in_agent_tree || in_worktree || in_cgroup
        })
        .map(|b| BurnerProcess {
            pid: b.pid,
            comm: tacho_core::decide::watch::burner_command(&b.comm, &b.args)
                .unwrap_or_else(|| b.comm.clone()),
            elapsed_secs: b.elapsed_secs,
            starttime: proc_pid_starttime(b.pid),
        })
        .collect();

    let mut tidy_snapshot = build_tidy_snapshot(state, host.live_state, &live_keys, now);
    let candidate_paths: Vec<String> = tidy_snapshot
        .worktrees
        .iter()
        .map(|w| w.path.clone())
        .collect();
    let live_holds = super::tidy::scan_live_process_holds(host.procs, &candidate_paths);
    tidy_snapshot.held_paths.extend(live_holds.paths);
    tidy_snapshot.held_ports.extend(live_holds.ports);

    let fleet_main = fleet_main_checkout(state, host.repo);
    let mut main_checkout_workers = BTreeMap::new();
    for key in &live_keys {
        let dir = state
            .agents
            .get(key)
            .map(|a| a.worktree.as_str())
            .filter(|w| !w.is_empty())
            .or_else(|| {
                host.live_state
                    .agents
                    .get(key)
                    .and_then(|a| a.cwd.as_deref())
                    .filter(|w| !w.is_empty())
            });
        if let Some(d) = dir {
            if resolves_to_fleet_main_checkout(Path::new(d), fleet_main.as_deref()) {
                main_checkout_workers.insert(key.clone(), d.to_string());
            }
        }
    }

    let root_disk_bytes = host
        .root_disk_bytes
        .or_else(|| env_json::<(u64, u64)>("TACHO_ROOT_DISK_BYTES_JSON"));

    let world = WorldObservation {
        now,
        main_sha: host.main_sha.clone(),
        open_prs: state.open_prs_cache.clone(),
        merged_prs: state.merged_prs.clone(),
        closed_prs: state.closed_prs.clone(),
        tidy_snapshot,
        live_keys,
        agent_samples,
        agent_log_tails,
        batch_outcomes: new_batch_outcomes(state),
        deploy_runs: env_json::<Vec<DeployRunObs>>("TACHO_DEPLOY_RUNS_JSON").unwrap_or_default(),
        behind_commits: repo_obs.behind_commits,
        detached_checkout: repo_obs.detached,
        failed_main_jobs: env_json("TACHO_MAIN_FAILED_JOBS_JSON").unwrap_or_default(),
        ready_trains,
        burners,
        main_ci: observe_main_ci(state, host.repo),
        main_checkout_workers,
        root_disk_bytes,
    };
    (decision_state_of(state, now), world)
}

/// Explicit offline / test override of a forge observation, read from `var` as JSON.
pub fn env_json<T: serde::de::DeserializeOwned>(var: &str) -> Option<T> {
    let raw = std::env::var(var).ok()?;
    match serde_json::from_str(&raw) {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("[tacho] ignoring invalid {var}: {e}");
            None
        }
    }
}

/// Executes an explicit `argv` slice (no shell) in its own process group with a strict timeout,
/// waiting on its pidfd and killing the process group on expiration.
pub fn run_argv(cwd: Option<&Path>, argv: &[String], timeout: Duration) -> Result<String, String> {
    use std::io::Read;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    let Some((prog, args)) = argv.split_first() else {
        return Err("empty command argv".to_string());
    };
    let mut cmd = Command::new(prog);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if let Some(dir) = cwd.filter(|d| d.is_dir()) {
        cmd.current_dir(dir);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn {prog}: {e}"))?;
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    let out_reader = std::thread::Builder::new()
        .name("tacho-argv-stdout".to_string())
        .spawn(move || {
            let mut buf = Vec::new();
            stdout.read_to_end(&mut buf).map(|_| buf)
        })
        .map_err(|e| format!("failed to spawn stdout reader: {e}"))?;
    let err_reader = std::thread::Builder::new()
        .name("tacho-argv-stderr".to_string())
        .spawn(move || {
            let mut buf = Vec::new();
            stderr.read_to_end(&mut buf).map(|_| buf)
        })
        .map_err(|e| format!("failed to spawn stderr reader: {e}"))?;

    let exited = crate::sampler::wait_child_exit(&child, timeout)
        .map_err(|e| format!("wait_child_exit({prog}): {e}"))?;
    if !exited {
        let rc = unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ESRCH) {
                eprintln!("[tacho] failed to kill timed-out {prog} group: {err}");
            }
        }
    }
    let status = child
        .wait()
        .map_err(|e| format!("wait({prog}) failed: {e}"))?;
    let out_bytes = out_reader
        .join()
        .map_err(|_| format!("{prog} stdout reader panicked"))?
        .map_err(|e| format!("{prog} stdout read failed: {e}"))?;
    let err_bytes = err_reader
        .join()
        .map_err(|_| format!("{prog} stderr reader panicked"))?
        .map_err(|e| format!("{prog} stderr read failed: {e}"))?;
    if !exited {
        return Err(format!("{prog} timed out after {}s", timeout.as_secs()));
    }
    if !status.success() {
        let stderr_str = String::from_utf8_lossy(&err_bytes).trim().to_string();
        return Err(format!(
            "{prog} exited with {:?}: {}",
            status.code(),
            stderr_str
        ));
    }
    String::from_utf8(out_bytes).map_err(|e| format!("{prog} invalid UTF-8 stdout: {e}"))
}

/// Outcome of refreshing world state (`open_prs`, `main_sha`, `main_ci`) from the forge and git.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RefreshOutcome {
    /// `true` when any persisted world field (`open_prs_cache`, `main_sha`, `main_ci`) changed.
    pub changed: bool,
    /// Any forge/git refresh errors encountered (last-known state is preserved for failed fields).
    pub errors: Vec<String>,
    /// Fresh observations over the original state, including in read-only mode.
    pub observed: FleetState,
}

/// Queries the forge (`open_prs`, `main_ci_run`) and remote/local `main` SHA with bounded waits,
/// updating `FleetState` and emitting attention events on failure while retaining last-known state.
pub fn refresh_world(
    store: &mut crate::fleet::FleetStore,
    forge: &dyn tacho_core::decide::plugin::ForgePlugin,
    repo: Option<&Path>,
    hands: &mut dyn super::Hands,
    timeout: Duration,
    persist: bool,
) -> std::io::Result<RefreshOutcome> {
    let state = store.load()?;
    let now = super::now_secs();
    let mut outcome = RefreshOutcome::default();
    let mut runner = |argv: &[String]| run_argv(repo, argv, timeout);

    let new_prs =
        match env_json::<Vec<tacho_core::decide::dispatch::PullFacts>>("TACHO_OPEN_PRS_JSON") {
            Some(prs) => Some(prs),
            None => match forge.open_prs(&mut runner) {
                Ok(prs) => Some(prs),
                Err(e) => {
                    let msg = format!("forge {} open_prs refresh failed: {e}", forge.forge_name());
                    if persist {
                        store.record_decision(
                            "watch",
                            "refresh_open_prs_failed",
                            forge.forge_name(),
                            &msg,
                        )?;
                        hands.emit("system", "watch:attention", &msg);
                    }
                    outcome.errors.push(msg);
                    None
                }
            },
        };

    let workflow = state
        .workflow_path
        .as_deref()
        .and_then(|path| Path::new(path).file_name())
        .and_then(|name| name.to_str())
        .unwrap_or("CI");
    let new_ci =
        match env_json::<tacho_core::decide::ops::snapshot::MainCiRun>("TACHO_MAIN_CI_JSON") {
            Some(ci) => Some(Some(ci)),
            None => match forge.main_ci_run("main", workflow, &mut runner) {
                Ok(ci) => Some(ci),
                Err(e) => {
                    let msg = format!("forge {} main_ci refresh failed: {e}", forge.forge_name());
                    if persist {
                        store.record_decision(
                            "watch",
                            "refresh_main_ci_failed",
                            forge.forge_name(),
                            &msg,
                        )?;
                        hands.emit("system", "watch:attention", &msg);
                    }
                    outcome.errors.push(msg);
                    None
                }
            },
        };

    let new_sha = repo.filter(|r| inside_git_checkout(r)).and_then(|r| {
        crate::sampler::run_git(r, &["ls-remote", "origin", "refs/heads/main"], timeout)
            .ok()
            .and_then(|out| {
                out.lines()
                    .next()
                    .and_then(|l| l.split_whitespace().next())
                    .filter(|s| !s.is_empty())
                    .map(String::from)
            })
            .or_else(|| local_main_sha(r))
    });

    let prs_changed = new_prs
        .as_ref()
        .is_some_and(|prs| prs != &state.open_prs_cache);
    let ci_changed = new_ci.as_ref().is_some_and(|ci| ci != &state.main_ci);
    let sha_changed = new_sha
        .as_ref()
        .is_some_and(|sha| state.main_sha.as_ref() != Some(sha));
    outcome.changed = prs_changed || ci_changed || sha_changed;

    let update = |st: &mut FleetState| {
        if let Some(prs) = &new_prs {
            st.open_prs_cache = prs.clone();
            st.world_refreshed_at.insert("prs".to_string(), now);
        }
        if let Some(ci) = &new_ci {
            st.main_ci = ci.clone();
            st.world_refreshed_at.insert("ci".to_string(), now);
        }
        if let Some(sha) = &new_sha {
            st.main_sha = Some(sha.clone());
            st.world_refreshed_at.insert("main".to_string(), now);
        }
    };
    outcome.observed = state;
    update(&mut outcome.observed);
    if persist && (new_prs.is_some() || new_ci.is_some() || new_sha.is_some()) {
        store.mutate(None, update)?;
    }

    Ok(outcome)
}

#[cfg(test)]
#[path = "observe_tests.rs"]
mod tests;
