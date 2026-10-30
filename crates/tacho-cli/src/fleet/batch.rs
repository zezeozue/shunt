//! Speculative batch verification (`tacho batch`), flake ledger (`tacho flakes`),
//! origin-aware retry/bisection (`Daemon`, `Manual`, `Train`), detached background mode
//! with env-file precedence, and area hang timeout reporting.

mod checkout;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use tacho_core::decide::dispatch::base_key;
use tacho_core::decide::verify::{
    area_hang_report, areas_for_files, batch_refs_moved, batches, checks_for_files, classify,
    counterexample, failed_tests, flake_rows, flake_tasks, job_argv, job_for, new_batch_id,
    next_probe, parents, parse_workflow_yaml, record_flakes, reruns_on_red, reusable_batch,
    select_costly_areas, service_argv, summary_line, AreaCheck, AreaRule, BatchMode, BatchRun,
    Bisect, FailedRecord, PassedRecord, ProbeStep,
};
use tacho_core::{Scrubber, TaskStatus};

use crate::cli::{BatchArgs, FlakesArgs};

use super::store::{
    emit_decision_event, load_fleet_env, now_ns, now_secs, open_fleet_store,
    record_batch_pid_identity, resolve_live_main_sha, resolve_live_pr_heads,
};
use crate::kit::{apply_private_tmp_env, private_tmp_dir, use_private_tmp};

/// Origin of a batch verification run:
/// - `Daemon`: reruns the same tree once on red (recording flakes to the flake ledger,
///   auto-queueing `flake-<slug>` at 2 flakes, never counting proptest counterexamples as flakes,
///   and never reusing a flake batch as a pass) and bisects prefixes on a second red.
/// - `Manual`: stops immediately at `red-once` on first failure without flake-retrying or bisecting.
/// - `Train`: stops immediately at `red-once` on first failure without flake-retrying or bisecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BatchOrigin {
    #[default]
    Daemon,
    Manual,
    Train,
}

impl BatchOrigin {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "manual" => Self::Manual,
            "train" => Self::Train,
            _ => Self::Daemon,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::Manual => "manual",
            Self::Train => "train",
        }
    }
}

/// Rich options for running a verification batch (`execute_batch_with`).
#[derive(Debug, Clone)]
pub struct BatchOptions {
    pub prs: Vec<u64>,
    pub areas: Option<Vec<String>>,
    pub foreground: bool,
    pub origin: BatchOrigin,
    pub cmd: Option<String>,
    pub mode: Option<String>,
    pub timeout: Option<u64>,
    pub dry_run: bool,
    pub env_file: Option<PathBuf>,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            prs: Vec::new(),
            areas: None,
            foreground: true,
            origin: BatchOrigin::Daemon,
            cmd: None,
            mode: None,
            timeout: None,
            dry_run: false,
            env_file: None,
        }
    }
}

impl From<BatchArgs> for BatchOptions {
    fn from(args: BatchArgs) -> Self {
        let areas = std::env::var("TACHO_BATCH_AREAS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty());
        let foreground = match std::env::var("TACHO_BATCH_FOREGROUND").ok().as_deref() {
            Some("0") | Some("false") => false,
            _ => std::env::var("TACHO_BATCH_DETACHED").ok().as_deref() != Some("1"),
        };
        let origin = std::env::var("TACHO_BATCH_ORIGIN")
            .ok()
            .map(|s| BatchOrigin::parse(&s))
            .unwrap_or(BatchOrigin::Daemon);
        Self {
            prs: args.prs,
            areas,
            foreground,
            origin,
            cmd: args.cmd,
            mode: args.mode,
            timeout: args.timeout,
            dry_run: args.dry_run,
            env_file: None,
        }
    }
}

/// Returns `true` if a failure log contains a proptest counterexample.
/// Proptest counterexamples are deterministic property violations and must NEVER be counted as flakes.
pub fn has_proptest_counterexample(log: &str) -> bool {
    if counterexample(log).is_some() {
        return true;
    }
    let lower = log.to_ascii_lowercase();
    lower.contains("minimal failing input")
        || lower.contains("proptest:")
        || lower.contains("test failed: minimal")
        || lower.contains("counterexample:")
}

/// Filter `area_rules` and changed `files` according to an optional `--areas` list.
pub fn select_batch_checks(
    files: &[String],
    area_rules: &[AreaRule],
    requested_areas: Option<&[String]>,
) -> (Vec<String>, Vec<AreaCheck>) {
    select_batch_checks_for_origin(files, area_rules, requested_areas, true, false)
}

/// Filter `area_rules` and changed `files` with costly-area pruning (`image`, `android-release`, `console-browser`).
pub fn select_batch_checks_for_origin(
    files: &[String],
    area_rules: &[AreaRule],
    requested_areas: Option<&[String]>,
    pull_request: bool,
    train: bool,
) -> (Vec<String>, Vec<AreaCheck>) {
    if let Some(req) = requested_areas.filter(|a| !a.is_empty()) {
        let req_set: BTreeSet<&str> = req.iter().map(String::as_str).collect();
        let all_checks = checks_for_files(files, area_rules);
        let mut areas = Vec::new();
        let mut checks = Vec::new();
        for chk in all_checks {
            if req_set.contains(chk.area.as_str()) {
                if !areas.contains(&chk.area) {
                    areas.push(chk.area.clone());
                }
                checks.push(chk);
            }
        }
        for r in req {
            if !areas.contains(r) {
                areas.push(r.clone());
                checks.push(AreaCheck {
                    area: r.clone(),
                    units: Vec::new(),
                });
            }
        }
        return (areas, checks);
    }
    let raw_checks = checks_for_files(files, area_rules);
    let raw_areas: Vec<String> = raw_checks.iter().map(|c| c.area.clone()).collect();
    let effective_areas = select_costly_areas(&raw_areas, files, pull_request, train);
    let effective_set: BTreeSet<&str> = effective_areas.iter().map(String::as_str).collect();
    let mut checks: Vec<AreaCheck> = raw_checks
        .into_iter()
        .filter(|c| effective_set.contains(c.area.as_str()))
        .collect();
    let empty_specs = BTreeMap::new();
    for area in &effective_areas {
        if !checks.iter().any(|c| &c.area == area) {
            let units = match area.as_str() {
                "console-browser" => {
                    let specs = tacho_core::decide::verify::related_console_specs(
                        files,
                        &empty_specs,
                        "frontend/admin/",
                    );
                    if specs.is_empty() {
                        vec!["all".to_string()]
                    } else {
                        specs
                    }
                }
                "deploy" => {
                    let d = tacho_core::decide::verify::related_deploy_checks(
                        files,
                        tacho_core::decide::verify::DEFAULT_DEPLOY_CHECKS,
                    );
                    if d.is_empty() {
                        vec!["all".to_string()]
                    } else {
                        d
                    }
                }
                _ => vec!["all".to_string()],
            };
            checks.push(AreaCheck {
                area: area.clone(),
                units,
            });
        }
    }
    for chk in &mut checks {
        if chk.area == "console-browser"
            && (chk.units.is_empty() || chk.units == vec!["all".to_string()])
        {
            let specs = tacho_core::decide::verify::related_console_specs(
                files,
                &empty_specs,
                "frontend/admin/",
            );
            if !specs.is_empty() {
                chk.units = specs;
            }
        } else if chk.area == "deploy"
            && (chk.units.is_empty() || chk.units == vec!["all".to_string()])
        {
            let d = tacho_core::decide::verify::related_deploy_checks(
                files,
                tacho_core::decide::verify::DEFAULT_DEPLOY_CHECKS,
            );
            if !d.is_empty() {
                chk.units = d;
            }
        }
    }
    (effective_areas, checks)
}

/// Spawns a detached background batch runner writing to `$TACHO_HOME/logs/batch-<id>.log`,
/// loading the environment file with process-environment precedence, and returns the log path.
pub fn launch_detached_batch(opts: &BatchOptions) -> io::Result<PathBuf> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let batch_id = new_batch_id(&state.batches);
    let home = crate::paths::resolve_home();
    let logs_dir = home.join("logs");
    fs::create_dir_all(&logs_dir)?;
    let log_path = logs_dir.join(format!("batch-{batch_id}.log"));

    let merged_env = load_fleet_env(opts.env_file.as_deref());
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let err_file = log_file.try_clone()?;

    let exe = crate::client::resolve_tacho_bin();
    let mut cmd = Command::new(exe);
    cmd.arg("batch").arg("--foreground");
    for pr in &opts.prs {
        cmd.arg(pr.to_string());
    }
    if let Some(ref areas) = opts.areas {
        if !areas.is_empty() {
            cmd.args(["--areas", &areas.join(",")]);
        }
    }
    if let Some(ref c) = opts.cmd {
        cmd.args(["--cmd", c]);
    }
    if let Some(ref m) = opts.mode {
        cmd.args(["--mode", m]);
    }
    if let Some(t) = opts.timeout {
        cmd.args(["--timeout", &t.to_string()]);
    }
    if opts.dry_run {
        cmd.arg("--dry-run");
    }

    for (k, v) in &merged_env {
        cmd.env(k, v);
    }
    use_private_tmp(&mut cmd, &format!("batch-{batch_id}"))?;
    cmd.env("TACHO_HOME", &home)
        .env("TACHO_BATCH_FOREGROUND", "1")
        .env("TACHO_BATCH_DETACHED", "0")
        .env("TACHO_BATCH_ORIGIN", opts.origin.as_str())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(err_file))
        .process_group(0);

    let child = cmd.spawn()?;
    let pid = child.id();
    record_batch_pid_identity(&batch_id, pid)?;
    let because = format!(
        "spawned detached {} batch logging to {}",
        opts.origin.as_str(),
        log_path.display()
    );
    store.mutate(
        Some(("verify", "batch_detached", &batch_id, &because)),
        |_| {},
    )?;
    println!("{}", log_path.display());
    Ok(log_path)
}

/// Executes `tacho flakes`.
pub fn execute_flakes(args: FlakesArgs) -> io::Result<()> {
    let store = open_fleet_store()?;
    let state = store.load()?;
    let rows = flake_rows(&state.flakes);
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&rows).unwrap_or_default()
        );
        return Ok(());
    }
    if rows.is_empty() {
        println!("No flaky tests recorded.");
        return Ok(());
    }
    println!(
        "{:<42} {:<12} {:>6} {:<20} BATCHES",
        "TEST", "AREA", "COUNT", "TASK"
    );
    for r in &rows {
        println!(
            "{:<42} {:<12} {:>6} {:<20} {:?}",
            r.test,
            if r.area.is_empty() { "-" } else { &r.area },
            r.count,
            r.task_key.as_deref().unwrap_or("-"),
            r.batches
        );
    }
    Ok(())
}

/// Executes `tacho batch [<pr...>]` — runs speculative batch verification + automatic binary bisection on failure.
pub fn execute_batch(args: BatchArgs) -> io::Result<()> {
    execute_batch_with(BatchOptions::from(args))
}

/// Executes batch verification with full [`BatchOptions`] (`--areas`, `--foreground`/detached, [`BatchOrigin`]).
pub fn execute_batch_with(opts: BatchOptions) -> io::Result<()> {
    if !opts.foreground && !opts.dry_run {
        let _ = launch_detached_batch(&opts)?;
        return Ok(());
    }

    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let cwd = std::env::current_dir()?;

    let batch_mode = match opts.mode.as_deref().unwrap_or("presubmit") {
        "presubmit" => BatchMode::Presubmit,
        "postsubmit" => BatchMode::Postsubmit,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid --mode '{other}' (expected 'presubmit' or 'postsubmit')"),
            ))
        }
    };
    let timeout_secs = opts
        .timeout
        .or(state.batch_timeout_secs)
        .unwrap_or(600)
        .max(1);

    let main_sha = resolve_live_main_sha(&cwd);

    let pr_nums: Vec<u64> = if !opts.prs.is_empty() {
        opts.prs.clone()
    } else if let Some(ref req_areas) = opts.areas.as_ref().filter(|a| !a.is_empty()) {
        let req_set: BTreeSet<&str> = req_areas.iter().map(String::as_str).collect();
        let mut matched = Vec::new();
        for pr in &state.open_prs_cache {
            let pr_areas = areas_for_files(&pr.files, &state.area_rules);
            if pr_areas.iter().any(|a| req_set.contains(a.as_str())) {
                matched.push(pr.number);
            }
        }
        if matched.is_empty() {
            println!("No PRs need batch verification for areas {:?}.", req_areas);
            return Ok(());
        }
        matched
    } else {
        let planned = batches(
            &state.open_prs_cache,
            &state.holds,
            &state.passed,
            &state.failed,
            &state.batches,
            &state.area_rules,
            6,
        );
        if let Some(first) = planned.first() {
            first.prs.clone()
        } else {
            println!("No PRs need batch verification right now.");
            return Ok(());
        }
    };

    let mut shas = BTreeMap::new();
    let mut areas = BTreeSet::new();
    let mut all_files = Vec::new();
    for &n in &pr_nums {
        if let Some(p) = state.open_prs_cache.iter().find(|p| p.number == n) {
            shas.insert(n, p.head.clone());
            all_files.extend(p.files.iter().cloned());
            for a in areas_for_files(&p.files, &state.area_rules) {
                areas.insert(a);
            }
        } else {
            return Err(io::Error::other(format!(
                "PR #{n} has no observed head or changed files"
            )));
        }
    }
    let (selected_areas, mut checks) = select_batch_checks_for_origin(
        &all_files,
        &state.area_rules,
        opts.areas.as_deref(),
        batch_mode == BatchMode::Presubmit,
        opts.origin == BatchOrigin::Train,
    );
    let mut area_list: Vec<String> =
        if let Some(req) = opts.areas.as_ref().filter(|a| !a.is_empty()) {
            req.to_vec()
        } else if !selected_areas.is_empty() {
            selected_areas
        } else {
            areas.into_iter().collect()
        };
    if opts.origin == BatchOrigin::Train {
        let ci_sha = super::train::detect_last_ci_change_sha(&cwd, &main_sha);
        let main_ci_state = tacho_core::decide::land::MainCi {
            sha: state
                .main_ci
                .as_ref()
                .map(|m| m.sha.clone())
                .unwrap_or_default(),
            ok: state
                .main_ci
                .as_ref()
                .is_some_and(|m| m.conclusion.eq_ignore_ascii_case("success")),
        };
        let all_areas_set: BTreeSet<String> =
            state.area_rules.iter().map(|r| r.area.clone()).collect();
        let extra_proof_areas = tacho_core::decide::land::ci_proof_areas(
            ci_sha.as_deref(),
            &main_ci_state,
            |anc, desc| {
                Command::new("git")
                    .args(["merge-base", "--is-ancestor", anc, desc])
                    .current_dir(&cwd)
                    .status()
                    .is_ok_and(|s| s.success())
            },
            &all_areas_set,
            "gate",
        );
        for extra in extra_proof_areas {
            if !area_list.contains(&extra) {
                area_list.push(extra.clone());
            }
            if !checks.iter().any(|c| c.area == extra) {
                checks.push(AreaCheck {
                    area: extra,
                    units: Vec::new(),
                });
            }
        }
    }

    if let Some(reused) = reusable_batch(&state.batches, &pr_nums, &shas, &main_sha, &area_list) {
        println!(
            "Reusing green batch {} for PRs {:?} on main {}",
            reused.id,
            pr_nums,
            &main_sha[..main_sha.len().min(12)]
        );
        return Ok(());
    }

    let batch_id = new_batch_id(&state.batches);
    if opts.dry_run {
        println!("Batch ID: {batch_id}");
        println!("Origin:   {}", opts.origin.as_str());
        println!("Mode:     {batch_mode:?}");
        println!("PRs:      {:?}", pr_nums);
        println!("Main SHA: {main_sha}");
        println!("Areas:    {:?}", area_list);
        println!("Checks:   {:?}", checks);
        return Ok(());
    }

    let now = now_secs();
    let batch_start_ns = now_ns();
    let heads: Vec<&str> = pr_nums
        .iter()
        .map(|number| {
            shas.get(number)
                .map(String::as_str)
                .ok_or_else(|| io::Error::other("batch head is missing"))
        })
        .collect::<io::Result<_>>()?;
    let checkout = checkout::BatchCheckout::new(&cwd, &main_sha)?;
    let tree_sha = checkout.prepare(&main_sha, &heads)?;
    let check_cwd = checkout.path();
    let check_cmd = if let Some(ref custom_cmd) = opts.cmd {
        custom_cmd.clone()
    } else {
        let candidate_wf = state.workflow_path.clone().or_else(|| {
            for rel in [".github/workflows/ci.yml", ".github/workflows/ci.yaml"] {
                if check_cwd.join(rel).is_file() {
                    return Some(rel.to_string());
                }
            }
            None
        });
        if let Some(wf_path) = candidate_wf {
            let raw_wf = fs::read_to_string(check_cwd.join(&wf_path)).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to read workflow '{wf_path}': {e}"),
                )
            })?;
            let wf_val = parse_workflow_yaml(&raw_wf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut job_cmds = Vec::new();
            for chk in &checks {
                let area = &chk.area;
                let job_name = state
                    .workflow_jobs
                    .get(area)
                    .map(String::as_str)
                    .unwrap_or(area.as_str());
                let mut exprs = state.workflow_expressions.clone();
                exprs
                    .entry("tacho.mode".to_string())
                    .or_insert_with(|| format!("{batch_mode:?}").to_ascii_lowercase());
                exprs
                    .entry("tacho.units".to_string())
                    .or_insert_with(|| chk.units.join(","));
                let ci_job = job_for(&wf_val, area, job_name, &exprs)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let svc_name = ci_job
                    .service
                    .as_ref()
                    .map(|s| format!("tacho-svc-{batch_id}-{}", s.name));
                if let (Some(svc), Some(sname)) = (&ci_job.service, &svc_name) {
                    let s_argv = service_argv(svc, sname);
                    job_cmds.push(s_argv.join(" "));
                }
                let j_argv = job_argv(
                    &ci_job,
                    check_cwd.to_str().unwrap_or("."),
                    svc_name.as_deref(),
                    &[],
                );
                job_cmds.push(
                    j_argv
                        .iter()
                        .map(|a| format!("'{}'", a.replace('\'', "'\\''")))
                        .collect::<Vec<_>>()
                        .join(" "),
                );
                if let Some(sname) = svc_name {
                    job_cmds.push(format!("docker rm -f {sname} >/dev/null 2>&1 || true"));
                }
            }
            if job_cmds.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("workflow '{wf_path}' produced no jobs for areas {area_list:?}"),
                ));
            }
            job_cmds.join(" && ")
        } else {
            let mut rule_cmds = Vec::new();
            for chk in &checks {
                if let Some(rule) = state.area_rules.iter().find(|r| r.area == chk.area) {
                    for (_, c) in &rule.checks {
                        rule_cmds.push(c.clone());
                    }
                }
            }
            if rule_cmds.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "no workflow file or --cmd configured for batch verification of areas {:?}",
                        area_list
                    ),
                ));
            }
            rule_cmds.join(" && ")
        }
    };
    println!(
        "Running batch {batch_id} ({batch_mode:?}, origin={}) on PRs {:?}: `{check_cmd}`",
        opts.origin.as_str(),
        pr_nums
    );

    let merged_env = load_fleet_env(opts.env_file.as_deref());
    record_batch_pid_identity(&batch_id, std::process::id())?;
    let fleet_tmp = private_tmp_dir(&format!("batch-{batch_id}"))?;
    let run_probe = |k: usize, label: &str| -> io::Result<(bool, bool, String, u64, u64)> {
        let prefix_heads: Vec<&str> = heads.iter().copied().take(k).collect();
        let expected_tree = checkout.prepare(&main_sha, &prefix_heads)?;
        let p_start = now_ns();
        let prefix_prs: Vec<String> = pr_nums.iter().take(k).map(|n| n.to_string()).collect();
        let units_csv: Vec<String> = checks
            .iter()
            .flat_map(|c| c.units.iter().cloned())
            .collect();
        let mut cmd = Command::new("timeout");
        cmd.args([
            "--signal=KILL",
            &format!("{timeout_secs}s"),
            "bash",
            "-c",
            &check_cmd,
        ])
        .current_dir(check_cwd);
        apply_private_tmp_env(&mut cmd, &fleet_tmp);
        for (ek, ev) in &merged_env {
            cmd.env(ek, ev);
        }
        let out = cmd
            .env("TACHO_BATCH_ID", &batch_id)
            .env(
                "TACHO_BATCH_MODE",
                format!("{batch_mode:?}").to_ascii_lowercase(),
            )
            .env("TACHO_BATCH_ORIGIN", opts.origin.as_str())
            .env("TACHO_BATCH_UNITS", units_csv.join(","))
            .env("TACHO_BATCH_PROBE_K", k.to_string())
            .env("TACHO_BATCH_PROBE_PRS", prefix_prs.join(","))
            .env("TACHO_BATCH_LABEL", label)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .output()?;
        let p_end = now_ns();
        let raw_txt = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let txt = Scrubber::new().scrub(&raw_txt);
        let timed_out = out.status.code() == Some(124) || out.status.code() == Some(137);
        if out.status.success() {
            checkout.verify_unchanged(&expected_tree)?;
        }
        Ok((out.status.success(), timed_out, txt, p_start, p_end))
    };

    let (mut ok, timed_out, mut log_text, p0_start, p0_end) = run_probe(pr_nums.len(), "r0")?;
    if ok {
        if let Some(report_dir) = std::env::var("TACHO_SHOT_REPORT_DIR")
            .ok()
            .map(PathBuf::from)
            .or_else(|| {
                let p = check_cwd.join(".tachoshots");
                p.is_dir().then_some(p)
            })
        {
            let recorded_head = if shas.len() == 1 {
                shas.values().next().cloned().unwrap_or_default()
            } else {
                super::store::git_stdout(check_cwd, &["rev-parse", "HEAD"]).unwrap_or_default()
            };
            let _ = super::train::persist_shot_report_to_run_dir(
                &report_dir,
                &batch_id,
                &recorded_head,
            );
            let fix_shots = opts.origin == BatchOrigin::Train
                || std::env::var("TACHO_FIX_SHOTS").ok().as_deref() == Some("1");
            let shot_rules = super::train::load_shot_rules(check_cwd);
            match super::train::stage_and_commit_merged_screenshots(
                check_cwd,
                &report_dir,
                &batch_id,
                &all_files,
                fix_shots,
                &shot_rules,
            ) {
                Ok(Some(_new_commit)) => {
                    return Err(io::Error::other("generated screenshots need collecting into the PR heads before verification"));
                }
                Ok(None) => {}
                Err(shot_err) => {
                    ok = false;
                    if !log_text.is_empty() {
                        log_text.push('\n');
                    }
                    log_text.push_str(&shot_err.to_string());
                }
            }
        }
    }
    store.emit_duration_slice(
        &format!("batch:{batch_id}"),
        &format!("check PRs {:?}", pr_nums),
        p0_start,
        p0_end,
        &check_cmd,
    )?;

    if timed_out {
        let area_name = area_list.first().map(String::as_str).unwrap_or("workspace");
        let hang = area_hang_report(&batch_id, area_name, timeout_secs, &log_text);
        let finished = now_secs();
        let b_run = BatchRun {
            id: batch_id.clone(),
            group: area_name.to_string(),
            prs: pr_nums.clone(),
            shas: shas.clone(),
            main_sha: main_sha.clone(),
            areas: area_list.clone(),
            checks: checks.clone(),
            mode: batch_mode,
            status: "hang".to_string(),
            started_at: now,
            finished_at: Some(finished),
            tree: Some(tree_sha),
            pid: None,
            log: format!("{}\n{}", hang.summary, hang.log_tail),
            culprit: None,
            skipped: Vec::new(),
            flakes: Vec::new(),
        };
        store.mutate(
            Some(("verify", "batch_hang", &batch_id, &hang.summary)),
            |st| {
                st.batches.push(b_run);
            },
        )?;
        eprintln!("Batch {batch_id} HUNG: {}", hang.summary);
        return Err(io::Error::new(io::ErrorKind::TimedOut, hang.summary));
    }

    // Manual and Train batches stop at `red-once` without flake-retrying or bisecting!
    if !ok && matches!(opts.origin, BatchOrigin::Manual | BatchOrigin::Train) {
        let finished = now_secs();
        let sum = summary_line(&log_text);
        let b_run = BatchRun {
            id: batch_id.clone(),
            group: area_list
                .first()
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
            prs: pr_nums.clone(),
            shas: shas.clone(),
            main_sha: main_sha.clone(),
            areas: area_list.clone(),
            checks: checks.clone(),
            mode: batch_mode,
            status: "red-once".to_string(),
            started_at: now,
            finished_at: Some(finished),
            tree: Some(tree_sha),
            pid: None,
            log: log_text.clone(),
            culprit: if pr_nums.len() == 1 {
                Some(pr_nums[0])
            } else {
                None
            },
            skipped: Vec::new(),
            flakes: Vec::new(),
        };
        let because = format!(
            "{} batch {batch_id} stopped at red-once without bisecting: {sum}",
            opts.origin.as_str()
        );
        store.mutate(
            Some(("verify", "batch_red_once", &batch_id, &because)),
            |st| {
                st.batches.push(b_run);
            },
        )?;
        emit_decision_event(&format!("batch:{batch_id}"), "verify:red_once", &because);
        eprintln!("Batch {batch_id} RED-ONCE: {sum}");
        return Err(io::Error::other(because));
    }

    let mut flaked_on_retry = false;
    let mut recorded_flake_names = Vec::new();

    // Daemon batch: if the check failed without a deterministic proptest counterexample,
    // retry once (`r0-again`) to detect flakes.
    if !ok && !has_proptest_counterexample(&log_text) {
        let f_tests_initial = failed_tests(&log_text);
        let (retry_ok, _retry_to, retry_log, r_start, r_end) =
            run_probe(pr_nums.len(), "r0-again")?;
        store.emit_duration_slice(
            &format!("batch:{batch_id}"),
            "retry r0-again",
            r_start,
            r_end,
            "flake retry on failure without counterexample",
        )?;
        if retry_ok {
            flaked_on_retry = true;
            recorded_flake_names = if f_tests_initial.is_empty() {
                vec![summary_line(&log_text)]
            } else {
                f_tests_initial
            };
        } else {
            log_text = retry_log;
        }
    }

    let finished = now_secs();

    // Ref-movement refusal: re-read live `main` and open PR heads from the remote before recording results!
    let latest_state = store.load()?;
    let current_main = resolve_live_main_sha(&cwd);
    let current_shas = resolve_live_pr_heads(&cwd, &pr_nums, &shas, &latest_state);
    if let Some(moved_reason) = batch_refs_moved(&main_sha, &current_main, &shas, &current_shas) {
        let b_run = BatchRun {
            id: batch_id.clone(),
            group: area_list
                .first()
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
            prs: pr_nums.clone(),
            shas: shas.clone(),
            main_sha: main_sha.clone(),
            areas: area_list.clone(),
            checks: checks.clone(),
            mode: batch_mode,
            status: "head-moved".to_string(),
            started_at: now,
            finished_at: Some(finished),
            tree: Some(tree_sha),
            pid: None,
            log: moved_reason.clone(),
            culprit: None,
            skipped: Vec::new(),
            flakes: Vec::new(),
        };
        store.mutate(
            Some(("verify", "batch_head_moved", &batch_id, &moved_reason)),
            |st| {
                st.batches.push(b_run);
            },
        )?;
        eprintln!("Batch {batch_id} REFUSED: {moved_reason}");
        return Err(io::Error::other(moved_reason));
    }

    if flaked_on_retry {
        let b_run = BatchRun {
            id: batch_id.clone(),
            group: area_list
                .first()
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
            prs: pr_nums.clone(),
            shas: shas.clone(),
            main_sha: main_sha.clone(),
            areas: area_list.clone(),
            checks: checks.clone(),
            mode: batch_mode,
            status: "flake".to_string(),
            started_at: now,
            finished_at: Some(finished),
            tree: Some(tree_sha.clone()),
            pid: None,
            log: log_text.clone(),
            culprit: None,
            skipped: Vec::new(),
            flakes: recorded_flake_names.clone(),
        };
        let because = format!(
            "batch {batch_id} recorded as flake for PRs {:?}: {:?}",
            pr_nums, recorded_flake_names
        );
        store.mutate(Some(("verify", "batch_flake", &batch_id, &because)), |st| {
            let area_hint = area_list.first().map(String::as_str).unwrap_or("");
            let updated = record_flakes(
                &st.flakes,
                &recorded_flake_names,
                area_hint,
                &batch_id,
                &log_text,
                finished,
            );
            let (with_tasks, new_items) =
                flake_tasks(&updated, &st.todo, &st.agents, &st.retired, finished);
            st.flakes = with_tasks;
            st.todo.extend(new_items);
            st.batches.push(b_run);
        })?;
        emit_decision_event(&format!("batch:{batch_id}"), "verify:flake", &because);
        eprintln!(
            "Batch {batch_id} FLAKE recorded: {:?}",
            recorded_flake_names
        );
        return Ok(());
    }

    if ok {
        let b_run = BatchRun {
            id: batch_id.clone(),
            group: area_list
                .first()
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
            prs: pr_nums.clone(),
            shas: shas.clone(),
            main_sha: main_sha.clone(),
            areas: area_list.clone(),
            checks: checks.clone(),
            mode: batch_mode,
            status: "passed".to_string(),
            started_at: now,
            finished_at: Some(finished),
            tree: Some(tree_sha.clone()),
            pid: None,
            log: String::new(),
            culprit: None,
            skipped: Vec::new(),
            flakes: Vec::new(),
        };
        let because = format!("batch {batch_id} passed for PRs {:?}", pr_nums);
        store.mutate(Some(("verify", "batch_pass", &batch_id, &because)), |st| {
            for &pr in &pr_nums {
                let sha = shas.get(&pr).cloned().unwrap_or_else(|| main_sha.clone());
                st.failed.remove(&pr);
                st.passed.insert(
                    pr,
                    PassedRecord {
                        pr,
                        sha,
                        main_sha: main_sha.clone(),
                        batch_id: batch_id.clone(),
                        passed_at: finished,
                        tree: tree_sha.clone(),
                        areas: area_list.clone(),
                    },
                );
            }
            st.batches.push(b_run);
        })?;
        for &pr in &pr_nums {
            if let Some(a) = state.agents.values().find(|a| a.pr == Some(pr)) {
                store.emit_state_band(
                    &format!("task:{}/state", base_key(&a.key)),
                    "Done",
                    TaskStatus::Done,
                    &because,
                    now_ns(),
                )?;
            }
        }
        emit_decision_event(&format!("batch:{batch_id}"), "verify:pass", &because);
        println!(
            "Batch {batch_id} PASSED (tree {})",
            &tree_sha[..tree_sha.len().min(12)]
        );
    } else {
        let mut kind = classify(&log_text).to_string();
        let mut sum = summary_line(&log_text);
        let mut f_tests = failed_tests(&log_text);
        let mut ce = counterexample(&log_text);

        // Run real binary bisection probe loop (`next_probe(&bisect)`), probing k=0 (`main`)
        // before blaming a single or first PR.
        let members: Vec<(u64, String)> = pr_nums
            .iter()
            .map(|&p| (p, shas.get(&p).cloned().unwrap_or_default()))
            .collect();
        let mut bisect = Bisect {
            members,
            results: BTreeMap::from([(pr_nums.len(), false)]),
        };
        while let ProbeStep::Probe(k) = next_probe(&bisect) {
            let (probe_ok, _probe_to, probe_log, ps, pe) = run_probe(k, &format!("bisect-k{k}"))?;
            store.emit_duration_slice(
                &format!("batch:{batch_id}"),
                &format!("bisect k={k}"),
                ps,
                pe,
                &format!("probe k={k} ok={probe_ok}"),
            )?;
            if !probe_ok && k > 0 {
                log_text = probe_log;
                kind = classify(&log_text).to_string();
                sum = summary_line(&log_text);
                f_tests = failed_tests(&log_text);
                ce = counterexample(&log_text);
            }
            bisect.results.insert(k, probe_ok);
        }

        let terminal_step = next_probe(&bisect);
        if terminal_step == ProbeStep::MainBroken {
            let b_run = BatchRun {
                id: batch_id.clone(),
                group: area_list
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "default".to_string()),
                prs: pr_nums.clone(),
                shas: shas.clone(),
                main_sha: main_sha.clone(),
                areas: area_list.clone(),
                checks: checks.clone(),
                mode: batch_mode,
                status: "main-broken".to_string(),
                started_at: now,
                finished_at: Some(finished),
                tree: Some(tree_sha),
                pid: None,
                log: log_text.clone(),
                culprit: None,
                skipped: Vec::new(),
                flakes: Vec::new(),
            };
            let because = format!("batch {batch_id} aborted: base main is red ({sum})");
            store.mutate(Some(("verify", "main_broken", &batch_id, &because)), |st| {
                st.batches.push(b_run);
            })?;
            store.emit_duration_slice(
                &format!("batch:{batch_id}"),
                "main-broken",
                batch_start_ns,
                now_ns(),
                &because,
            )?;
            eprintln!("Batch {batch_id} MAIN BROKEN: {sum}");
            return Err(io::Error::other(because));
        }

        let culprit = match terminal_step {
            ProbeStep::Culprit((pr, _)) => Some(pr),
            _ => pr_nums.first().copied(),
        };

        let b_run = BatchRun {
            id: batch_id.clone(),
            group: area_list
                .first()
                .cloned()
                .unwrap_or_else(|| "default".to_string()),
            prs: pr_nums.clone(),
            shas: shas.clone(),
            main_sha: main_sha.clone(),
            areas: area_list.clone(),
            checks: checks.clone(),
            mode: batch_mode,
            status: "failed".to_string(),
            started_at: now,
            finished_at: Some(finished),
            tree: Some(tree_sha),
            pid: None,
            log: log_text.clone(),
            culprit,
            skipped: Vec::new(),
            flakes: Vec::new(),
        };
        let because = format!("batch {batch_id} failed ({sum}), culprit={culprit:?}");
        store.mutate(Some(("verify", "batch_fail", &batch_id, &because)), |st| {
            if let Some(c_pr) = culprit {
                if kind == "code" {
                    let sha = shas.get(&c_pr).cloned().unwrap_or_default();
                    st.failed.insert(
                        c_pr,
                        FailedRecord {
                            pr: c_pr,
                            sha,
                            batch_id: batch_id.clone(),
                            failed_at: finished,
                            kind: kind.clone(),
                            summary: sum.clone(),
                            failed_tests: f_tests.clone(),
                            counterexample: ce.clone(),
                            log: log_text.clone(),
                        },
                    );
                }
            }
            st.batches.push(b_run);
        })?;
        emit_decision_event(&format!("batch:{batch_id}"), "verify:fail", &because);
        let parent_map = parents(&state.open_prs_cache);
        if let Some(c_pr) = culprit {
            if let Some(a) = state.agents.values().find(|a| a.pr == Some(c_pr)) {
                store.emit_state_band(
                    &format!("task:{}/state", base_key(&a.key)),
                    "Failed",
                    TaskStatus::Failed,
                    &because,
                    now_ns(),
                )?;
            }
            let reruns = reruns_on_red(&pr_nums, c_pr, &parent_map);
            eprintln!("Batch {batch_id} FAILED: {sum} (culprit PR #{c_pr}, reruns={reruns:?})");
            if !reruns.is_empty() && kind == "code" {
                let mut rerun_opts = opts.clone();
                rerun_opts.prs = reruns;
                rerun_opts.dry_run = false;
                if let Err(rerun_err) = execute_batch_with(rerun_opts) {
                    eprintln!("Rerun batch failed: {rerun_err}");
                }
            }
        } else {
            eprintln!("Batch {batch_id} FAILED: {sum}");
        }
        return Err(io::Error::other(sum));
    }
    Ok(())
}
