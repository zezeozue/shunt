//! Work queue (`todo`), holds (`hold`, `release`), task retirement (`retire`),
//! and priority next-step instruction (`next`).
//!
//! Provider session management (`session`, `attach`, `stop`, `tell`) lives in [`super::session`]
//! and is re-exported here for convenience.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tacho_core::decide::dispatch::{
    area_report, base_key, compare_items, hold_problem, may_start, next_work, release_problem,
    resolve_with_providers, retire, retire_problem, with_hold, without_hold, TodoItem,
    STALE_HOLD_SECS,
};
use tacho_core::decide::land::{open_for, queue, train_refusal};
use tacho_core::decide::verify::{batches, slug_problem};
use tacho_core::{Scrubber, TaskStatus};
use tacho_daemon::FleetState;

use crate::cli::{HoldArgs, NextArgs, ReleaseArgs, RetireArgs, TodoArgs};

pub use super::session::{
    build_session_argv, deliver_tell_action, execute_attach, execute_session, execute_session_with,
    execute_stop, execute_tell, execute_tell_with, infer_provider_from_model,
    spawn_worker_for_item, spawn_worker_in_dir, SessionOptions,
};
use super::store::{
    emit_decision_event, live_agent_keys, load_fleet_meta, mutate_fleet_meta, now_ns, now_secs,
    open_fleet_store, FleetMeta, RetiredMeta,
};

/// Resolved brief text and optional source file path from positional or `--brief-file` arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBrief {
    pub text: String,
    pub path: Option<String>,
}

/// Auto-detect whether a positional `brief` argument is a path to a brief file on disk or inline text.
///
/// Rules:
/// 1. If `brief_file` is explicitly provided, read from `brief_file`.
/// 2. If `brief` is a single-line string without spaces (or ends with `.md`/`.txt` or starts with
///    `./`, `../`, `/`, `briefs/`):
///    - If `Path::new(brief)` exists as a file relative to `cwd`, read it and record `path`.
///    - Else if `cwd.join("briefs").join(brief)` exists as a file, read it and record `path`.
///    - Else if `brief` looks explicitly like a file path (`.md`, `.txt`, `./`, `../`, `/`, `briefs/`),
///      return an error rather than queueing a broken path string as the brief.
/// 3. Otherwise treat `brief` as literal inline brief text.
pub fn detect_brief_source(
    cwd: &Path,
    brief: Option<&str>,
    brief_file: Option<&Path>,
) -> io::Result<ResolvedBrief> {
    if let Some(bf) = brief_file {
        let p = if bf.is_absolute() {
            bf.to_path_buf()
        } else {
            cwd.join(bf)
        };
        let content = fs::read_to_string(&p)?;
        return Ok(ResolvedBrief {
            text: content,
            path: Some(bf.to_string_lossy().to_string()),
        });
    }

    let raw = brief.map(str::trim).unwrap_or("");
    if raw.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provide a brief string or --brief-file <path>",
        ));
    }

    let single_line = !raw.contains('\n');
    let looks_like_path = single_line
        && !raw.contains(' ')
        && (raw.ends_with(".md")
            || raw.ends_with(".txt")
            || raw.starts_with("./")
            || raw.starts_with("../")
            || raw.starts_with('/')
            || raw.starts_with("briefs/"));

    if single_line && !raw.contains(' ') {
        let direct = if Path::new(raw).is_absolute() {
            PathBuf::from(raw)
        } else {
            cwd.join(raw)
        };
        if direct.is_file() {
            let content = fs::read_to_string(&direct)?;
            return Ok(ResolvedBrief {
                text: content,
                path: Some(raw.to_string()),
            });
        }
        let under_briefs = cwd.join("briefs").join(raw);
        if under_briefs.is_file() {
            let content = fs::read_to_string(&under_briefs)?;
            return Ok(ResolvedBrief {
                text: content,
                path: Some(format!("briefs/{raw}")),
            });
        }
        if looks_like_path {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("brief file not found: {raw}"),
            ));
        }
    }

    Ok(ResolvedBrief {
        text: brief.unwrap_or_default().to_string(),
        path: None,
    })
}

/// Format retired task lines (newest first) from `FleetState` and `FleetMeta`.
pub fn format_retired_lines(st: &FleetState, meta: &FleetMeta) -> Vec<String> {
    let mut entries: Vec<(i64, String, String)> = Vec::new();
    let mut seen = BTreeSet::new();
    for (k, r) in &meta.retired {
        seen.insert(k.clone());
        let why = if r.why.trim().is_empty() {
            "retired"
        } else {
            r.why.trim()
        };
        entries.push((r.at, k.clone(), why.to_string()));
    }
    for k in &st.retired {
        if !seen.contains(k) {
            entries.push((0, k.clone(), "retired by user".to_string()));
        }
    }
    entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    entries
        .into_iter()
        .map(|(_, k, why)| format!("{k:<28} retired: {why}"))
        .collect()
}

/// Rich options for `todo`, supporting `--now` urgent priority and `--retired` listing.
#[derive(Debug, Clone, Default)]
pub struct TodoOptions {
    pub key: Option<String>,
    pub brief: Option<String>,
    pub brief_file: Option<PathBuf>,
    pub area: Option<String>,
    pub base: Option<String>,
    pub after: Option<String>,
    pub priority: bool,
    pub now: bool,
    pub retired: bool,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub compare: Option<String>,
    pub json: bool,
}

impl From<TodoArgs> for TodoOptions {
    fn from(args: TodoArgs) -> Self {
        Self {
            key: args.key,
            brief: args.brief,
            brief_file: args.brief_file,
            area: args.area,
            base: args.base,
            after: args.after,
            priority: args.priority,
            now: args.now,
            retired: args.retired,
            provider: args.provider,
            model: args.model,
            effort: args.effort,
            compare: args.compare,
            json: args.json,
        }
    }
}

/// Executes `tacho todo [<key> "<brief>" ...]`.
pub fn execute_todo(args: TodoArgs) -> io::Result<()> {
    execute_todo_with(TodoOptions::from(args))
}

/// Executes `tacho todo` with [`TodoOptions`].
pub fn execute_todo_with(args: TodoOptions) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let scrubber = Scrubber::new();

    if args.retired && args.key.is_none() {
        let meta = load_fleet_meta().unwrap_or_default();
        if args.json {
            let retired_vec: Vec<String> = state.retired.iter().cloned().collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&retired_vec).unwrap_or_default()
            );
            return Ok(());
        }
        let lines = format_retired_lines(&state, &meta);
        if lines.is_empty() {
            println!("No retired tasks.");
        } else {
            println!("RETIRED TASKS ({}):", lines.len());
            for line in lines {
                println!("  {}", scrubber.scrub(&line));
            }
        }
        return Ok(());
    }

    let Some(key) = args.key else {
        let live = live_agent_keys(&state);
        let passed_prs: BTreeSet<u64> = state.passed.keys().copied().collect();
        let failed_prs: BTreeSet<u64> = state.failed.keys().copied().collect();
        if args.json {
            let raw = serde_json::to_string_pretty(&state.todo).unwrap_or_default();
            println!("{}", scrubber.scrub(&raw));
            return Ok(());
        }
        if state.todo.is_empty() {
            println!("Todo queue is empty.");
            return Ok(());
        }
        println!(
            "{:<22} {:<12} {:<8} {:<10} {:<24} BRIEF",
            "KEY", "AREA", "PRIO", "PROVIDER", "STATUS"
        );
        for item in &state.todo {
            let (ok, reason) = may_start(
                item,
                &state.agents,
                &state.retired,
                &state.holds,
                &live,
                &passed_prs,
                &failed_prs,
                &state.merged_prs,
                &state.closed_prs,
            );
            let status_str = if ok {
                format!("ready ({reason})")
            } else {
                format!("waiting ({reason})")
            };
            let prov = item
                .compare
                .as_deref()
                .or(item.model.as_deref())
                .or(item.provider.as_deref())
                .unwrap_or(&state.default_provider);
            let brief_short: String = item
                .brief
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(48)
                .collect();
            let line = format!(
                "{:<22} {:<12} {:<8} {:<10} {:<24} {}",
                item.key,
                if item.area.is_empty() {
                    "-"
                } else {
                    &item.area
                },
                item.priority,
                prov,
                status_str.chars().take(24).collect::<String>(),
                brief_short
            );
            println!("{}", scrubber.scrub(&line));
        }
        return Ok(());
    };

    if let Some(prob) = slug_problem(&key) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, prob));
    }

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let resolved = detect_brief_source(&cwd, args.brief.as_deref(), args.brief_file.as_deref())?;
    let brief = resolved.text;

    let after: Vec<String> = args
        .after
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();

    let effective_provider = args.provider.clone().or_else(|| {
        args.model
            .as_deref()
            .and_then(infer_provider_from_model)
            .map(String::from)
    });

    if args.compare.is_none() {
        resolve_with_providers(
            effective_provider.as_deref(),
            args.model.as_deref(),
            args.effort.as_deref(),
            &state.default_provider,
            state.default_model.as_deref(),
            state.default_effort.as_deref(),
            &state.providers,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    }

    let raw_item = TodoItem {
        key: key.clone(),
        brief,
        area: args.area.unwrap_or_default(),
        base: args.base,
        after,
        priority: if args.priority || args.now { 1 } else { 0 },
        created_at: now_secs(),
        provider: effective_provider,
        model: args.model,
        effort: args.effort,
        compare: args.compare,
    };

    let items = compare_items(&raw_item, state.default_effort.as_deref())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let added_keys: Vec<String> = items.iter().map(|i| i.key.clone()).collect();
    let because = format!("queued {:?}", added_keys);
    store.mutate(Some(("dispatch", "todo", &key, &because)), |st| {
        for item in items {
            st.retired.remove(&item.key);
            st.retired.remove(base_key(&item.key));
            st.todo.retain(|t| t.key != item.key);
            if item.priority > 0 {
                let pos = st
                    .todo
                    .iter()
                    .position(|t| t.priority == 0)
                    .unwrap_or(st.todo.len());
                st.todo.insert(pos, item);
            } else {
                st.todo.push(item);
            }
        }
    })?;

    mutate_fleet_meta(|m| {
        for k in &added_keys {
            m.retired.remove(k);
            m.retired.remove(base_key(k));
        }
    })?;

    let ts = now_ns();
    for k in &added_keys {
        store.emit_state_band(
            &format!("task:{k}/state"),
            "Queued",
            TaskStatus::Queued,
            &because,
            ts,
        )?;
    }
    emit_decision_event(&format!("task:{key}"), "dispatch:todo", &because);
    println!("Queued: {}", added_keys.join(", "));
    Ok(())
}

/// Executes `tacho hold <target> [--why "..."]`.
pub fn execute_hold(args: HoldArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;

    if args.target.is_none() {
        if state.holds.is_empty() {
            println!("No active holds.");
            return Ok(());
        }
        let now = now_secs();
        for (target, h) in &state.holds {
            println!(
                "{:<20} {:>6}s  {}",
                target,
                (now - h.since).max(0),
                h.reason
            );
        }
        return Ok(());
    }

    let target = args.target.unwrap();
    let known_areas: BTreeSet<String> = state.area_rules.iter().map(|r| r.area.clone()).collect();
    if let Some(prob) = hold_problem(
        &target,
        &state.todo,
        &state.agents,
        &state.retired,
        &known_areas,
    ) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, prob));
    }

    let reason = args.reason.unwrap_or_else(|| "manual hold".to_string());
    if reason.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hold reason cannot be empty",
        ));
    }
    let now = now_secs();
    store.mutate(Some(("dispatch", "hold", &target, &reason)), |st| {
        st.holds = with_hold(&st.holds, &target, &reason, now);
    })?;
    emit_decision_event("system", "dispatch:hold", &format!("{target}: {reason}"));
    println!("Held {target}: {reason}");
    Ok(())
}

/// Executes `tacho release <target>`.
pub fn execute_release(args: ReleaseArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    if let Some(prob) = release_problem(&args.target, &state.holds) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, prob));
    }
    store.mutate(
        Some(("dispatch", "release", &args.target, "released hold")),
        |st| {
            st.holds = without_hold(&st.holds, &args.target);
        },
    )?;
    emit_decision_event("system", "dispatch:release", &args.target);
    println!("Released hold on {}", args.target);
    Ok(())
}

/// Validates a retirement reason (`--why`), rejecting empty or whitespace-only strings.
pub fn validate_retire_reason(why: &str) -> io::Result<String> {
    let trimmed = why.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "say why with --why, so the retired list records what made the work unnecessary",
        ));
    }
    Ok(trimmed.to_string())
}

/// Options for `retire` with mandatory reason validation.
#[derive(Debug, Clone)]
pub struct RetireOptions {
    pub key: String,
    pub why: String,
}

/// Executes `tacho retire` with explicit [`RetireOptions`].
pub fn execute_retire_with(opts: RetireOptions) -> io::Result<()> {
    let why = validate_retire_reason(&opts.why)?;
    execute_retire(RetireArgs {
        key: opts.key,
        reason: Some(why),
    })
}

/// Executes `tacho retire <key> [--why "..."]`.
pub fn execute_retire(args: RetireArgs) -> io::Result<()> {
    let why = match args.reason.as_deref() {
        Some(r) => validate_retire_reason(r)?,
        None => "retired by user".to_string(),
    };
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let live = live_agent_keys(&state);
    if let Some(prob) = retire_problem(&args.key, &state.todo, &state.agents, &state.retired, &live)
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, prob));
    }
    let now = now_secs();
    let task_brief = state
        .todo
        .iter()
        .find(|t| t.key == args.key)
        .map(|t| t.brief.clone())
        .or_else(|| state.agents.get(&args.key).map(|a| a.brief.clone()))
        .unwrap_or_default();
    let held_for = state
        .holds
        .get(&args.key)
        .map(|h| h.reason.clone())
        .unwrap_or_default();

    store.mutate(Some(("dispatch", "retire", &args.key, &why)), |st| {
        let (new_todo, new_retired, new_holds) =
            retire(&args.key, &st.todo, &st.agents, &st.retired, &st.holds);
        st.todo = new_todo;
        st.retired = new_retired;
        st.holds = new_holds;
    })?;

    mutate_fleet_meta(|meta| {
        meta.retired.insert(
            args.key.clone(),
            RetiredMeta {
                key: args.key.clone(),
                why: why.clone(),
                at: now,
                task: task_brief,
                held_for,
            },
        );
    })?;

    store.emit_state_band(
        &format!("task:{}/state", base_key(&args.key)),
        "Cancelled",
        TaskStatus::Cancelled,
        &why,
        now_ns(),
    )?;
    emit_decision_event(&format!("task:{}", args.key), "dispatch:retire", &why);
    println!("Retired {}", args.key);
    Ok(())
}

/// Computes the single highest-priority operator/orchestrator instruction for `tacho next`.
///
/// Exact priority order:
/// 1. `hands` rebase (conflicting open PRs), skipping any PR that a live agent currently holds
///    or is already rebasing (`rebase-<n>` / `rebase-pr<n>`) and any PR on hold (`pr:<n>`).
/// 2. Failed PR fix (`st.failed`), skipping held PRs or PRs with a live fix agent.
/// 3. Ask owner to land ready PRs / open merge train (`ask the owner to land ...`).
/// 4. Stale hold (`> 24h`): `"decide: release or retire <target> (held <hours>h: <reason>)"`.
/// 5. Working (`live` agents or running batches): `"working: N agent(s), M batch(es)"`.
/// 6. Fallback: `"run tick"`.
pub fn compute_next_instruction(st: &FleetState, live: &BTreeSet<String>, now: i64) -> String {
    let mut agent_held_prs = BTreeSet::new();
    for key in live {
        if let Some(a) = st.agents.get(key) {
            if let Some(pr) = a.pr {
                agent_held_prs.insert(pr);
            }
            if let Some(bpr) = a.base_pr {
                agent_held_prs.insert(bpr);
            }
        }
        let root = base_key(key);
        for prefix in ["rebase-pr", "rebase-", "fix-pr", "fix-"] {
            if let Some(num_str) = root.strip_prefix(prefix) {
                if let Ok(pr) = num_str.parse::<u64>() {
                    agent_held_prs.insert(pr);
                }
            }
        }
    }

    let mut conflicting_prs: Vec<&tacho_core::decide::dispatch::PullFacts> = st
        .open_prs_cache
        .iter()
        .filter(|pr| {
            pr.state.eq_ignore_ascii_case("OPEN")
                && pr.mergeable.eq_ignore_ascii_case("CONFLICTING")
                && !st.holds.contains_key(&format!("pr:{}", pr.number))
                && !agent_held_prs.contains(&pr.number)
                && !st.retired.contains(&format!("rebase-{}", pr.number))
                && !st.retired.contains(&format!("rebase-pr{}", pr.number))
        })
        .collect();
    conflicting_prs.sort_by_key(|p| p.number);
    if let Some(pr) = conflicting_prs.first() {
        return format!(
            "tacho session rebase-{} \"Rebase conflicting PR #{} ({}) onto {}\" --base {}",
            pr.number, pr.number, pr.branch, pr.base, pr.base
        );
    }

    let mut failed_prs: Vec<u64> = st
        .failed
        .keys()
        .copied()
        .filter(|pr| {
            !st.holds.contains_key(&format!("pr:{pr}"))
                && !agent_held_prs.contains(pr)
                && !st.merged_prs.contains(pr)
                && !st.closed_prs.contains(pr)
        })
        .collect();
    failed_prs.sort_unstable();
    if let Some(pr_num) = failed_prs.first().copied() {
        let rec = &st.failed[&pr_num];
        let branch = st
            .open_prs_cache
            .iter()
            .find(|p| p.number == pr_num)
            .map(|p| p.branch.as_str())
            .unwrap_or("main");
        return format!(
            "tacho session fix-{pr_num} \"Fix PR #{pr_num}: {}\" --base {branch}",
            rec.summary
        );
    }

    let current_main = st.main_sha.as_deref().unwrap_or("");
    let ready = queue(
        &st.open_prs_cache,
        &st.passed,
        &st.batches,
        &st.holds,
        current_main,
        &HashMap::new(),
        &st.area_rules,
    );
    if let Some(train) = open_for(&st.trains) {
        if !train.prs.is_empty()
            && train_refusal(train, current_main, &st.open_prs_cache, &st.holds).is_none()
            && train
                .prs
                .iter()
                .filter(|n| !train.landed_prs.contains(n))
                .all(|n| ready.iter().any(|item| item.pr == *n))
        {
            let pr_list = train
                .prs
                .iter()
                .map(|n| format!("#{n}"))
                .collect::<Vec<_>>()
                .join(", ");
            return format!("ask the owner to land train {} (PRs {})", train.id, pr_list);
        }
    }
    let ready_prs: Vec<u64> = ready
        .into_iter()
        .map(|item| item.pr)
        .filter(|pr| {
            !st.merged_prs.contains(pr)
                && !st.closed_prs.contains(pr)
                && !st.failed.contains_key(pr)
        })
        .collect();
    if !ready_prs.is_empty() {
        let pr_list = ready_prs
            .iter()
            .map(|n| format!("#{n}"))
            .collect::<Vec<_>>()
            .join(", ");
        return format!("ask the owner to land PR {pr_list}");
    }

    let mut stale_holds: Vec<(&String, &tacho_core::decide::dispatch::Hold)> = st
        .holds
        .iter()
        .filter(|(_, h)| now.saturating_sub(h.since) >= STALE_HOLD_SECS)
        .collect();
    stale_holds.sort_by_key(|(k, h)| (h.since, *k));
    if let Some((target, hold)) = stale_holds.first() {
        let hours = now.saturating_sub(hold.since) / 3600;
        return format!(
            "decide: release or retire {} (held {}h: {})",
            target, hours, hold.reason
        );
    }

    let running_batches = st
        .batches
        .iter()
        .filter(|b| b.finished_at.is_none() || b.status == "running")
        .count();
    if !live.is_empty() || running_batches > 0 {
        return format!(
            "working: {} agent(s), {} batch(es)",
            live.len(),
            running_batches
        );
    }

    "run tick".to_string()
}

/// Executes `tacho next` (dry-run preview of what the next orchestrator tick will dispatch/verify/land).
pub fn execute_next(args: NextArgs) -> io::Result<()> {
    let store = open_fleet_store()?;
    let state = store.load()?;
    let now = now_secs();
    let live = live_agent_keys(&state);
    let free_slots = state.max_workers.saturating_sub(live.len());
    let passed_prs: BTreeSet<u64> = state.passed.keys().copied().collect();
    let failed_prs: BTreeSet<u64> = state.failed.keys().copied().collect();
    let scrubber = Scrubber::new();
    let instruction = scrubber.scrub(&compute_next_instruction(&state, &live, now));

    let (chosen, _) = next_work(
        &state.todo,
        &state.agents,
        &state.retired,
        &state.holds,
        &live,
        &passed_prs,
        &failed_prs,
        &state.merged_prs,
        &state.closed_prs,
        free_slots,
    );
    let planned_batches = batches(
        &state.open_prs_cache,
        &state.holds,
        &state.passed,
        &state.failed,
        &state.batches,
        &state.area_rules,
        6,
    );
    let areas = area_report(
        &state.agents,
        &state.todo,
        &state.retired,
        &state.holds,
        &live,
        &passed_prs,
        &failed_prs,
        &state.merged_prs,
        &state.closed_prs,
    );
    if args.json {
        let raw = serde_json::to_string_pretty(&serde_json::json!({
            "instruction": instruction,
            "free_slots": free_slots,
            "next_work": chosen,
            "planned_batches": planned_batches,
            "areas": areas,
        }))
        .unwrap_or_default();
        println!("{}", scrubber.scrub(&raw));
        return Ok(());
    }

    println!("Next: {instruction}");
    println!(
        "Workers: {}/{} active ({} free slots)",
        live.len(),
        state.max_workers,
        free_slots
    );
    if chosen.is_empty() {
        println!("Next work to dispatch: (none)");
    } else {
        println!("Next work to dispatch:");
        for t in &chosen {
            println!(
                "  - {} (area={}, base={})",
                t.key,
                if t.area.is_empty() { "-" } else { &t.area },
                t.base.as_deref().unwrap_or("main")
            );
        }
    }
    if !planned_batches.is_empty() {
        println!("Planned batches:");
        for b in &planned_batches {
            println!(
                "  - {} [{}] PRs {:?} ({})",
                b.id,
                b.group,
                b.prs,
                scrubber.scrub(&b.because)
            );
        }
    }
    if !areas.is_empty() {
        println!("Area gates:");
        for ar in &areas {
            println!(
                "  - {:<14} {:<10} base={:<6} ({})",
                ar.area,
                ar.state,
                ar.base_pr
                    .map(|p| format!("#{p}"))
                    .unwrap_or_else(|| "main".to_string()),
                scrubber.scrub(&ar.detail)
            );
        }
    }
    Ok(())
}
