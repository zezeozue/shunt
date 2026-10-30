//! Snapshot collection from `FleetState`, host `/proc` machine inspection, and `$TACHO_HOME/snapshot.json` loading.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tacho_core::decide::dispatch::{may_start, STALE_HOLD_SECS};
use tacho_core::decide::land::{open_for, queue};
use tacho_core::decide::verify::flake_rows;
use tacho_core::decide::watch::{
    agent_row, chain_rows, needs_you, summarize_costs, tree_cpu, zombie_lines, CostRecord,
    ProcEntry,
};
use tacho_daemon::{FleetState, FleetStore};

use crate::fleet::store::{live_agent_keys, now_secs};

use super::model::{
    ActionableItem, AgentStatusRow, CostSection, CpuOwnerRow, FlakeRowView, HeadlineSection,
    HeldSection, HoldRow, MachineSection, PerPrCost, PipelineSection, ProgressSection,
    QueuedTaskView, StatusSnapshot, TopSpendRow, MAX_SNAPSHOT_AGE_SECS, SNAPSHOT_VERSION,
    VALID_SECTIONS,
};

fn scan_host_procs() -> Vec<ProcEntry> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return out;
    };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(String::from) else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
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
        let state = fields[0].to_string();
        let ppid: u32 = fields[1].parse().unwrap_or(0);
        let pgid: u32 = fields[2].parse().unwrap_or(0);
        let utime: u64 = fields[11].parse().unwrap_or(0);
        let stime: u64 = fields[12].parse().unwrap_or(0);
        let cwd = fs::read_link(p_dir.join("cwd"))
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        out.push(ProcEntry {
            pid,
            ppid,
            pgid,
            state,
            cpu_ticks: utime + stime,
            elapsed_secs: 0,
            comm: comm.clone(),
            args: comm,
            cwd,
            env_starve: false,
        });
    }
    out
}

pub(crate) fn parse_cpus(cpu_max: &str) -> Option<f64> {
    let mut parts = cpu_max.split_whitespace();
    let quota_str = parts.next()?;
    if quota_str == "max" {
        return None;
    }
    let quota: f64 = quota_str.parse().ok()?;
    let period: f64 = parts.next().unwrap_or("100000").parse().ok()?;
    if quota <= 0.0 || period <= 0.0 {
        return None;
    }
    Some(quota / period)
}

fn read_host_machine(state: &FleetState, live: &BTreeSet<String>) -> MachineSection {
    let usable_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let quota_cores = fs::read_to_string("/sys/fs/cgroup/cpu.max")
        .ok()
        .and_then(|s| parse_cpus(&s))
        .unwrap_or(usable_cpus as f64);

    let (load1, load5) = fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| {
            let mut parts = s.split_whitespace();
            let l1 = parts.next()?.parse::<f64>().ok()?;
            let l5 = parts.next()?.parse::<f64>().ok()?;
            Some((l1, l5))
        })
        .unwrap_or((0.0, 0.0));

    let (mem_total_gb, mem_used_gb) = fs::read_to_string("/proc/meminfo")
        .ok()
        .map(|s| {
            let mut total_kb = 0u64;
            let mut avail_kb = 0u64;
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    total_kb = rest
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
                    avail_kb = rest
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                }
            }
            let total_gb = (total_kb as f64) / (1024.0 * 1024.0);
            let used_gb = (total_kb.saturating_sub(avail_kb) as f64) / (1024.0 * 1024.0);
            (
                (total_gb * 10.0).round() / 10.0,
                (used_gb * 10.0).round() / 10.0,
            )
        })
        .unwrap_or((0.0, 0.0));

    let disk_free_gb = {
        let home = crate::paths::resolve_home();
        let target = if home.exists() {
            home
        } else {
            PathBuf::from(".")
        };
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let c_path = std::ffi::CString::new(target.to_string_lossy().as_bytes()).ok();
        if let Some(cp) = c_path {
            if unsafe { libc::statvfs(cp.as_ptr(), &mut stat) } == 0 {
                let free_bytes = (stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64);
                ((free_bytes as f64) / (1024.0 * 1024.0 * 1024.0) * 10.0).round() / 10.0
            } else {
                0.0
            }
        } else {
            0.0
        }
    };

    let procs = scan_host_procs();
    let zombies = zombie_lines(&procs);
    let mut by_owner = Vec::new();
    let mut tracked_cores = 0.0;

    for key in live {
        if let Some(a) = state.agents.get(key) {
            let ticks = a.pid.map(|pid| tree_cpu(pid, &procs)).unwrap_or(0);
            let elapsed = (now_secs() - a.started_at).max(1) as f64;
            let cores = (((ticks as f64) / 100.0 / elapsed) * 100.0).round() / 100.0;
            tracked_cores += cores;
            by_owner.push(CpuOwnerRow {
                owner: key.clone(),
                kind: "agent".to_string(),
                cores,
            });
        }
    }
    for b in &state.batches {
        if b.status == "running" || b.status == "bisecting" {
            let ticks = b.pid.map(|pid| tree_cpu(pid, &procs)).unwrap_or(0);
            let elapsed = (now_secs() - b.started_at).max(1) as f64;
            let cores = (((ticks as f64) / 100.0 / elapsed) * 100.0).round() / 100.0;
            tracked_cores += cores;
            by_owner.push(CpuOwnerRow {
                owner: b.id.clone(),
                kind: "batch".to_string(),
                cores,
            });
        }
    }
    by_owner.sort_by(|a, b| {
        b.cores
            .partial_cmp(&a.cores)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.owner.cmp(&b.owner))
    });

    let cores_in_use = if tracked_cores > 0.0 {
        (tracked_cores * 100.0).round() / 100.0
    } else {
        (load1.min(quota_cores) * 100.0).round() / 100.0
    };
    let cpu_percent = if quota_cores > 0.0 {
        ((cores_in_use / quota_cores) * 1000.0).round() / 10.0
    } else {
        0.0
    };
    let max_cpu_percent = std::env::var("TACHO_MAX_CPU_PERCENT")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(90.0);

    MachineSection {
        cores_in_use,
        quota_cores,
        cpu_percent,
        max_cpu_percent,
        load1,
        load5,
        usable_cpus,
        mem_used_gb,
        mem_total_gb,
        disk_free_gb,
        by_owner,
        zombies,
    }
}

/// Builds a versioned [`StatusSnapshot`] from [`FleetState`] at timestamp `at`.
pub fn build_snapshot_from_fleet(state: &FleetState, at: i64, now: i64) -> StatusSnapshot {
    let live = live_agent_keys(state);
    let passed_prs: BTreeSet<u64> = state.passed.keys().copied().collect();
    let failed_prs: BTreeSet<u64> = state.failed.keys().copied().collect();

    let raw_needs = needs_you(
        &state.agents,
        &state.todo,
        &state.retired,
        &state.holds,
        &live,
        &state.open_prs_cache,
        &state.passed,
        &state.failed,
        now,
    );
    let mut needs_items = Vec::new();
    let mut attention_items = Vec::new();
    for item in raw_needs {
        let act = ActionableItem {
            id: format!("{}:{}", item.kind, item.target),
            target: item.target.clone(),
            kind: item.kind.clone(),
            summary: item.summary.clone(),
            fix: item.command_hint.clone(),
            command_hint: item.command_hint.clone(),
        };
        if item.kind == "stale-hold" {
            attention_items.push(act);
        } else {
            needs_items.push(act);
        }
    }

    for (k, a) in &state.agents {
        if a.finished_at.is_some()
            && a.pr.is_none()
            && !a.pr_none
            && !state.retired.contains(k)
            && !live.contains(k)
            && !needs_items.iter().any(|i| &i.target == k)
        {
            let fix = format!("tacho attach {k} <pr|none>");
            needs_items.push(ActionableItem {
                id: format!("no-pr:{k}"),
                target: k.clone(),
                kind: "no-pr".to_string(),
                summary: format!("{k} finished without a PR"),
                fix: fix.clone(),
                command_hint: fix,
            });
        }
    }

    for b in &state.batches {
        if b.status == "failed" || b.status == "red" || b.status == "main-broken" {
            let fix = format!("tacho replay {}", b.id);
            attention_items.push(ActionableItem {
                id: format!("batch:{}", b.id),
                target: b.id.clone(),
                kind: "red-batch".to_string(),
                summary: format!("batch {} ({:?}) ended {}", b.id, b.prs, b.status),
                fix: fix.clone(),
                command_hint: fix,
            });
        }
    }

    let mut combined_needs = needs_items.clone();
    combined_needs.extend(attention_items.clone());

    let mut hold_rows = Vec::new();
    let mut stale_rows = Vec::new();
    for (target, h) in &state.holds {
        let age_secs = (now - h.since).max(0);
        let stale = age_secs >= STALE_HOLD_SECS;
        let row = HoldRow {
            target: target.clone(),
            why: h.reason.clone(),
            reason: h.reason.clone(),
            since: h.since,
            age_secs,
            stale,
            fix: format!("tacho release {target}"),
        };
        if stale {
            stale_rows.push(row.clone());
        }
        hold_rows.push(row);
    }
    let mut refused = Vec::new();
    if let Some(ref sha) = state.refused_self_update_sha {
        refused.push(format!("self-update refused at commit {sha}"));
    }
    let held_sec = HeldSection {
        holds: hold_rows,
        stale_holds: stale_rows.clone(),
        refused: refused.clone(),
    };
    let stale_sec = HeldSection {
        holds: stale_rows.clone(),
        stale_holds: stale_rows,
        refused,
    };

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let current_main = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&cwd)
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let ready_list = queue(
        &state.open_prs_cache,
        &state.passed,
        &state.batches,
        &state.holds,
        &current_main,
        &HashMap::new(),
        &state.area_rules,
    );
    let ready_json: Vec<serde_json::Value> = ready_list
        .iter()
        .map(|r| {
            serde_json::json!({
                "pr": r.pr,
                "sha": r.sha,
                "batch_id": r.batch_id,
                "title": r.title,
            })
        })
        .collect();
    let batches_json: Vec<serde_json::Value> = state
        .batches
        .iter()
        .map(|b| {
            serde_json::json!({
                "id": b.id,
                "status": b.status,
                "prs": b.prs,
                "areas": b.areas,
                "mode": format!("{:?}", b.mode).to_ascii_lowercase(),
                "started_at": b.started_at,
                "finished_at": b.finished_at,
            })
        })
        .collect();
    let trains_json: Vec<serde_json::Value> = state
        .trains
        .iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "status": t.status,
                "prs": t.prs,
                "base": t.base,
                "tree": t.tree,
            })
        })
        .collect();
    let mut queue_views = Vec::new();
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
        queue_views.push(QueuedTaskView {
            key: item.key.clone(),
            area: item.area.clone(),
            priority: item.priority,
            provider: item
                .provider
                .clone()
                .unwrap_or_else(|| state.default_provider.clone()),
            model: item.model.clone().or_else(|| state.default_model.clone()),
            ready: ok,
            reason,
            brief: item.brief.lines().next().unwrap_or("").to_string(),
        });
    }
    let pipeline = PipelineSection {
        open_prs: state.open_prs_cache.len(),
        batches: batches_json,
        trains: trains_json,
        ready: ready_json,
        queue: queue_views,
    };

    let mut prog_agents = Vec::new();
    let mut agent_rows = Vec::new();
    for a in state.agents.values() {
        if state.retired.contains(&a.key) {
            continue;
        }
        let samples = state.samples.get(&a.key).map(Vec::as_slice).unwrap_or(&[]);
        let notes = state.notes.get(&a.key).map(Vec::as_slice).unwrap_or(&[]);
        let prow = agent_row(a, samples, notes, now);
        let status_label = if live.contains(&a.key) {
            "running".to_string()
        } else if a.pr.is_some() || a.pr_none {
            "done".to_string()
        } else if a.finished_at.is_some() {
            "stopped".to_string()
        } else {
            "idle".to_string()
        };
        agent_rows.push(AgentStatusRow {
            key: a.key.clone(),
            status: status_label,
            verdict: prow.verdict.clone(),
            phase: prow.phase.clone(),
            phase_age_secs: prow.phase_age_secs,
            phase_long: prow.phase_long,
            area: a.area.clone(),
            provider: a.provider.clone(),
            model: a.model.clone(),
            pr: a.pr,
            idle_secs: prow.idle_secs,
            note: prow.note.clone(),
            worktree: a.worktree.clone(),
        });
        prog_agents.push(prow);
    }
    let chains = chain_rows(&state.agents, &state.batches, &state.retired, now);
    let progress = ProgressSection {
        agents: prog_agents,
        chains,
    };

    let machine = read_host_machine(state, &live);

    let cost_records: Vec<CostRecord> = state.costs.clone();
    let summary = summarize_costs(&cost_records, &state.agents);
    let mut by_model: BTreeMap<String, f64> = BTreeMap::new();
    let mut by_place: BTreeMap<String, f64> = BTreeMap::new();
    let mut hour_usd = 0.0;
    let mut today_usd = 0.0;
    let mut week_usd = 0.0;
    let mut top_today: Vec<TopSpendRow> = Vec::new();
    let mut pr_costs: Vec<f64> = Vec::new();

    for r in &cost_records {
        let model_name = if r.model.is_empty() {
            r.provider.clone()
        } else {
            r.model.clone()
        };
        *by_model.entry(model_name.clone()).or_default() += r.usd;
        let place = state
            .agents
            .get(&r.key)
            .map(|a| {
                if a.area.is_empty() {
                    "unscoped".to_string()
                } else {
                    a.area.clone()
                }
            })
            .unwrap_or_else(|| "unscoped".to_string());
        *by_place.entry(place).or_default() += r.usd;
        let ts = state
            .agents
            .get(&r.key)
            .map(|a| a.finished_at.unwrap_or(a.started_at))
            .unwrap_or(now);
        if now - ts <= 3_600 {
            hour_usd += r.usd;
        }
        if now - ts <= 86_400 {
            today_usd += r.usd;
            top_today.push(TopSpendRow {
                key: r.key.clone(),
                model: model_name,
                usd: r.usd,
            });
        }
        if now - ts <= 7 * 86_400 {
            week_usd += r.usd;
        }
        if state.agents.get(&r.key).and_then(|a| a.pr).is_some() {
            pr_costs.push(r.usd);
        }
    }
    top_today.sort_by(|a, b| {
        b.usd
            .partial_cmp(&a.usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_today.truncate(5);
    let per_pr = if pr_costs.is_empty() {
        PerPrCost::default()
    } else {
        let sum: f64 = pr_costs.iter().sum();
        let max = pr_costs.iter().copied().fold(0.0_f64, f64::max);
        PerPrCost {
            samples: pr_costs.len(),
            avg_usd: ((sum / (pr_costs.len() as f64)) * 100.0).round() / 100.0,
            max_usd: (max * 100.0).round() / 100.0,
        }
    };
    let cost = CostSection {
        hour: (hour_usd * 100.0).round() / 100.0,
        today: (today_usd * 100.0).round() / 100.0,
        week: (week_usd * 100.0).round() / 100.0,
        total_usd: (summary.total_usd * 100.0).round() / 100.0,
        runs: summary.runs,
        by_model,
        by_place,
        by_provider: summary.by_provider,
        by_area: summary.by_area,
        top_today,
        per_pr,
    };

    let flakes: Vec<FlakeRowView> = flake_rows(&state.flakes)
        .into_iter()
        .map(|r| FlakeRowView {
            test: r.test,
            area: r.area,
            count: r.count as usize,
            last_seen: r.last_seen,
            task_key: r.task_key,
            batches: r.batches,
            excerpt: r.excerpt,
        })
        .collect();

    let health = if !needs_items.is_empty() {
        format!("needs you ({})", needs_items.len())
    } else if !attention_items.is_empty() {
        format!("attention ({})", attention_items.len())
    } else {
        "healthy".to_string()
    };
    let active_batches = state
        .batches
        .iter()
        .filter(|b| b.status == "running" || b.status == "bisecting")
        .count();
    let open_trains = usize::from(open_for(&state.trains).is_some());
    let headline = HeadlineSection {
        health,
        needs_you: needs_items.len(),
        attention: attention_items.len(),
        agents: live.len(),
        max_agents: state.max_workers.max(1),
        todo: state.todo.len(),
        open_prs: state.open_prs_cache.len(),
        batches: active_batches,
        trains: open_trains,
        ready: ready_list.len(),
        held: held_sec.holds.len(),
        stale_holds: held_sec.stale_holds.len(),
        cost_today: cost.today,
    };

    StatusSnapshot {
        version: SNAPSHOT_VERSION,
        at,
        headline,
        needs_you: needs_items,
        needs: combined_needs,
        attention: attention_items,
        pipeline,
        agents: agent_rows,
        machine,
        cost,
        held: held_sec,
        stale: stale_sec,
        progress,
        flakes,
    }
}

/// Validates requested `--section` names, returning canonicalized section names or an error string.
pub fn validate_sections(requested: &[String]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for raw in requested {
        for part in raw.split(',') {
            let s = part.trim().to_ascii_lowercase();
            if s.is_empty() {
                continue;
            }
            if !VALID_SECTIONS.contains(&s.as_str()) {
                return Err(format!(
                    "unknown section '{s}' (choose from: {})",
                    VALID_SECTIONS.join(", ")
                ));
            }
            if !out.contains(&s) {
                out.push(s);
            }
        }
    }
    Ok(out)
}

fn stale_snapshot_message(age_secs: i64) -> String {
    format!(
        "the snapshot is {} min old and the daemon writes one every tick; it has stopped. run: tacho tick",
        age_secs / 60
    )
}

/// Loads `$TACHO_HOME/snapshot.json` (either a versioned `StatusSnapshot` or a `FleetState` written by `tick`),
/// enforcing the 300s staleness check when a snapshot timestamp is present, or builds a fresh snapshot from `FleetStore`.
pub fn load_status_snapshot(home: &Path, now: i64) -> Result<StatusSnapshot, String> {
    let snap_path = home.join("snapshot.json");
    if snap_path.is_file() {
        let raw_bytes = fs::read(&snap_path)
            .map_err(|e| format!("failed to read {}: {e}", snap_path.display()))?;
        let val: serde_json::Value = serde_json::from_slice(&raw_bytes)
            .map_err(|_| "snapshot.json is malformed; run: tacho tick".to_string())?;

        if let Some(ver) = val.get("version").and_then(|v| v.as_u64()) {
            if ver != u64::from(SNAPSHOT_VERSION) {
                return Err("snapshot.json has an unsupported version; run: tacho tick".to_string());
            }
            let at = val.get("at").and_then(|v| v.as_i64()).unwrap_or(0);
            let age = now - at;
            if age > MAX_SNAPSHOT_AGE_SECS {
                return Err(stale_snapshot_message(age));
            }
            if let Ok(parsed) = serde_json::from_value::<StatusSnapshot>(val.clone()) {
                return Ok(parsed);
            }
            let mut base = build_snapshot_from_fleet(&FleetState::default(), at, now);
            if let Some(h) = val.get("headline") {
                if let Ok(hd) = serde_json::from_value(h.clone()) {
                    base.headline = hd;
                }
            }
            if let Some(ny) = val.get("needs_you").or_else(|| val.get("needs")) {
                if let Ok(items) = serde_json::from_value::<Vec<ActionableItem>>(ny.clone()) {
                    base.needs_you = items.clone();
                    base.needs = items;
                }
            }
            if let Some(att) = val.get("attention") {
                if let Ok(items) = serde_json::from_value::<Vec<ActionableItem>>(att.clone()) {
                    base.attention = items.clone();
                    let mut combined = base.needs_you.clone();
                    combined.extend(items);
                    base.needs = combined;
                }
            }
            if let Some(m) = val.get("machine") {
                if let Ok(ms) = serde_json::from_value(m.clone()) {
                    base.machine = ms;
                }
            }
            if let Some(c) = val.get("cost") {
                if let Ok(cs) = serde_json::from_value(c.clone()) {
                    base.cost = cs;
                }
            }
            if let Some(h) = val.get("held") {
                if let Ok(hs) = serde_json::from_value::<HeldSection>(h.clone()) {
                    base.held = hs.clone();
                    base.stale = hs;
                }
            }
            if let Some(fl) = val.get("flakes") {
                if let Ok(fs_rows) = serde_json::from_value(fl.clone()) {
                    base.flakes = fs_rows;
                }
            }
            return Ok(base);
        }

        let st: FleetState = serde_json::from_value(val).map_err(|_| {
            "snapshot.json is missing version or valid fleet state; run: tacho tick".to_string()
        })?;
        let at = st.last_tick_ts.unwrap_or(now);
        let age = now - at;
        if age > MAX_SNAPSHOT_AGE_SECS {
            return Err(stale_snapshot_message(age));
        }
        return Ok(build_snapshot_from_fleet(&st, at, now));
    }

    let db_path = home.join("events.sqlite");
    let st = if db_path.exists() {
        FleetStore::open(&db_path)
            .and_then(|s| s.load())
            .unwrap_or_default()
    } else {
        FleetState::default()
    };
    if let Some(last_ts) = st.last_tick_ts {
        let age = now - last_ts;
        if age > MAX_SNAPSHOT_AGE_SECS {
            return Err(stale_snapshot_message(age));
        }
    }
    let at = st.last_tick_ts.unwrap_or(now);
    Ok(build_snapshot_from_fleet(&st, at, now))
}
