//! Process tree sampling (`scan_procs`), progress verdicts (`tacho progress`),
//! orchestrator tick (`tacho tick`), and `tacho watchdog` `OPEN`/`UPDATE`/`CLOSE` issue lifecycle.

use std::io;
use std::path::PathBuf;

use tacho_core::decide::dispatch::TodoItem;
use tacho_core::decide::watch::{agent_row, chain_rows, down_alert, down_reason, ProcEntry};
use tacho_core::Scrubber;
use tacho_daemon::FleetStore;

use crate::cli::{ProgressArgs, TickArgs, WatchdogArgs};

use super::dispatch::{deliver_tell_action, spawn_worker_for_item};
use super::ops::write_scrubbed_snapshot;
use super::store::{
    emit_decision_event, live_agent_keys, load_fleet_meta, mutate_fleet_meta, now_secs,
    open_fleet_store, WatchdogIssueState,
};

/// Executes `tacho progress`.
pub fn execute_progress(args: ProgressArgs) -> io::Result<()> {
    let store = open_fleet_store()?;
    let state = store.load()?;
    let now = now_secs();

    let mut agent_rows = Vec::new();
    for a in state.agents.values() {
        if state.retired.contains(&a.key) {
            continue;
        }
        let samples = state.samples.get(&a.key).map(Vec::as_slice).unwrap_or(&[]);
        let notes = state.notes.get(&a.key).map(Vec::as_slice).unwrap_or(&[]);
        agent_rows.push(agent_row(a, samples, notes, now));
    }
    let chains = chain_rows(&state.agents, &state.batches, &state.retired, now);

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agents": agent_rows,
                "chains": chains,
            }))
            .unwrap_or_default()
        );
        return Ok(());
    }

    println!("=== Agent Progress ===");
    if agent_rows.is_empty() {
        println!("  (no active or recent agents)");
    } else {
        println!(
            "{:<20} {:<12} {:<10} {:<8} {:>8}  NOTE",
            "KEY", "AREA", "VERDICT", "PHASE", "IDLE"
        );
        for r in &agent_rows {
            let phase_str = if r.phase_long {
                format!("{}!", r.phase)
            } else {
                r.phase.clone()
            };
            println!(
                "{:<20} {:<12} {:<10} {:<8} {:>7}s  {}",
                r.key,
                if r.area.is_empty() { "-" } else { &r.area },
                r.verdict,
                phase_str,
                r.idle_secs,
                r.note
            );
        }
    }

    if !chains.is_empty() {
        println!("\n=== Area Chains ===");
        println!(
            "{:<14} {:>6} {:>6} {:>10} {:>10} {:>10} {:>10}",
            "AREA", "1H", "24H", "MED_RUN", "MED_BATCH", "OWNER_W", "MACH_W"
        );
        for c in &chains {
            println!(
                "{:<14} {:>6} {:>6} {:>9}s {:>9}s {:>9}s {:>9}s",
                c.area,
                c.steps_last_hour,
                c.steps_last_day,
                c.median_run_secs,
                c.median_batch_secs,
                c.owner_wait_secs,
                c.machine_wait_secs
            );
        }
    }
    Ok(())
}

/// Collects host `/proc` entries for supervision and tidy checks.
pub fn scan_procs() -> Vec<ProcEntry> {
    tacho_daemon::exec::observe::scan_procs()
}

pub struct CliHands {
    pub home: PathBuf,
}

impl tacho_daemon::exec::Hands for CliHands {
    fn spawn_worker(&mut self, store: &mut FleetStore, item: &TodoItem) -> io::Result<Option<u32>> {
        spawn_worker_for_item(store, item).map(Some)
    }

    fn spawn_batch(
        &mut self,
        store: &mut FleetStore,
        plan: &tacho_core::decide::verify::BatchPlan,
    ) -> io::Result<Option<u32>> {
        crate::fleet::batch::spawn_daemon_batch(store, plan).map(Some)
    }

    fn deliver_tell(
        &mut self,
        store: &mut FleetStore,
        agent_key: &str,
        msg: &str,
    ) -> io::Result<()> {
        deliver_tell_action(store, agent_key, msg)
    }

    fn emit(&mut self, track: &str, name: &str, because: &str) {
        emit_decision_event(track, name, because);
    }

    fn publish(&mut self, store: &FleetStore) -> io::Result<()> {
        let st = store.load()?;
        let val = serde_json::to_value(&st).map_err(io::Error::other)?;
        write_scrubbed_snapshot(&self.home.join("snapshot.json"), &val)
    }

    fn notify_owner(&mut self, subject: &str, body: &str) -> Result<bool, String> {
        if cfg!(test) {
            return Ok(false);
        }
        tacho_core::decide::plugin::notify_owner_command(subject, body)
    }

    fn prune_root_disk_volumes(&mut self) -> Result<String, String> {
        tacho_daemon::exec::prune_runner_volumes_command(None)
    }
}

/// Executes a single orchestrator & supervisor pass (`tacho tick`) driven entirely by `tacho_core::decide::decide`.
pub fn execute_tick(args: TickArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    execute_tick_with(&mut store, args)
}

/// Executes a single orchestrator & supervisor pass against an open [`FleetStore`].
pub fn execute_tick_with(store: &mut FleetStore, args: TickArgs) -> io::Result<()> {
    let home = crate::paths::resolve_home();
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let live = tacho_core::LiveState::default();
    let mut hands = CliHands { home: home.clone() };
    let gh_forge = std::env::var("TACHO_GH_REPO")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(tacho_core::decide::plugin::GitHubForge::new);
    let mut opts = tacho_daemon::exec::TickOptions::new(&live);
    opts.dry_run = args.dry_run;
    opts.repo = Some(cwd);
    if let Some(ref f) = gh_forge {
        opts.forge = Some(f);
    }
    tacho_daemon::exec::run_tick(&home, store, &mut hands, opts).map(|_| ())
}

/// Lifecycle action decided by the watchdog for the `"tacho is down"` issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchdogStep {
    /// Open a new `"tacho is down"` issue.
    Open { title: String, body: String },
    /// Comment on the existing open `"tacho is down"` issue after cooldown.
    Update { number: u64, body: String },
    /// Comment recovery and close the open `"tacho is down"` issue.
    Close { number: u64, body: String },
    /// Nothing to do (healthy with no open issue, or within comment cooldown).
    None,
}

/// Evaluates and transitions the watchdog `"tacho is down"` issue lifecycle (`OPEN`, `UPDATE`, `CLOSE`, `NONE`).
pub fn run_watchdog_lifecycle(
    reason: Option<&str>,
    open_issue: Option<WatchdogIssueState>,
    now: i64,
    cooldown_secs: i64,
) -> (WatchdogStep, Option<WatchdogIssueState>) {
    let scrubber = Scrubber::new();
    match (reason, open_issue) {
        (Some(why), None) => {
            let clean = scrubber.scrub(why);
            (
                WatchdogStep::Open {
                    title: "tacho is down".to_string(),
                    body: format!("Watchdog detected stale orchestrator tick: {clean}"),
                },
                Some(WatchdogIssueState {
                    number: 1,
                    last_comment_at: now,
                }),
            )
        }
        (Some(why), Some(mut issue)) => {
            if now.saturating_sub(issue.last_comment_at) >= cooldown_secs {
                let clean = scrubber.scrub(why);
                issue.last_comment_at = now;
                (
                    WatchdogStep::Update {
                        number: issue.number,
                        body: format!("Still down at {now}: {clean}"),
                    },
                    Some(issue),
                )
            } else {
                (WatchdogStep::None, Some(issue))
            }
        }
        (None, Some(issue)) => (
            WatchdogStep::Close {
                number: issue.number,
                body: format!("Orchestrator recovered at {now}; closing watchdog issue."),
            },
            None,
        ),
        (None, None) => (WatchdogStep::None, None),
    }
}

/// Executes `tacho watchdog` (checks whether the orchestrator tick is stale while work is pending).
pub fn execute_watchdog(args: WatchdogArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let now = now_secs();
    let live = live_agent_keys(&state);
    let has_active_work = !live.is_empty() || !state.todo.is_empty();
    let max_stale = args.max_stale.unwrap_or(600);
    let cooldown = args.cooldown.unwrap_or(3_600);
    let reason = down_reason(state.last_tick_ts, now, max_stale, has_active_work);

    let meta = load_fleet_meta().unwrap_or_default();
    let (step, mut next_issue) =
        run_watchdog_lifecycle(reason.as_deref(), meta.watchdog_issue, now, cooldown);
    let mut hands = CliHands {
        home: crate::paths::resolve_home(),
    };
    let gh_forge = std::env::var("TACHO_GH_REPO")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(tacho_core::decide::plugin::GitHubForge::new);
    match &step {
        WatchdogStep::Open { title, body } => {
            use tacho_daemon::exec::Hands;
            let _ = hands.notify_owner(title, body);
            if let Some(ref forge) = gh_forge {
                use tacho_core::decide::plugin::ForgePlugin;
                if let Ok(Some(num)) = forge.set_issue(title, body) {
                    if let Some(ref mut iss) = next_issue {
                        iss.number = num;
                    }
                }
            }
        }
        WatchdogStep::Update { number, body } => {
            use tacho_daemon::exec::Hands;
            let _ = hands.notify_owner("tacho is down", body);
            if let Some(ref forge) = gh_forge {
                use tacho_core::decide::plugin::ForgePlugin;
                let _ = forge.comment_issue(*number, body);
            }
        }
        WatchdogStep::Close { number, body } => {
            store.record_decision("watch", "watchdog_close", &number.to_string(), body)?;
            if let Some(ref forge) = gh_forge {
                use tacho_core::decide::plugin::ForgePlugin;
                let _ = forge.close_issue("tacho is down", body);
            }
        }
        WatchdogStep::None => {}
    }
    mutate_fleet_meta(|m| {
        m.watchdog_issue = next_issue;
    })?;

    let alert = down_alert(
        reason.as_deref(),
        state.last_watchdog_alert_ts,
        now,
        cooldown,
    );
    if let Some(msg) = alert {
        store.mutate(
            Some(("watch", "watchdog_alert", "orchestrator", &msg)),
            |st| {
                st.last_watchdog_alert_ts = Some(now);
            },
        )?;
        eprintln!("WATCHDOG ALERT: {msg}");
        return Err(io::Error::other(msg));
    }
    println!("Watchdog OK.");
    Ok(())
}
