//! Landing verified merge trains and ready PRs (`tacho land`) with base SHA and post-merge
//! tree hash verification.

use std::collections::HashMap;
use std::io;
use std::process::Command;

use tacho_core::decide::land::{finished, mismatch, open_for, queue, train_refusal, TrainRecord};
use tacho_core::TaskStatus;

use crate::cli::LandArgs;

use super::store::{emit_decision_event, now_ns, open_fleet_store};

/// Options for `tacho land`.
#[derive(Debug, Clone, Default)]
pub struct LandOptions {
    pub train: Option<String>,
    pub dry_run: bool,
}

impl From<LandArgs> for LandOptions {
    fn from(args: LandArgs) -> Self {
        Self {
            train: args.train.or(args.train_flag),
            dry_run: args.dry_run,
        }
    }
}

/// Verifies pre-merge base SHA and post-merge `main^{tree}` integrity for a merge train.
///
/// 1. Checks that `current_main` matches `train.base` (refusing if `main` moved since verification).
/// 2. Checks that `post_merge_main_tree` equals `train.tree` (flagging any tree mismatch).
pub fn verify_train_land_integrity(
    train: &TrainRecord,
    current_main: &str,
    post_merge_main_tree: &str,
) -> Result<(), String> {
    if !train.base.is_empty() && !current_main.is_empty() && train.base != current_main {
        return Err(format!(
            "refusing to land train {}: main moved from {} to {}",
            train.id,
            &train.base[..train.base.len().min(12)],
            &current_main[..current_main.len().min(12)]
        ));
    }
    let landed = finished(train);
    if let Some(err) = mismatch(&landed, post_merge_main_tree) {
        return Err(err);
    }
    Ok(())
}

/// Executes `tacho land`.
pub fn execute_land(args: LandArgs) -> io::Result<()> {
    execute_land_with(LandOptions::from(args))
}

/// Executes `tacho land` with [`LandOptions`].
pub fn execute_land_with(args: LandOptions) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let cwd = std::env::current_dir()?;
    let current_main = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&cwd)
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let current_tree = Command::new("git")
        .args(["rev-parse", "HEAD^{tree}"])
        .current_dir(&cwd)
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();

    let target_open = if let Some(ref tid) = args.train {
        let found = state
            .trains
            .iter()
            .find(|t| (t.id == *tid || format!("train/{}", t.id) == *tid) && t.status == "open")
            .cloned();
        if found.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("open train '{tid}' not found"),
            ));
        }
        found
    } else {
        open_for(&state.trains).cloned()
    };

    if let Some(open) = target_open {
        if let Some(refusal_reason) =
            train_refusal(&open, &current_main, &state.open_prs_cache, &state.holds)
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, refusal_reason));
        }
        if args.dry_run {
            println!("Would land train {}: PRs {:?}", open.id, open.prs);
            return Ok(());
        }
        let merge_enabled = match std::env::var("TACHO_LAND_MERGE") {
            Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
            Err(_) => state.merge_prs,
        };
        let gh_forge = std::env::var("TACHO_GH_REPO")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|r| {
                tacho_core::decide::plugin::GitHubForge::new(r).with_allow_merge(merge_enabled)
            });
        let post_merge_tree = if let (Some(ref forge), true) = (&gh_forge, merge_enabled) {
            use tacho_core::decide::plugin::ForgePlugin;
            for &pr in &open.prs {
                let _ = forge.mark_pr_ready(pr);
                forge.merge_pr(pr).map_err(io::Error::other)?;
            }
            let _ = Command::new("git")
                .args(["fetch", "-q", "origin", "main"])
                .current_dir(&cwd)
                .status();
            Command::new("git")
                .args(["rev-parse", "origin/main^{tree}"])
                .current_dir(&cwd)
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| current_tree.clone())
        } else {
            current_tree.clone()
        };
        let mut landed = finished(&open);
        if let Some(err) = mismatch(&landed, &post_merge_tree) {
            landed.status = "mismatch".to_string();
            store.mutate(Some(("land", "train_mismatch", &open.id, &err)), |st| {
                for t in &mut st.trains {
                    if t.id == open.id {
                        *t = landed.clone();
                    }
                }
            })?;
            store.emit_state_band(
                &format!("train:{}", open.id),
                "mismatch",
                TaskStatus::Failed,
                &err,
                now_ns(),
            )?;
            emit_decision_event(&format!("train:{}", open.id), "land:mismatch", &err);
            let mut hands = super::watch::CliHands {
                home: crate::paths::resolve_home(),
            };
            let _ = tacho_daemon::exec::Hands::notify_owner(
                &mut hands,
                &format!("[tacho] train {} mismatch", open.id),
                &err,
            );
            if let Some(ref forge) = gh_forge {
                use tacho_core::decide::plugin::ForgePlugin;
                let _ = forge.set_issue(&format!("train {} mismatch", open.id), &err);
            }
            return Err(io::Error::other(err));
        }
        let because = format!("landed train {} (PRs {:?})", open.id, open.prs);
        store.mutate(Some(("land", "land_train", &open.id, &because)), |st| {
            for &p in &open.prs {
                st.merged_prs.insert(p);
            }
            for t in &mut st.trains {
                if t.id == open.id {
                    *t = landed.clone();
                }
            }
        })?;
        store.emit_state_band(
            &format!("train:{}", open.id),
            "landed",
            TaskStatus::Done,
            &because,
            now_ns(),
        )?;
        emit_decision_event(&format!("train:{}", open.id), "land:done", &because);
        println!("{because}");
        return Ok(());
    }

    let ready = queue(
        &state.open_prs_cache,
        &state.passed,
        &state.batches,
        &state.holds,
        &current_main,
        &HashMap::new(),
        &state.area_rules,
    );
    if ready.is_empty() {
        println!("Nothing ready to land.");
        return Ok(());
    }
    for item in &ready {
        println!(
            "Ready to land PR #{} ({}) from batch {}",
            item.pr, item.title, item.batch_id
        );
    }
    Ok(())
}
