//! Unified SQLite WAL fleet state and decision journal stored in `$TACHO_HOME/events.sqlite`.
//!
//! Stores the fleet state document (`FleetState`) in the `fleet_state` table and records
//! every orchestrator/supervisor decision with its `because` reason into both the `decisions`
//! table and the Perfetto `events` timeline table.

use prost::Message;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tacho_core::decide::dispatch::{
    builtin_providers, AgentRun, Hold, ProviderSpec, PullFacts, TodoItem,
};
use tacho_core::decide::land::{TrainPrState, TrainRecord};
use tacho_core::decide::verify::{AreaRule, BatchRun, FailedRecord, FlakeEntry, PassedRecord};
use tacho_core::decide::watch::{CostRecord, PhaseNote, ProgressSample};
use tacho_core::decide::{DecisionPlan, DecisionState, WorldObservation};
use tacho_core::{Kind, Scrubber, TaskStatus};
use tacho_proto::tacho;

/// A recorded worktree allocation in fleet state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorktreeEntry {
    pub slug: String,
    pub path: String,
    pub branch: String,
    pub base_sha: String,
    #[serde(default)]
    pub base_pr: Option<u64>,
    pub port: u16,
    pub port_last: u16,
    pub created_at: i64,
}

/// A recorded orchestrator or supervisor decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub id: i64,
    pub ts: i64,
    pub category: String,
    pub action: String,
    pub target: String,
    pub because: String,
}

/// Complete persistent fleet state stored in `$TACHO_HOME/events.sqlite`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FleetState {
    pub max_workers: usize,
    pub default_provider: String,
    pub default_model: Option<String>,
    pub default_effort: Option<String>,
    pub todo: Vec<TodoItem>,
    pub agents: BTreeMap<String, AgentRun>,
    pub retired: BTreeSet<String>,
    pub holds: BTreeMap<String, Hold>,
    pub worktrees: BTreeMap<String, WorktreeEntry>,
    pub batches: Vec<BatchRun>,
    pub passed: BTreeMap<u64, PassedRecord>,
    pub failed: BTreeMap<u64, FailedRecord>,
    pub flakes: BTreeMap<String, FlakeEntry>,
    pub trains: Vec<TrainRecord>,
    pub train_pr: Option<TrainPrState>,
    pub notes: BTreeMap<String, Vec<PhaseNote>>,
    pub samples: BTreeMap<String, Vec<ProgressSample>>,
    pub nudged: BTreeSet<String>,
    pub rebase_attempts: HashMap<u64, String>,
    pub fix_notified: HashMap<u64, String>,
    pub costs: Vec<CostRecord>,
    pub area_rules: Vec<AreaRule>,
    pub generators: Vec<(String, String)>,
    pub open_prs_cache: Vec<PullFacts>,
    pub merged_prs: BTreeSet<u64>,
    pub closed_prs: BTreeSet<u64>,
    pub last_tick_ts: Option<i64>,
    pub last_watchdog_alert_ts: Option<i64>,
    pub providers: BTreeMap<String, ProviderSpec>,
    pub inbox: BTreeMap<String, Vec<String>>,
    pub db_up_cmd: Option<String>,
    pub workflow_path: Option<String>,
    pub workflow_jobs: BTreeMap<String, String>,
    pub workflow_expressions: BTreeMap<String, String>,
    pub train_compile_cmd: Option<String>,
    pub notified_failures: BTreeSet<String>,
    pub notified_deploys: BTreeSet<u64>,
    pub self_update_paths: Vec<String>,
    pub refused_self_update_sha: Option<String>,
    pub batch_timeout_secs: Option<u64>,
    /// Main's last completed CI run as last observed by the hands; `None` when unknown.
    pub main_ci: Option<tacho_core::decide::ops::snapshot::MainCiRun>,
    /// Remote main sha as last fetched by the world refresher; `None` when unknown.
    pub main_sha: Option<String>,
    /// Epoch seconds of the last successful forge refresh, per source (`prs`, `main`, `ci`).
    pub world_refreshed_at: BTreeMap<String, i64>,
    /// Whether `tacho land` is permitted to run `gh pr merge`. Defaults to `false` (owner merges by hand).
    #[serde(default)]
    pub merge_prs: bool,
    /// Recorded main repository checkout path for this fleet; `None` when not yet configured.
    #[serde(default)]
    pub repo_root: Option<String>,
}

impl Default for FleetState {
    fn default() -> Self {
        Self {
            max_workers: 4,
            default_provider: "claude".to_string(),
            default_model: None,
            default_effort: None,
            todo: Vec::new(),
            agents: BTreeMap::new(),
            retired: BTreeSet::new(),
            holds: BTreeMap::new(),
            worktrees: BTreeMap::new(),
            batches: Vec::new(),
            passed: BTreeMap::new(),
            failed: BTreeMap::new(),
            flakes: BTreeMap::new(),
            trains: Vec::new(),
            train_pr: None,
            notes: BTreeMap::new(),
            samples: BTreeMap::new(),
            nudged: BTreeSet::new(),
            rebase_attempts: HashMap::new(),
            fix_notified: HashMap::new(),
            costs: Vec::new(),
            area_rules: Vec::new(),
            generators: Vec::new(),
            open_prs_cache: Vec::new(),
            merged_prs: BTreeSet::new(),
            closed_prs: BTreeSet::new(),
            last_tick_ts: None,
            last_watchdog_alert_ts: None,
            providers: builtin_providers(),
            inbox: BTreeMap::new(),
            db_up_cmd: None,
            workflow_path: None,
            workflow_jobs: BTreeMap::new(),
            workflow_expressions: BTreeMap::new(),
            train_compile_cmd: None,
            notified_failures: BTreeSet::new(),
            notified_deploys: BTreeSet::new(),
            self_update_paths: vec!["crates/".to_string(), "scripts/".to_string()],
            refused_self_update_sha: None,
            batch_timeout_secs: None,
            main_ci: None,
            main_sha: None,
            world_refreshed_at: BTreeMap::new(),
            merge_prs: false,
            repo_root: None,
        }
    }
}

/// SQLite-backed store for [`FleetState`] and [`DecisionRecord`]s sharing `$TACHO_HOME/events.sqlite`.
pub struct FleetStore {
    conn: Connection,
    home: std::path::PathBuf,
    appender: Option<crate::appender::AppenderHandle>,
}

fn sql_err(e: rusqlite::Error) -> io::Error {
    io::Error::other(e.to_string())
}

fn state_error(error: serde_json::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "invalid fleet state ({:?}, line {}, column {})",
            error.classify(),
            error.line(),
            error.column()
        ),
    )
}

/// Alias for [`FleetStore`] sharing `$TACHO_HOME/events.sqlite`.
pub type FleetDb = FleetStore;

impl FleetStore {
    /// Opens or initializes the fleet tables inside `$TACHO_HOME/events.sqlite`.
    pub fn open(db_path: &Path) -> io::Result<Self> {
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sql_err)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(sql_err)?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(sql_err)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(sql_err)?;

        let existing_tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('fleet_state', 'decisions', 'decision_ticks', 'events')",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        if existing_tables < 4 {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS fleet_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    updated_at INTEGER NOT NULL,
                    state_json TEXT NOT NULL
                );",
                [],
            )
            .map_err(sql_err)?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    because TEXT NOT NULL
                );",
                [],
            )
            .map_err(sql_err)?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_decisions_ts ON decisions(ts);",
                [],
            )
            .map_err(sql_err)?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS decision_ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    obs_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL
                );",
                [],
            )
            .map_err(sql_err)?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_decision_ticks_ts ON decision_ticks(ts);",
                [],
            )
            .map_err(sql_err)?;

            // Also ensure the `events` table exists so offline fleet/kit commands can append
            // duration slices and state bands directly to `$TACHO_HOME/events.sqlite`.
            conn.execute(
                "CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY,
                    ts_ns INTEGER NOT NULL,
                    kind INTEGER NOT NULL,
                    track_key TEXT NOT NULL,
                    cause_seq INTEGER,
                    agent TEXT,
                    body BLOB NOT NULL,
                    counter_value INTEGER,
                    custom_name TEXT
                );",
                [],
            )
            .map_err(sql_err)?;
            for col_sql in [
                "ALTER TABLE events ADD COLUMN counter_value INTEGER;",
                "ALTER TABLE events ADD COLUMN custom_name TEXT;",
            ] {
                if let Err(e) = conn.execute(col_sql, []) {
                    if !e.to_string().contains("duplicate column") {
                        return Err(sql_err(e));
                    }
                }
            }
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_ns);",
                [],
            )
            .map_err(sql_err)?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_kind_seq ON events(kind, seq);",
                [],
            )
            .map_err(sql_err)?;
        }

        let home = db_path.parent().map(Path::to_path_buf).unwrap_or_default();
        Ok(Self {
            conn,
            home,
            appender: None,
        })
    }

    /// Routes trace inputs through the daemon's sequence allocator.
    pub fn attach_appender(&mut self, appender: crate::appender::AppenderHandle) {
        self.appender = Some(appender);
    }

    /// Directory containing this store's database (`$TACHO_HOME`).
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// Loads the current [`FleetState`] from SQLite, returning `FleetState::default()` if none exists yet.
    pub fn load(&self) -> io::Result<FleetState> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT state_json FROM fleet_state WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_err)?;
        match raw {
            Some(json) => serde_json::from_str(&json).map_err(state_error),
            None => Ok(FleetState::default()),
        }
    }

    /// Saves the updated [`FleetState`] atomically to SQLite.
    pub fn save(&self, state: &FleetState) -> io::Result<()> {
        let now = now_secs();
        let json = serde_json::to_string(state).map_err(io::Error::other)?;
        self.conn.execute(
            "INSERT INTO fleet_state (id, updated_at, state_json)
             VALUES (1, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at, state_json = excluded.state_json",
            params![now, json],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Atomically mutates [`FleetState`] inside an immediate transaction and optionally records a decision.
    pub fn mutate<F, R>(
        &mut self,
        decision: Option<(&str, &str, &str, &str)>,
        f: F,
    ) -> io::Result<R>
    where
        F: FnOnce(&mut FleetState) -> R,
    {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let raw: Option<String> = tx
            .query_row(
                "SELECT state_json FROM fleet_state WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_err)?;
        let mut state: FleetState = match raw {
            Some(json) => serde_json::from_str(&json).map_err(state_error)?,
            None => FleetState::default(),
        };
        let result = f(&mut state);
        let now = now_secs();
        let json = serde_json::to_string(&state).map_err(io::Error::other)?;
        tx.execute(
            "INSERT INTO fleet_state (id, updated_at, state_json)
             VALUES (1, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at, state_json = excluded.state_json",
            params![now, json],
        )
        .map_err(sql_err)?;
        if let Some((category, action, target, because)) = decision {
            let scrubber = Scrubber::new();
            let clean_target = scrubber.scrub(target);
            let clean_because = scrubber.scrub(because);
            tx.execute(
                "INSERT INTO decisions (ts, category, action, target, because)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![now, category, action, clean_target, clean_because],
            )
            .map_err(sql_err)?;
        }
        tx.commit().map_err(sql_err)?;
        Ok(result)
    }

    /// Appends an explicit decision entry to the `decisions` journal.
    pub fn record_decision(
        &self,
        category: &str,
        action: &str,
        target: &str,
        because: &str,
    ) -> io::Result<()> {
        let now = now_secs();
        let scrubber = Scrubber::new();
        let clean_target = scrubber.scrub(target);
        let clean_because = scrubber.scrub(because);
        self.conn
            .execute(
                "INSERT INTO decisions (ts, category, action, target, because)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![now, category, action, clean_target, clean_because],
            )
            .map_err(sql_err)?;
        Ok(())
    }

    /// Queries the most recent `limit` decisions, optionally filtered by `category` or `target`.
    pub fn recent_decisions(
        &self,
        limit: usize,
        category: Option<&str>,
        target: Option<&str>,
    ) -> io::Result<Vec<DecisionRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, ts, category, action, target, because
                 FROM decisions
                 WHERE (?1 IS NULL OR category = ?1)
                   AND (?2 IS NULL OR target = ?2)
                 ORDER BY id DESC
                 LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![category, target, limit as i64], |r| {
                Ok(DecisionRecord {
                    id: r.get(0)?,
                    ts: r.get(1)?,
                    category: r.get(2)?,
                    action: r.get(3)?,
                    target: r.get(4)?,
                    because: r.get(5)?,
                })
            })
            .map_err(sql_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(sql_err)?);
        }
        out.reverse();
        Ok(out)
    }

    /// Appends a contiguous state band transition (`Kind::TaskState`) on `track_key` into the
    /// SQLite `events` table so `tacho trace` renders task and train lifecycles as state bands.
    pub fn emit_state_band(
        &self,
        track_key: &str,
        label: &str,
        status: TaskStatus,
        because: &str,
        ts_ns: u64,
    ) -> io::Result<()> {
        if let Some(appender) = &self.appender {
            let input = if let Some(slug) = track_key
                .strip_prefix("task:")
                .and_then(|s| s.strip_suffix("/state"))
            {
                tacho_core::Input::TaskState {
                    source_ts_ns: Some(ts_ns),
                    slug: slug.to_string(),
                    state: status,
                    because: Some(because.to_string()),
                }
            } else {
                tacho_core::Input::Custom {
                    source_ts_ns: Some(ts_ns),
                    track: track_key.to_string(),
                    op: tacho_core::CustomOp::State,
                    name: Some(label.to_string()),
                    id: None,
                    args: BTreeMap::from([("because".to_string(), because.to_string())]),
                    value: None,
                    flow_out: None,
                    flow_in: None,
                }
            };
            return appender
                .send_input(input, ts_ns)
                .map_err(|error| io::Error::other(error.to_string()));
        }
        let proto_state: tacho::TaskStatus = status.into();
        let ev_body = tacho::Event {
            body: Some(tacho::event::Body::TaskState(tacho::TaskState {
                state: Some(proto_state as i32),
                because: Some(because.chars().take(128).collect()),
            })),
        };
        let mut buf = Vec::new();
        ev_body
            .encode(&mut buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let started_tx = self.conn.is_autocommit();
        if started_tx {
            self.conn
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(sql_err)?;
        }
        let res = (|| -> io::Result<()> {
            let next_seq: i64 = self
                .conn
                .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM events", [], |r| {
                    r.get(0)
                })
                .map_err(sql_err)?;
            let last_ts: i64 = self
                .conn
                .query_row(
                    "SELECT COALESCE(MAX(ts_ns), 0) FROM events WHERE track_key = ?1",
                    params![track_key],
                    |r| r.get(0),
                )
                .map_err(sql_err)?;
            let eff_ts = (ts_ns as i64).max(last_ts + 1_000);
            self.conn
                .execute(
                    "INSERT INTO events (seq, ts_ns, kind, track_key, cause_seq, agent, body, counter_value, custom_name)
                     VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, ?6)",
                    params![
                        next_seq,
                        eff_ts,
                        Kind::TaskState as u32,
                        track_key,
                        &buf,
                        label
                    ],
                )
                .map_err(sql_err)?;
            Ok(())
        })();
        if started_tx {
            if res.is_ok() {
                self.conn.execute_batch("COMMIT").map_err(sql_err)?;
            } else {
                let _ = self.conn.execute_batch("ROLLBACK");
            }
        }
        res
    }

    /// Appends a paired duration slice (`Kind::Custom` + `Kind::CustomEnded`) on `track_key`
    /// into the SQLite `events` table so `tacho trace` renders batch runs, bisection probes,
    /// and orchestrator ticks as duration slices instead of zero-duration instants.
    pub fn emit_duration_slice(
        &self,
        track_key: &str,
        name: &str,
        start_ts_ns: u64,
        end_ts_ns: u64,
        because: &str,
    ) -> io::Result<()> {
        if let Some(appender) = &self.appender {
            let end = end_ts_ns.max(start_ts_ns.saturating_add(1_000_000));
            let inputs = [
                (tacho_core::CustomOp::Begin, start_ts_ns),
                (tacho_core::CustomOp::End, end),
            ]
            .into_iter()
            .map(|(op, ts)| {
                (
                    tacho_core::Input::Custom {
                        source_ts_ns: Some(ts),
                        track: track_key.to_string(),
                        op,
                        name: Some(name.to_string()),
                        id: None,
                        args: BTreeMap::from([("because".to_string(), because.to_string())]),
                        value: None,
                        flow_out: None,
                        flow_in: None,
                    },
                    ts,
                )
            })
            .collect();
            return appender
                .send_input_batch(inputs)
                .map_err(|error| io::Error::other(error.to_string()));
        }
        let begin_body = tacho::Event {
            body: Some(tacho::event::Body::Custom(tacho::Custom {
                name: Some(name.chars().take(128).collect()),
                id: None,
                args: vec![tacho::CustomArg {
                    key: Some("because".to_string()),
                    value: Some(because.chars().take(256).collect()),
                }],
            })),
        };
        let end_body = tacho::Event {
            body: Some(tacho::event::Body::CustomEnded(tacho::CustomEnded {
                id: None,
                args: Vec::new(),
                abandoned: Some(false),
            })),
        };
        let mut begin_buf = Vec::new();
        begin_body
            .encode(&mut begin_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut end_buf = Vec::new();
        end_body
            .encode(&mut end_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let started_tx = self.conn.is_autocommit();
        if started_tx {
            self.conn
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(sql_err)?;
        }
        let res = (|| -> io::Result<()> {
            let next_seq: i64 = self
                .conn
                .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM events", [], |r| {
                    r.get(0)
                })
                .map_err(sql_err)?;
            let last_ts: i64 = self
                .conn
                .query_row(
                    "SELECT COALESCE(MAX(ts_ns), 0) FROM events WHERE track_key = ?1",
                    params![track_key],
                    |r| r.get(0),
                )
                .map_err(sql_err)?;
            let begin_ts = (start_ts_ns as i64).max(last_ts + 1_000);
            let finish_ts = (end_ts_ns as i64).max(begin_ts + 1_000_000); // minimum 1ms visible slice
            self.conn
                .execute(
                    "INSERT INTO events (seq, ts_ns, kind, track_key, cause_seq, agent, body, counter_value, custom_name)
                     VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, ?6)",
                    params![
                        next_seq,
                        begin_ts,
                        Kind::Custom as u32,
                        track_key,
                        &begin_buf,
                        name
                    ],
                )
                .map_err(sql_err)?;
            self.conn
                .execute(
                    "INSERT INTO events (seq, ts_ns, kind, track_key, cause_seq, agent, body, counter_value, custom_name)
                     VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, NULL, NULL)",
                    params![
                        next_seq + 1,
                        finish_ts,
                        Kind::CustomEnded as u32,
                        track_key,
                        next_seq,
                        &end_buf
                    ],
                )
                .map_err(sql_err)?;
            Ok(())
        })();
        if started_tx {
            if res.is_ok() {
                self.conn.execute_batch("COMMIT").map_err(sql_err)?;
            } else {
                let _ = self.conn.execute_batch("ROLLBACK");
            }
        }
        res
    }

    /// Commits a tick's journal and state only while its observed fleet state is still current.
    pub fn commit_tick<F>(
        &mut self,
        observed: &FleetState,
        ts: i64,
        record: (&DecisionState, &WorldObservation, &DecisionPlan),
        update: F,
    ) -> io::Result<i64>
    where
        F: FnOnce(&mut FleetState),
    {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(sql_err)?;
        let result = (|| {
            let mut current = self.load()?;
            if &current != observed {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "fleet state changed during observation; no actions executed",
                ));
            }
            update(&mut current);
            self.save(&current)?;
            let id = self.record_decision_tick(ts, record.0, record.1, record.2)?;
            self.conn.execute_batch("COMMIT").map_err(sql_err)?;
            Ok(id)
        })();
        if result.is_err() && !self.conn.is_autocommit() {
            self.conn.execute_batch("ROLLBACK").map_err(sql_err)?;
        }
        result
    }

    /// Records a complete orchestrator decision tick with both its input state and its output plan
    /// into `decision_ticks` so the tick can be deterministically replayed offline.
    pub fn record_decision_tick(
        &self,
        ts: i64,
        state: &DecisionState,
        obs: &WorldObservation,
        plan: &DecisionPlan,
    ) -> io::Result<i64> {
        let state_json = serde_json::to_string(state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let obs_json = serde_json::to_string(obs)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let plan_json = serde_json::to_string(plan)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.conn
            .execute(
                "INSERT INTO decision_ticks (ts, state_json, obs_json, plan_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![ts, state_json, obs_json, plan_json],
            )
            .map_err(sql_err)?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Loads the latest logged decision tick for replay.
    pub fn load_latest_decision_tick(
        &self,
    ) -> io::Result<Option<(i64, DecisionState, WorldObservation, DecisionPlan)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, state_json, obs_json, plan_json
                 FROM decision_ticks
                 ORDER BY id DESC
                 LIMIT 1",
            )
            .map_err(sql_err)?;
        let mut rows = stmt.query([]).map_err(sql_err)?;
        if let Some(row) = rows.next().map_err(sql_err)? {
            let id: i64 = row.get(0).map_err(sql_err)?;
            let s_json: String = row.get(1).map_err(sql_err)?;
            let o_json: String = row.get(2).map_err(sql_err)?;
            let p_json: String = row.get(3).map_err(sql_err)?;
            let st = serde_json::from_str(&s_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let obs = serde_json::from_str(&o_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let plan = serde_json::from_str(&p_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some((id, st, obs, plan)))
        } else {
            Ok(None)
        }
    }

    /// Loads a specific logged decision tick by ID.
    pub fn load_decision_tick(
        &self,
        tick_id: i64,
    ) -> io::Result<(DecisionState, WorldObservation, DecisionPlan)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT state_json, obs_json, plan_json
                 FROM decision_ticks
                 WHERE id = ?1",
            )
            .map_err(sql_err)?;
        let mut rows = stmt.query([tick_id]).map_err(sql_err)?;
        if let Some(row) = rows.next().map_err(sql_err)? {
            let s_json: String = row.get(0).map_err(sql_err)?;
            let o_json: String = row.get(1).map_err(sql_err)?;
            let p_json: String = row.get(2).map_err(sql_err)?;
            let st = serde_json::from_str(&s_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let obs = serde_json::from_str(&o_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let plan = serde_json::from_str(&p_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok((st, obs, plan))
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("decision tick {tick_id} not found"),
            ))
        }
    }

    /// Prunes decision ticks older than `cutoff_secs`.
    pub fn prune_decision_ticks_older_than(&mut self, cutoff_secs: i64) -> io::Result<usize> {
        let deleted = self
            .conn
            .execute("DELETE FROM decision_ticks WHERE ts < ?1", [cutoff_secs])
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Returns the id of the latest recorded tick when this tick would duplicate it: the
    /// same decide inputs (ignoring the wall clock) and the same plan (ignoring the tick
    /// timestamp it stamps into `next_state`).
    ///
    /// The wall clock is excluded from the input fingerprint because a periodic refresh
    /// always advances it. It still counts whenever it changes the outcome: a time-based
    /// rule that fires produces a different plan, and that tick is recorded.
    pub fn unchanged_since_latest_tick(
        &self,
        state: &DecisionState,
        obs: &WorldObservation,
        plan: &DecisionPlan,
    ) -> io::Result<Option<i64>> {
        let Some((id, prev_state, prev_obs, prev_plan)) = self.load_latest_decision_tick()? else {
            return Ok(None);
        };
        let same = input_fingerprint(state, obs)? == input_fingerprint(&prev_state, &prev_obs)?
            && plan_fingerprint(plan)? == plan_fingerprint(&prev_plan)?;
        Ok(same.then_some(id))
    }
}

/// Stable hash of decide inputs with every wall-clock-derived field zeroed.
fn input_fingerprint(state: &DecisionState, obs: &WorldObservation) -> io::Result<u64> {
    let mut state = state.clone();
    state.last_tick_ts = None;
    let mut obs = obs.clone();
    obs.now = 0;
    obs.tidy_snapshot.now = 0;
    obs.root_disk_bytes = None;
    for sample in obs.agent_samples.values_mut() {
        sample.ts = 0;
    }
    for wt in &mut obs.tidy_snapshot.worktrees {
        wt.age_secs = 0;
    }
    for burner in &mut obs.burners {
        burner.elapsed_secs = 0;
    }
    canonical_hash(&(state, obs))
}

/// Stable hash of a plan, ignoring the tick timestamp `decide` stamps into `next_state`.
fn plan_fingerprint(plan: &DecisionPlan) -> io::Result<u64> {
    let mut plan = plan.clone();
    plan.next_state.last_tick_ts = None;
    canonical_hash(&plan)
}

/// Hashes `value`'s JSON form with object keys sorted, so `HashMap` iteration order (and
/// serde_json's optional `preserve_order`) cannot make equal values hash differently.
fn canonical_hash<T: Serialize>(value: &T) -> io::Result<u64> {
    use std::hash::{Hash, Hasher};

    fn feed(v: &serde_json::Value, h: &mut std::collections::hash_map::DefaultHasher) {
        match v {
            serde_json::Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                h.write_u8(b'{');
                for k in keys {
                    k.hash(h);
                    feed(&map[k], h);
                }
                h.write_u8(b'}');
            }
            serde_json::Value::Array(items) => {
                h.write_u8(b'[');
                h.write_usize(items.len());
                for item in items {
                    feed(item, h);
                }
            }
            other => other.to_string().hash(h),
        }
    }

    let v =
        serde_json::to_value(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    feed(&v, &mut h);
    Ok(h.finish())
}

/// Converts persistent [`FleetState`] into the pure [`DecisionState`] input to
/// [`tacho_core::decide::decide`] (flake tasks included).
pub fn decision_state_of(state: &FleetState) -> DecisionState {
    crate::exec::observe::decision_state_of(state, now_secs())
}

/// Resolves `HEAD` SHA in `repo` when `repo` is inside a git checkout.
pub fn git_head_sha(repo: &Path) -> Option<String> {
    if !crate::exec::observe::inside_git_checkout(repo) {
        return None;
    }
    crate::sampler::run_git(
        repo,
        &["rev-parse", "HEAD"],
        std::time::Duration::from_secs(5),
    )
    .ok()
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
}

/// The newest main sha fleet state already knows (batches, passed PRs, worktree bases, or the
/// HEAD of a known checkout). `None` when nothing is known; never invented.
pub fn known_main_sha(
    home_dir: &Path,
    state: &FleetState,
    live_state: &tacho_core::LiveState,
) -> Option<String> {
    let non_empty = |s: &str| (!s.trim().is_empty()).then(|| s.trim().to_string());
    state
        .batches
        .iter()
        .rev()
        .find_map(|b| non_empty(&b.main_sha))
        .or_else(|| state.passed.values().find_map(|p| non_empty(&p.main_sha)))
        .or_else(|| {
            state.worktrees.values().find_map(|w| {
                non_empty(&w.base_sha)
                    .or_else(|| (!w.path.is_empty()).then(|| git_head_sha(Path::new(&w.path)))?)
            })
        })
        .or_else(|| {
            state
                .agents
                .values()
                .filter(|a| !a.worktree.is_empty())
                .find_map(|a| git_head_sha(Path::new(&a.worktree)))
        })
        .or_else(|| {
            live_state
                .agents
                .values()
                .filter_map(|a| a.cwd.as_deref())
                .find_map(|cwd| git_head_sha(Path::new(cwd)))
        })
        .or_else(|| git_head_sha(home_dir))
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .and_then(|cwd| git_head_sha(&cwd))
        })
}

/// Delivers a `tell` message emitted by `decide` to `$TACHO_HOME/inbox/<agent_key>.jsonl`
/// (and `<worktree>/.tacho-inbox.md` when a worktree exists) and records the decision.
pub fn deliver_tell_message(
    home_dir: &Path,
    store: &FleetStore,
    state: &FleetState,
    live_state: &tacho_core::LiveState,
    agent_key: &str,
    msg: &str,
) -> io::Result<()> {
    use std::io::Write;
    let now = now_secs();
    let inbox_dir = home_dir.join("inbox");
    std::fs::create_dir_all(&inbox_dir)?;
    let safe_key: String = agent_key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let inbox_path = inbox_dir.join(format!("{safe_key}.jsonl"));
    let mut line = serde_json::to_vec(&serde_json::json!({
        "ts": now,
        "to": agent_key,
        "msg": msg,
    }))
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    line.push(b'\n');
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inbox_path)?;
    f.write_all(&line)?;

    let fleet_main = crate::exec::observe::fleet_main_checkout(state, None);
    let worktree_dir = state
        .agents
        .get(agent_key)
        .map(|a| a.worktree.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            state
                .worktrees
                .get(agent_key)
                .map(|w| w.path.as_str())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            live_state
                .agents
                .get(agent_key)
                .and_then(|a| a.cwd.as_deref())
                .filter(|s| !s.is_empty())
        });
    if let Some(wt) = worktree_dir {
        let wt_path = Path::new(wt);
        if crate::exec::observe::resolves_to_fleet_main_checkout(wt_path, fleet_main.as_deref()) {
            let reason = format!(
                "refusing tell to {agent_key}: worktree '{wt}' resolves to the main checkout"
            );
            store.record_decision("watch", "tell_refused", agent_key, &reason)?;
            return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
        }
        if crate::exec::observe::verify_worker_worktree(wt_path).is_ok() {
            // The inbox `.jsonl` written above is the delivery of record; the worktree copy is
            // a convenience for agents that read their checkout, so its failure is only logged.
            let md_path = wt_path.join(".tacho-inbox.md");
            let appended = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&md_path)
                .and_then(|mut mf| writeln!(mf, "\n## Message ({now})\n{msg}"));
            if let Err(e) = appended {
                eprintln!(
                    "[tacho-daemon] failed to append message to {}: {e}",
                    md_path.display()
                );
            }
        }
    }

    store.record_decision("dispatch", "tell", agent_key, msg)
}

/// Executes daemon actions through its launcher, inbox and attached trace appender.
pub struct DaemonHands {
    pub home: std::path::PathBuf,
    pub live_state: tacho_core::LiveState,
    pub appender: Option<crate::appender::AppenderHandle>,
    pub launcher: Option<std::sync::Arc<dyn crate::exec::Launcher>>,
}

impl crate::exec::Hands for DaemonHands {
    fn spawn_worker(&mut self, store: &mut FleetStore, item: &TodoItem) -> io::Result<Option<u32>> {
        self.launcher
            .as_ref()
            .map(|launcher| launcher.spawn_worker(store, item))
            .transpose()
    }

    fn deliver_tell(
        &mut self,
        store: &mut FleetStore,
        agent_key: &str,
        msg: &str,
    ) -> io::Result<()> {
        let state = store.load()?;
        deliver_tell_message(&self.home, store, &state, &self.live_state, agent_key, msg)
    }

    fn emit(&mut self, track: &str, name: &str, because: &str) {
        let Some(appender) = &self.appender else {
            return;
        };
        let ts = now_ns();
        let input = tacho_core::Input::Custom {
            source_ts_ns: Some(ts),
            track: track.to_string(),
            op: tacho_core::CustomOp::Instant,
            name: Some(name.to_string()),
            id: None,
            args: BTreeMap::from([("because".to_string(), because.to_string())]),
            value: None,
            flow_out: None,
            flow_in: None,
        };
        if let Err(e) = appender.send_input(input, ts) {
            eprintln!("[tacho-daemon] failed to emit {name} event: {e}");
        }
    }

    fn emit_duration_slice(
        &mut self,
        store: &FleetStore,
        track_key: &str,
        name: &str,
        start_ts_ns: u64,
        end_ts_ns: u64,
        because: &str,
    ) -> io::Result<()> {
        let Some(appender) = &self.appender else {
            return store.emit_duration_slice(track_key, name, start_ts_ns, end_ts_ns, because);
        };
        let begin_ts = start_ts_ns;
        let finish_ts = end_ts_ns.max(begin_ts + 1_000_000);
        let begin = tacho_core::Input::Custom {
            source_ts_ns: Some(begin_ts),
            track: track_key.to_string(),
            op: tacho_core::CustomOp::Begin,
            name: Some(name.to_string()),
            id: None,
            args: BTreeMap::from([("because".to_string(), because.to_string())]),
            value: None,
            flow_out: None,
            flow_in: None,
        };
        let end = tacho_core::Input::Custom {
            source_ts_ns: Some(finish_ts),
            track: track_key.to_string(),
            op: tacho_core::CustomOp::End,
            name: Some(name.to_string()),
            id: None,
            args: BTreeMap::new(),
            value: None,
            flow_out: None,
            flow_in: None,
        };
        let _ = appender.send_input(begin, begin_ts);
        let _ = appender.send_input(end, finish_ts);
        Ok(())
    }

    fn notify_owner(&mut self, subject: &str, body: &str) -> Result<bool, String> {
        tacho_core::decide::plugin::notify_owner_command(subject, body)
    }

    fn prune_root_disk_volumes(&mut self) -> Result<String, String> {
        crate::exec::prune_runner_volumes_command(None)
    }
}

/// Observes the world, runs `decide`, and (unless `dry_run`) records and executes the plan
/// through the shared executor ([`crate::exec::run_tick`]).
pub fn execute_decide_tick(
    home_dir: &Path,
    live_state: &tacho_core::LiveState,
    explicit_main_sha: Option<String>,
    dry_run: bool,
) -> io::Result<(Option<i64>, DecisionState, WorldObservation, DecisionPlan)> {
    execute_decide_tick_with_launcher(home_dir, live_state, explicit_main_sha, dry_run, None, None)
}

/// Executes a tick with the same process launcher and appender as the sampler.
pub fn execute_decide_tick_with_launcher(
    home_dir: &Path,
    live_state: &tacho_core::LiveState,
    explicit_main_sha: Option<String>,
    dry_run: bool,
    launcher: Option<std::sync::Arc<dyn crate::exec::Launcher>>,
    appender: Option<crate::appender::AppenderHandle>,
) -> io::Result<(Option<i64>, DecisionState, WorldObservation, DecisionPlan)> {
    let mut store = FleetStore::open(&home_dir.join("events.sqlite"))?;
    if let Some(ref appender) = appender {
        store.attach_appender(appender.clone());
    }
    let mut hands = DaemonHands {
        home: home_dir.to_path_buf(),
        live_state: live_state.clone(),
        appender,
        launcher,
    };
    let gh_forge = std::env::var("TACHO_GH_REPO")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(tacho_core::decide::plugin::GitHubForge::new);
    let mut opts = crate::exec::TickOptions::new(live_state);
    opts.dry_run = dry_run;
    opts.explicit_main_sha = explicit_main_sha;
    if let Some(ref f) = gh_forge {
        opts.forge = Some(f);
    }
    let out = crate::exec::run_tick(home_dir, &mut store, &mut hands, opts)?;
    Ok((out.tick_id, out.state, out.obs, out.plan))
}

/// Runs an autonomous world-observation and `decide` pass using the current [`tacho_core::LiveState`].
pub fn run_autonomous_decide_tick_with_live(
    home_dir: &Path,
    live_state: &tacho_core::LiveState,
) -> io::Result<DecisionPlan> {
    let (_, _, _, plan) = execute_decide_tick(home_dir, live_state, None, false)?;
    Ok(plan)
}

/// Runs an autonomous world-observation and `decide` pass reading live agent states from `recorder`.
pub fn run_autonomous_decide_tick(
    home_dir: &Path,
    recorder: &std::sync::Arc<std::sync::Mutex<tacho_core::Recorder>>,
) -> io::Result<DecisionPlan> {
    let live_state = recorder
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .live_state()
        .clone();
    run_autonomous_decide_tick_with_live(home_dir, &live_state)
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "fleet_state_tests.rs"]
mod state_integrity_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fleet_store_roundtrip_and_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("events.sqlite");
        let mut store = FleetStore::open(&db).unwrap();

        store
            .mutate(
                Some(("dispatch", "queue", "feat-1", "queued by user")),
                |st| {
                    st.todo.push(TodoItem {
                        key: "feat-1".to_string(),
                        brief: "Add feature 1".to_string(),
                        area: "core".to_string(),
                        ..Default::default()
                    });
                },
            )
            .unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.todo.len(), 1);
        assert_eq!(loaded.todo[0].key, "feat-1");

        let decisions = store.recent_decisions(10, None, None).unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, "queue");
        assert_eq!(decisions[0].target, "feat-1");
        assert_eq!(decisions[0].because, "queued by user");
    }

    #[test]
    fn concurrent_decide_ticks_never_overlap() {
        let dirs: Vec<_> = (0..8).map(|_| tempfile::tempdir().unwrap()).collect();
        let start = std::sync::Barrier::new(dirs.len());
        std::thread::scope(|s| {
            let handles: Vec<_> = dirs
                .iter()
                .map(|d| {
                    let start = &start;
                    s.spawn(move || {
                        start.wait();
                        execute_decide_tick(
                            d.path(),
                            &tacho_core::LiveState::default(),
                            Some("a".repeat(40)),
                            true,
                        )
                        .map(|_| ())
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap().unwrap();
            }
        });
        assert_eq!(
            crate::exec::tick_probe::MAX_IN_FLIGHT.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "two decide ticks ran at the same time"
        );
    }

    #[test]
    fn test_autonomous_decide_tick_records_tick_delivers_tell_and_removes_worktree() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let db = home.join("events.sqlite");
        let mut store = FleetStore::open(&db).unwrap();

        let main_repo_dir = home.join("main-repo");
        std::fs::create_dir_all(&main_repo_dir).unwrap();
        let run_git = |args: &[&str]| {
            let st = std::process::Command::new("git")
                .args(args)
                .current_dir(&main_repo_dir)
                .status()
                .unwrap();
            assert!(st.success(), "git {args:?} failed");
        };
        run_git(&["init", "-q", "-b", "main"]);
        run_git(&["config", "user.email", "t@example.com"]);
        run_git(&["config", "user.name", "Tester"]);
        std::fs::write(main_repo_dir.join("README.md"), "init\n").unwrap();
        run_git(&["add", "README.md"]);
        run_git(&["commit", "-q", "-m", "initial"]);
        let base_sha = crate::sampler::run_git(
            &main_repo_dir,
            &["rev-parse", "HEAD"],
            std::time::Duration::from_secs(5),
        )
        .unwrap()
        .trim()
        .to_string();
        let stale_wt_dir = home.join("wt-stale");
        run_git(&[
            "worktree",
            "add",
            "-q",
            "-b",
            "feat/stale",
            stale_wt_dir.to_str().unwrap(),
            "HEAD",
        ]);
        let live_wt_dir = home.join("wt-live");
        std::fs::create_dir_all(&live_wt_dir).unwrap();

        store
            .mutate(None, |st| {
                st.inbox
                    .entry("agent-alpha".to_string())
                    .or_default()
                    .push("Focus on fixing the parser edge case".to_string());
                st.retired.insert("wt-stale".to_string());
                st.retired.insert("main-checkout".to_string());
                st.worktrees.insert(
                    "main-checkout".to_string(),
                    WorktreeEntry {
                        slug: "main-checkout".to_string(),
                        path: main_repo_dir.to_string_lossy().to_string(),
                        branch: "main".to_string(),
                        base_sha: base_sha.clone(),
                        base_pr: None,
                        port: 0,
                        port_last: 0,
                        created_at: 100,
                    },
                );
                st.worktrees.insert(
                    "wt-stale".to_string(),
                    WorktreeEntry {
                        slug: "wt-stale".to_string(),
                        path: stale_wt_dir.to_string_lossy().to_string(),
                        branch: "feat/stale".to_string(),
                        base_sha: base_sha.clone(),
                        base_pr: None,
                        port: 0,
                        port_last: 0,
                        created_at: 100,
                    },
                );
            })
            .unwrap();

        let mut recorder = tacho_core::Recorder::new();
        let mut events = Vec::new();
        recorder.apply(
            tacho_core::Input::RunStart {
                source_ts_ns: Some(1_000_000_000),
                provider: "claude".to_string(),
                session_id: "sess-1".to_string(),
                agent: Some("agent-alpha".to_string()),
                model: Some("claude-sonnet".to_string()),
                task: Some("Fix parser".to_string()),
                cwd: Some(live_wt_dir.to_string_lossy().to_string()),
                pid: Some(std::process::id()),
                role: None,
                parent_session: None,
                git_tree: None,
            },
            1_000_000_000,
            &mut events,
        );
        let shared_recorder = std::sync::Arc::new(std::sync::Mutex::new(recorder));

        let plan = run_autonomous_decide_tick(home, &shared_recorder).unwrap();
        assert!(
            plan.tell_actions.contains(&(
                "agent-alpha".to_string(),
                "Focus on fixing the parser edge case".to_string()
            )),
            "expected tell_actions in plan, got: {:?}",
            plan.tell_actions
        );
        assert!(
            !stale_wt_dir.exists(),
            "stale linked worktree should have been removed by decide tidy action"
        );
        assert!(
            main_repo_dir.exists() && main_repo_dir.join("README.md").exists(),
            "main checkout must NEVER be removed even if recorded in worktrees and retired"
        );

        let inbox_file = home.join("inbox/agent-alpha.jsonl");
        assert!(inbox_file.exists(), "expected delivered inbox jsonl file");
        let inbox_contents = std::fs::read_to_string(&inbox_file).unwrap();
        assert!(inbox_contents.contains("Focus on fixing the parser edge case"));

        let (_, _, recorded_obs, recorded_plan) =
            store.load_latest_decision_tick().unwrap().unwrap();
        assert_eq!(recorded_obs.main_sha, base_sha);
        assert!(!recorded_obs.main_sha.is_empty());
        assert_eq!(recorded_plan.tell_actions, plan.tell_actions);
    }

    fn count_decision_ticks(store: &FleetStore) -> i64 {
        store
            .conn
            .query_row("SELECT COUNT(*) FROM decision_ticks", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn test_decide_tick_with_unchanged_inputs_records_no_new_row() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let live = tacho_core::LiveState::default();
        let sha = Some("b".repeat(40));

        let (first, ..) = execute_decide_tick(home, &live, sha.clone(), false).unwrap();
        let (second, ..) = execute_decide_tick(home, &live, sha.clone(), false).unwrap();
        let store = FleetStore::open(&home.join("events.sqlite")).unwrap();
        assert_eq!(
            first, second,
            "an unchanged tick must point at the previous row"
        );
        assert_eq!(count_decision_ticks(&store), 1);

        // A changed input (a different main) is a new tick.
        let (third, ..) = execute_decide_tick(home, &live, Some("c".repeat(40)), false).unwrap();
        assert_ne!(third, second);
        assert_eq!(count_decision_ticks(&store), 2);
    }

    #[test]
    fn test_input_fingerprint_ignores_wall_clock() {
        let state = DecisionState::default();
        let mut a = WorldObservation {
            now: 100,
            ..Default::default()
        };
        a.tidy_snapshot.now = 100;
        let mut b = a.clone();
        b.now = 999;
        b.tidy_snapshot.now = 999;
        assert_eq!(
            input_fingerprint(&state, &a).unwrap(),
            input_fingerprint(&state, &b).unwrap()
        );
        b.main_sha = "d".repeat(40);
        assert_ne!(
            input_fingerprint(&state, &a).unwrap(),
            input_fingerprint(&state, &b).unwrap()
        );
    }
}
