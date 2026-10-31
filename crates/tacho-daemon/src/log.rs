//! SQLite WAL-backed persistent event log.
//!
//! Stores recorded events in WAL mode with batched transactions as specified in design section 1 and 8.

use prost::Message;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use tacho_core::{Event, Kind, Recorder};
use tacho_proto::tacho;

/// Persistent event log backed by SQLite in WAL mode.
pub struct EventLog {
    conn: Connection,
}

impl EventLog {
    /// Opens or creates the event database at `path`, enabling WAL mode.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        // Configure WAL mode and performance pragmas
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.pragma_update(None, "cache_size", -64000)?;

        // Ensure table exists with core columns (flows are derived deterministically from cause_seq * 256 + ordinal)
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
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_ns);",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_kind_seq ON events(kind, seq);",
            [],
        )?;

        for col_sql in [
            "ALTER TABLE events ADD COLUMN counter_value INTEGER;",
            "ALTER TABLE events ADD COLUMN custom_name TEXT;",
        ] {
            if let Err(e) = conn.execute(col_sql, []) {
                if !e.to_string().contains("duplicate column") {
                    return Err(e);
                }
            }
        }

        Ok(Self { conn })
    }

    /// Opens an in-memory database, useful for fast unit tests.
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

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
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_ns);",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_kind_seq ON events(kind, seq);",
            [],
        )?;

        Ok(Self { conn })
    }

    /// Returns the maximum sequence number present in the log, or 0 if empty.
    pub fn max_seq(&self) -> rusqlite::Result<u64> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT COALESCE(MAX(seq), 0) FROM events")?;
        let max_seq: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(max_seq as u64)
    }

    /// Appends a batch of events within a single SQLite transaction.
    pub fn insert_batch<'a, I>(&mut self, events: I) -> rusqlite::Result<()>
    where
        I: IntoIterator<Item = &'a Event>,
    {
        let mut iter = events.into_iter().peekable();
        if iter.peek().is_none() {
            return Ok(());
        }

        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO events (seq, ts_ns, kind, track_key, cause_seq, agent, body, counter_value, custom_name)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
            )?;

            let mut body_buf = Vec::with_capacity(512);

            for ev in iter {
                body_buf.clear();
                ev.body
                    .encode(&mut body_buf)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

                stmt.execute(params![
                    ev.seq,
                    ev.ts_ns,
                    ev.kind as u32,
                    ev.track_key,
                    ev.cause_seq,
                    ev.agent,
                    &body_buf,
                    ev.counter_value,
                    ev.custom_name,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Prunes events older than `cutoff_ts_ns` in batches of `batch_size`.
    /// Returns the total number of deleted rows.
    pub fn prune_older_than(
        &mut self,
        cutoff_ts_ns: u64,
        batch_size: usize,
    ) -> rusqlite::Result<usize> {
        let mut total_deleted = 0;
        loop {
            let deleted = self.conn.execute(
                "DELETE FROM events WHERE seq IN (
                    SELECT seq FROM events WHERE ts_ns < ?1 LIMIT ?2
                )",
                params![cutoff_ts_ns, batch_size],
            )?;
            total_deleted += deleted;
            if deleted == 0 {
                break;
            }
        }
        Ok(total_deleted)
    }

    /// Reads all logged events in ascending sequence order.
    pub fn load_all(&self) -> rusqlite::Result<Vec<Event>> {
        self.query_range(None, None)
    }

    /// Queries logged events within an optional `[since_ns, until_ns]` timestamp window.
    pub fn query_range(
        &self,
        since_ns: Option<u64>,
        until_ns: Option<u64>,
    ) -> rusqlite::Result<Vec<Event>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT seq, ts_ns, kind, track_key, cause_seq, agent, body, counter_value, custom_name
             FROM events
             WHERE (?1 IS NULL OR ts_ns >= ?1) AND (?2 IS NULL OR ts_ns <= ?2)
             ORDER BY seq ASC",
        )?;

        let rows = stmt.query_map(params![since_ns, until_ns], |row| {
            let seq: u64 = row.get(0)?;
            let ts_ns: u64 = row.get(1)?;
            let kind_int: u32 = row.get(2)?;
            let track_key: String = row.get(3)?;
            let cause_seq: Option<u64> = row.get(4)?;
            let agent: Option<String> = row.get(5)?;
            let body_bytes: Vec<u8> = row.get(6)?;

            let counter_val: Option<i64> = row.get(7).optional()?.flatten();
            let custom_name: Option<String> = row.get(8).optional()?.flatten();

            let kind = Kind::ALL
                .get(kind_int as usize)
                .copied()
                .unwrap_or(Kind::Daemon);

            let body = tacho::Event::decode(&body_bytes[..]).unwrap_or(tacho::Event { body: None });

            let mut ev = Event::new(seq, ts_ns, kind, track_key, cause_seq, agent, body);

            if let Some(c) = counter_val {
                ev = ev.with_counter(c);
            }
            if let Some(cn) = custom_name {
                ev = ev.with_custom_name(cn);
            }

            Ok(ev)
        })?;

        let mut events = Vec::new();
        for r in rows {
            events.push(r?);
        }
        Recorder::populate_flows(&mut events);
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("events.db");
        let mut log = EventLog::open(&db_path).unwrap();

        assert_eq!(log.max_seq().unwrap(), 0);

        let ev1 = Event::new(
            1,
            1000,
            Kind::TaskQueued,
            "task:feat-1",
            None,
            Some("agent:1".to_string()),
            tacho::Event {
                body: Some(tacho::event::Body::TaskQueued(tacho::TaskQueued {
                    slug: Some("feat-1".to_string()),
                    ..Default::default()
                })),
            },
        );

        let ev2 = Event::new(
            2,
            2000,
            Kind::TaskState,
            "task:feat-1/state",
            Some(1),
            None,
            tacho::Event {
                body: Some(tacho::event::Body::TaskState(tacho::TaskState {
                    state: Some(tacho::TaskStatus::TaskDone as i32),
                    ..Default::default()
                })),
            },
        );

        log.insert_batch(&[ev1, ev2]).unwrap();
        assert_eq!(log.max_seq().unwrap(), 2);

        let loaded = log.load_all().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].seq, 1);
        // Flow IDs are derived deterministically as cause_seq * 256 + ordinal (TASK_STATE = 5 -> 261)
        assert_eq!(loaded[0].flow_ids, vec![256 + 5]);
        assert_eq!(loaded[1].seq, 2);
        assert_eq!(loaded[1].terminating_flow_ids, vec![256 + 5]);

        let range = log.query_range(Some(1500), None).unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].seq, 2);
    }
}
