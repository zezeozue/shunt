//! Configuration parameters for ring sizes, sampler intervals, and detector thresholds.
//!
//! Stored in `$TACHO_HOME/config.toml` with defaults defined in design section 6 and 7.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Daemon-wide configuration structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Ring buffer capacities in bytes (deprecated; log is single source).
    pub rings: RingConfig,
    /// Host resource sampler settings.
    pub sampler: SamplerConfig,
    /// Stall, loop, spin, and burn detector parameters.
    pub detectors: DetectorConfig,
    /// Event log retention in days (default: 7). Pruned hourly in small batches.
    pub retention_days: u32,
    /// Optional OTLP HTTP receiver address (e.g. "127.0.0.1:4318").
    pub otlp_addr: Option<String>,
    /// Secret scrubbing before writing to the event log or external outputs (default: true).
    pub scrub: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rings: RingConfig::default(),
            sampler: SamplerConfig::default(),
            detectors: DetectorConfig::default(),
            retention_days: 7,
            otlp_addr: None,
            scrub: true,
        }
    }
}

/// Ring buffer capacity settings (design section 6.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RingConfig {
    /// Capacity of the core ring buffer in bytes (default: 8 MiB).
    pub core_bytes: usize,
    /// Capacity of the agents ring buffer in bytes (default: 64 MiB).
    pub agents_bytes: usize,
    /// Capacity of the machine ring buffer in bytes (default: 8 MiB).
    pub machine_bytes: usize,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            core_bytes: 8 * 1024 * 1024,
            agents_bytes: 64 * 1024 * 1024,
            machine_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Periodic machine sampler configuration (design section 5.3 & 8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SamplerConfig {
    /// Observe fleet plans without executing actions or updating fleet state.
    pub dry_run: bool,
    /// Polling interval in milliseconds (default: 2000 ms).
    pub interval_ms: u64,
    /// Filesystem mount points to monitor via statvfs (default: ["/"]).
    pub mounts: Vec<String>,
    /// Whether per-agent cgroup accounting is enabled (default: true).
    pub cgroup: bool,
    /// Minimum interval between worktree file-change scans in seconds (default: 30 s).
    pub worktree_scan_secs: u64,
    /// Coarse refresh for time-based `decide` rules, in seconds (default: 60 s).
    ///
    /// The daemon runs `decide` when a relevant event is committed or a tick is requested;
    /// this refresh, checked on the sampler's timerfd tick, only exists so rules that depend
    /// on elapsed time alone (stall age, tidy age) fire without a triggering event.
    pub decide_refresh_secs: u64,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            dry_run: false,
            interval_ms: 2000,
            mounts: vec!["/".to_string()],
            cgroup: true,
            worktree_scan_secs: 30,
            decide_refresh_secs: 60,
        }
    }
}

/// Anomaly detector thresholds (design section 7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DetectorConfig {
    /// Silent timeout: seconds without an event while not in Wait (default: 300 s).
    pub silent_s: u64,
    /// Loop detection: repeated identical tool input digest count (default: 4).
    pub loop_count: usize,
    /// Spin detection: turn count without files edited or build/test commands (default: 8).
    pub spin_turns: usize,
    /// Token burn threshold in window (default: 200,000 tokens).
    pub burn_tokens: u64,
    /// Token burn monitoring window in seconds (default: 600 s).
    pub burn_window_s: u64,
    /// Consecutive test/build command failure count (default: 3).
    pub failing_count: usize,
    /// Check whether file edits fall outside task declared paths (default: true).
    pub off_brief: bool,
    /// Optional shell command to execute when a stall detector enters (`sh -c <cmd>`).
    /// Receives TACHO_AGENT, TACHO_STALL_KIND, TACHO_STALL_REASON, TACHO_SEQ, TACHO_TS_NS.
    pub on_stall: Option<String>,
    /// Timeout in seconds for the on_stall effect hook command (default: 60 s).
    pub on_stall_timeout_secs: u64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            silent_s: 300,
            loop_count: 4,
            spin_turns: 8,
            burn_tokens: 200_000,
            burn_window_s: 600,
            failing_count: 3,
            off_brief: true,
            on_stall: None,
            on_stall_timeout_secs: 60,
        }
    }
}

impl Config {
    /// Loads configuration from the given file if present, or returns default settings.
    pub fn load_or_default(path: &Path) -> Result<Self, toml::de::Error> {
        if path.exists() {
            let content = match fs::read_to_string(path) {
                Ok(c) => c,
                Err(_) => return Ok(Self::default()),
            };
            toml::from_str(&content)
        } else {
            Ok(Self::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.rings.core_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.rings.agents_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.rings.machine_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.sampler.interval_ms, 2000);
        assert!(cfg.sampler.cgroup);
        assert_eq!(cfg.sampler.worktree_scan_secs, 30);
        assert_eq!(cfg.sampler.decide_refresh_secs, 60);
        assert_eq!(cfg.detectors.silent_s, 300);
        assert_eq!(cfg.detectors.loop_count, 4);
        assert_eq!(cfg.detectors.on_stall_timeout_secs, 60);
    }

    #[test]
    fn test_config_load_custom() {
        let temp = tempfile::tempdir().unwrap();
        let cfg_path = temp.path().join("config.toml");
        let toml_data = r#"
[rings]
core_bytes = 4194304
agents_bytes = 16777216
machine_bytes = 4194304

[sampler]
interval_ms = 500
mounts = ["/", "/data"]

[detectors]
silent_s = 60
"#;
        fs::write(&cfg_path, toml_data).unwrap();
        let cfg = Config::load_or_default(&cfg_path).unwrap();
        assert_eq!(cfg.rings.core_bytes, 4194304);
        assert_eq!(cfg.rings.agents_bytes, 16777216);
        assert_eq!(cfg.sampler.interval_ms, 500);
        assert_eq!(cfg.sampler.mounts, vec!["/", "/data"]);
        assert_eq!(cfg.detectors.silent_s, 60);
        assert_eq!(cfg.detectors.loop_count, 4); // preserved default
    }
}
