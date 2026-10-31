//! Filesystem path resolution and directory management for tachograph.
//!
//! Enforces mode 0700 on all created directories as specified in design section 1.

use std::env;
use std::fs::{DirBuilder, Permissions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// Resolved directories and file locations used by the tachograph daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    /// Persistent state directory ($TACHO_HOME).
    pub home: PathBuf,
    /// Ephemeral runtime directory for sockets and locks ($TACHO_RUNTIME).
    pub runtime: PathBuf,
}

impl DaemonPaths {
    /// Resolves daemon paths from environment variables and defaults,
    /// ensuring all directories exist with mode 0700.
    pub fn resolve() -> io::Result<Self> {
        let home = resolve_home_path();
        let runtime = resolve_runtime_path(&home);

        let paths = Self { home, runtime };
        paths.ensure_dirs()?;
        Ok(paths)
    }

    /// Creates a custom DaemonPaths instance, useful for tests and custom roots.
    pub fn custom(home: impl Into<PathBuf>, runtime: impl Into<PathBuf>) -> io::Result<Self> {
        let paths = Self {
            home: home.into(),
            runtime: runtime.into(),
        };
        paths.ensure_dirs()?;
        Ok(paths)
    }

    /// Ensures all required daemon directories exist with 0700 permissions.
    pub fn ensure_dirs(&self) -> io::Result<()> {
        ensure_dir_0700(&self.home)?;
        ensure_dir_0700(&self.runtime)?;
        ensure_dir_0700(&self.trace_dir())?;
        Ok(())
    }

    /// Path to the SQLite WAL event log file ($TACHO_HOME/events.sqlite).
    pub fn events_db(&self) -> PathBuf {
        let sqlite = self.home.join("events.sqlite");
        let db = self.home.join("events.db");
        if !sqlite.exists() && db.exists() {
            db
        } else {
            sqlite
        }
    }

    /// Explicit path to $TACHO_HOME/events.sqlite.
    pub fn events_sqlite(&self) -> PathBuf {
        self.home.join("events.sqlite")
    }

    /// Path to the daemon configuration file ($TACHO_HOME/config.toml).
    pub fn config_file(&self) -> PathBuf {
        self.home.join("config.toml")
    }

    /// Path to the trace output directory ($TACHO_HOME/trace).
    pub fn trace_dir(&self) -> PathBuf {
        self.home.join("trace")
    }

    /// Path to the final trace saved on exit ($TACHO_HOME/trace/last.perfetto-trace).
    pub fn last_trace(&self) -> PathBuf {
        self.trace_dir().join("last.perfetto-trace")
    }

    /// Path to the primary API UNIX domain socket ($TACHO_RUNTIME/api.sock).
    pub fn api_sock(&self) -> PathBuf {
        self.runtime.join("api.sock")
    }

    /// Path to the snapshot-dedicated UNIX domain socket ($TACHO_RUNTIME/trace.sock).
    pub fn trace_sock(&self) -> PathBuf {
        self.runtime.join("trace.sock")
    }

    /// Path to the daemon instance lock file ($TACHO_RUNTIME/daemon.lock).
    pub fn lock_file(&self) -> PathBuf {
        self.runtime.join("daemon.lock")
    }
}

/// Resolves `$TACHO_HOME`:
/// 1. `$TACHO_HOME`
/// 2. `$XDG_STATE_HOME/tacho`
/// 3. `$HOME/.local/state/tacho`
fn resolve_home_path() -> PathBuf {
    if let Ok(val) = env::var("TACHO_HOME") {
        if !val.trim().is_empty() {
            return PathBuf::from(val);
        }
    }
    if let Ok(val) = env::var("XDG_STATE_HOME") {
        if !val.trim().is_empty() {
            return PathBuf::from(val).join("tacho");
        }
    }
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".local/state/tacho")
}

/// Resolves `$TACHO_RUNTIME`:
/// 1. `$TACHO_RUNTIME`
/// 2. `$TACHO_HOME/run` (when `$TACHO_HOME` is explicitly set; falls back to `/tmp/tacho-<hash>` if path > 100 bytes)
/// 3. `$XDG_RUNTIME_DIR/tacho`
/// 4. `$TACHO_HOME/run`
pub(crate) fn resolve_runtime_path(home: &Path) -> PathBuf {
    if let Ok(val) = env::var("TACHO_RUNTIME") {
        if !val.trim().is_empty() {
            return PathBuf::from(val);
        }
    }
    if let Ok(val) = env::var("TACHO_HOME") {
        if !val.trim().is_empty() {
            let candidate = home.join("run");
            if candidate.join("trace.sock").as_os_str().len() <= 100 {
                return candidate;
            }
            let mut h: u64 = 0xcbf29ce484222325;
            for &b in val.trim().as_bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            return PathBuf::from(format!("/tmp/tacho-{:016x}", h));
        }
    }
    if let Ok(val) = env::var("XDG_RUNTIME_DIR") {
        if !val.trim().is_empty() {
            return PathBuf::from(val).join("tacho");
        }
    }
    home.join("run")
}

/// Creates a directory recursively with mode 0700 if it does not exist,
/// or chmod's an existing directory to 0700.
pub fn ensure_dir_0700(path: &Path) -> io::Result<()> {
    if !path.exists() {
        DirBuilder::new().mode(0o700).recursive(true).create(path)?;
    } else {
        std::fs::set_permissions(path, Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_paths_0700() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let runtime = temp.path().join("runtime");

        let paths = DaemonPaths::custom(&home, &runtime).unwrap();
        assert_eq!(paths.home, home);
        assert_eq!(paths.runtime, runtime);
        assert_eq!(paths.events_db(), home.join("events.sqlite"));
        assert_eq!(paths.events_sqlite(), home.join("events.sqlite"));
        assert_eq!(paths.api_sock(), runtime.join("api.sock"));
        assert_eq!(paths.trace_sock(), runtime.join("trace.sock"));
        assert_eq!(paths.lock_file(), runtime.join("daemon.lock"));
        assert_eq!(paths.last_trace(), home.join("trace/last.perfetto-trace"));

        let meta = std::fs::metadata(&home).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        let meta_rt = std::fs::metadata(&runtime).unwrap();
        assert_eq!(meta_rt.permissions().mode() & 0o777, 0o700);
    }
}
