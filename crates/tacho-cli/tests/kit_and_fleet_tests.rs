//! End-to-end integration tests for the worker kit (`start`, `prove`, `job`, `wait`, `kill`, `pr`,
//! `note`, `starve`), hook guardrails (`TACHO_GUARDRAILS=1`), fleet orchestrator (`todo`, `hold`,
//! `release`, `retire`, `attach`, `batch`, `flakes`, `train`, `land`, `tidy`, `tick`, `events`),
//! and Perfetto trace projection (`tacho trace`).

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use tacho_core::decide::dispatch::PullFacts;
use tacho_core::decide::verify::AreaRule;
use tacho_daemon::FleetStore;

fn tacho_bin() -> String {
    env!("CARGO_BIN_EXE_tacho").to_string()
}

fn tacho_cmd() -> Command {
    let mut cmd = Command::new(tacho_bin());
    cmd.env("TACHO_TEST_MODE", "1")
        .env_remove("TACHO_NOTIFY")
        .env_remove("TACHO_ROOT_PRUNE");
    cmd
}

/// Blocks until process `pid` exits, woken by its pidfd. Returns at once if
/// the process is already gone.
fn wait_pid_exit(pid: u32) {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let pid = libc::pid_t::try_from(pid).expect("pid fits pid_t");
    // SAFETY: plain syscall; a non-negative result is a fresh fd we own.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if raw < 0 {
        let err = std::io::Error::last_os_error();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ESRCH),
            "pidfd_open({pid}) failed: {err}"
        );
        return;
    }
    // SAFETY: `raw` was just returned by pidfd_open; closed on drop.
    let fd = unsafe { OwnedFd::from_raw_fd(raw as libc::c_int) };
    loop {
        let mut pfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd; no timeout, the pidfd turns readable on exit.
        if unsafe { libc::poll(&mut pfd, 1, -1) } > 0 {
            return;
        }
        let err = std::io::Error::last_os_error();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::Interrupted,
            "poll(pidfd {pid}) failed: {err}"
        );
    }
}

fn init_git_repo(dir: &Path) -> String {
    let run = |args: &[&str]| {
        let st = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git command failed to spawn");
        assert!(st.success(), "git {:?} failed", args);
    };
    run(&["init", "-b", "main"]);
    run(&["config", "user.email", "dev@example.com"]);
    run(&["config", "user.name", "Developer"]);
    fs::write(dir.join("README.md"), "# Test Repo\n").unwrap();
    run(&["add", "README.md"]);
    run(&["commit", "-m", "Initial commit"]);
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(dir)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn the_daemon_starts_a_queued_worker_in_its_allocated_worktree() {
    daemon_worker_case(false);
}

#[test]
fn a_dry_run_daemon_leaves_workers_queued_even_when_it_has_a_launcher() {
    daemon_worker_case(true);
}

fn daemon_worker_case(dry_run: bool) {
    use std::io::{BufRead, BufReader};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixStream;

    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let runtime = tmp.path().join("runtime");
    let repo = tmp.path().join("repo");
    let worktrees = tmp.path().join("worktrees");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&runtime).unwrap();
    let sha = init_git_repo(&repo);
    assert!(Command::new("git")
        .args(["remote", "add", "origin"])
        .arg(&repo)
        .current_dir(&repo)
        .status()
        .unwrap()
        .success());
    let start = tacho_cmd()
        .args(["start", "daemon-worker", "--base", &sha, "--dir"])
        .arg(&worktrees)
        .env("TACHO_HOME", &home)
        .current_dir(&repo)
        .output()
        .unwrap();
    assert!(
        start.status.success(),
        "{}",
        String::from_utf8_lossy(&start.stderr)
    );
    let todo = tacho_cmd()
        .args([
            "todo",
            "daemon-worker",
            "Record the worker directory",
            "--area",
            "docs",
        ])
        .env("TACHO_HOME", &home)
        .current_dir(&repo)
        .output()
        .unwrap();
    assert!(todo.status.success());

    // Arm the directory watch before starting the daemon; socket creation wakes the test.
    let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
    assert!(raw >= 0);
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let path = std::ffi::CString::new(runtime.as_os_str().as_bytes()).unwrap();
    assert!(
        unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), libc::IN_CREATE) } >= 0
    );
    let _daemon = ChildGuard(
        tacho_cmd()
            .args(if dry_run {
                vec!["daemon", "--dry-run"]
            } else {
                vec!["daemon"]
            })
            .current_dir(&repo)
            .env("TACHO_HOME", &home)
            .env("TACHO_RUNTIME", &runtime)
            .env("TACHO_REPO", &repo)
            .env("TACHO_OPEN_PRS_JSON", "[]")
            .env(
                "TACHO_MAIN_CI_JSON",
                serde_json::json!({"id":1,"status":"completed",
            "conclusion":"success","sha":sha,"title":"CI","age_seconds":0})
                .to_string(),
            )
            .env(
                "TACHO_AGENT_CMD",
                "cat >/dev/null; pwd > worker-directory; printf 'PR: none\\n'",
            )
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let socket = runtime.join("api.sock");
    let mut events = [0u8; 4096];
    while !socket.exists() {
        let count = unsafe { libc::read(fd.as_raw_fd(), events.as_mut_ptr().cast(), events.len()) };
        assert!(count > 0, "directory watch failed");
    }
    let mut stream = UnixStream::connect(&socket).unwrap();
    writeln!(
        stream,
        "{}",
        serde_json::json!({"cmd":"tick", "main_sha":sha})
    )
    .unwrap();
    let mut reply = String::new();
    BufReader::new(stream).read_line(&mut reply).unwrap();
    let response: serde_json::Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(response["status"], "ok", "{reply}");
    let state = FleetStore::open(&home.join("events.sqlite"))
        .unwrap()
        .load()
        .unwrap();
    if dry_run {
        assert!(state.agents.is_empty());
        assert!(state.todo.iter().any(|item| item.key == "daemon-worker"));
        assert!(!worktrees.join("daemon-worker/worker-directory").exists());
    } else {
        let agent = state
            .agents
            .get("daemon-worker")
            .expect("daemon must start the queued worker");
        let pid = agent.pid.expect("worker must have a real process");
        wait_pid_exit(pid);
        let expected = worktrees.join("daemon-worker");
        assert_eq!(Path::new(&agent.worktree), expected);
        assert_eq!(
            fs::read_to_string(expected.join("worker-directory"))
                .unwrap()
                .trim(),
            expected.to_str().unwrap()
        );
        assert!(!repo.join("worker-directory").exists());
        assert!(!state.todo.iter().any(|item| item.key == "daemon-worker"));
    }
    let mut stream = UnixStream::connect(&socket).unwrap();
    writeln!(
        stream,
        "{}",
        serde_json::json!({"input":{"type":"custom",
        "track":"test", "op":"instant", "name":"after-worker-start"}})
    )
    .unwrap();
    let mut reader = BufReader::new(stream);
    let mut reply = String::new();
    reader.read_line(&mut reply).unwrap();
    writeln!(reader.get_mut(), "{{\"cmd\":\"flush\"}}").unwrap();
    reply.clear();
    reader.read_line(&mut reply).unwrap();
    let events = tacho_daemon::EventLog::open(&home.join("events.sqlite"))
        .unwrap()
        .load_all()
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.custom_name.as_deref() == Some("after-worker-start")),
        "worker state changes must not discard recorder events"
    );
}

#[test]
fn test_kit_start_prove_and_pr_preflight() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    let wt_root = tmp.path().join("worktrees");
    fs::create_dir_all(&repo_dir).unwrap();
    let base_sha = init_git_repo(&repo_dir);

    // Configure an area rule with a fast check command so `tacho prove` doesn't need a full cargo workspace
    let mut store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |st| {
            st.area_rules.push(AreaRule {
                area: "docs".to_string(),
                prefixes: vec!["docs/".to_string(), "README.md".to_string()],
                unit_prefixes: std::collections::BTreeMap::new(),
                checks: vec![("docs-check".to_string(), "test -f README.md".to_string())],
                isolate_batch: false,
                needs_screenshot: false,
            });
        })
        .unwrap();

    // 1. Run `tacho start feat-alpha`
    let out = tacho_cmd()
        .args([
            "start",
            "feat-alpha",
            "--base",
            &base_sha,
            "--dir",
            wt_root.to_str().unwrap(),
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "tacho start failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let wt_dir = wt_root.join("feat-alpha");
    assert!(wt_dir.join(".tachoenv").is_file());

    // Idempotency: re-running `tacho start feat-alpha` reuses the exact same port block
    let st1 = store.load().unwrap();
    let p1 = st1.worktrees.get("feat-alpha").unwrap().port;
    let out2 = tacho_cmd()
        .args(["start", "feat-alpha", "--dir", wt_root.to_str().unwrap()])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        out2.status.success(),
        "idempotent tacho start failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out2.stdout),
        String::from_utf8_lossy(&out2.stderr)
    );
    let st2 = store.load().unwrap();
    assert_eq!(st2.worktrees.get("feat-alpha").unwrap().port, p1);

    // 2. Make a change in the worktree and commit it
    fs::write(wt_dir.join("README.md"), "# Test Repo\nUpdated docs.\n").unwrap();
    Command::new("git")
        .args(["add", "README.md"])
        .current_dir(&wt_dir)
        .status()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "Update README documentation"])
        .current_dir(&wt_dir)
        .status()
        .unwrap();

    // 3. `tacho pr --dry-run` BEFORE `tacho prove` must be refused
    let pr_before_prove = tacho_cmd()
        .args([
            "pr",
            "Update README documentation",
            "-m",
            "Adds documentation details to README.",
            "--dry-run",
        ])
        .current_dir(&wt_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(!pr_before_prove.status.success());
    assert!(
        String::from_utf8_lossy(&pr_before_prove.stderr).contains("no proof found"),
        "stderr: {}",
        String::from_utf8_lossy(&pr_before_prove.stderr)
    );

    // 4. Run `tacho prove`
    let prove_out = tacho_cmd()
        .args(["prove"])
        .current_dir(&wt_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        prove_out.status.success(),
        "tacho prove failed: {}",
        String::from_utf8_lossy(&prove_out.stderr)
    );
    assert!(wt_dir.join(".tachoproof/summary.json").is_file());

    // 5. `tacho pr --dry-run` now succeeds and prints the Proof table
    let pr_ok = tacho_cmd()
        .args([
            "pr",
            "Update README documentation",
            "-m",
            "Adds documentation details to README.",
            "--dry-run",
        ])
        .current_dir(&wt_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        pr_ok.status.success(),
        "tacho pr --dry-run failed: {}",
        String::from_utf8_lossy(&pr_ok.stderr)
    );
    let stdout = String::from_utf8_lossy(&pr_ok.stdout);
    assert!(stdout.contains("## Proof"), "stdout: {stdout}");
    assert!(stdout.contains("docs"), "stdout: {stdout}");

    // 6. Dirtying a tracked file invalidates the proof tree and causes `tacho pr` to refuse
    fs::write(wt_dir.join("README.md"), "# Dirty after prove\n").unwrap();
    let pr_dirty = tacho_cmd()
        .args([
            "pr",
            "Update README documentation",
            "-m",
            "Adds documentation details to README.",
            "--dry-run",
        ])
        .current_dir(&wt_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(!pr_dirty.status.success());
    assert!(
        String::from_utf8_lossy(&pr_dirty.stderr).contains("uncommitted changes"),
        "stderr: {}",
        String::from_utf8_lossy(&pr_dirty.stderr)
    );
}

#[test]
fn test_kit_job_wait_and_kill() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    fs::create_dir_all(&repo_dir).unwrap();
    init_git_repo(&repo_dir);

    // 1. Short job name `db` (2 chars) is accepted and `tacho wait` streams all output
    let job_out = tacho_cmd()
        .args(["job", "db", "--", "bash", "-c", "echo hello-job && exit 0"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        job_out.status.success(),
        "job failed: {}",
        String::from_utf8_lossy(&job_out.stderr)
    );

    let wait_out = tacho_cmd()
        .args(["wait", "db", "--timeout", "10"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        wait_out.status.success(),
        "tacho wait db failed: stdout={} stderr={}",
        String::from_utf8_lossy(&wait_out.stdout),
        String::from_utf8_lossy(&wait_out.stderr)
    );
    assert!(String::from_utf8_lossy(&wait_out.stdout).contains("hello-job"));

    // 2. Long-running job returns exit code 3 on timeout, and `tacho kill` terminates its process group
    let bg_out = tacho_cmd()
        .args(["job", "srv", "--", "sleep", "60"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        bg_out.status.success(),
        "tacho job srv failed: stdout={} stderr={}",
        String::from_utf8_lossy(&bg_out.stdout),
        String::from_utf8_lossy(&bg_out.stderr)
    );

    let wait_timeout = tacho_cmd()
        .args(["wait", "srv", "--timeout", "1"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert_eq!(wait_timeout.status.code(), Some(3));

    let kill_out = tacho_cmd()
        .args(["kill", "srv", "--now"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        kill_out.status.success(),
        "tacho kill srv --now failed: stdout={} stderr={}",
        String::from_utf8_lossy(&kill_out.stdout),
        String::from_utf8_lossy(&kill_out.stderr)
    );
}

#[test]
fn test_hook_guardrails_opt_in_and_passive_default() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");

    let bad_payload = serde_json::json!({
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {
            "command": "git stash"
        }
    })
    .to_string();

    // 1. Default mode (without TACHO_GUARDRAILS=1) is 100% passive: exits 0 and prints nothing to stdout
    let mut child_passive = tacho_cmd()
        .args(["hook", "claude"])
        .env("TACHO_HOME", &tacho_home)
        .env_remove("TACHO_GUARDRAILS")
        .env_remove("TACHO_MANAGED_WORKER")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child_passive
        .stdin
        .take()
        .unwrap()
        .write_all(bad_payload.as_bytes())
        .unwrap();
    let out_passive = child_passive.wait_with_output().unwrap();
    assert_eq!(out_passive.status.code(), Some(0));
    assert!(out_passive.stdout.is_empty());

    // 2. Managed worker mode (`TACHO_GUARDRAILS=1`) blocks `git stash`, `run_in_background`, `--no-verify`, etc. with exit code 2
    for payload in [
        serde_json::json!({
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": { "command": "git stash" }
        }),
        serde_json::json!({
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": { "command": "cargo test", "run_in_background": true }
        }),
        serde_json::json!({
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": { "command": "git commit --no-verify -m 'wip'" }
        }),
    ] {
        let mut child = tacho_cmd()
            .args(["hook", "claude"])
            .env("TACHO_HOME", &tacho_home)
            .env("TACHO_GUARDRAILS", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(payload.to_string().as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert_eq!(out.status.code(), Some(2), "expected exit 2 for {payload}");
        assert!(!out.stderr.is_empty());
    }
}

#[test]
fn test_fleet_batch_bisect_flake_train_land_and_trace() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    fs::create_dir_all(&repo_dir).unwrap();
    let main_sha = init_git_repo(&repo_dir);

    // 1. Queue a compare task (`--compare sonnet,codex`) and verify expansion
    let todo_out = tacho_cmd()
        .args([
            "todo",
            "opt-cache",
            "Optimize query cache",
            "--area",
            "core",
            "--compare",
            "sonnet,codex",
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        todo_out.status.success(),
        "tacho todo failed: stdout={} stderr={}",
        String::from_utf8_lossy(&todo_out.stdout),
        String::from_utf8_lossy(&todo_out.stderr)
    );
    let store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let st = store.load().unwrap();
    assert_eq!(st.todo.len(), 2);
    assert_eq!(st.todo[0].key, "opt-cache-sonnet");
    assert_eq!(st.todo[1].key, "opt-cache-codex");

    // 2. Seed open PRs #10 and #11 for batch verification
    let mut store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    store
        .mutate(None, |st| {
            st.open_prs_cache = vec![
                PullFacts {
                    number: 10,
                    branch: "feat-10".to_string(),
                    head: main_sha.clone(),
                    base: "main".to_string(),
                    state: "OPEN".to_string(),
                    draft: false,
                    mergeable: "MERGEABLE".to_string(),
                    files: vec!["src/a.rs".to_string()],
                    title: "Feature 10".to_string(),
                    ..Default::default()
                },
                PullFacts {
                    number: 11,
                    branch: "feat-11".to_string(),
                    head: main_sha.clone(),
                    base: "main".to_string(),
                    state: "OPEN".to_string(),
                    draft: false,
                    mergeable: "MERGEABLE".to_string(),
                    files: vec!["src/b.rs".to_string()],
                    title: "Feature 11".to_string(),
                    ..Default::default()
                },
            ];
        })
        .unwrap();

    // 3. Test flake retry (`r0` fails, `r0-again` passes -> batch passes and records flake)
    let flake_marker = tmp.path().join("flake-once");
    let flake_cmd = format!(
        "if [ ! -f '{}' ]; then touch '{}'; echo 'test core::flaky_test ... FAILED'; exit 1; else exit 0; fi",
        flake_marker.display(),
        flake_marker.display()
    );
    let batch_flake = tacho_cmd()
        .args(["batch", "10", "--cmd", &flake_cmd])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        batch_flake.status.success(),
        "batch flake retry should pass: {}",
        String::from_utf8_lossy(&batch_flake.stderr)
    );
    let st_after_flake = store.load().unwrap();
    assert!(
        !st_after_flake.passed.contains_key(&10),
        "flake retry must not be treated as passed"
    );
    assert_eq!(st_after_flake.batches.last().unwrap().status, "flake");
    assert!(!st_after_flake.flakes.is_empty());

    // 4. Test binary bisection blaming culprit PR #11 (`k >= 2` fails, `k < 2` passes) and auto-rerunning PR #10
    let bisect_cmd = "if [ \"$TACHO_BATCH_PROBE_K\" -ge 2 ]; then echo 'test b::bad_test ... FAILED'; echo 'proptest: minimal failing input: x = 0'; exit 1; else exit 0; fi";
    store
        .mutate(None, |st| {
            st.passed.clear();
            st.batches.clear();
        })
        .unwrap();
    let batch_bisect = tacho_cmd()
        .args(["batch", "10", "11", "--cmd", bisect_cmd])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(!batch_bisect.status.success());
    let st_after_bisect = store.load().unwrap();
    assert!(
        st_after_bisect.failed.contains_key(&11),
        "PR #11 should be blamed as culprit"
    );
    assert!(
        !st_after_bisect.failed.contains_key(&10),
        "PR #10 must not be blamed"
    );
    assert!(
        st_after_bisect.passed.contains_key(&10),
        "PR #10 should pass on automatic rerun"
    );

    // 5. Open a merge train from the green batch for PR #10 and land it
    let green_batch_id = st_after_bisect
        .batches
        .iter()
        .find(|b| b.status == "passed" && b.prs == vec![10])
        .unwrap()
        .id
        .clone();

    let train_out = tacho_cmd()
        .args(["train", "--from-batch", &green_batch_id])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        train_out.status.success(),
        "tacho train failed: stdout={} stderr={}",
        String::from_utf8_lossy(&train_out.stdout),
        String::from_utf8_lossy(&train_out.stderr)
    );

    let land_out = tacho_cmd()
        .args(["land"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        land_out.status.success(),
        "tacho land failed: {}",
        String::from_utf8_lossy(&land_out.stderr)
    );
    let st_after_land = store.load().unwrap();
    assert!(st_after_land.merged_prs.contains(&10));

    // 6. Export a Perfetto trace (`tacho trace`) and verify non-empty valid snapshot
    let trace_path = tmp.path().join("fleet.perfetto-trace");
    let trace_out = tacho_cmd()
        .args(["trace", "-o", trace_path.to_str().unwrap()])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        trace_out.status.success(),
        "tacho trace failed: {}",
        String::from_utf8_lossy(&trace_out.stderr)
    );
    let trace_bytes = fs::read(&trace_path).unwrap();
    assert!(
        trace_bytes.len() > 256,
        "trace should contain fleet tracks and events"
    );
}

#[test]
fn test_fleet_tick_dispatch_reap_land_mismatch_and_tidy_rescue() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    let wt_root = tmp.path().join("worktrees");
    fs::create_dir_all(&repo_dir).unwrap();
    let main_sha = init_git_repo(&repo_dir);

    // 1. Start a worktree and make uncommitted edits, then verify `tacho tidy --min-age 0` rescues it to `refs/tacho/rescue/...`
    let start_out = tacho_cmd()
        .args([
            "start",
            "dirty-wt",
            "--base",
            &main_sha,
            "--dir",
            wt_root.to_str().unwrap(),
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        start_out.status.success(),
        "tacho start dirty-wt failed: stdout={} stderr={}",
        String::from_utf8_lossy(&start_out.stdout),
        String::from_utf8_lossy(&start_out.stderr)
    );
    let wt_dir = wt_root.join("dirty-wt");
    fs::write(wt_dir.join("uncommitted.txt"), "precious work\n").unwrap();

    let tidy_out = tacho_cmd()
        .args(["tidy", "--min-age", "0"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        tidy_out.status.success(),
        "tidy failed: {}",
        String::from_utf8_lossy(&tidy_out.stderr)
    );
    let refs_out = Command::new("git")
        .args(["for-each-ref", "refs/tacho/rescue/"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    let refs_str = String::from_utf8_lossy(&refs_out.stdout);
    assert!(
        refs_str.contains("refs/tacho/rescue/dirty-wt-"),
        "expected rescue ref in: {refs_str}"
    );

    // 2. Queue a task, run `tacho tick` with a mock `TACHO_AGENT_CMD` that prints `PR: #42`, then run `tacho tick` again to reap it
    let todo_out = tacho_cmd()
        .args([
            "todo",
            "auto-task",
            "Implement feature 42",
            "--area",
            "core",
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        todo_out.status.success(),
        "tacho todo auto-task failed: stdout={} stderr={}",
        String::from_utf8_lossy(&todo_out.stdout),
        String::from_utf8_lossy(&todo_out.stderr)
    );

    let tick1 = tacho_cmd()
        .args(["tick"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .env("TACHO_REPO", &repo_dir)
        .env("TACHO_AGENT_CMD", "echo 'Working...' && echo 'PR: #42'")
        .output()
        .unwrap();
    assert!(
        tick1.status.success(),
        "tick1 failed: stdout={} stderr={}",
        String::from_utf8_lossy(&tick1.stdout),
        String::from_utf8_lossy(&tick1.stderr)
    );

    // Wait for the mock agent to exit via its pidfd (no inotify needed): once
    // the process is gone its log is complete.
    let agent_pid = FleetStore::open(&tacho_home.join("events.sqlite"))
        .and_then(|store| store.load())
        .expect("load fleet state after first tick")
        .agents
        .get("auto-task")
        .and_then(|agent| agent.pid)
        .expect("first tick must record the auto-task agent pid");
    wait_pid_exit(agent_pid);
    let log_file = tacho_home.join("logs").join("auto-task.log");
    let log = fs::read_to_string(&log_file)
        .unwrap_or_else(|e| panic!("read {}: {e}", log_file.display()));
    assert!(log.contains("PR: #42"), "agent log lacks PR line:\n{log}");
    let tick2 = tacho_cmd()
        .args(["tick"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .env("TACHO_REPO", &repo_dir)
        .output()
        .unwrap();
    assert!(
        tick2.status.success(),
        "tick2 failed: stdout={} stderr={}",
        String::from_utf8_lossy(&tick2.stdout),
        String::from_utf8_lossy(&tick2.stderr)
    );

    let store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let st = store.load().unwrap();
    let run = st
        .agents
        .get("auto-task")
        .expect("auto-task should be in agents");
    assert_eq!(run.pr, Some(42));
    assert_eq!(run.pr_source.as_deref(), Some("stated"));
    assert!(tacho_home.join("snapshot.json").is_file());
}

#[test]
fn test_strict_start_base_db_plugin_live_tell_and_batch_head_moved() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    let wt_root = tmp.path().join("worktrees");
    fs::create_dir_all(&repo_dir).unwrap();
    let main_sha = init_git_repo(&repo_dir);

    // 1. `tacho start` strictly refuses an unresolvable PR number or revision without falling back to HEAD
    let bad_pr_start = tacho_cmd()
        .args([
            "start",
            "bad-pr-wt",
            "--base",
            "pr:999999",
            "--dir",
            wt_root.to_str().unwrap(),
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        !bad_pr_start.status.success(),
        "tacho start with non-existent PR base must fail rather than falling back to HEAD"
    );

    let bad_rev_start = tacho_cmd()
        .args([
            "start",
            "bad-rev-wt",
            "--base",
            "nonexistent-branch-xyz",
            "--dir",
            wt_root.to_str().unwrap(),
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(!bad_rev_start.status.success());

    // 2. `tacho start` runs per-agent database setup plugin (`TACHO_DB_UP_CMD`) and writes DATABASE_URL into `.tachoenv`
    let db_start = tacho_cmd()
        .args([
            "start",
            "db-wt",
            "--base",
            &main_sha,
            "--dir",
            wt_root.to_str().unwrap(),
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .env(
            "TACHO_DB_UP_CMD",
            "echo \"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:${DB_PORT}/test_${SLUG}\"",
        )
        .output()
        .unwrap();
    assert!(
        db_start.status.success(),
        "tacho start with TACHO_DB_UP_CMD failed: {}",
        String::from_utf8_lossy(&db_start.stderr)
    );
    let env_txt = fs::read_to_string(wt_root.join("db-wt").join(".tachoenv")).unwrap();
    let parsed_env = tacho_core::decide::verify::parse_env(&env_txt);
    let db_url = parsed_env
        .get("DATABASE_URL")
        .expect("DATABASE_URL should be present in .tachoenv");
    assert!(db_url.starts_with("postgres://postgres:postgres@127.0.0.1:"));
    assert!(db_url.ends_with("/test_db-wt"));

    // 3. Live `tacho tell` delivers to a running worker's inbox without killing/restarting it,
    // and the next `tacho hook claude` drains and injects the message.
    let mut store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let my_pid = std::process::id(); // Guaranteed alive PID
    let my_starttime = fs::read_to_string(format!("/proc/{my_pid}/stat"))
        .ok()
        .and_then(|s| {
            let rparen = s.rfind(')')?;
            s[rparen + 2..]
                .split_whitespace()
                .nth(19)
                .and_then(|v| v.parse::<u64>().ok())
        });
    let now_live = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    store
        .mutate(None, |st| {
            st.agents.insert(
                "live-worker".to_string(),
                tacho_core::decide::dispatch::AgentRun {
                    key: "live-worker".to_string(),
                    pid: Some(my_pid),
                    starttime: my_starttime,
                    provider: "claude".to_string(),
                    worktree: wt_root.join("db-wt").to_string_lossy().to_string(),
                    started_at: now_live,
                    ..Default::default()
                },
            );
        })
        .unwrap();
    drop(store);

    let tell_out = tacho_cmd()
        .args(["tell", "live-worker", "Please also add a migration test"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        tell_out.status.success(),
        "tacho tell live-worker failed: stdout={} stderr={}",
        String::from_utf8_lossy(&tell_out.stdout),
        String::from_utf8_lossy(&tell_out.stderr)
    );
    let mut store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let inbox_file = tacho_home.join("inbox").join("live-worker.txt");
    assert!(
        inbox_file.is_file(),
        "inbox file should exist for live worker"
    );

    let hook_payload = serde_json::json!({
        "hook_event_name": "PreToolUse",
        "tool_name": "Read",
        "tool_input": { "file_path": "README.md" }
    })
    .to_string();
    let mut hook_child = tacho_cmd()
        .args(["hook", "claude"])
        .env("TACHO_HOME", &tacho_home)
        .env("TACHO_AGENT", "live-worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    hook_child
        .stdin
        .take()
        .unwrap()
        .write_all(hook_payload.as_bytes())
        .unwrap();
    let hook_out = hook_child.wait_with_output().unwrap();
    assert_eq!(hook_out.status.code(), Some(0));
    let hook_stderr = String::from_utf8_lossy(&hook_out.stderr);
    assert!(
        hook_stderr.contains("Please also add a migration test"),
        "expected live tell message in hook stderr, got: {hook_stderr}"
    );
    assert!(
        !inbox_file.exists(),
        "inbox file should be drained after hook delivery"
    );

    // 4. `tacho batch` refuses to mark a batch as passed if a PR's head SHA moves while the batch is running
    Command::new("git")
        .args(["branch", "feat-77", &main_sha])
        .current_dir(&repo_dir)
        .output()
        .unwrap();

    store
        .mutate(None, |st| {
            st.open_prs_cache = vec![PullFacts {
                number: 77,
                branch: "feat-77".to_string(),
                head: main_sha.clone(),
                base: "main".to_string(),
                state: "OPEN".to_string(),
                mergeable: "MERGEABLE".to_string(),
                files: vec!["src/lib.rs".to_string()],
                title: "Feature 77".to_string(),
                ..Default::default()
            }];
        })
        .unwrap();

    // While the batch command runs, advance branch feat-77 to simulate a force-push mid-batch
    let move_head_cmd = "git commit --allow-empty -m 'moved head' && git branch -f feat-77 HEAD";
    let batch_moved = tacho_cmd()
        .args(["batch", "77", "--cmd", move_head_cmd])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        !batch_moved.status.success(),
        "batch should refuse to pass when PR head moves mid-batch"
    );
    let st_after_move = store.load().unwrap();
    assert!(
        !st_after_move.passed.contains_key(&77),
        "PR #77 must NOT be marked passed when its head moved during batch"
    );
    assert!(st_after_move
        .batches
        .iter()
        .any(|b| b.prs == vec![77] && b.status == "head-moved"));
}

#[test]
fn test_status_sections_staleness_needs_and_scrubber() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    fs::create_dir_all(&repo_dir).unwrap();
    let main_sha = init_git_repo(&repo_dir);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Seed FleetStore with rich fleet state, including a GitHub token secret in status_note to test Scrubber
    let secret_token = "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let self_pid = std::process::id();
    let self_starttime = fs::read_to_string(format!("/proc/{self_pid}/stat"))
        .ok()
        .and_then(|s| {
            let rparen = s.rfind(')')?;
            s[rparen + 2..]
                .split_whitespace()
                .nth(19)
                .and_then(|v| v.parse::<u64>().ok())
        });
    store
        .mutate(None, |st| {
            st.agents.insert(
                "worker-alpha".to_string(),
                tacho_core::decide::dispatch::AgentRun {
                    key: "worker-alpha".to_string(),
                    pid: Some(self_pid),
                    starttime: self_starttime,
                    area: "core".to_string(),
                    brief: format!("Investigate auth header {secret_token}"),
                    status_note: format!("read: checking token {secret_token}"),
                    started_at: now - 120,
                    provider: "claude".to_string(),
                    model: Some("claude-sonnet-4-6".to_string()),
                    ..Default::default()
                },
            );
            st.agents.insert(
                "worker-no-pr".to_string(),
                tacho_core::decide::dispatch::AgentRun {
                    key: "worker-no-pr".to_string(),
                    pid: None,
                    area: "api".to_string(),
                    brief: "Fix endpoint timeout".to_string(),
                    status_note: "exited".to_string(),
                    started_at: now - 600,
                    finished_at: Some(now - 300),
                    exit_code: Some(1),
                    provider: "claude".to_string(),
                    ..Default::default()
                },
            );
            st.holds.insert(
                "pr:55".to_string(),
                tacho_core::decide::dispatch::Hold {
                    reason: "waiting on schema review".to_string(),
                    since: now - 30 * 3600, // > 24h -> stale hold
                },
            );
            st.flakes = tacho_core::decide::verify::record_flakes(
                &st.flakes,
                &["core::wal::flaky_checkpoint".to_string()],
                "core",
                "b001",
                "assertion failed",
                now - 60,
            );
        })
        .unwrap();

    // 1. Full JSON status (`tacho status --json`) includes version=1, all sections, and scrubs secrets
    let status_json_out = tacho_cmd()
        .args(["status", "--json"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        status_json_out.status.success(),
        "status --json failed: {}",
        String::from_utf8_lossy(&status_json_out.stderr)
    );
    let json_str = String::from_utf8_lossy(&status_json_out.stdout);
    assert!(
        !json_str.contains(secret_token),
        "Scrubber must redact secret token from status --json"
    );
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["version"], 1);
    for section_key in [
        "headline",
        "needs",
        "needs_you",
        "attention",
        "pipeline",
        "agents",
        "machine",
        "cost",
        "held",
        "stale",
        "progress",
        "flakes",
    ] {
        assert!(
            parsed.get(section_key).is_some(),
            "missing section '{section_key}' in status --json"
        );
    }
    assert!(parsed["machine"]["quota_cores"].as_f64().unwrap_or(0.0) > 0.0);
    assert_eq!(parsed["held"]["holds"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["stale"]["stale_holds"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["flakes"].as_array().unwrap().len(), 1);

    // 2. Filtered `--section` JSON and human output
    let sec_json_out = tacho_cmd()
        .args([
            "status",
            "--json",
            "--section",
            "headline",
            "--section",
            "machine",
            "--section",
            "held",
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        sec_json_out.status.success(),
        "status --section failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sec_json_out.stdout),
        String::from_utf8_lossy(&sec_json_out.stderr)
    );
    let sec_parsed: serde_json::Value = serde_json::from_slice(&sec_json_out.stdout).unwrap();
    assert!(sec_parsed.get("headline").is_some());
    assert!(sec_parsed.get("machine").is_some());
    assert!(sec_parsed.get("held").is_some());
    assert!(sec_parsed.get("pipeline").is_none());

    // 3. `tacho needs` subcommand alias and human text status
    let needs_out = tacho_cmd()
        .args(["needs"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        needs_out.status.success(),
        "tacho needs failed: stdout={} stderr={}",
        String::from_utf8_lossy(&needs_out.stdout),
        String::from_utf8_lossy(&needs_out.stderr)
    );
    let needs_txt = String::from_utf8_lossy(&needs_out.stdout);
    assert!(
        needs_txt.to_ascii_lowercase().contains("needs you"),
        "stdout: {needs_txt}"
    );
    assert!(needs_txt.contains("worker-no-pr"), "stdout: {needs_txt}");

    let status_txt_out = tacho_cmd()
        .args(["status"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        status_txt_out.status.success(),
        "tacho status failed: stdout={} stderr={}",
        String::from_utf8_lossy(&status_txt_out.stdout),
        String::from_utf8_lossy(&status_txt_out.stderr)
    );
    let status_txt = String::from_utf8_lossy(&status_txt_out.stdout);
    assert!(
        !status_txt.contains(secret_token),
        "Scrubber must redact secret token from human status text"
    );
    assert!(status_txt.contains("tacho:"));
    assert!(status_txt.to_ascii_lowercase().contains("machine:"));

    // 4. Unknown `--section` exits with code 2
    let bad_sec = tacho_cmd()
        .args(["status", "--section", "nonexistent_section"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert_eq!(bad_sec.status.code(), Some(2));

    // 5. Stale snapshot (> 300s old) is refused with exact fix command (`run: tacho tick`)
    let stale_snap = serde_json::json!({
        "version": 1,
        "at": now - 600,
    });
    fs::write(
        tacho_home.join("snapshot.json"),
        serde_json::to_string(&stale_snap).unwrap(),
    )
    .unwrap();
    let stale_out = tacho_cmd()
        .args(["status"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(!stale_out.status.success());
    let stale_err = String::from_utf8_lossy(&stale_out.stderr);
    assert!(
        stale_err.contains("the snapshot is 10 min old") && stale_err.contains("run: tacho tick"),
        "expected staleness refusal in stderr, got: {stale_err}"
    );
    let _ = main_sha;
}

#[test]
fn test_hook_guard_subcommand_and_cpu_burners() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");

    let run_guard = |payload: serde_json::Value| -> (Option<i32>, String) {
        let mut child = tacho_cmd()
            .args(["hook", "guard"])
            .env("TACHO_HOME", &tacho_home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(payload.to_string().as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    };

    // 1. Standalone `yes` and `stress-ng` CPU burners are refused with exit code 2
    let (code_yes, err_yes) = run_guard(serde_json::json!({
        "tool_name": "Bash",
        "tool_input": { "command": "yes" }
    }));
    assert_eq!(code_yes, Some(2), "stderr: {err_yes}");
    assert!(err_yes.contains("tacho starve"));

    let (code_stress, err_stress) = run_guard(serde_json::json!({
        "tool_name": "Bash",
        "tool_input": { "command": "stress-ng --cpu 4" }
    }));
    assert_eq!(code_stress, Some(2), "stderr: {err_stress}");

    // 2. `run_in_background: true` and `git stash` are refused with exit code 2
    let (code_bg, _) = run_guard(serde_json::json!({
        "tool_name": "Bash",
        "tool_input": { "command": "cargo test", "run_in_background": true }
    }));
    assert_eq!(code_bg, Some(2));

    let (code_stash, _) = run_guard(serde_json::json!({
        "tool_name": "Bash",
        "tool_input": { "command": "git stash" }
    }));
    assert_eq!(code_stash, Some(2));

    // 3. Piped `yes | head -n 1` and `tacho starve -- yes` are allowed (exit 0)
    let (code_piped, _) = run_guard(serde_json::json!({
        "tool_name": "Bash",
        "tool_input": { "command": "yes | head -n 1" }
    }));
    assert_eq!(code_piped, Some(0));

    let (code_starve, _) = run_guard(serde_json::json!({
        "tool_name": "Bash",
        "tool_input": { "command": "tacho starve -- yes" }
    }));
    assert_eq!(code_starve, Some(0));
}

#[test]
fn test_cli_surface_version_setup_db_session_todo_hold_retire_events_tidy_replay() {
    let tmp = tempfile::tempdir().unwrap();
    let tacho_home = tmp.path().join("tacho-home");
    let repo_dir = tmp.path().join("repo");
    fs::create_dir_all(&repo_dir).unwrap();
    let main_sha = init_git_repo(&repo_dir);

    // 1. `tacho version` and `tacho version --json`
    let ver_out = tacho_cmd()
        .args(["version", "--json"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        ver_out.status.success(),
        "version --json failed: stdout={} stderr={}",
        String::from_utf8_lossy(&ver_out.stdout),
        String::from_utf8_lossy(&ver_out.stderr)
    );
    let ver_json: serde_json::Value = serde_json::from_slice(&ver_out.stdout).unwrap();
    assert_eq!(ver_json["running_commit"], main_sha);
    assert!(ver_json["rule"].as_str().unwrap().contains("Auto-updates"));

    // 2. `tacho setup --dry-run` and `tacho setup`
    let setup_dry = tacho_cmd()
        .args(["setup", "--dry-run"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        setup_dry.status.success(),
        "setup --dry-run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&setup_dry.stdout),
        String::from_utf8_lossy(&setup_dry.stderr)
    );
    assert!(!tacho_home.join("fleet.env").exists());

    let setup_real = tacho_cmd()
        .args(["setup"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        setup_real.status.success(),
        "setup failed: stdout={} stderr={}",
        String::from_utf8_lossy(&setup_real.stdout),
        String::from_utf8_lossy(&setup_real.stderr)
    );
    assert!(tacho_home.join("fleet.env").is_file());

    // 3. `tacho db <name> [checkout]` and `tacho db <name> --rm`
    let db_up = tacho_cmd()
        .args(["db", "feat-db", repo_dir.to_str().unwrap()])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        db_up.status.success(),
        "db feat-db failed: stdout={} stderr={}",
        String::from_utf8_lossy(&db_up.stdout),
        String::from_utf8_lossy(&db_up.stderr)
    );
    let env_txt = fs::read_to_string(repo_dir.join(".tachoenv")).unwrap();
    assert!(env_txt.contains("DATABASE_URL=postgres://"));

    let db_rm = tacho_cmd()
        .args(["db", "feat-db", repo_dir.to_str().unwrap(), "--rm"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        db_rm.status.success(),
        "db feat-db --rm failed: stdout={} stderr={}",
        String::from_utf8_lossy(&db_rm.stdout),
        String::from_utf8_lossy(&db_rm.stderr)
    );
    let env_after_rm = fs::read_to_string(repo_dir.join(".tachoenv")).unwrap();
    assert!(!env_after_rm.contains("DATABASE_URL="));

    // 4. `tacho session` fleet-wide default configuration and `--clear`
    let sess_set = tacho_cmd()
        .args([
            "session",
            "--provider",
            "gemini",
            "--model",
            "gemini-pro",
            "--effort",
            "high",
        ])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        sess_set.status.success(),
        "session set failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sess_set.stdout),
        String::from_utf8_lossy(&sess_set.stderr)
    );
    let store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let st_sess = store.load().unwrap();
    assert_eq!(st_sess.default_provider, "gemini");
    assert_eq!(
        st_sess.default_model.as_deref(),
        Some("gemini-3.1-pro-preview")
    );
    assert_eq!(st_sess.default_effort.as_deref(), Some("high"));

    let sess_clear = tacho_cmd()
        .args(["session", "--clear"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        sess_clear.status.success(),
        "session --clear failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sess_clear.stdout),
        String::from_utf8_lossy(&sess_clear.stderr)
    );
    let st_cleared = store.load().unwrap();
    assert_eq!(st_cleared.default_provider, "claude");
    assert!(st_cleared.default_model.is_none());

    // 5. `tacho todo`, `tacho hold --why`, `tacho retire --why`, and `tacho todo --retired`
    let todo_add = tacho_cmd()
        .args(["todo", "task-retire", "Refactor metrics"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        todo_add.status.success(),
        "todo task-retire failed: stdout={} stderr={}",
        String::from_utf8_lossy(&todo_add.stdout),
        String::from_utf8_lossy(&todo_add.stderr)
    );

    let hold_why = tacho_cmd()
        .args(["hold", "task-retire", "--why", "waiting on upstream API"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        hold_why.status.success(),
        "hold task-retire failed: stdout={} stderr={}",
        String::from_utf8_lossy(&hold_why.stdout),
        String::from_utf8_lossy(&hold_why.stderr)
    );
    assert_eq!(
        store.load().unwrap().holds["task-retire"].reason,
        "waiting on upstream API"
    );

    let retire_why = tacho_cmd()
        .args(["retire", "task-retire", "--why", "obsolete"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        retire_why.status.success(),
        "retire task-retire failed: stdout={} stderr={}",
        String::from_utf8_lossy(&retire_why.stdout),
        String::from_utf8_lossy(&retire_why.stderr)
    );

    let todo_retired = tacho_cmd()
        .args(["todo", "--retired", "--json"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        todo_retired.status.success(),
        "todo --retired --json failed: stdout={} stderr={}",
        String::from_utf8_lossy(&todo_retired.stdout),
        String::from_utf8_lossy(&todo_retired.stderr)
    );
    let retired_list: Vec<String> = serde_json::from_slice(&todo_retired.stdout).unwrap();
    assert!(retired_list.contains(&"task-retire".to_string()));

    // 6. `tacho events --since 1h -n 5` and invalid `--since` exit code 2
    let ev_ok = tacho_cmd()
        .args(["events", "--since", "1h", "-n", "5", "--json"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        ev_ok.status.success(),
        "events --since 1h failed: stdout={} stderr={}",
        String::from_utf8_lossy(&ev_ok.stdout),
        String::from_utf8_lossy(&ev_ok.stderr)
    );
    let ev_arr: Vec<serde_json::Value> = serde_json::from_slice(&ev_ok.stdout).unwrap();
    assert!(!ev_arr.is_empty() && ev_arr.len() <= 5);

    let ev_bad = tacho_cmd()
        .args(["events", "--since", "not-a-duration"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert_eq!(ev_bad.status.code(), Some(2));

    // 7. `tacho tidy --stale-runs` validation (`0` -> exit 2) and retiring old abandoned runs
    let tidy_bad = tacho_cmd()
        .args(["tidy", "--stale-runs", "0"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert_eq!(tidy_bad.status.code(), Some(2));

    let mut store = FleetStore::open(&tacho_home.join("events.sqlite")).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    store
        .mutate(None, |st| {
            st.agents.insert(
                "ancient-run".to_string(),
                tacho_core::decide::dispatch::AgentRun {
                    key: "ancient-run".to_string(),
                    pid: None,
                    started_at: now - 10 * 86_400,
                    finished_at: Some(now - 10 * 86_400 + 60),
                    exit_code: Some(1),
                    ..Default::default()
                },
            );
            st.passed.insert(
                88,
                tacho_core::decide::verify::PassedRecord {
                    pr: 88,
                    sha: main_sha.clone(),
                    main_sha: main_sha.clone(),
                    batch_id: "b088".to_string(),
                    passed_at: now,
                    tree: main_sha.clone(),
                    areas: vec!["core".to_string()],
                },
            );
            st.batches.push(tacho_core::decide::verify::BatchRun {
                id: "b088".to_string(),
                group: "core".to_string(),
                prs: vec![88],
                shas: std::collections::BTreeMap::from([(88, main_sha.clone())]),
                main_sha: main_sha.clone(),
                areas: vec!["core".to_string()],
                checks: Vec::new(),
                mode: tacho_core::decide::verify::BatchMode::Presubmit,
                status: "passed".to_string(),
                started_at: now - 30,
                finished_at: Some(now),
                tree: Some(main_sha.clone()),
                pid: None,
                log: String::new(),
                culprit: None,
                skipped: Vec::new(),
                flakes: Vec::new(),
            });
            st.open_prs_cache = vec![PullFacts {
                number: 88,
                branch: "feat-88".to_string(),
                head: main_sha.clone(),
                base: "main".to_string(),
                state: "OPEN".to_string(),
                mergeable: "MERGEABLE".to_string(),
                files: vec!["README.md".to_string()],
                title: "Feature 88".to_string(),
                ..Default::default()
            }];
        })
        .unwrap();

    let tidy_stale = tacho_cmd()
        .args(["tidy", "--stale-runs", "7"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        tidy_stale.status.success(),
        "tidy --stale-runs 7 failed: stdout={} stderr={}",
        String::from_utf8_lossy(&tidy_stale.stdout),
        String::from_utf8_lossy(&tidy_stale.stderr)
    );
    assert!(store.load().unwrap().retired.contains("ancient-run"));

    // 8. `tacho train [prs]... --max`, `tacho land [train] --dry-run`, `tacho release-train [pr] --approve`, and `tacho decide-replay`
    let train_pos = tacho_cmd()
        .args(["train", "88", "--max", "1"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        train_pos.status.success(),
        "train 88 --max 1 failed: {}",
        String::from_utf8_lossy(&train_pos.stderr)
    );

    let land_pos = tacho_cmd()
        .args(["land", "t001", "--dry-run"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        land_pos.status.success(),
        "land t001 --dry-run failed: {}",
        String::from_utf8_lossy(&land_pos.stderr)
    );

    let rel_train = tacho_cmd()
        .args(["release-train", "88", "--approve"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .env("TACHO_COMPILE_CMD", "true")
        .output()
        .unwrap();
    assert!(
        rel_train.status.success(),
        "release-train 88 --approve failed: {}",
        String::from_utf8_lossy(&rel_train.stderr)
    );
    assert_eq!(store.load().unwrap().train_pr.unwrap().number, Some(88));

    let decide_replay = tacho_cmd()
        .args(["decide-replay", "b088", "--cmd", "true", "--dry-run"])
        .current_dir(&repo_dir)
        .env("TACHO_HOME", &tacho_home)
        .output()
        .unwrap();
    assert!(
        decide_replay.status.success(),
        "decide-replay failed: {}",
        String::from_utf8_lossy(&decide_replay.stderr)
    );
}
