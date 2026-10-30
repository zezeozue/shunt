//! Managed worker sessions (`session`), provider argv resolution, worker process spawning,
//! PR attachment (`attach`), process stop (`stop`), and live operator guidance (`tell`).

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use tacho_core::decide::dispatch::{
    base_key, builtin_providers, resolve_with_providers, resume_task, with_hold, AgentRun,
    Provider, ProviderSpec, TellPlan, TodoItem,
};
use tacho_core::decide::watch::{verdict, Verdict};
use tacho_core::decide::{decide, DecisionState, WorldObservation};
use tacho_core::TaskStatus;
use tacho_daemon::FleetStore;

use crate::cli::{AttachArgs, SessionArgs, StopArgs, TellArgs};

use super::dispatch::detect_brief_source;
use super::store::{
    classify_agent_process, emit_decision_event, kill_stopped_and_wait, live_agent_keys, now_ns,
    now_secs, open_fleet_store, proc_pid_cgroup, proc_pid_starttime, send_signal, verify_agent_pid,
    write_atomic, AgentProcess,
};
use crate::kit::use_private_tmp;

/// Infer the agent provider from a model name or shorthand:
/// - `gpt-*`, `o1*`, `o3*`, `o4*`, `codex*` -> `"codex"`
/// - `claude-*`, `sonnet*`, `opus*`, `haiku*` -> `"claude"`
/// - `gemini-*`, `flash*`, `pro` -> `"gemini"`
pub fn infer_provider_from_model(model: &str) -> Option<&'static str> {
    let m = model.trim().to_ascii_lowercase();
    if m.is_empty() {
        return None;
    }
    if m.starts_with("gpt-")
        || m.starts_with("o1")
        || m.starts_with("o3")
        || m.starts_with("o4")
        || m.starts_with("codex")
    {
        Some("codex")
    } else if m.starts_with("claude-")
        || m == "sonnet"
        || m.starts_with("sonnet-")
        || m == "opus"
        || m.starts_with("opus-")
        || m == "haiku"
        || m.starts_with("haiku-")
    {
        Some("claude")
    } else if m.starts_with("gemini-") || m.starts_with("flash") || m == "pro" {
        Some("gemini")
    } else {
        None
    }
}

/// Build the exact argv vector (program + args, no shell wrapper) for a provider session.
#[allow(clippy::too_many_arguments)]
pub fn build_session_argv(
    provider: &str,
    model: Option<&str>,
    effort: Option<&str>,
    session_id: Option<&str>,
    prompt: &str,
    worktree: &str,
    key: &str,
    providers: &BTreeMap<String, ProviderSpec>,
) -> (String, Vec<String>, BTreeMap<String, String>) {
    let builtins = builtin_providers();
    let spec = providers
        .get(provider)
        .or_else(|| builtins.get(provider))
        .cloned()
        .unwrap_or_else(ProviderSpec::claude);
    let planned = if session_id.is_some() {
        spec.resume(key, worktree, session_id, prompt, model, effort)
    } else {
        spec.start(key, worktree, prompt, model, effort)
    };
    (
        planned.program,
        planned.args,
        planned.env.into_iter().collect(),
    )
}

/// Spawns a provider agent process for a `TodoItem` in its worktree.
pub fn spawn_worker_for_item(store: &mut FleetStore, item: &TodoItem) -> io::Result<u32> {
    spawn_worker_in_dir(store, item, None)
}

/// Spawns a provider agent process for a `TodoItem`, using `wt_override` if supplied or
/// resolving/provisioning an isolated linked worktree on a named branch off `main`.
///
/// Refuses to start (and places a hold + pages the owner) if the resolved directory is the
/// repository's main checkout or is not an isolated linked git worktree on a named branch.
pub fn spawn_worker_in_dir(
    store: &mut FleetStore,
    item: &TodoItem,
    wt_override: Option<&Path>,
) -> io::Result<u32> {
    spawn_worker_with_trace(store, item, wt_override, true)
}

pub(crate) fn spawn_daemon_worker(store: &mut FleetStore, item: &TodoItem) -> io::Result<u32> {
    spawn_worker_with_trace(store, item, None, false)
}

fn spawn_worker_with_trace(
    store: &mut FleetStore,
    item: &TodoItem,
    wt_override: Option<&Path>,
    send_trace: bool,
) -> io::Result<u32> {
    let state = store.load()?;
    let inferred_prov = item.provider.clone().or_else(|| {
        item.model
            .as_deref()
            .and_then(infer_provider_from_model)
            .map(String::from)
    });
    let choice = resolve_with_providers(
        inferred_prov.as_deref(),
        item.model.as_deref(),
        item.effort.as_deref(),
        &state.default_provider,
        state.default_model.as_deref(),
        state.default_effort.as_deref(),
        &state.providers,
    )
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let home = crate::paths::resolve_home();
    let logs_dir = home.join("logs");
    fs::create_dir_all(&logs_dir)?;
    let log_path = logs_dir.join(format!("{}.log", item.key));

    let cwd = std::env::current_dir()?;
    let root_key = base_key(&item.key).to_string();
    let wt_path = if let Some(explicit_dir) = wt_override {
        explicit_dir.to_path_buf()
    } else if let Some(w) = state
        .worktrees
        .get(&item.key)
        .or_else(|| state.worktrees.get(&root_key))
    {
        PathBuf::from(&w.path)
    } else if let Some(prior) = state
        .agents
        .get(&item.key)
        .or_else(|| tacho_core::decide::dispatch::latest(&state.agents, &state.retired, &root_key))
        .filter(|a| !a.worktree.is_empty())
    {
        PathBuf::from(&prior.worktree)
    } else if crate::kit::start::is_within_fleet_root(&cwd, store.home(), &state)
        && tacho_daemon::exec::observe::verify_worker_worktree(&cwd).is_ok()
    {
        cwd.clone()
    } else if crate::kit::start::is_within_fleet_root(&cwd, store.home(), &state)
        && tacho_daemon::exec::observe::inside_git_checkout(&cwd)
    {
        let wt_slug = if tacho_core::decide::verify::slug_problem(&item.key).is_none() {
            item.key.clone()
        } else {
            root_key.clone()
        };
        let (path, code) = crate::kit::start::provision_worktree_in_store(
            store,
            crate::cli::StartArgs {
                slug: wt_slug,
                base: item.base.clone(),
                dir: None,
            },
        )?;
        if code != 0 {
            return Err(io::Error::other(format!(
                "refusing to start worker {}: worktree provisioning exited with code {code}",
                item.key
            )));
        }
        path
    } else {
        let reason = format!(
            "refusing to start worker {} without its own provisioned worktree (cwd '{}' is outside fleet root '{}')",
            item.key,
            cwd.display(),
            store.home().display()
        );
        let now = now_secs();
        let root_for_hold = root_key.clone();
        let reason_for_store = reason.clone();
        store.mutate(
            Some(("dispatch", "start_refused", &item.key, &reason)),
            move |st| {
                st.holds = with_hold(&st.holds, &root_for_hold, &reason_for_store, now);
            },
        )?;
        return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
    };

    if let Err(reason) = tacho_daemon::exec::observe::verify_worker_worktree(&wt_path) {
        let now = now_secs();
        let root_for_hold = root_key.clone();
        let reason_for_store = reason.clone();
        store.mutate(
            Some(("dispatch", "start_refused", &item.key, &reason)),
            move |st| {
                st.holds = with_hold(&st.holds, &root_for_hold, &reason_for_store, now);
            },
        )?;
        return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
    }

    let log_file = File::create(&log_path)?;
    let log_err = log_file.try_clone()?;

    let prompt = format!(
        "Task key: {}\nArea: {}\nBase: {}\n\n{}\n\nWhen finished, run `tacho prove` and `tacho pr`, and end your final message with `PR: #<n>` (or `PR: none`).",
        item.key,
        if item.area.is_empty() {
            "unscoped"
        } else {
            &item.area
        },
        item.base.as_deref().unwrap_or("main"),
        item.brief
    );

    let prior_session =
        tacho_core::decide::dispatch::latest(&state.agents, &state.retired, &root_key)
            .and_then(|a| a.session_id.as_deref());
    let wt_str = wt_path.to_string_lossy();
    let (prog, prog_args, prog_env) = build_session_argv(
        &choice.provider,
        choice.model.as_deref(),
        choice.effort.as_deref(),
        prior_session,
        &prompt,
        &wt_str,
        &item.key,
        &state.providers,
    );

    let mut command = if let Ok(custom) = std::env::var("TACHO_AGENT_CMD") {
        let mut c = Command::new("bash");
        c.args(["-c", &custom]);
        c
    } else {
        let mut c = Command::new(&prog);
        c.args(&prog_args);
        for (ek, ev) in &prog_env {
            c.env(ek, ev);
        }
        c
    };
    use_private_tmp(&mut command, &item.key)?;
    command
        .current_dir(&wt_path)
        .env("TACHO_AGENT", &item.key)
        .env("TACHO_TASK", &item.key)
        .env("TACHO_MANAGED_WORKER", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .process_group(0);

    let mut child = command.spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(prompt.as_bytes())?;
        stdin.write_all(b"\n")?;
    }
    let pid = child.id();
    let now = now_secs();
    let base_pr = item.base.as_deref().and_then(|s| s.parse::<u64>().ok());
    let starttime = proc_pid_starttime(pid);
    let cgroup = proc_pid_cgroup(pid);

    let run = AgentRun {
        key: item.key.clone(),
        pid: Some(pid),
        area: item.area.clone(),
        brief: item.brief.clone(),
        worktree: wt_path.to_string_lossy().to_string(),
        log: log_path.to_string_lossy().to_string(),
        status_note: "started".to_string(),
        started_at: now,
        finished_at: None,
        exit_code: None,
        pr: None,
        pr_none: false,
        pr_source: None,
        base_pr,
        resume_at: None,
        provider: choice.provider.clone(),
        model: choice.model.clone(),
        effort: choice.effort.clone(),
        session_id: None,
        compare: item.compare.clone(),
        starttime,
        cgroup,
    };

    let because = format!(
        "dispatched {} on provider={} model={:?} (pid={})",
        item.key, choice.provider, choice.model, pid
    );
    store.mutate(Some(("dispatch", "start", &item.key, &because)), |st| {
        st.agents.insert(item.key.clone(), run);
        st.todo.retain(|t| t.key != item.key);
    })?;
    store.emit_state_band(
        &format!("task:{}/state", root_key),
        "InProgress",
        TaskStatus::InProgress,
        &because,
        now_ns(),
    )?;
    // API ticks already occupy a socket worker; tracing back through that socket can deadlock.
    if send_trace {
        emit_decision_event(&format!("agent:{}", item.key), "dispatch:start", &because);
    }
    Ok(pid)
}

/// Options for `tacho session`, including fleet-wide provider selection and `--clear`.
#[derive(Debug, Clone, Default)]
pub struct SessionOptions {
    pub key: Option<String>,
    pub brief: Option<String>,
    pub resume: bool,
    pub clear: bool,
    pub area: Option<String>,
    pub base: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
}

/// Executes `tacho session` with [`SessionOptions`] (supporting fleet-wide default setting, `--clear`, and session launch).
pub fn execute_session_with(opts: SessionOptions) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;

    if opts.clear {
        store.mutate(
            Some((
                "dispatch",
                "session_clear",
                "default",
                "cleared session provider/model defaults",
            )),
            |st| {
                st.default_provider = "claude".to_string();
                st.default_model = None;
                st.default_effort = None;
            },
        )?;
        println!("Cleared session defaults (provider=claude)");
        return Ok(());
    }

    let Some(key) = opts.key else {
        let inferred_prov = opts.provider.clone().or_else(|| {
            opts.model
                .as_deref()
                .and_then(infer_provider_from_model)
                .map(String::from)
        });
        if inferred_prov.is_some() || opts.model.is_some() || opts.effort.is_some() {
            let choice = resolve_with_providers(
                inferred_prov.as_deref(),
                opts.model.as_deref(),
                opts.effort.as_deref(),
                &state.default_provider,
                state.default_model.as_deref(),
                state.default_effort.as_deref(),
                &state.providers,
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let because = format!(
                "set default session provider={} model={:?} effort={:?}",
                choice.provider, choice.model, choice.effort
            );
            store.mutate(
                Some(("dispatch", "session_default", &choice.provider, &because)),
                |st| {
                    st.default_provider = choice.provider.clone();
                    st.default_model = choice.model.clone();
                    st.default_effort = choice.effort.clone();
                },
            )?;
            println!(
                "Default session: provider={} model={} effort={}",
                choice.provider,
                choice.model.as_deref().unwrap_or("-"),
                choice.effort.as_deref().unwrap_or("-")
            );
        } else {
            println!(
                "Default session: provider={} model={} effort={}",
                state.default_provider,
                state.default_model.as_deref().unwrap_or("-"),
                state.default_effort.as_deref().unwrap_or("-")
            );
        }
        return Ok(());
    };

    execute_session(SessionArgs {
        key,
        brief: opts.brief,
        resume: opts.resume,
        area: opts.area,
        base: opts.base,
        provider: opts.provider,
        model: opts.model,
        effort: opts.effort,
    })
}

/// Executes `tacho session <key> ["<brief>" | --resume]`.
pub fn execute_session(args: SessionArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;

    if args.resume {
        let root = base_key(&args.key);
        let Some(last) = state
            .agents
            .get(&args.key)
            .or_else(|| tacho_core::decide::dispatch::latest(&state.agents, &state.retired, root))
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no previous run found for '{}'", args.key),
            ));
        };
        let item = resume_task(last, "", "", now_secs());
        let pid = spawn_worker_for_item(&mut store, &item)?;
        println!("Resumed {} as {} (pid {pid})", args.key, item.key);
        return Ok(());
    }

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let brief = if let Some(ref b) = args.brief {
        detect_brief_source(&cwd, Some(b), None)?.text
    } else {
        format!("Interactive session for {}", args.key)
    };
    let effective_provider = args.provider.or_else(|| {
        args.model
            .as_deref()
            .and_then(infer_provider_from_model)
            .map(String::from)
    });
    let item = TodoItem {
        key: args.key.clone(),
        brief,
        area: args.area.unwrap_or_default(),
        base: args.base,
        after: Vec::new(),
        priority: 1,
        created_at: now_secs(),
        provider: effective_provider,
        model: args.model,
        effort: args.effort,
        compare: None,
    };
    let pid = spawn_worker_for_item(&mut store, &item)?;
    println!("Started session {} (pid {pid})", args.key);
    Ok(())
}

/// Executes `tacho attach <key> <pr|none>`.
pub fn execute_attach(args: AttachArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    if !state.agents.contains_key(&args.key) {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("unknown agent run '{}'", args.key),
        ));
    }
    let target_str = args
        .pr
        .trim()
        .trim_start_matches('#')
        .trim_start_matches("pr:");
    let (pr_num, is_none) = if target_str.eq_ignore_ascii_case("none") {
        (None, true)
    } else {
        let n: u64 = target_str.parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("expected PR number or 'none', got '{}'", args.pr),
            )
        })?;
        (Some(n), false)
    };

    let because = if is_none {
        format!("attached PR: none to {}", args.key)
    } else {
        format!("attached PR #{} to {}", pr_num.unwrap(), args.key)
    };

    store.mutate(Some(("dispatch", "attach", &args.key, &because)), |st| {
        if let Some(a) = st.agents.get_mut(&args.key) {
            a.pr = pr_num;
            a.pr_none = is_none;
            a.pr_source = Some("attached".to_string());
            if a.finished_at.is_none() {
                a.finished_at = Some(now_secs());
            }
            if a.exit_code.is_none() {
                a.exit_code = Some(0);
            }
        }
    })?;
    let (label, status) = if is_none {
        ("Done", TaskStatus::Done)
    } else {
        ("Review", TaskStatus::Review)
    };
    store.emit_state_band(
        &format!("task:{}/state", base_key(&args.key)),
        label,
        status,
        &because,
        now_ns(),
    )?;
    emit_decision_event(&format!("agent:{}", args.key), "dispatch:attach", &because);
    println!("{because}");
    Ok(())
}

/// Executes `tacho stop <key>`.
pub fn execute_stop(args: StopArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    let state = store.load()?;
    let Some(agent) = state.agents.get(&args.key) else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("unknown agent '{}'", args.key),
        ));
    };
    let builtins = builtin_providers();
    let provider_spec = state
        .providers
        .get(&agent.provider)
        .or_else(|| builtins.get(&agent.provider))
        .cloned()
        .unwrap_or_else(ProviderSpec::claude);
    let stop_plan = provider_spec.stop(
        &agent.key,
        &agent.worktree,
        agent.session_id.as_deref(),
        agent.pid,
    );
    if let Some((prog, stop_args)) = stop_plan.custom_cmd {
        let status = Command::new(&prog).args(&stop_args).status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "stop command '{prog}' exited with {status}"
            )));
        }
    }
    if let Some(pid) = stop_plan.pid {
        if verify_agent_pid(pid, &args.key, agent.cgroup.as_deref(), agent.starttime) {
            send_signal(-(pid as i32), libc::SIGTERM)?;
            send_signal(pid as i32, libc::SIGTERM)?;
        }
    }
    let now = now_secs();
    store.mutate(
        Some(("dispatch", "stop", &args.key, "stopped by user")),
        |st| {
            if let Some(a) = st.agents.get_mut(&args.key) {
                a.finished_at = Some(now);
                a.exit_code = Some(143);
                a.status_note = "stopped".to_string();
            }
            st.holds = with_hold(&st.holds, base_key(&args.key), "stopped by user", now);
        },
    )?;
    emit_decision_event(
        &format!("agent:{}", args.key),
        "dispatch:stop",
        "stopped by user",
    );
    println!(
        "Stopped {} and placed hold on {}",
        args.key,
        base_key(&args.key)
    );
    Ok(())
}

/// Error kind used when a resume is refused because the old process may still own the worktree.
pub(crate) const RESUME_REFUSED: io::ErrorKind = io::ErrorKind::ResourceBusy;

/// Makes sure no process still owns the worktree before a session for `key` is resumed.
///
/// Succeeds when the recorded process is verified gone, or verified and stopped (it is then
/// killed and reaped). Refuses with [`RESUME_REFUSED`] when it is running or its identity
/// cannot be verified, because resuming would put two agents on one worktree.
pub(crate) fn ensure_resumable(
    key: &str,
    pid: Option<u32>,
    cgroup: Option<&str>,
    starttime: Option<u64>,
) -> io::Result<()> {
    match classify_agent_process(pid, key, cgroup, starttime) {
        AgentProcess::Gone => Ok(()),
        AgentProcess::Stopped(stopped_pid) => kill_stopped_and_wait(stopped_pid),
        AgentProcess::Running => Err(io::Error::new(
            RESUME_REFUSED,
            format!("refusing to resume {key}: its process is still running"),
        )),
        AgentProcess::Unverified => Err(io::Error::new(
            RESUME_REFUSED,
            format!(
                "refusing to resume {key}: a process holds its pid but has no recorded identity"
            ),
        )),
    }
}

/// Executes a single `tell` action decided by `tacho_core::decide::decide`.
pub fn deliver_tell_action(
    store: &mut FleetStore,
    target_key: &str,
    message: &str,
) -> io::Result<()> {
    let state = store.load()?;
    let root = base_key(target_key);
    let Some(last) = state
        .agents
        .get(target_key)
        .or_else(|| tacho_core::decide::dispatch::latest(&state.agents, &state.retired, root))
        .cloned()
    else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("unknown agent '{target_key}'"),
        ));
    };

    let live_keys = live_agent_keys(&state);
    let is_live = live_keys.contains(&last.key);
    let now = now_secs();
    let is_idle = if is_live {
        state
            .samples
            .get(&last.key)
            .map(|s| {
                if s.len() >= 2 {
                    let (v, _) = verdict(s, now);
                    matches!(v, Verdict::Stalled | Verdict::Hung)
                        || s[s.len() - 1].cpu_ticks == s[s.len() - 2].cpu_ticks
                } else {
                    false
                }
            })
            .unwrap_or(false)
    } else {
        true
    };
    let builtins = builtin_providers();
    let provider_spec = state
        .providers
        .get(&last.provider)
        .or_else(|| builtins.get(&last.provider))
        .cloned()
        .unwrap_or_else(ProviderSpec::claude);
    let tell_plan = provider_spec.tell_session(
        &last.key,
        &last.worktree,
        last.session_id.as_deref(),
        message,
        is_live,
        is_idle,
        last.model.as_deref(),
        last.effort.as_deref(),
    );

    match tell_plan {
        TellPlan::LiveCommand {
            program,
            args: cmd_args,
        } => {
            let status = Command::new(&program).args(&cmd_args).status()?;
            if !status.success() {
                return Err(io::Error::other(format!(
                    "live tell command '{program}' exited with {status}"
                )));
            }
            let because = format!("live-command tell {}: {}", last.key, message);
            store.mutate(Some(("watch", "tell", &last.key, &because)), |st| {
                st.nudged.insert(last.key.clone());
                if let Some(a) = st.agents.get_mut(&last.key) {
                    a.status_note = format!("nudged (live): {message}");
                }
            })?;
            emit_decision_event(&format!("agent:{}", last.key), "watch:tell", &because);
            println!("Delivered live message to running session {}", last.key);
        }
        TellPlan::LiveInbox {
            key, message: msg, ..
        } => {
            let home = crate::paths::resolve_home();
            let inbox_dir = home.join("inbox");
            fs::create_dir_all(&inbox_dir)?;
            let inbox_file = inbox_dir.join(format!("{key}.txt"));
            let mut existing = fs::read_to_string(&inbox_file).unwrap_or_default();
            if !existing.is_empty() && !existing.ends_with('\n') {
                existing.push('\n');
            }
            existing.push_str(&msg);
            existing.push('\n');
            write_atomic(&inbox_file, existing.as_bytes())?;

            let because = format!("queued live inbox message for running session {key}: {msg}");
            store.mutate(Some(("watch", "tell", &key, &because)), |st| {
                st.nudged.insert(key.clone());
                st.inbox.entry(key.clone()).or_default().push(msg.clone());
                if let Some(a) = st.agents.get_mut(&key) {
                    a.status_note = format!("nudged (live inbox): {message}");
                }
            })?;
            emit_decision_event(&format!("agent:{key}"), "watch:tell", &because);
            println!(
                "Delivered live message to running worker {} (injected on next hook turn without restart)",
                key
            );
        }
        TellPlan::ResumeWithMessage { key, command, .. } => {
            // Never restart a running session and never put two agents on one worktree: the old
            // process must be verified gone, or verified and stopped (then killed and reaped).
            ensure_resumable(&key, last.pid, last.cgroup.as_deref(), last.starttime)?;
            if last.worktree.is_empty() || !Path::new(&last.worktree).is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Cannot resume worker {key} with message: worktree '{}' does not exist or is not a directory",
                        last.worktree
                    ),
                ));
            }
            if let Err(reason) =
                tacho_daemon::exec::observe::verify_worker_worktree(Path::new(&last.worktree))
            {
                let now = now_secs();
                let root_for_hold = base_key(&key).to_string();
                let reason_for_store = reason.clone();
                store.mutate(Some(("watch", "tell_refused", &key, &reason)), move |st| {
                    st.holds = with_hold(&st.holds, &root_for_hold, &reason_for_store, now);
                })?;
                return Err(io::Error::new(RESUME_REFUSED, reason));
            }
            let log_file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&last.log)
                .map_err(|e| {
                    io::Error::other(format!(
                        "Cannot resume worker {key}: failed to open log '{}': {e}",
                        last.log
                    ))
                })?;
            let log_err = log_file.try_clone().map_err(|e| {
                io::Error::other(format!(
                    "Cannot resume worker {key}: failed to clone log fd: {e}"
                ))
            })?;
            let mut cmd = Command::new(&command.program);
            use_private_tmp(&mut cmd, &key)?;
            cmd.args(&command.args)
                .current_dir(&last.worktree)
                .env("TACHO_AGENT", &key)
                .env("TACHO_TASK", &key)
                .env("TACHO_MANAGED_WORKER", "1")
                .env("PYTHONDONTWRITEBYTECODE", "1")
                .stdin(Stdio::null())
                .stdout(Stdio::from(log_file))
                .stderr(Stdio::from(log_err))
                .process_group(0);
            for (ek, ev) in &command.env {
                cmd.env(ek, ev);
            }
            let child = cmd.spawn().map_err(|e| {
                io::Error::other(format!(
                    "Cannot resume worker {key}: failed to spawn command '{}': {e}",
                    command.program
                ))
            })?;
            let resumed_pid = child.id();
            let resumed_starttime = proc_pid_starttime(resumed_pid);
            let resumed_cgroup = proc_pid_cgroup(resumed_pid);
            let because = format!("resumed session {key} with message: {message}");
            store.mutate(Some(("watch", "tell", &key, &because)), |st| {
                st.nudged.insert(key.clone());
                if let Some(a) = st.agents.get_mut(&key) {
                    a.pid = Some(resumed_pid);
                    a.starttime = resumed_starttime;
                    a.cgroup = resumed_cgroup;
                    a.finished_at = None;
                    a.exit_code = None;
                    a.status_note = format!("nudged: {message}");
                    a.brief = format!("{}\n\nGuidance from operator:\n{message}", a.brief);
                }
            })?;
            emit_decision_event(&format!("agent:{key}"), "watch:tell", &because);
            println!(
                "Sent message to {} (resumed session with message, PID {})",
                key, resumed_pid
            );
        }
    }
    Ok(())
}

/// Runs `decide`, records the decision tick in `decision_ticks`, and executes `tell` actions from the resulting `DecisionPlan`.
pub fn execute_tell_with(store: &mut FleetStore, args: TellArgs) -> io::Result<()> {
    let state = store.load()?;
    let root = base_key(&args.key);
    let Some(last) = state
        .agents
        .get(&args.key)
        .or_else(|| tacho_core::decide::dispatch::latest(&state.agents, &state.retired, root))
        .cloned()
    else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("unknown agent '{}'", args.key),
        ));
    };

    let now = now_secs();
    let mut inbox = state.inbox.clone();
    inbox.insert(last.key.clone(), vec![args.message.clone()]);
    let mut live_keys = live_agent_keys(&state);
    live_keys.insert(last.key.clone());

    let decision_state = DecisionState {
        max_parallel: state.max_workers,
        todo: state.todo.clone(),
        agents: state.agents.clone(),
        retired: state.retired.clone(),
        holds: state.holds.clone(),
        samples: state.samples.clone(),
        inbox,
        ..Default::default()
    };
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let resolved_main_sha = tacho_daemon::fleet::known_main_sha(
        store.home(),
        &state,
        &tacho_core::LiveState::default(),
    )
    .unwrap_or_else(|| super::store::resolve_live_main_sha(&cwd));
    let world_obs = WorldObservation {
        now,
        main_sha: resolved_main_sha.clone(),
        live_keys,
        ..Default::default()
    };
    let plan = decide(&decision_state, &world_obs);
    if crate::kit::start::is_full_hex_sha(&resolved_main_sha) {
        store
            .record_decision_tick(now, &decision_state, &world_obs, &plan)
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    for (agent_key, msg) in &plan.tell_actions {
        if agent_key == &last.key {
            deliver_tell_action(store, agent_key, msg)?;
        }
    }
    Ok(())
}

/// Executes `tacho tell <key> "<message>"`.
pub fn execute_tell(args: TellArgs) -> io::Result<()> {
    let mut store = open_fleet_store()?;
    execute_tell_with(&mut store, args)
}
