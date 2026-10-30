//! Command-line interface definitions for `tacho`.
//!
//! Help text is kept short, concrete, and plain-worded, with one example per command.

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "tacho",
    about = "Flight recorder for coding agents.",
    long_about = "tacho records agent turns, tool calls, and system stats into a persistent SQLite WAL log.",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the background flight recorder daemon.
    ///
    /// Example: tacho daemon
    Daemon(DaemonArgs),

    /// Emit an event or span to the flight recorder.
    ///
    /// Example: tacho emit state worker-1 Thinking --because "analyzing code"
    Emit(EmitArgs),

    /// Process hook payload from stdin and send to daemon. Always exits 0.
    ///
    /// Example: tacho hook claude < payload.json
    Hook(HookArgs),

    /// Run a command, teeing output to terminal and flight recorder.
    ///
    /// Example: tacho run -- cargo test
    Run(RunArgs),

    /// Write a trace snapshot to a Perfetto file.
    ///
    /// Example: tacho trace -o trace.pftrace
    Trace(TraceArgs),

    /// Show live status of all recorded agents and fleet sections.
    ///
    /// Example: tacho status --section needs
    Status(StatusArgs),

    /// Show items needing operator attention (`status --section needs`).
    ///
    /// Example: tacho needs
    Needs(StatusArgs),

    /// Show running commit, main commit, commits behind, and last self-update move.
    ///
    /// Example: tacho version
    Version(VersionArgs),

    /// Initialize or update the fleet environment file and systemd unit links.
    ///
    /// Example: tacho setup --dry-run
    Setup(SetupArgs),

    /// Start or tear down an isolated per-worktree database.
    ///
    /// Example: tacho db fix-auth --rm
    Db(DbArgs),

    /// Print a short timeline report for an agent.
    ///
    /// Example: tacho report worker-1 --since 30m
    Report(ReportArgs),

    /// Watch live state transitions and detector firings.
    ///
    /// Example: tacho watch --until stalled
    Watch(WatchArgs),

    /// Run a PerfettoSQL query against a fresh snapshot.
    ///
    /// Example: tacho query agents-now
    Query(QueryArgs),

    /// Install tachograph hooks into an agent's configuration.
    ///
    /// Example: tacho install claude --user
    Install(InstallArgs),

    /// Remove tachograph hooks from an agent's configuration.
    ///
    /// Example: tacho uninstall claude --user
    Uninstall(UninstallArgs),

    /// Manage background Perfetto system tracing sessions.
    ///
    /// Example: tacho system start --profile default
    System(SystemArgs),

    /// Tail on-disk agent JSONL transcripts into the flight recorder.
    ///
    /// Example: tacho tail ~/.claude/projects
    Tail(TailArgs),

    /// Create an isolated worker worktree at a pinned base SHA with an allocated port block.
    ///
    /// Example: tacho start fix-auth-race --base main
    Start(StartArgs),

    /// Run scoped proof checks for the current worktree and record .tachoproof/summary.json.
    ///
    /// Example: tacho prove
    Prove(ProveArgs),

    /// Start a detached background job in the current worktree (.tachorun/<name>).
    ///
    /// Example: tacho job dev-server -- cargo run
    Job(JobArgs),

    /// Wait up to 9 minutes for a named worktree job, streaming output (exits 3 if still running).
    ///
    /// Example: tacho wait dev-server
    Wait(WaitArgs),

    /// Stop a named worktree job and its entire process group.
    ///
    /// Example: tacho kill dev-server --now
    Kill(KillArgs),

    /// Validate proof and preflight guardrails, push branch, and open/update a draft PR.
    ///
    /// Example: tacho pr "Fix auth race" -m "Eliminates lock ordering inversion."
    Pr(PrArgs),

    /// Record a structured phase note (read, write, test, fix, push) for an agent.
    ///
    /// Example: tacho note fix-auth-race "test: reproducing race under starve"
    Note(NoteArgs),

    /// Run a command on a CPU-starved core slice to reproduce timing/concurrency flakes.
    ///
    /// Example: tacho starve -- cargo test flaky_wal
    Starve(StarveArgs),

    /// View or enqueue tasks on the fleet work queue.
    ///
    /// Example: tacho todo fix-auth "Fix login race" --area auth
    Todo(TodoArgs),

    /// Start or resume a managed worker agent session, or configure the fleet session default.
    ///
    /// Example: tacho session fix-auth --resume
    Session(SessionCliArgs),

    /// Place or list holds on tasks, PRs (pr:<n>), or areas (area:<name>).
    ///
    /// Example: tacho hold pr:42 --why "awaiting schema migration"
    Hold(HoldArgs),

    /// Release an active hold on a task, PR, or area.
    ///
    /// Example: tacho release pr:42
    Release(ReleaseArgs),

    /// Retire a task or agent lineage from the fleet.
    ///
    /// Example: tacho retire fix-auth --why "superseded by pr:99"
    Retire(RetireArgs),

    /// Confirm or attach a PR number (or 'none') to a finished agent run.
    ///
    /// Example: tacho attach fix-auth 42
    Attach(AttachArgs),

    /// Stop a running worker agent and place a hold on its task key.
    ///
    /// Example: tacho stop fix-auth
    Stop(StopArgs),

    /// Send operator guidance to a worker agent and schedule immediate resume.
    ///
    /// Example: tacho tell fix-auth "Check the WAL checkpoint lock"
    Tell(TellArgs),

    /// Preview what the next orchestrator tick will dispatch and verify.
    ///
    /// Example: tacho next
    Next(NextArgs),

    /// Show live agent progress verdicts (MOVING, STALLED, HUNG) and area chain throughput.
    ///
    /// Example: tacho progress
    Progress(ProgressArgs),

    /// List recorded test flakes and auto-queued flake fix tasks.
    ///
    /// Example: tacho flakes
    Flakes(FlakesArgs),

    /// Run a speculative verification batch across open PRs with automatic binary bisection.
    ///
    /// Example: tacho batch 41 42 --areas core --foreground
    Batch(BatchCliArgs),

    /// Replay a recorded verification batch or decision tick.
    ///
    /// Example: tacho replay b001
    Replay(ReplayArgs),

    /// Replay a recorded verification batch or pure decision tick (alias for `replay`).
    ///
    /// Example: tacho decide-replay latest
    DecideReplay(ReplayArgs),

    /// View or open a verified merge train from a passed batch or PR list.
    ///
    /// Example: tacho train 41 42 --max 4
    Train(TrainArgs),

    /// Assemble or approve a combined release-train pull request.
    ///
    /// Example: tacho release-train 99 --approve
    ReleaseTrain(ReleaseTrainArgs),

    /// Land verified PRs from the ready queue or open merge train.
    ///
    /// Example: tacho land t001
    Land(LandArgs),

    /// Show the ordered ready-to-land PR queue.
    ///
    /// Example: tacho queue
    Queue(QueueArgs),

    /// Clean up stale worktrees, batch checkouts, containers, and zombie processes.
    ///
    /// Example: tacho tidy --dry-run
    Tidy(TidyArgs),

    /// Execute one orchestrator & supervisor tick (supervise, resume, verify, dispatch).
    ///
    /// Example: tacho tick
    Tick(TickArgs),

    /// Query the persistent decision log (every decision and its 'because' reason).
    ///
    /// Example: tacho events --limit 25
    Events(EventsArgs),

    /// Show the compact fleet dashboard and 'NEEDS YOU' attention items.
    ///
    /// Example: tacho top
    Top(TopArgs),

    /// Serve read-only fleet status over local HTTP JSON.
    ///
    /// Example: tacho serve --addr 127.0.0.1:7420
    Serve(ServeArgs),

    /// Check whether the orchestrator tick loop is healthy while work is pending.
    ///
    /// Example: tacho watchdog
    Watchdog(WatchdogArgs),

    /// Accept or classify redrawn screenshots from a batch run (`tacho shots accept <run> [--all]`).
    ///
    /// Example: tacho shots accept batch-123
    Shots(ShotsArgs),
}

#[derive(Args, Debug)]
pub struct DaemonArgs {
    /// Observe and print decision plans without executing fleet actions.
    #[arg(long)]
    pub dry_run: bool,

    /// Optional OTLP HTTP listen address.
    ///
    /// Example: --otlp 127.0.0.1:4318
    #[arg(long)]
    pub otlp: Option<String>,

    /// Optional shell command to run when a stall detector fires.
    ///
    /// Example: --on-stall "notify-send \"Stall: $TACHO_AGENT ($TACHO_STALL_KIND)\""
    #[arg(long)]
    pub on_stall: Option<String>,

    /// Run daemon in foreground instead of background.
    #[arg(long)]
    pub foreground: bool,
}

#[derive(Args, Debug)]
pub struct EmitArgs {
    /// Read NDJSON inputs line-by-line from stdin.
    ///
    /// Example: tacho emit --json < events.jsonl
    #[arg(long)]
    pub json: bool,

    #[command(subcommand)]
    pub subcommand: Option<EmitSubcommands>,
}

#[derive(Subcommand, Debug)]
pub enum EmitSubcommands {
    /// Declare a task and its intended file paths.
    ///
    /// Example: tacho emit task auth-flow --paths src/auth.rs --brief-file brief.md
    Task {
        /// Task slug or name.
        slug: String,
        /// Comma-separated list of expected paths.
        #[arg(long)]
        paths: Option<String>,
        /// Path to task brief file.
        #[arg(long)]
        brief_file: Option<PathBuf>,
        /// Assigned agent identifier.
        #[arg(long)]
        agent: Option<String>,
        /// Base commit SHA.
        #[arg(long)]
        base_commit: Option<String>,
        /// Token budget for this task.
        #[arg(long)]
        token_budget: Option<u64>,
        /// Cost budget in micros for this task.
        #[arg(long)]
        cost_budget: Option<u64>,
    },

    /// Update an agent or task state.
    ///
    /// Example: tacho emit state worker-1 Thinking --because "analyzing code"
    State {
        /// Target agent or task slug.
        target: String,
        /// State name (e.g. Idle, Thinking, Tool, Waiting, Stalled).
        state: String,
        /// Human-readable explanation.
        #[arg(long)]
        because: Option<String>,
    },

    /// Record the beginning or end of a custom span.
    ///
    /// Example: tacho emit span begin agent:1 compile --id s1 --arg opt=2
    Span {
        #[command(subcommand)]
        action: SpanAction,
    },

    /// Record a single point-in-time instant event.
    ///
    /// Example: tacho emit instant agent:1 checkpoint --arg step=5
    Instant {
        /// Track key (e.g. agent:1).
        track: String,
        /// Name of the instant event.
        name: String,
        /// Custom key=value attributes.
        #[arg(long = "arg")]
        args: Vec<String>,
    },

    /// Record a counter value on a track.
    ///
    /// Example: tacho emit counter agent:1/tokens 42000 --unit tokens
    Counter {
        /// Track key (e.g. agent:1/tokens).
        track: String,
        /// Numeric counter value.
        value: i64,
        /// Optional measurement unit.
        #[arg(long)]
        unit: Option<String>,
    },

    /// Record a message sent between agents.
    ///
    /// Example: tacho emit message --from worker-1 --to coordinator --bytes 128
    Message {
        /// Sending agent name.
        #[arg(long)]
        from: String,
        /// Receiving agent name.
        #[arg(long)]
        to: String,
        /// Message size in bytes.
        #[arg(long)]
        bytes: Option<u64>,
        /// Optional message ID.
        #[arg(long)]
        id: Option<String>,
    },

    /// Record a review finding for a task.
    ///
    /// Example: tacho emit finding task-1 --severity major --path src/main.rs --line 42 --title "Bug"
    Finding {
        /// Target task slug.
        task: String,
        /// Severity: info, minor, major, critical.
        #[arg(long)]
        severity: String,
        /// File path where finding occurred.
        #[arg(long)]
        path: Option<String>,
        /// Line number in file.
        #[arg(long)]
        line: Option<u32>,
        /// Short title of finding.
        #[arg(long)]
        title: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SpanAction {
    /// Begin a span.
    ///
    /// Example: tacho emit span begin agent:1 build --id b1
    Begin {
        /// Track key (e.g. agent:1).
        track: String,
        /// Name of span.
        name: String,
        /// Unique span ID.
        #[arg(long)]
        id: Option<String>,
        /// Custom key=value attributes.
        #[arg(long = "arg")]
        args: Vec<String>,
    },

    /// End a span.
    ///
    /// Example: tacho emit span end agent:1 --id b1
    End {
        /// Track key (e.g. agent:1).
        track: String,
        /// Unique span ID matching begin.
        #[arg(long)]
        id: Option<String>,
        /// Custom key=value attributes.
        #[arg(long = "arg")]
        args: Vec<String>,
    },
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    Claude,
    Gemini,
    Jetski,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookTarget {
    Claude,
    Gemini,
    Jetski,
    Guard,
}

impl HookTarget {
    pub fn as_provider(self) -> Option<Provider> {
        match self {
            HookTarget::Claude => Some(Provider::Claude),
            HookTarget::Gemini => Some(Provider::Gemini),
            HookTarget::Jetski => Some(Provider::Jetski),
            HookTarget::Guard => None,
        }
    }
}

#[derive(Args, Debug)]
pub struct HookArgs {
    /// Agent provider (`claude`, `gemini`, `jetski`) or `guard` for PreToolUse Bash guardrails.
    ///
    /// Example: claude
    pub provider: HookTarget,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Command and arguments to execute.
    ///
    /// Example: tacho run -- cargo test
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

#[derive(Args, Debug)]
pub struct TraceArgs {
    /// Output file path. Defaults to $TACHO_HOME/trace/<timestamp>.perfetto-trace.
    ///
    /// Example: -o snapshot.pftrace
    #[arg(short, long)]
    pub out: Option<PathBuf>,

    /// Only include events within the specified duration window (e.g. 30m, 2h).
    ///
    /// Example: --since 1h
    #[arg(long)]
    pub since: Option<String>,

    /// Merge unprivileged Perfetto system trace alongside agent trace.
    ///
    /// Example: --system
    #[arg(long)]
    pub system: bool,

    /// Stream binary trace packets directly to standard output.
    ///
    /// Example: --stdout
    #[arg(long)]
    pub stdout: bool,

    /// Rebuild trace from SQLite WAL event log.
    ///
    /// Example: --from-log 30m
    #[arg(long)]
    pub from_log: Option<Option<String>>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct StatusArgs {
    /// Output status as structured JSON.
    ///
    /// Example: --json
    #[arg(long)]
    pub json: bool,

    /// Filter output to one or more named sections (headline, needs, attention, pipeline, agents, machine, cost, held, stale, progress, flakes).
    ///
    /// Example: --section needs --section machine
    #[arg(long = "section")]
    pub section: Vec<String>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct VersionArgs {
    /// Output version details as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct SetupArgs {
    /// Print planned setup actions without modifying files or symlinks.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct DbArgs {
    /// Worktree or database slug name.
    pub name: String,
    /// Optional checkout path (defaults to current working directory).
    pub checkout: Option<PathBuf>,
    /// Tear down the per-agent database instead of starting it.
    #[arg(long)]
    pub rm: bool,
}

#[derive(Args, Debug)]
pub struct ReportArgs {
    /// Agent name to report on.
    ///
    /// Example: worker-1
    pub agent: String,

    /// Filter events within the specified duration (e.g. 30m, 2h).
    ///
    /// Example: --since 30m
    #[arg(long)]
    pub since: Option<String>,

    /// Show claim verification summary (edits, test after last edit, commits, pushes, CLAIM flags, git worktree status).
    ///
    /// Example: --diff-claims
    #[arg(long)]
    pub diff_claims: bool,
}

#[derive(Args, Debug)]
pub struct WatchArgs {
    /// Output updates as NDJSON lines.
    ///
    /// Example: --json
    #[arg(long)]
    pub json: bool,

    /// Block until an agent reaches a target state: stalled, idle, or exit.
    ///
    /// Example: --until stalled
    #[arg(long)]
    pub until: Option<String>,
}

#[derive(Args, Debug)]
pub struct QueryArgs {
    /// Shipped query name (e.g. agents-now, open-waits), query file path, or - for stdin.
    ///
    /// Example: agents-now
    pub query: String,

    /// Path to an existing trace file to query instead of live snapshot.
    ///
    /// Example: --trace /path/to/trace.pftrace
    #[arg(long)]
    pub trace: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct InstallArgs {
    /// Agent provider to install hooks for: claude, gemini, or jetski.
    ///
    /// Example: claude
    pub provider: Provider,

    /// Install into project directory (e.g. ./.claude/settings.json). Default.
    #[arg(long, default_value_t = true, conflicts_with = "user")]
    pub project: bool,

    /// Install into user home directory (e.g. ~/.claude/settings.json).
    #[arg(long)]
    pub user: bool,
}

#[derive(Args, Debug)]
pub struct UninstallArgs {
    /// Agent provider to remove hooks for: claude, gemini, or jetski.
    ///
    /// Example: claude
    pub provider: Provider,

    /// Remove from project directory. Default.
    #[arg(long, default_value_t = true, conflicts_with = "user")]
    pub project: bool,

    /// Remove from user home directory.
    #[arg(long)]
    pub user: bool,
}

#[derive(Args, Debug)]
pub struct SystemArgs {
    #[command(subcommand)]
    pub command: SystemSubcommands,
}

#[derive(Subcommand, Debug)]
pub enum SystemSubcommands {
    /// Start a background Perfetto system tracing session.
    ///
    /// Example: tacho system start --profile default
    Start {
        /// Profile to use: default (unprivileged), full (root ftrace), or path to config file.
        #[arg(long, default_value = "default")]
        profile: String,
    },

    /// Stop the background Perfetto system tracing session.
    ///
    /// Example: tacho system stop
    Stop,

    /// Show status of the Perfetto system tracing session.
    ///
    /// Example: tacho system status
    Status,

    /// Print setup commands for traced and traced_probes.
    ///
    /// Example: tacho system setup --sudo
    Setup {
        /// Show root commands for full profile with ftrace.
        #[arg(long)]
        sudo: bool,
    },
}

#[derive(Args, Debug, Default)]
pub struct TailArgs {
    /// Path to a transcript .jsonl file or projects directory. Defaults to ~/.claude/projects.
    ///
    /// Example: ~/.claude/projects
    pub path: Option<PathBuf>,

    /// Scan and ingest new transcript lines once, then exit.
    ///
    /// Example: --once
    #[arg(long)]
    pub once: bool,

    /// Optional agent identifier to attribute transcript sessions to.
    ///
    /// Example: --agent worker-1
    #[arg(long)]
    pub agent: Option<String>,
}

#[derive(Args, Debug)]
pub struct StartArgs {
    /// Worker slug (3..80 chars, lowercase letters/digits/hyphens).
    pub slug: String,
    /// Base commit, branch, or PR number (e.g. main, 42, pr:42).
    #[arg(long)]
    pub base: Option<String>,
    /// Optional parent directory for worktrees.
    #[arg(long)]
    pub dir: Option<PathBuf>,
}

#[derive(Args, Debug, Default)]
pub struct ProveArgs {
    /// Optional worktree directory (defaults to current working directory).
    #[arg(long)]
    pub dir: Option<PathBuf>,
    /// Override base commit SHA.
    #[arg(long)]
    pub base: Option<String>,
    /// Print planned checks without executing them.
    #[arg(long)]
    pub dry_run: bool,
    /// Continue running remaining checks even if one fails.
    #[arg(long)]
    pub keep_going: bool,
    /// Screenshot file paths to record in the proof summary.
    #[arg(long = "screenshot")]
    pub screenshots: Vec<String>,
}

#[derive(Args, Debug)]
pub struct JobArgs {
    /// Job name (used for .tachorun/<name>.{pid,log,exit}).
    pub name: String,
    /// Command and arguments to run in a detached process group.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

#[derive(Args, Debug)]
pub struct WaitArgs {
    /// Job name started with `tacho job`.
    pub name: String,
    /// Maximum seconds to wait in this call (capped at 540s / 9m).
    #[arg(long)]
    pub timeout: Option<u64>,
}

#[derive(Args, Debug)]
pub struct KillArgs {
    /// Job name to stop.
    pub name: String,
    /// Send SIGKILL immediately instead of SIGTERM + grace period.
    #[arg(long)]
    pub now: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct PrArgs {
    /// Pull request title.
    pub title: String,
    /// Pull request description body.
    #[arg(short = 'm', long = "message", default_value = "")]
    pub message: String,
    /// Validate preflight guardrails and print body without pushing or calling gh.
    #[arg(long)]
    pub dry_run: bool,
    /// Allow opening a PR even when visual screenshot diffs exceed pixel tolerance.
    #[arg(long)]
    pub shots_anyway: bool,
}

#[derive(Args, Debug)]
pub struct NoteArgs {
    /// Agent or task key.
    pub key: String,
    /// Note formatted as `<phase>: <summary>` (phases: read, write, test, fix, push).
    pub note: String,
}

#[derive(Args, Debug)]
pub struct StarveArgs {
    /// Number of CPUs in the starvation slice (default: 1/8 of system CPUs).
    #[arg(long)]
    pub slice: Option<usize>,
    /// Explicit CPU set (e.g. "0-1" or "6,7").
    #[arg(long)]
    pub cpu_set: Option<String>,
    /// Maximum timeout in seconds (capped at 300s).
    #[arg(long)]
    pub timeout: Option<u64>,
    /// Print the starvation plan without spawning burners.
    #[arg(long)]
    pub dry_run: bool,
    /// Command to execute under CPU starvation.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct TodoArgs {
    /// Task key (omit to list the current queue).
    pub key: Option<String>,
    /// Task brief text.
    pub brief: Option<String>,
    /// Read task brief from a file.
    #[arg(long)]
    pub brief_file: Option<PathBuf>,
    /// Area chain name (tasks in the same area run one at a time and stack on each other).
    #[arg(long)]
    pub area: Option<String>,
    /// Base branch or PR number.
    #[arg(long)]
    pub base: Option<String>,
    /// Comma-separated prerequisites (task keys or `pr:<n>`).
    #[arg(long)]
    pub after: Option<String>,
    /// Boost task priority to the front of the queue.
    #[arg(long)]
    pub priority: bool,
    /// Boost task priority and dispatch immediately if a worker slot is available.
    #[arg(long)]
    pub now: bool,
    /// Include or list retired task keys.
    #[arg(long)]
    pub retired: bool,
    /// Agent provider (`claude`, `codex`, `gemini`).
    #[arg(long)]
    pub provider: Option<String>,
    /// Model shorthand or ID (`opus`, `sonnet`, `haiku`, `gpt-5.4`, `gemini-pro`, etc.).
    #[arg(long)]
    pub model: Option<String>,
    /// Reasoning effort level (`low`, `medium`, `high`).
    #[arg(long)]
    pub effort: Option<String>,
    /// Comma-separated models/providers for side-by-side comparison (e.g. `opus,codex,gemini`).
    #[arg(long)]
    pub compare: Option<String>,
    /// Output queue as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct SessionArgs {
    /// Task or session key.
    pub key: String,
    /// Optional brief for a new session.
    pub brief: Option<String>,
    /// Resume the latest stopped run in this key's lineage.
    #[arg(long)]
    pub resume: bool,
    /// Area name.
    #[arg(long)]
    pub area: Option<String>,
    /// Base branch or PR.
    #[arg(long)]
    pub base: Option<String>,
    /// Provider (`claude`, `codex`, `gemini`).
    #[arg(long)]
    pub provider: Option<String>,
    /// Model shorthand or ID.
    #[arg(long)]
    pub model: Option<String>,
    /// Reasoning effort level.
    #[arg(long)]
    pub effort: Option<String>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct SessionCliArgs {
    /// Optional task or session key (omit when setting or viewing fleet-wide default provider/model).
    pub key: Option<String>,
    /// Optional brief for a new session.
    pub brief: Option<String>,
    /// Resume the latest stopped run in this key's lineage.
    #[arg(long)]
    pub resume: bool,
    /// Clear fleet-wide session provider/model override back to default (`claude`).
    #[arg(long)]
    pub clear: bool,
    /// Area name.
    #[arg(long)]
    pub area: Option<String>,
    /// Base branch or PR.
    #[arg(long)]
    pub base: Option<String>,
    /// Provider (`claude`, `codex`, `gemini`).
    #[arg(long)]
    pub provider: Option<String>,
    /// Model shorthand or ID.
    #[arg(long)]
    pub model: Option<String>,
    /// Reasoning effort level.
    #[arg(long)]
    pub effort: Option<String>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct HoldArgs {
    /// Target to hold (`<key>`, `pr:<n>`, or `area:<name>`). Omit to list active holds.
    pub target: Option<String>,
    /// Reason for placing the hold (`--why` or `--reason`).
    #[arg(long = "why", visible_alias = "reason")]
    pub reason: Option<String>,
}

impl HoldArgs {
    pub fn why(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

#[derive(Args, Debug, Default, Clone)]
pub struct ReleaseArgs {
    /// Target to release (`<key>`, `pr:<n>`, or `area:<name>`).
    pub target: String,
}

#[derive(Args, Debug, Default, Clone)]
pub struct RetireArgs {
    /// Task or agent key to retire.
    pub key: String,
    /// Reason for retiring the task or agent lineage (`--why` or `--reason`).
    #[arg(long = "why", visible_alias = "reason")]
    pub reason: Option<String>,
}

impl RetireArgs {
    pub fn why(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

#[derive(Args, Debug, Default, Clone)]
pub struct AttachArgs {
    /// Agent key.
    pub key: String,
    /// PR number (e.g. `42`) or `none`.
    pub pr: String,
}

#[derive(Args, Debug, Default, Clone)]
pub struct StopArgs {
    /// Running agent key to stop.
    pub key: String,
}

#[derive(Args, Debug, Default, Clone)]
pub struct TellArgs {
    /// Agent key to message/nudge.
    pub key: String,
    /// Guidance message for the agent.
    pub message: String,
}

#[derive(Args, Debug, Default, Clone)]
pub struct NextArgs {
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ProgressArgs {
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct FlakesArgs {
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct BatchArgs {
    /// PR numbers to verify in this batch (omit to auto-plan from open PRs).
    pub prs: Vec<u64>,
    /// Custom verification command.
    #[arg(long)]
    pub cmd: Option<String>,
    /// Verification depth: `presubmit` (default) or `postsubmit`.
    #[arg(long)]
    pub mode: Option<String>,
    /// Per-area check timeout in seconds (reports an `area_hang_report` if exceeded).
    #[arg(long)]
    pub timeout: Option<u64>,
    /// Print batch plan without running checks.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct BatchCliArgs {
    #[command(flatten)]
    pub inner: BatchArgs,
    /// Comma-separated area names to restrict batch verification to (e.g. `core,api`).
    #[arg(long)]
    pub areas: Option<String>,
    /// Run batch verification in the foreground and stream output.
    #[arg(long)]
    pub foreground: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ReplayArgs {
    /// Batch ID or decision tick ID to replay (e.g. `b001`, `latest`, `tick:<ts>`).
    pub batch_id: String,
    /// Optional override command.
    #[arg(long)]
    pub cmd: Option<String>,
    /// Dry-run mode.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct TrainArgs {
    /// Optional PR numbers to include in the merge train.
    pub prs: Vec<u64>,
    /// Maximum number of PRs in the train.
    #[arg(long)]
    pub max: Option<usize>,
    /// Open a new verified merge train from a passed batch ID (e.g. `b001`).
    #[arg(long)]
    pub from_batch: Option<String>,
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ReleaseTrainArgs {
    /// Optional release-train PR number.
    pub pr: Option<u64>,
    /// Record review approval for the current release-train head SHA.
    #[arg(long)]
    pub approve: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct LandArgs {
    /// Optional train ID or branch to land.
    pub train: Option<String>,
    /// Optional `--train <id>` flag alias.
    #[arg(long = "train", hide = true)]
    pub train_flag: Option<String>,
    /// Dry-run without mutating state.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct QueueArgs {
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct TidyArgs {
    /// Preview cleanup decisions without removing anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Minimum idle age in seconds before cleaning up a worktree (default: 3600).
    #[arg(long)]
    pub min_age: Option<i64>,
    /// Retire unfinished or abandoned runs older than N days with no open PR.
    #[arg(long)]
    pub stale_runs: Option<i64>,
    /// Prune GitHub Actions caches and artifacts older than N days.
    #[arg(long)]
    pub actions_days: Option<i64>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct TickArgs {
    /// Preview tick decisions without spawning or mutating state.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct EventsArgs {
    /// Maximum number of decisions to display (default: 50).
    #[arg(short = 'n', long = "limit", visible_alias = "last")]
    pub limit: Option<usize>,
    /// Show events since duration (e.g. `15m`, `2h`, `1d`) or unix timestamp.
    #[arg(long)]
    pub since: Option<String>,
    /// Follow new events as they are appended.
    #[arg(short = 'f', long = "follow")]
    pub follow: bool,
    /// Filter by category (`dispatch`, `verify`, `land`, `watch`, `kit`).
    #[arg(long)]
    pub category: Option<String>,
    /// Filter by target key/PR/batch.
    #[arg(long)]
    pub target: Option<String>,
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default)]
pub struct TopArgs {
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Default)]
pub struct ServeArgs {
    /// Bind address (default: 127.0.0.1:7420).
    #[arg(long)]
    pub addr: Option<String>,
    /// Serve a single HTTP request and exit (useful for tests).
    #[arg(long)]
    pub once: bool,
}

#[derive(Args, Debug, Default)]
pub struct WatchdogArgs {
    /// Maximum allowed seconds since last tick when work is active (default: 600).
    #[arg(long)]
    pub max_stale: Option<i64>,
    /// Alert cooldown in seconds (default: 3600).
    #[arg(long)]
    pub cooldown: Option<i64>,
}

#[derive(Args, Debug, Clone)]
pub struct ShotsArgs {
    #[command(subcommand)]
    pub subcommand: ShotsSubcommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ShotsSubcommands {
    /// Copy a batch run's `after` screenshots into the branch and stage them (goldens by default, `--all` for every moved shot).
    Accept(ShotsAcceptArgs),
    /// Classify and commit intended golden screenshot changes from a run or `.tachoshots` directory.
    Fix(ShotsFixArgs),
}

#[derive(Args, Debug, Default, Clone)]
pub struct ShotsAcceptArgs {
    /// Batch or run ID whose `runs/<run>/shots/<area>/after/<repo_path>` artifacts to accept.
    pub run: String,
    /// Copy every image that moved (by default only moved goldens are copied).
    #[arg(long)]
    pub all: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ShotsFixArgs {
    /// Batch run ID, PR number, or shot-report directory to inspect.
    pub target: String,
}

#[cfg(test)]
mod shadow_tests {
    use super::*;

    #[test]
    fn the_daemon_accepts_a_read_only_shadow_mode() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["tacho", "daemon", "--dry-run", "--foreground"])?;
        assert!(matches!(
            cli.command,
            Commands::Daemon(DaemonArgs { dry_run: true, .. })
        ));
        Ok(())
    }
}
