// codexctl — thin shell-native controller for Codex App Server.
//
// Architecture: one persistent `codex app-server --listen ws://127.0.0.1:PORT`
// daemon, spawned lazily on first command, plus a per-invocation thin client
// that connects over websocket, does its work, and disconnects. State lives in
// $CODEX_HOME (auth, rollouts, sqlite); we add a small registry next to it for
// human-friendly thread names.
//
// v1 subcommands:
//   daemon (start | stop | status | logs)
//   start <name> --cwd <path> [--model <m>] [--sandbox s] [--objective "..."]
//   ls
//   say <name> "<msg>"
//   steer <name> "<msg>"        (turn/steer — requires an in-flight turn)
//   interrupt <name>
//   goal <name> [--set "<obj>" | --pause | --resume | --clear | --budget N]
//   status <name>
//   tail <name> [--filter ...]
//   rm <name> [--archive]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use codex_app_server_sdk::CodexClient;
use codex_app_server_sdk::client::{ClientOptions, WsConfig};
use codex_app_server_sdk::events::{ServerEvent, ServerNotification};
use codex_app_server_sdk::protocol::requests::{
    ClientInfo, InitializeCapabilities, InitializeParams, ThreadResumeParams, ThreadStartParams,
    TurnStartParams,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const CLIENT_NAME: &str = "codexctl";
const DEFAULT_PORT: u16 = 7373;
const READY_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Parser)]
#[command(name = "codexctl", version, about = "Thin controller for Codex App Server")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Manage the persistent app-server daemon.
    Daemon {
        #[command(subcommand)]
        op: DaemonOp,
    },
    /// Create a new named thread (with optional goal objective).
    Start {
        name: String,
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long, default_value = "gpt-5.5")]
        model: String,
        #[arg(long, default_value = "workspace-write")]
        sandbox: String,
        /// If set, also set the goal objective on the new thread.
        #[arg(long)]
        objective: Option<String>,
        #[arg(long, default_value_t = 2_000_000)]
        budget: u64,
        /// Skip the "Acknowledge with READY" materialize turn and goal setup.
        #[arg(long)]
        no_objective_yet: bool,
    },
    /// List registered named threads.
    Ls,
    /// Send a message as a fresh user turn.
    Say { name: String, message: String },
    /// Inject input into the active turn (turn/steer). Errors if idle.
    Steer { name: String, message: String },
    /// Interrupt the active turn for the named thread.
    Interrupt { name: String },
    /// Manage the persisted goal on a thread.
    Goal {
        name: String,
        #[arg(long, group = "op")]
        set: Option<String>,
        #[arg(long, group = "op")]
        pause: bool,
        #[arg(long, group = "op")]
        resume: bool,
        #[arg(long, group = "op")]
        clear: bool,
        #[arg(long)]
        budget: Option<u64>,
    },
    /// Show a concise status summary for a named thread.
    Status { name: String },
    /// Stream notifications for a named thread until ctrl-c.
    Tail {
        name: String,
        /// Suppress agent message deltas (item/agentMessage/delta).
        #[arg(long)]
        no_deltas: bool,
    },
    /// Archive a thread and drop it from the registry.
    Rm {
        name: String,
        #[arg(long)]
        keep_rollout: bool,
    },
}

#[derive(Subcommand)]
enum DaemonOp {
    /// Start the daemon if not running.
    Start {
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
    },
    /// Stop the running daemon.
    Stop,
    /// Show daemon status.
    Status,
    /// Print the daemon log path.
    Logs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Daemon { op } => match op {
            DaemonOp::Start { port } => daemon_start(port).await,
            DaemonOp::Stop => daemon_stop(),
            DaemonOp::Status => daemon_status(),
            DaemonOp::Logs => {
                println!("{}", daemon_log_path()?.display());
                Ok(())
            }
        },
        Cmd::Start {
            name,
            cwd,
            model,
            sandbox,
            objective,
            budget,
            no_objective_yet,
        } => {
            cmd_start(
                name,
                cwd,
                model,
                sandbox,
                objective,
                budget,
                no_objective_yet,
            )
            .await
        }
        Cmd::Ls => cmd_ls(),
        Cmd::Say { name, message } => cmd_say(name, message).await,
        Cmd::Steer { name, message } => cmd_steer(name, message).await,
        Cmd::Interrupt { name } => cmd_interrupt(name).await,
        Cmd::Goal {
            name,
            set,
            pause,
            resume,
            clear,
            budget,
        } => cmd_goal(name, set, pause, resume, clear, budget).await,
        Cmd::Status { name } => cmd_status(name).await,
        Cmd::Tail { name, no_deltas } => cmd_tail(name, no_deltas).await,
        Cmd::Rm { name, keep_rollout } => cmd_rm(name, keep_rollout).await,
    }
}

// -------------------------- registry --------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
struct Registry {
    daemon_port: Option<u16>,
    threads: BTreeMap<String, ThreadEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadEntry {
    thread_id: String,
    cwd: Option<String>,
    model: Option<String>,
    sandbox: Option<String>,
    created_at_unix: u64,
}

fn config_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("no home dir"))?;
    let p = home.join(".codexctl");
    fs::create_dir_all(&p)?;
    Ok(p)
}

fn registry_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("threads.json"))
}

fn pid_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("daemon.pid"))
}

fn port_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("daemon.port"))
}

fn daemon_log_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("daemon.log"))
}

fn read_registry() -> Result<Registry> {
    let path = registry_path()?;
    if !path.exists() {
        return Ok(Registry::default());
    }
    let bytes = fs::read(&path)?;
    if bytes.is_empty() {
        return Ok(Registry::default());
    }
    Ok(serde_json::from_slice(&bytes).unwrap_or_default())
}

fn write_registry(reg: &Registry) -> Result<()> {
    let path = registry_path()?;
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(reg)?;
    fs::write(&tmp, body)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn lookup_thread(name: &str) -> Result<ThreadEntry> {
    let reg = read_registry()?;
    reg.threads
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow!("no thread named '{name}'; try `codexctl ls`"))
}

// -------------------------- daemon lifecycle --------------------------

fn read_daemon_port() -> Option<u16> {
    fs::read_to_string(port_path().ok()?)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn read_daemon_pid() -> Option<u32> {
    fs::read_to_string(pid_path().ok()?)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn pid_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn daemon_running() -> bool {
    match read_daemon_pid() {
        Some(pid) => pid_alive(pid),
        None => false,
    }
}

async fn daemon_start(port: u16) -> Result<()> {
    if daemon_running() {
        let cur_port = read_daemon_port().unwrap_or(port);
        println!("daemon: already running on port {cur_port}");
        return Ok(());
    }
    spawn_daemon(port).await
}

fn daemon_stop() -> Result<()> {
    let Some(pid) = read_daemon_pid() else {
        println!("daemon: not running");
        return Ok(());
    };
    if !pid_alive(pid) {
        println!("daemon: stale pidfile (pid {pid} not alive); cleaning up");
        let _ = fs::remove_file(pid_path()?);
        return Ok(());
    }
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), Signal::SIGTERM).context("send SIGTERM")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !pid_alive(pid) {
            let _ = fs::remove_file(pid_path()?);
            let _ = fs::remove_file(port_path()?);
            println!("daemon: stopped (pid {pid})");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    let _ = fs::remove_file(pid_path()?);
    let _ = fs::remove_file(port_path()?);
    println!("daemon: SIGKILLed (pid {pid})");
    Ok(())
}

fn daemon_status() -> Result<()> {
    match (read_daemon_pid(), read_daemon_port()) {
        (Some(pid), Some(port)) if pid_alive(pid) => {
            println!("daemon: running pid={pid} port={port}");
        }
        (Some(pid), _) => println!("daemon: stale pidfile (pid {pid} not alive)"),
        _ => println!("daemon: not running"),
    }
    Ok(())
}

async fn spawn_daemon(port: u16) -> Result<()> {
    let log = daemon_log_path()?;
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .with_context(|| format!("open log {}", log.display()))?;
    let stderr = log_file.try_clone()?;
    let listen = format!("ws://127.0.0.1:{port}");
    // Use a private sqlite state-db dir to avoid migration drift against any
    // other codex tooling (desktop app, ad-hoc app-server invocations, etc.).
    // Honor CODEX_SQLITE_HOME if the user already set one.
    let sqlite_home = std::env::var("CODEX_SQLITE_HOME")
        .unwrap_or_else(|_| config_dir().unwrap().join("state").to_string_lossy().into_owned());
    fs::create_dir_all(&sqlite_home).ok();
    let child = std::process::Command::new("codex")
        .args(["app-server", "--listen", &listen, "-c", "features.goals=true"])
        .env("CODEX_SQLITE_HOME", &sqlite_home)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(stderr))
        .spawn()
        .context("spawn codex app-server")?;

    fs::write(pid_path()?, format!("{}\n", child.id()))?;
    fs::write(port_path()?, format!("{port}\n"))?;
    // Don't wait on the child; let it run as our daemon.
    std::mem::forget(child);

    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if probe_daemon(port).await {
            println!("daemon: started on port {port}");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    anyhow::bail!(
        "daemon spawned but did not become ready in {}s; see {}",
        READY_TIMEOUT.as_secs(),
        log.display()
    )
}

async fn probe_daemon(port: u16) -> bool {
    use tokio::net::TcpStream;
    matches!(
        tokio::time::timeout(
            Duration::from_millis(200),
            TcpStream::connect(("127.0.0.1", port)),
        )
        .await,
        Ok(Ok(_))
    )
}

async fn ensure_daemon() -> Result<u16> {
    if let Some(port) = read_daemon_port() {
        if daemon_running() {
            return Ok(port);
        }
    }
    spawn_daemon(DEFAULT_PORT).await?;
    Ok(DEFAULT_PORT)
}

// -------------------------- client connection --------------------------

async fn connect() -> Result<CodexClient> {
    let port = ensure_daemon().await?;
    let cfg = WsConfig::new(
        format!("ws://127.0.0.1:{port}"),
        HashMap::new(),
        ClientOptions::default(),
    );
    let client = CodexClient::connect_ws(cfg).await.context("connect ws")?;
    let mut init = InitializeParams::new(ClientInfo::new(
        CLIENT_NAME,
        "codexctl",
        env!("CARGO_PKG_VERSION"),
    ));
    init.capabilities = Some(InitializeCapabilities {
        experimental_api: Some(true),
        extra: Default::default(),
    });
    client.initialize(init).await.context("initialize")?;
    client.initialized().await.context("initialized")?;
    Ok(client)
}

// -------------------------- commands --------------------------

async fn cmd_start(
    name: String,
    cwd: Option<PathBuf>,
    model: String,
    sandbox: String,
    objective: Option<String>,
    budget: u64,
    no_objective_yet: bool,
) -> Result<()> {
    let mut reg = read_registry()?;
    if reg.threads.contains_key(&name) {
        anyhow::bail!("thread '{name}' already registered; pick another name or `codexctl rm {name}` first");
    }
    let cwd_str = cwd
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        });

    let client = connect().await?;
    let mut params = ThreadStartParams::default();
    params.cwd = cwd_str.clone();
    params.model = Some(model.clone());
    params.sandbox = Some(sandbox.clone());
    params
        .extra
        .insert("skipGitRepoCheck".into(), Value::Bool(true));
    let thread = client.thread_start(params).await.context("thread/start")?;
    let thread_id = thread.thread.id.clone();

    let run_setup = !no_objective_yet && objective.is_some();
    if run_setup {
        let _ = client
            .turn_start(TurnStartParams::text(
                thread_id.clone(),
                "Acknowledge with the single word READY. Do not run any commands or edit any files.",
            ))
            .await
            .context("materialize turn/start")?;
        wait_for(
            &client,
            |evt| {
                matches!(
                    evt,
                    ServerEvent::Notification(ServerNotification::TurnCompleted(_))
                )
            },
            Duration::from_secs(120),
        )
        .await?;

        let obj = objective.as_ref().unwrap();
        if obj.len() > 4000 {
            anyhow::bail!(
                "objective is {} chars; app-server caps at 4000. Use `codexctl start ... --no-objective-yet` then `codexctl say <name>` for detail and `codexctl goal <name> --set` with a short objective.",
                obj.len()
            );
        }
        client
            .send_raw_request(
                "thread/goal/set",
                json!({
                    "threadId": thread_id,
                    "objective": obj,
                    "tokenBudget": budget,
                }),
                None,
            )
            .await
            .context("thread/goal/set")?;
    }

    reg.threads.insert(
        name.clone(),
        ThreadEntry {
            thread_id: thread_id.clone(),
            cwd: cwd_str,
            model: Some(model),
            sandbox: Some(sandbox),
            created_at_unix: unix_now(),
        },
    );
    write_registry(&reg)?;
    println!("name: {name}");
    println!("thread: {thread_id}");
    if run_setup {
        println!("goal: active");
    }
    Ok(())
}

fn cmd_ls() -> Result<()> {
    let reg = read_registry()?;
    if reg.threads.is_empty() {
        println!("(no threads; create with `codexctl start <name> --cwd <path>`)");
        return Ok(());
    }
    println!("{:<24}  {:<40}  {}", "NAME", "THREAD", "CWD");
    for (name, entry) in &reg.threads {
        println!(
            "{:<24}  {:<40}  {}",
            name,
            entry.thread_id,
            entry.cwd.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

async fn cmd_say(name: String, message: String) -> Result<()> {
    let entry = lookup_thread(&name)?;
    let client = connect().await?;
    resume_thread(&client, &entry).await?;
    let turn = client
        .turn_start(TurnStartParams::text(entry.thread_id.clone(), &message))
        .await
        .context("turn/start")?;
    println!("turn: {}", turn.turn.id);
    Ok(())
}

async fn cmd_steer(name: String, message: String) -> Result<()> {
    let entry = lookup_thread(&name)?;
    let client = connect().await?;
    resume_thread(&client, &entry).await?;
    let active_turn = active_turn_id(&client, &entry.thread_id)
        .await
        .context("look up active turn for steer")?
        .ok_or_else(|| {
            anyhow!(
                "no active turn on thread {} — turn/steer requires a turn in flight; use `codexctl say` instead",
                entry.thread_id
            )
        })?;
    let resp = client
        .send_raw_request(
            "turn/steer",
            json!({
                "threadId": entry.thread_id,
                "expectedTurnId": active_turn,
                "input": [{"type": "text", "text": message}],
            }),
            None,
        )
        .await
        .context("turn/steer")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

/// Look up the currently in-flight turn for `thread_id` via `thread/read`.
/// Returns the turn id if status==inProgress; None if the thread is idle.
async fn active_turn_id(client: &CodexClient, thread_id: &str) -> Result<Option<String>> {
    let resp = client
        .send_raw_request(
            "thread/read",
            json!({"threadId": thread_id, "includeTurns": true}),
            None,
        )
        .await
        .context("thread/read")?;
    let turns = resp
        .get("thread")
        .and_then(|t| t.get("turns"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    // Find the most recent turn with status "inProgress". Server returns turns
    // in append order, so the last in-progress is the active one.
    Ok(turns
        .iter()
        .rev()
        .find(|turn| {
            turn.get("status").and_then(|s| s.as_str()) == Some("inProgress")
        })
        .and_then(|turn| turn.get("id").and_then(|id| id.as_str().map(str::to_owned))))
}

async fn cmd_interrupt(name: String) -> Result<()> {
    let entry = lookup_thread(&name)?;
    let client = connect().await?;
    resume_thread(&client, &entry).await?;
    let active_turn = active_turn_id(&client, &entry.thread_id)
        .await
        .context("look up active turn for interrupt")?
        .ok_or_else(|| {
            anyhow!(
                "no active turn on thread {} — turn/interrupt requires a turn in flight",
                entry.thread_id
            )
        })?;
    let resp = client
        .send_raw_request(
            "turn/interrupt",
            json!({"threadId": entry.thread_id, "turnId": active_turn}),
            None,
        )
        .await
        .context("turn/interrupt")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

async fn cmd_goal(
    name: String,
    set: Option<String>,
    pause: bool,
    resume: bool,
    clear: bool,
    budget: Option<u64>,
) -> Result<()> {
    let entry = lookup_thread(&name)?;
    let client = connect().await?;
    resume_thread(&client, &entry).await?;

    if clear {
        let resp = client
            .send_raw_request(
                "thread/goal/clear",
                json!({"threadId": entry.thread_id}),
                None,
            )
            .await
            .context("thread/goal/clear")?;
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    let mut params = json!({"threadId": entry.thread_id});
    if let Some(obj) = set {
        if obj.len() > 4000 {
            anyhow::bail!("objective is {} chars; app-server caps at 4000", obj.len());
        }
        params["objective"] = Value::String(obj);
    }
    if pause {
        params["status"] = Value::String("paused".into());
    } else if resume {
        params["status"] = Value::String("active".into());
    }
    if let Some(b) = budget {
        params["tokenBudget"] = Value::Number(b.into());
    }
    let resp = client
        .send_raw_request("thread/goal/set", params, None)
        .await
        .context("thread/goal/set")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

async fn cmd_status(name: String) -> Result<()> {
    let entry = lookup_thread(&name)?;
    let client = connect().await?;
    resume_thread(&client, &entry).await?;
    let goal = client
        .send_raw_request(
            "thread/goal/get",
            json!({"threadId": entry.thread_id}),
            None,
        )
        .await
        .ok();
    println!("name: {name}");
    println!("thread: {}", entry.thread_id);
    if let Some(cwd) = &entry.cwd {
        println!("cwd: {cwd}");
    }
    if let Some(g) = goal
        && let Some(goal_obj) = g.get("goal")
    {
        println!("goal: {}", serde_json::to_string_pretty(goal_obj)?);
    }
    Ok(())
}

async fn cmd_tail(name: String, no_deltas: bool) -> Result<()> {
    let entry = lookup_thread(&name)?;
    let client = connect().await?;
    resume_thread(&client, &entry).await?;
    println!("(tailing {}; ctrl-c to exit)", entry.thread_id);
    // The app-server emits notifications for every loaded thread on this
    // connection; for v1 we trust that thread/resume above is the only
    // subscription we have on this fresh client. Multi-thread filtering will
    // come when we attach to a long-running daemon with multiple threads
    // pre-loaded.
    loop {
        let evt = tokio::select! {
            evt = client.next_event() => evt?,
            _ = tokio::signal::ctrl_c() => {
                println!("\n(interrupted)");
                return Ok(());
            }
        };
        let ServerEvent::Notification(notif) = evt else {
            continue;
        };
        match notif {
            ServerNotification::ItemAgentMessageDelta(d) if !no_deltas => {
                if let Some(text) = d.delta.or(d.text) {
                    print!("{text}");
                    std::io::stdout().flush().ok();
                }
            }
            ServerNotification::TurnStarted(t) => {
                println!("[turn] started {}", t.turn.id);
            }
            ServerNotification::TurnCompleted(t) => {
                println!("\n[turn] completed status={:?}", t.turn.status);
            }
            ServerNotification::ItemCompleted(item) => {
                let kind = item
                    .item
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                if kind == "commandExecution" {
                    let cmd = item
                        .item
                        .get("command")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let rc = item.item.get("exitCode").and_then(|v| v.as_i64());
                    println!(
                        "[exec] rc={:?} {}",
                        rc,
                        cmd.chars().take(120).collect::<String>()
                    );
                }
            }
            ServerNotification::Unknown { method, params } => {
                if method == "thread/goal/updated"
                    && let Some(g) = params.get("goal")
                {
                    let st = g.get("status").and_then(|s| s.as_str()).unwrap_or("?");
                    let used = g.get("tokensUsed").and_then(|t| t.as_u64()).unwrap_or(0);
                    println!("[goal] status={st} tokens={used}");
                }
            }
            _ => {}
        }
    }
}

async fn cmd_rm(name: String, keep_rollout: bool) -> Result<()> {
    let entry = lookup_thread(&name)?;
    if !keep_rollout {
        let client = connect().await?;
        let _ = client
            .send_raw_request(
                "thread/archive",
                json!({"threadId": entry.thread_id}),
                None,
            )
            .await;
    }
    let mut reg = read_registry()?;
    reg.threads.remove(&name);
    write_registry(&reg)?;
    println!("removed: {name}");
    Ok(())
}

async fn resume_thread(client: &CodexClient, entry: &ThreadEntry) -> Result<()> {
    let mut params = ThreadResumeParams::default();
    params.thread_id = entry.thread_id.clone();
    params.cwd = entry.cwd.clone();
    params.model = entry.model.clone();
    params.sandbox = entry.sandbox.clone();
    client
        .thread_resume(params)
        .await
        .with_context(|| format!("thread/resume {}", entry.thread_id))?;
    Ok(())
}

async fn wait_for<F>(client: &CodexClient, pred: F, timeout: Duration) -> Result<ServerEvent>
where
    F: Fn(&ServerEvent) -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let evt = tokio::time::timeout(remaining, client.next_event())
            .await
            .map_err(|_| anyhow!("timed out waiting for expected event"))??;
        if pred(&evt) {
            return Ok(evt);
        }
    }
    Err(anyhow!("timed out waiting for expected event"))
}
