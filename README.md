# codexctl

A thin shell-native controller for the Codex App Server.

`codex app-server` exposes a powerful JSON-RPC surface — `thread/start`,
`thread/resume`, `turn/start`, `turn/steer`, `turn/interrupt`,
`thread/goal/{set,get,clear}`, plus a full notification stream — but there's
no scriptable CLI on top. `codexctl` is that CLI.

It's intentionally small: one persistent app-server daemon (lazy-spawned on
first use), named threads, a handful of subcommands that map onto the
protocol. No phone app, no web UI, no Cloudflare. Just `bash`-friendly
`codexctl <verb> <name> ...` that you can pipe and script.

## Why

If you've ever wanted to:

- Kick off a long-running Codex goal from one terminal, then from another
  terminal nudge the agent mid-turn (`codexctl steer …`),
- Tail an autonomous agent's stream into a tmux pane while it works
  (`codexctl tail …`),
- Pause a goal while a sub-agent finishes, then resume it
  (`codexctl goal foo --pause` / `codexctl goal foo --resume`),
- Have multiple named threads coexist with a shared name registry,

then this is for you. The desktop Codex app keeps its own app-server private
to its Electron parent — `codexctl` runs its own dedicated daemon next to
the desktop app, sharing `$CODEX_HOME` (auth, config) but with its own
threads.

## Status

Early. v0.1. Built against codex 0.129.0 and `codex-app-server-sdk` 0.5.x.

## Install

```sh
git clone https://github.com/voltropy/codexctl ~/src/codexctl
cd ~/src/codexctl
cargo install --path .
```

Requires `codex` (the OpenAI Codex CLI) on PATH.

## Quick start

```sh
# Start a long-running goal in this repo. The daemon auto-spawns on first use.
codexctl start refactor --cwd "$PWD" \
  --objective "Refactor the payment module per docs/payment-refactor.md; commit per logical step." \
  --budget 2000000

# In another terminal, watch what's happening.
codexctl tail refactor

# Steer mid-turn without interrupting.
codexctl steer refactor "Skip the deprecation cleanup for now; focus on the auth bug."

# Pause goal-continuation while you check things.
codexctl goal refactor --pause
codexctl status refactor
codexctl goal refactor --resume

# When the agent finishes the goal, archive it.
codexctl rm refactor
```

## Subcommands

| Command | What it does |
|---|---|
| `codexctl daemon start [--port N]` | Spawn the persistent `codex app-server` if not running. |
| `codexctl daemon stop` | SIGTERM the daemon, fall back to SIGKILL after 5s. |
| `codexctl daemon status` | Show pid + port. |
| `codexctl daemon logs` | Print path to daemon log. |
| `codexctl start <name> --cwd <path> [--objective "..." --budget N]` | New named thread. With `--objective`, runs a materialize turn then `thread/goal/set`. |
| `codexctl ls` | List named threads. |
| `codexctl say <name> "<msg>"` | Append a fresh user turn. |
| `codexctl steer <name> "<msg>"` | `turn/steer` — append to the active turn. Errors if idle. |
| `codexctl interrupt <name>` | Cancel the active turn. |
| `codexctl goal <name> --set "<obj>" \| --pause \| --resume \| --clear \| --budget N` | Manage the persisted goal. |
| `codexctl status <name>` | Concise summary including current goal status. |
| `codexctl tail <name> [--no-deltas]` | Stream notifications until ctrl-c. |
| `codexctl rm <name> [--keep-rollout]` | Archive the thread and drop the registry entry. |

## State

- Thread name registry: `~/.codexctl/threads.json`
- Daemon pid: `~/.codexctl/daemon.pid`
- Daemon port: `~/.codexctl/daemon.port` (default `7373`)
- Daemon log: `~/.codexctl/daemon.log`
- Codex's own state lives in `$CODEX_HOME` (default `~/.codex`) — auth,
  config, sqlite state-db, rollouts. `codexctl` doesn't override these.

## Limitations

- v1 spawns its own app-server, distinct from the one Codex.app uses.
  Threads and goal state aren't shared with the desktop app.
- `~/.codex` sqlite migrations may drift if you run codex builds from
  multiple sources. If you see "migration N was previously applied but has
  been modified", set `CODEX_SQLITE_HOME` to a private dir before
  `codexctl daemon start`.
- The 4000-character cap on `thread/goal/set` objectives is enforced by
  the app-server. Use `--no-objective-yet`, then `say` the long brief, then
  `goal --set` with a short objective for the autonomy continuation.
- `ServerNotification` typings in the SDK don't yet cover
  `thread/goal/{updated,cleared}` — codexctl handles these via the SDK's
  `Unknown { method, params }` fallback. Functional, just not pretty.

## Architecture

```
codexctl <subcommand>
    │
    │ ws://127.0.0.1:7373 (default)
    ↓
 codex app-server  (persistent, owned by codexctl)
    │
    ├── ~/.codex/auth.json, config.toml
    ├── ~/.codex/state.db (sqlite, persisted threads + goals)
    └── ~/.codex/sessions (rollouts)
```

Each `codexctl` invocation is a fresh JSON-RPC client; the daemon outlives
them. Long-running commands (`tail`) hold the connection open; everything
else does its work and disconnects.

## License

MIT.
