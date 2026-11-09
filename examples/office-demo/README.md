# office-demo — "The Office" first-run experience

Watch real agents walk into a pixel office and do real work. One command brings
up a pi worker, an event bridge, and the [pixel-agents](https://github.com/pixel-agents-hq/pixel-agents)
(MIT) browser office — then narrates a short scenario and hands you the keyboard.

## Run it

```bash
# prerequisites: node >=22, git, and pi built once:
npm --prefix ../../pi install && npm --prefix ../../pi run build
```

**One-click**: `demo.cmd` (Windows) or `demo.sh` (macOS/Linux). Re-running kills
leftovers, clears old characters, picks a free port, and reuses the worker if
it is still alive — safe to run again and again.

```bash
# or manually:
MODEL_API_KEY=<your key> node runner.mjs        # bash
$env:MODEL_API_KEY="..." ; node runner.mjs      # powershell
```

If `MODEL_API_KEY` isn't set, the launchers try `.local/env.cmd` /
`.local/env.sh` first — drop your key there for a true double-click experience
(that dir is gitignored).

Optional model overrides: `MODEL_BASE_URL`, `MODEL_NAME`, `WORKER_LOCAL_TOKEN`,
`DEMO_WORKER_PORT` (default 18731). First run downloads pixel-agents via `npx`.

Your browser opens the office automatically. After the scripted scenario, an
interactive prompt lets you type any natural-language task — a new character
walks in and does it.

## How it works (no source changes anywhere)

```
pi worker (:18731, worker-events/1 SSE)
   └─ bridge.mjs — discovers tasks via GET /tasks, tails each task's SSE spine,
      and projects events into Claude-shaped .jsonl transcripts under
      ~/.claude/projects/<fixture-slug>/
   └─ npx pixel-agents — stock server, renders each transcript as a character
```

Neither pi nor pixel-agents is modified. The bridge is ~110 lines; the worker
API is the only thing it touches.

## Authoring your own scenario

`scenario.md` is the human-readable script. `plan.json` is the compiled beat
list the runner executes. Beat types:

| type | fields | effect |
|---|---|---|
| `narrate` | `text` | prints narration |
| `dispatch` | `key`, `instruction`, `label?`, `project?` | creates a task → new character |
| `wait` | `seconds` | pause for pacing |
| `wait-tasks` | `timeoutSeconds` | wait until all dispatched tasks reach a terminal state |
| `show-outputs` | `pattern?`, `text?` | prints files produced in task workspaces |
| `interactive` | — | free-form natural-language task loop |

Edit `plan.json` directly, or write `scenario.md` and hand-translate — a
compiler pass is deliberately not required at runtime so every replay is
deterministic.

## Notes & limits

- Tasks run in task-owned git clones (pi `COMMITTED_BASE_ONLY`); your fixture
  repo is never dirtied. Outputs land in `.local/pi-state/workspaces/<taskId>/`.
- pixel-agents watches project dirs under `~/.claude/projects` that exist at
  **its** startup; the runner creates the fixture slug dir first for that
  reason. The bridge only writes under the fixture slug — it will not touch
  real Claude Code session data.
- `WORKER_LOCAL_TOKEN` is generated per run if unset (bridge and worker share
  it); set it yourself to reuse.
- Stop with Ctrl+C — the runner tears down worker, bridge, and office.
