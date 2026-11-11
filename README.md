# Native language Workers

Pi runs a TypeScript host with an embedded Pi SDK inside a managed Node child. Codex runs a Rust host controlling its own pinned app-server process. Neither ordinary execution path starts the Java Worker. ACS remains its existing Java runtime with a thin authenticated `worker-host/1` binding.

## Ordinary Java wrapper development freeze (2026-10-29)

By the user's decision, new Pi/Codex features, client adoption and dedicated qualification target these native hosts. The old Java ordinary paths (`PiTaskProvider` / `CodexTaskProvider` based on `LocalNativeTaskProvider`) are frozen: no new feature parity, client adapters, dedicated model/E2E campaigns, or automatic fallback. Retain their code, existing tests, historical evidence and running tasks; this documentation change does not disable a deployment or migrate its data.

This freeze does not cover ACS, shared core/protocol/SPI, or `PiStageProvider` / `CodexStageProvider` DevOps stage execution inside the same Java modules. Do not remove or freeze an entire provider module by name. Existing shared builds/checks stay in place. Native defects should be fixed in the native path; only a demonstrated major blocker and a separate explicit user decision may reopen a bounded legacy work item. There is no automatic resume date. The cross-repository decision is recorded in EnterpriseDevopsAI at `docs/worker/legacy-pi-codex-wrapper-freeze-2026-10-29.md`.

Each native host owns separate SQLite state, immutable request receipts, a cloned committed Git baseline, native context, and artifact bytes. The existing Business/PAC `WORKER_LOCAL` consumer can connect through `/local/v1`; `/worker-host/v1/manifest` and `/capabilities` describe the source and the actually measured subset. HOST execution uses the local OS account and is not an isolation profile.

Read the host-specific build instructions under [pi](pi/README.md) and [codex](codex/README.md). Node 24.16+ is required for Pi's SQLite API; Codex's executable and generated protocol schema must match its pin. The generic launcher is Windows PowerShell 5.1 compatible:

```powershell
# Configure a long-lived WORKER_LOCAL_TOKEN in the environment (min 24 chars).
# The launcher optionally dot-sources a local credential file when one exists;
# credentials stay outside the repository — no project .env is used.
.\Run-NativeWorker.ps1 -Engine pi -State C:\worker-state\pi `
  -Initialize -Project C:\src\my-git-project -ProjectId my-project -Port 18731 `
  -Model deepseek-v4.1-flash -ModelBaseUrl https://opencode.ai/zen/go/v1
```

Use `-Engine codex` for the Rust host, with `-CodexExecutable`/`-CodexHostExecutable` when they are outside the usual build locations. Later starts use the same state directory without `-Initialize`. Do not initialize over an existing host's state with different bindings. Connect the loopback URL and control token through the existing client connection configuration.

For macOS, use [Run-NativeWorker.sh](Run-NativeWorker.sh) from a shell that has
loaded the approved model/control environment. Build the selected host first;
see its own README for fixed SDK/app-server inputs and measured platform limits.
The launcher inherits credentials and does not source a Windows path or write a
credential file:

```bash
./Run-NativeWorker.sh --engine pi --state /absolute/path/to/pi-state \
  --initialize --project /absolute/path/to/committed-project --project-id my-project \
  --model deepseek-v4.1-flash --model-base-url https://opencode.ai/zen/go/v1
```

The Mac launcher selects Pi's explicit `macos-runner-v1` execution profile for
initialization and serving. File/question tools remain in the no-fork model
process; bash/run_command/run_build/run_tests route through the shared
[command Runner](command-runner/README.md). The old file-only
`--pi-execution-profile macos-in-process-v1` and native toolset
`--pi-execution-profile native` remain available. Existing state cannot silently
change its selected profile.

For Codex, also supply `--codex-executable` for the pinned Mac app-server and,
when needed, `--codex-host-executable` for the built Worker. Later starts omit
`--initialize`. This Mac launcher initializes Codex with the explicit
`macos-runner-v1` tool profile: native file edits plus host-mediated dynamic
run_command/run_build/run_tests tools. The model process retains no-fork rules;
the Runner executes the command. Its bundled path is passed at initialization
and content-bound along with the selected physical Node executable.
`--codex-tool-profile macos-in-process-v1` retains the former file-only profile.
Use `--codex-tool-profile native` only for the separately described native
toolset and its unproven-fork limits. The model ID remains unchanged; the fixed
tool metadata is bound and checked by the host. Port zero selects an ephemeral loopback port; the host prints
its actual `LOCAL_TASK_READY` address. OpenCode Go also requires a stable
conversation session header; the existing `LLM_HEADER_x_opencode_session`
environment reference supplies `x-opencode-session`. Real model credentials
and that header must be present in the approved calling environment.

macOS uses its own process ownership proof. Runner mode allows normal command
forks and explicitly proves registered-group cleanup while retaining arbitrary
escaped processes/remote effects as NOT_PROVEN. The native profile retains its
conservative UNKNOWN behavior after fork; file-only mode retains no-fork proof.
Consult each host's README for the exact qualified subset. The native Swift Mac
worker host (`macos-worker`) is a separate product and lives in a private
repository; it is not part of this open-source tree.

The model above is the NWF-001 test profile explicitly requested for this implementation. Hosts require an explicit model binding for other uses. The Zen session header is read via `LLM_HEADER_x_opencode_session` in the unified environment. Its value, model key, and control token are not copied into project configuration.

Shared contracts and golden vectors are vendored in [contracts/worker-host/v1](contracts/worker-host/v1/README.md). The cross-engine worker-local contract suite is in [contract/](contract/README.md); Business supplies a smoke using its actual `NativeTasks` consumer. NWF-001 coordination (EnterpriseDevopsAI `docs/tasks/NWF-001/STATUS.md`) records exact tested versions, evidence and the remaining full-parity work. First-slice checks do not grant strict checkpoint pause, unknown-effect recovery, enterprise/stage, AMC, Linux or complete ACS parity.
