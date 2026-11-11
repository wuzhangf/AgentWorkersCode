# worker-host/1 — first native vertical slice

NWF-001 W0/W1/W2. The implementation scope is recorded in the [coordination](../../../../EnterpriseDevopsAI/docs/tasks/NWF-001/STATUS.md), not inferred from this contract. Unpublished routes and unverified capabilities remain work to do.

`/worker-host/v1/manifest` and `/capabilities` describe a real host. Pi/TypeScript and Codex/Rust retain the existing `/local/v1` transport consumed by Business and PAC. ACS binds `/worker-host/v1` (also `/api/v2/worker-host/v1`) to its existing `/api/v2` services. Its manifest declares `payloadContract: business-codeagent-v2`; source DTOs, identifiers, guards, operation names and receipts remain unchanged. This first binding is not a claim that all three payload dialects are already interchangeable.

Every endpoint requires bearer authentication. Native hosts bind loopback only. `X-Worker-Local-Store`, when supplied, must match the authenticated source, before returning cached results. Durable `authorityId/storeId/workerId` identify the source; `bootId` changes on process restart and never participates in deduplication. There is one writer per state directory.

## Admission and lookup

The compatible native flow is:

1. `GET /local/v1/identity`, `/projects`, `/engines`.
2. `POST /local/v1/task-preflights` with `{projectId,engineId}`.
3. Persist an original request key and preflight revision at the consumer.
4. `POST /local/v1/tasks` with `{requestKey,projectId,engineId,optionsRevision,instruction}` and matching `Idempotency-Key`.
5. Read `/local/v1/task-requests/{requestKey}` after an uncertain reply. A missing receipt does not authorize resending under a new key.
6. Observe `/local/v1/tasks/{taskId}` and its `events/output/metrics/messages/input-requests/artifacts` subresources.

Follow-up, steer, answer and stop retain the source generation/session/turn/command guards. Unknown input types are rejected. A cached command is replayed only if the authenticated source, scope, body and original guards match. A changed current generation must not invalidate a read of the matching original receipt. STOP may be admitted independently of another unresolved command while still checking source and owner guards.

`POST /worker-host/v1/operations/lookup` is read-only. Native payload is `{requestKey,taskId?}`; without taskId it reads the original task admission, with taskId it reads that task's original interaction. It never creates an intent or invokes a model. ACS uses its declared `business-codeagent-v2` lookup payload and source registration requirements.

## Durable facts

The host transaction records admission and observation. Persist `DISPATCHING` before calling the engine. A restart in this window produces `UNKNOWN` unless the original engine result is independently established. Never call prompt again to repair uncertainty. Native context remains in the pinned kernel's own store; the host stores references and public projections. No private reasoning, model credentials, control bearer or model header values belong in events or evidence.

`ACCEPTED`, `DISPATCHING`, `REQUESTED`, `APPLIED`, `REJECTED`, `NOOP`, `SUPERSEDED` and `UNKNOWN` are command facts, distinct from run status. A protocol ACK alone does not establish consumed input, stopped writers, checkpoint safety or file delivery. Availability, implementation and qualification are independent. `NOT_RUN` or `PARTIAL` is not `VERIFIED`.

## Control intents and checkpoints

`POST /local/v1/tasks/{taskId}/commands` accepts the same guarded body `{requestKey,intent,generation,nativeSessionId,nativeTurnId,targetCommandId}` for `STOP`, `PAUSE`, `RESUME` and `EXTEND_BUDGET`. Each intent is durably accepted under its own original key before dispatch, replays under the same key/body, and is looked up by `GET /local/v1/tasks/{taskId}/commands/{requestKey}` or `POST /worker-host/v1/operations/lookup` with `taskId`.

- `STOP` closes intake, drains owned preparation, interrupts the known native turn, terminates the managed process tree and records its ownership proof. Remote tool/service effects are never proven by local exit and remain `UNKNOWN`.
- `PAUSE` is a **boundary admission** request: the host durably accepts it, then the engine gate must hold every subsequent model request, tool call, compaction and queued input at a boundary. `APPLIED` requires a sealed `worker-checkpoint/1` manifest plus proof all owned writers are quiet. If the boundary is not reached within its deadline the command stays `REQUESTED` and the task reports `PAUSE_REQUESTED` — it never reports `PAUSED` on suspicion.
- `RESUME` validates the checkpoint's native session seal, engine and model binding digests, then continues the same logical run as a **new segment**. A live gate releases in place; a dead or restarted host relaunches the engine against the recorded native session and dispatches a durable continuation input. Interrupt-and-continue after a restart is `INTERRUPT_CONTINUATION`, not a strict `BOUNDARY_CHECKPOINT` resume, and capability semantics must say so.

For Pi's live held-gate resume, a still-pending question keeps its original input-request identity, while its response guards are rebound to the current generation and command. Clients reload the question before answering; old-generation answers remain rejected. The source records `INPUT_REQUEST_REBOUND` rather than reviving expired requests. A confirmed `PAUSED` task is excluded from the execution-deadline stop sweep; active tasks retain their deadline and RESUME starts the existing fresh execution window.

The `worker-checkpoint/1` manifest (see `host.schema.json#/$defs/checkpointManifest`) records the run/segment/generation, native session reference and its file digest, engine/model binding digests, unconsumed inputs, pending effects, usage, workspace manifest, last event sequence and owner epoch. A checkpoint whose digests no longer verify is refused, not repaired silently. `PAUSED` state and its checkpoint survive host restart; an unproven pause (`PAUSE_REQUESTED` window, crashed dispatch) reconciles as `UNKNOWN` and is never replayed.

Native host 64-bit versions, sequences and usage counters use decimal strings. Existing ACS DTO integer fields retain the declared source contract during this binding stage. Times use UTC RFC3339. New canonical JSON follows existing EDC `CanonicalJson`: UTF-8, code-point-sorted object keys, no whitespace, shortest string escapes, integer-only numeric literals. Do not alter old source key derivation. The golden vectors include BMP/astral ordering and values exceeding JavaScript's safe integer range as strings.

## Budgets, scope, environment and slots

Task creation may carry `budget` (`#/$defs/budgetSpec`), `writeScope` (`#/$defs/writeScopeSpec`) and `environment` (`#/$defs/environmentBinding`). Decimal budget counters are strings; `enforcement` is `SOFT` — usage arrives post-hoc from engine-reported token events, so a cap is enforced at the next admission boundary (next model request/tool call/turn), never presented as a provider-side hard limit. Missing provider cost stays `UNKNOWN`, never zero.

- Exceeding a cap ends the run at a real boundary: `BUDGET_EXCEEDED`, `budget.exceeded` event with actual counters, owned writers stopped, receipts/history/artifacts remain readable. STOP is independently acceptable. Follow-up is refused until a guarded `EXTEND_BUDGET` command intent supplies larger caps; extension resumes as a new segment under the same requestKey/idempotency machinery.
- `writeScope` defaults to the task workspace; `extraWriteDirs`/`denyPaths` are recorded in the task binding and enforced by the layer that can actually interpose (Pi: tool-call gate returns a scope error and emits `write.scope.violation`; Codex: app-server sandbox policy plus host-side verification of reported touched paths). Which layer enforced what is recorded honestly; unenforceable deny rules are GAP, not silently ignored.
- `environment` binding is validated at admission: `requiresCommands` must resolve, `requiresDirs` must exist — a miss ends the task `BINDING_UNAVAILABLE` with no fallback to host environment. `envAllowlist` admits only validated non-secret `UPPER_SNAKE` names (`*KEY*`/`*TOKEN*`/`*SECRET*`/`*PASSWORD*`/`LLM_*`/`MODEL_*`/`WORKER_*`/`ACS_*`/`PAC_*` refused) for toolchain vars like `JAVA_HOME`; credentials never travel this path.
- `GET /worker-host/v1/tasks/{id}/audit` exposes the durable chain (subject/requestKey, intents+receipts, dispatch marks, native turn ids, effect transitions, artifact writes, state transitions) without prompt bodies. `GET .../diagnostics?redact=content` replaces message/tool bodies with `{sha256,size}` references; secrets and out-of-scope paths never appear.
- `NWF_*_MAX_ACTIVE_TASKS` caps concurrent active tasks per host; beyond it tasks are durably `QUEUED` (accepted, undispatched) and admitted FIFO as slots free. A queued intent survives restart and dispatches exactly once under the existing guard machinery — queueing never double-dispatches.

## Terminals and managed resources

Hosts that advertise a terminal surface accept `POST /local/v1/tasks/{taskId}/terminals` `{argv}` and expose `input` (base64 bytes), `output` (absolute-offset cursor over a bounded retained ring; expired cursors report truncation rather than fabricating history), `signal`, and `DELETE`. The advertised `pty` value is authoritative — a pipe implementation reports `pty:false`, and resize returns `NOT_SUPPORTED` unless a real PTY backend exists. Terminal processes run with `cwd` equal to the task workspace and are owned: task STOP kills every terminal it owns with a process-tree-drained proof (`TERMINALS_STOPPED`), and a host restart reconciles persisted live terminals to `LOST` rather than pretending they survived. Terminal lifecycle facts are durable and appear in the audit projection; input content is never logged.

Task creation may also declare `mcpServers[{id,command,args,env}]` (stdio only, literal env values — secret-shaped names and `$VAR`/`${VAR}`/`!cmd` indirection refused), `skills[{name,sourcePath}]` and `extensions[{name,sourcePath}]`. Declared skill/extension directories are copied into the task-managed profile — global or workspace discovery is not authority — and a content SHA-256 binding is persisted and re-verified on every relaunch. `mcp.bound`/`mcp.unbound` record what the engine session actually registered under each `mcp__<server>__` namespace; `skill.loaded`, `extension.loaded`, `extension.load_failed` and `extension.error` are durable facts. Unsupported surfaces remain capability gaps — nothing is silently accepted.

## Context packages, auxiliary sessions and transitions

`POST /local/v1/tasks/{taskId}/context-packages` (`#/$defs/contextPackageRequest`) freezes bounded source material into an immutable package: file sources copy current bytes (bounded per file and per package; every path must resolve inside the task workspace), `text`/`summary` sources store their literal content. The response carries `{packageId,digest}` over canonical content; `GET` list/detail expose the stored bytes. A package never changes after creation — later workspace writes cannot mutate frozen material (N16).

`POST /local/v1/tasks/{taskId}/auxiliary-sessions` (`#/$defs/auxiliarySessionRequest`) answers `question` in a **separate** engine session against an isolated workspace snapshot, optionally fed by `contextPackageIds` as fixed material and bounded by its own `budget` (independent ledger, same SOFT semantics). `GET` exposes `{state,answer}`; `stop` kills only the auxiliary session. Its output never enters the main session implicitly — `POST .../auxiliary-sessions/{id}/deliver` is the explicit, guarded delivery that injects the answer as a normal follow-up bound by the usual generation/session/turn guards (N17). Auxiliary sessions are read-only research by isolation; they are not writable collaboration.

`POST /local/v1/tasks/{taskId}/transitions` (`#/$defs/transitionRequest`) performs a **same-kernel** assignment takeover: the current owner is quiesced first (the engine's real STOP/checkpoint path, including terminal and effect settle), then a new owner epoch/generation/segment is admitted on the same native session. `targetProfile` requests the next owner cannot honour — a different engine, an incompatible model or tool set — are refused `INCOMPATIBLE_TRANSITION` with no state change. Durable transition records carry `{fromEpoch,toEpoch,fromSegment,toSegment,compatibility}`; requests bound to a superseded epoch stay fenced (N18). Cross-engine migration is not claimed here — it belongs to the AMC hand-off surface and remains a gap.

## Writable collaborations and adoption

`POST /local/v1/tasks/{taskId}/collaborations` (`#/$defs/collaborationRequest`) launches bounded writable child executions as a dependency DAG. Each child gets its own isolated checkout (snapshot copy, never the parent tree), an independent SOFT budget, and a durable child history the parent never loses. `dependsOn` gates admission — children launch only after their dependencies succeed; a failed or cancelled dependency marks dependents blocked rather than silently failing them. Stopping the parent or the collaboration stops every owned child writer with the usual proof (N19).

`POST .../collaborations/{collabId}/adoptions` (`#/$defs/adoptionRequest`) is the only path by which child output reaches the parent workspace: a finished child's candidate file set is diffed against the spawn-time baseline digest, parent-tree drift under an adopted path is a `409` conflict with per-file material and zero writes, `mode:'ALL'` or `paths` subsets adopt only clean files, an empty diff records `NO_CHANGES`, and repeat adoption is idempotent — no force-overwrite, no implicit merge (N20). Durable facts record adopted file SHA-256s, never contents.

## Execution and results

Clone a registered committed Git baseline to a host-owned workspace. Do not import or overwrite the user's uncommitted work. HOST is not a sandbox. Extensions, hooks, environment isolation, strict pause and recovery are qualified only when the real binding and boundary are tested. Interrupt-and-continue is not boundary checkpoint pause.

An artifact is actual bytes with size/hash and source ownership. Validate path boundaries and symlinks before access. A model's text naming a file is not an artifact. Cost absent from the provider remains unknown. Stop/cleanup must prove ownership of the managed process tree; host death does not by itself prove every child stopped.

Use [the shared testkit](../../../worker-testkit/parity/README.md) for measured subsets. P01–P32 and N01–N36 remain the full design target; first-slice success does not qualify the full local/managed/shared profiles.
