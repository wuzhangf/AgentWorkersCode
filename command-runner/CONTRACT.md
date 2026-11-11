# Local command runner/1

Frozen integration contract for Pi/Codex Mac command profiles. Owner: root.

Start `node workers/command-runner/main.mjs serve --state ABS_DIR --port 0`.
Environment `WORKER_COMMAND_RUNNER_TOKEN` is a random owner token (24+ chars),
never a CLI argument. Keep the child's stdin pipe open: EOF closes intake,
stops registered groups and exits. On readiness stdout emits exactly:
`COMMAND_RUNNER_READY {"url":"http://127.0.0.1:PORT","storeId":"...","bootId":"..."}`.
Node >=24.16 <25. No npm dependencies. The model/control credential must not
be inherited by this process. Only loopback, bearer auth, no browser Origin.

JSON HTTP, all routes require that owner bearer. Mutations require body
`requestKey` and matching `Idempotency-Key`, except context registration.
Identical key+body returns the original receipt; changed body/binding is 409.
Response body is the DTO directly, not an envelope. Errors `{code:"..."}`.

## Contexts

`POST /v1/contexts` body:
```json
{"contextId":"ctx-task-generation","workspace":"/absolute/task-clone","binding":{"taskId":"native-x","generation":"g","segmentId":"s","nativeSessionId":"session","currentCommandId":"cmd","executionProfile":"macos-runner-v1","engineBindingDigest":"..."},"envAllowlist":[]}
```
The host generates this ID and binding; it is not model-selected. Workspace
must exist and is canonicalized. Re-registering identical data returns the
same context; changed data refuses. Closed contexts never reopen. Each new
generation/turn binding gets a new context. `GET /v1/contexts/{id}` returns
`closed`, `activeCommands` and `unknownCommands` (arrays of command IDs),
`quiescent` (closed with both arrays empty), and `stopProof`. Binding may carry
additional fields such as nativeTurnId; the whole object is immutable.

`POST /v1/contexts/{id}/stop` body `{requestKey}` immediately closes intake,
then cancels every owned command. Response is the updated context DTO.
HTTP call can await cleanup (bounded <=15s); endpoint remains responsive.

## Commands

`POST /v1/commands` body:
```json
{"requestKey":"original-key","contextId":"ctx-task-generation","kind":"command","argv":["node","--version"],"cwd":".","timeoutMs":60000}
```
Alternatively replace `argv` with `shell:{script:"...",executable:"/bin/bash"}`.
Only `/bin/bash`, `/bin/zsh`, `/bin/sh`. Never both forms. `kind` is command,
build or test; it labels the operation, not a claim of test success. cwd is
inside the registered workspace; no arbitrary environment values from models.
Timeout default 120000, min 100, max 600000. Host environment allowlist is
validated; MODEL/LLM/WORKER/PAC/ACS credentials and secret-like names excluded.

Returns 202 accepted command DTO. `GET /v1/requests/{requestKey}` queries the
same command; absent returns 404, not permission to resend under a new key.
`GET /v1/commands/{commandId}` DTO:
```json
{"commandId":"command-uuid","requestKey":"...","contextId":"...","kind":"test","status":"SUCCEEDED","exitCode":0,"signal":null,"stdout":"bounded","stderr":"bounded","outputTruncated":false,"stopProof":{"scope":"REGISTERED_PROCESS_GROUP","groupExited":true,"arbitraryEscapedProcesses":"NOT_PROVEN"}}
```
Statuses ACCEPTED, DISPATCHING, RUNNING, SUCCEEDED, FAILED, CANCELLED,
TIMED_OUT, UNKNOWN. Terminal success requires exitCode=0 plus registered group
cleanup; nonzero is FAILED. kind=test still needs a real test report/assertion
to grant test qualification. Output is bounded (1 MiB per stream). Commands
with observed escaped descendants, monitor/setup failure or uncertain crash
remain UNKNOWN. Normal fork does not by itself mean UNKNOWN.

`GET /v1/commands/{id}/output?cursor=0` returns
`{items:[{sequence:"1",stream:"stdout",data:"..."}],cursor:"N"}`.
`POST /v1/commands/{id}/stop` body `{requestKey}` cancels only this command.
`GET /v1/identity` exposes schemaVersion command-runner/1, storeId and bootId.

## Recovery and scope

Persist original intent and DISPATCHING before releasing a gated guardian.
An independent guardian in the owned group kills that group when Runner dies;
EOF of the owner lease also closes Runner admission and cleans its groups.
Restart keeps store identity, changes boot identity and converts dispatched
nonterminal records to UNKNOWN without executing them. Never retry UNKNOWN.

The macos-runner-v1 profile is a trusted development profile whose cleanup
proof explicitly covers registered process groups. It is not an arbitrary
code sandbox or Windows Job-equivalent containment. Observable detach/escape
taints the result. Unobserved arbitrary escape and remote effects are never
claimed excluded. Hosts preserve that scope in task stopProof/capability
semantics rather than silently inheriting the file-only profile's no-fork
proof. Unknown commands block new artifact sealing/reopen.

stdout readiness and stderr diagnostics must not contain credentials, shell
environment values or raw request bodies. A single state owner is enforced.
