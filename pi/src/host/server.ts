import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { type Socket } from 'node:net';
import { fork, execFileSync, spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { mkdirSync, realpathSync, readFileSync, writeFileSync, rmSync, accessSync, statSync, constants, existsSync, readdirSync, lstatSync, copyFileSync } from 'node:fs';
import { resolve, join, dirname, basename, isAbsolute, relative } from 'node:path';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Store, type Row, id, digest, exact, now, hash, publicText } from './store.js';
import { git, gitBytes, inspectProject, artifacts, artifact } from './artifacts.js';
import { Materials, MATERIAL_CHUNK_MAX } from './materials.js';
import { WindowsJob } from '../control/ownership.js';
import { MacOwnership, MAC_QUIESCENCE_METHOD } from '../control/macos-ownership.js';
import { executionProfile, IN_PROCESS_PROFILE, RUNNER_PROFILE, isNoForkProfile, SANDBOX_EXEC, NO_FORK_POLICY } from '../control/profiles.js';
import { engineBinding } from './provenance.js';
import { CommandRunner, RunnerRequestRejected } from './runner.js';
import { MacTaskTools } from './mac-tools.js';

export const PI_VERSION = '1.4.0';
type Live = { child: ChildProcess; stopping: boolean; ready: boolean; pending: Map<string, string>; stopped?: boolean; job?: WindowsJob; mac?: MacOwnership; pauseWaiters: Map<string, (reply: any) => void>; resumeWaiters: Map<string, (reply: any) => void>; runners?: Map<string, Promise<CommandRunner>>; runnerAdmissionClosed?: boolean; runnerProof?: Row };
const quiescenceMethod = process.platform === 'win32' ? 'WINDOWS_JOB_ACTIVE_ZERO' : process.platform === 'darwin' ? MAC_QUIESCENCE_METHOD : 'OWNED_PROCESS_GROUP';
/** A host-owned pipe session bound to a task workspace. Never a PTY: no ConPTY exists
 *  here, so `pty` is always false and resize stays unsupported. Output is a bounded
 *  ring tracked by absolute byte offset; terminal processes die with their Job. */
type Terminal = { terminalId: string; taskId: string; child: ChildProcess; job?: WindowsJob; argv: string[]; createdAt: string; pty: false; written: number; chunks: Buffer[]; buffered: number; exited: boolean; exitCode: number | null; signal: string | null; closed: boolean; exitedAt?: string };
const TERMINAL_BUFFER = 262144; const MAX_TERMINALS_PER_TASK = 8; const MAX_ARGV = 64;
/** worker-events/1 SSE surface: durable-spine replay then a short poll for live
 *  rows; a comment heartbeat keeps quiet streams warm, and a slow consumer that
 *  lets ~1MiB of frames back up is dropped instead of growing host memory. */
const SSE_POLL_MS = 300; const SSE_HEARTBEAT_MS = 15000; const SSE_BUFFER_MAX = 1048576;
/** A task-scoped auxiliary research session: separate SDK child against a frozen
 *  snapshot copy of the task workspace. Isolation is by copy — nothing it does
 *  reaches the real workspace; OS-level read-only is not claimed. */
type AuxLive = Live & { taskId: string; auxSessionId: string; instruction: string };
/** A writable collaboration child: a separate SDK child against its own snapshot
 *  COPY of the task workspace. Unlike a read-only aux session, a child is expected
 *  to write — results only reach the parent workspace through explicit adoption. */
type CollabLive = Live & { taskId: string; collaborationId: string; childId: string; instruction: string };
const PACKAGE_MAX_SOURCES = 20; const PACKAGE_MAX_FILES = 20; const PACKAGE_MAX_FILE_BYTES = 262144; const PACKAGE_MAX_BYTES = 1048576; const PACKAGE_MAX_TEXT = 65536; const PACKAGE_LIMIT = 32;
const AUX_MAX_PER_TASK = 4; const AUX_MAX_PACKAGES = 8; const AUX_MAX_QUESTION = 16384; const AUX_ANSWER_MAX = 65536; const AUX_SNAPSHOT_MAX_FILES = 4096; const AUX_SNAPSHOT_MAX_BYTES = 33554432; const AUX_DEADLINE_MS = 600000;
/** Task execution window: the host default stays 600000 (the historical fixed
 *  value) unless --task-deadline-ms overrides it; a per-task create-body
 *  deadlineMs wins over both. Every value — operator or request — is bounded
 *  to [60000, 86400000] so no path can mint a sub-minute or unbounded window. */
const TASK_DEADLINE_DEFAULT_MS = 600000; const TASK_DEADLINE_MIN_MS = 60000; const TASK_DEADLINE_MAX_MS = 86400000;
/** Durable terminal verdicts. PAUSED is deliberately absent (a recoverable
 *  boundary, not an end); UNKNOWN is a reconciliation verdict, not terminal. */
const TASK_TERMINAL_STATES = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'];
const COLLAB_MAX_CHILDREN = 8; const COLLAB_MAX_PER_TASK = 4; const COLLAB_MAX_GOAL = 16384; const COLLAB_MAX_PATHS = 256; const COLLAB_MAX_ADOPTIONS = 32; const COLLAB_CANDIDATE_MAX_FILES = 8192; const COLLAB_DEADLINE_MS = 600000; const COLLAB_CHILD_ID = /^[a-z0-9-]{1,64}$/;
const COLLAB_PREFLIGHT_MAX = 32; const COLLAB_PREFLIGHT_TTL_MS = 300000; const COLLAB_PREVIEW_MAX = 16; const COLLAB_PREVIEW_TTL_MS = 300000; const CHILD_MSG_TEXT_MAX = 8192; const HANDOFF_EXPORT_MAX_LIMIT = 100; const HANDOFF_EXPORT_INLINE_TEXT = 4096; const HANDOFF_EXPORT_CHUNK_MAX = 65536;
/** Git baseline export (GAP-04): layered patch payloads and bundles share the
 *  handoff chunk route, so each bounded payload stays under one part limit —
 *  oversized content degrades to an honest TOO_LARGE fact row. The untracked
 *  manifest is a fact list; file bodies already flow through artifact rows. */
const GIT_PART_MAX = 8388608; const GIT_UNTRACKED_MAX = 256; const GIT_SUBMODULE_MAX = 32;
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
// Budget caps arrive as contract decimal strings; safe integers are tolerated.
const budgetCap = (v: any) => { const n = typeof v === 'string' && /^(0|[1-9][0-9]*)$/.test(v) ? Number(v) : v; return Number.isSafeInteger(n) && n > 0 ? n : null; };
export class PiHost {
  readonly store: Store; readonly bootId = randomUUID(); private live = new Map<string, Live>(); private auxLive = new Map<string, AuxLive>(); private collabLive = new Map<string, CollabLive>(); private terminals = new Map<string, Terminal>(); private sseConnections = new Set<Socket>(); private closed = false; private storageClosed = false; private materialsManager?: Materials;
  private get materials() { return this.materialsManager ??= new Materials(this.store, join(this.store.directory, 'materials')); }
  readonly engineBinding: ReturnType<typeof engineBinding>;
  readonly executionProfile: string;
  get profileId() { return 'MANAGED'; }
  private get quiescenceMethod() { return this.executionProfile === RUNNER_PROFILE ? 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUP_ZERO' : quiescenceMethod; }
  get executionTools() { return this.executionProfile === RUNNER_PROFILE ? ['read', 'write', 'edit', 'ask_user', 'bash', 'run_command', 'run_build', 'run_tests'] : this.executionProfile === IN_PROCESS_PROFILE ? ['read', 'write', 'edit', 'ask_user'] : ['read', 'write', 'edit', 'bash', 'ask_user']; }
  readonly server = createServer((req, res) => { void this.http(req, res); });
  async sweepTaskDeadlines(at = Date.now()) {
    for (const task of this.store.tasks()) {
      // An acknowledged boundary pause deliberately parks the session. Its
      // next RESUME already starts a fresh execution window; do not turn that
      // recoverable checkpoint into a terminal STOP while the user is away.
      if (task.quiescent || task.state === 'PAUSED' || at <= Number(task.deadlineEpochMillis)) continue;
      await this.stop(task.taskId, { requestKey: 'deadline-' + task.taskId, intent: 'STOP', generation: task.generation, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, targetCommandId: task.currentCommandId });
    }
  }
  constructor(readonly config: { state: string; token: string; executionProfile?: string; childModule?: string; faultPoint?: string; pauseDeadlineMs?: number; resumeDeadlineMs?: number; maxActiveTasks?: number; macTools?: MacTaskTools; taskDeadlineMs?: number; ssePollMs?: number; sseHeartbeatMs?: number }) {
    if (!config.token || config.token.length < 24) throw new Error('LOCAL_TOKEN_REQUIRED');
    // Fail fast before the store lock is taken: a bad operator deadline would
    // otherwise be discovered only at the first task create.
    if (config.taskDeadlineMs !== undefined && (!Number.isSafeInteger(config.taskDeadlineMs) || config.taskDeadlineMs < TASK_DEADLINE_MIN_MS || config.taskDeadlineMs > TASK_DEADLINE_MAX_MS)) throw new Error('INVALID_TASK_DEADLINE_MS');
    this.executionProfile = executionProfile(config.executionProfile);
    this.engineBinding = engineBinding(this.executionProfile);
    const existingStore = existsSync(join(resolve(config.state), 'ledger.sqlite'));
    this.store = new Store(resolve(config.state));
    const boundProfile = this.store.getMeta('execution-profile') ?? (existingStore ? 'native' : this.executionProfile);
    if (boundProfile !== this.executionProfile) { this.store.close(); throw new Error('EXECUTION_PROFILE_CHANGED'); }
    this.store.setMeta('execution-profile', boundProfile);
    for (const task of this.store.tasks()) {
      if (task.state === 'PAUSED' && task.checkpointManifest && (process.platform === 'win32' || task.quiescent && task.stopProof?.method === this.quiescenceMethod)) {
        // Windows owns kill-on-close Jobs. macOS requires a prior durable no-fork
        // teardown proof; an abrupt crash without that proof stays UNKNOWN.
        task.quiescent = true;
        for (const q of Object.values(task.inputRequests ?? {}) as Row[]) if (['PENDING', 'DISPATCH_STARTED'].includes(q.state)) q.state = 'EXPIRED';
        this.store.save(task); this.store.event(task, 'RESTART_RECONCILIATION', { reasonCode: 'BOUNDARY_CHECKPOINT_SURVIVED_RESTART' });
      } else if (task.state === 'BUDGET_EXCEEDED' && (process.platform === 'win32' || task.quiescent && task.stopProof?.method === this.quiescenceMethod)) {
        // Retain a durable terminal budget verdict only with platform-qualified
        // writer quiescence (Windows Job, or a recorded macOS no-fork proof).
        task.quiescent = true;
        this.markEffectsUnknown(task);
        for (const q of Object.values(task.inputRequests ?? {}) as Row[]) if (['PENDING', 'DISPATCH_STARTED'].includes(q.state)) q.state = 'EXPIRED';
        this.store.save(task); this.store.event(task, 'RESTART_RECONCILIATION', { reasonCode: 'BUDGET_EXCEEDED_SURVIVED_RESTART' });
      } else if (!task.quiescent) {
        task.state = task.launchState === 'PREPARING' ? 'FAILED' : 'UNKNOWN';
        // Same honest-recording rule as state(): the reconciliation verdict is
        // durably recorded now — a real terminal observation for FAILED, no
        // end time for UNKNOWN.
        if (task.state === 'FAILED') task.endedAt ??= now(); else { delete task.endedAt; delete task.completedAt; }
        task.reasonCode = task.launchState === 'PREPARING' ? 'LOCAL_LAUNCH_INTERRUPTED' : 'RESTART_RECONCILIATION_REQUIRED';
        if (this.quiescenceMethod === 'WINDOWS_JOB_ACTIVE_ZERO' && task.pid) {
          // Windows Job handles are never inherited: the dead owner's Jobs
          // closed and killed every member, so a dispatched child cannot have
          // survived its host. Same proof scope as WINDOWS_JOB_ACTIVE_ZERO —
          // brokered out-of-tree spawns stay unseen either way. The outcome
          // stays UNKNOWN (resumption impossible); only writer quiescence is
          // proven — but the task must not hold an admission slot forever.
          task.quiescent = true;
          task.stopProof ??= { ownerBootId: task.ownerBootId ?? task.bootId, pid: task.pid, method: 'WINDOWS_JOB_CLOSED_ON_HOST_DEATH', observedAt: now() };
        } else {
          // An absent child proves only the transport is gone. It does not prove escaped tools exited.
          task.quiescent = task.launchState === 'PREPARING' && !task.pid;
        }
        this.markEffectsUnknown(task);
        for (const q of Object.values(task.inputRequests ?? {}) as Row[]) if (q.state === 'PENDING') q.state = 'EXPIRED';
        this.store.save(task); this.store.event(task, 'RESTART_RECONCILIATION', { reasonCode: task.reasonCode });
      }
    }
    // Host death closed the owned Jobs; any recorded live terminal is gone.
    for (const task of this.store.tasks()) {
      if (Object.values(task.terminals ?? {}).some((t: any) => t.state === 'RUNNING')) this.update(task.taskId, t => { for (const term of Object.values(t.terminals ?? {}) as Row[]) if (term.state === 'RUNNING') { term.state = 'LOST'; term.reasonCode = 'HOST_RESTART_JOB_CLOSED'; } this.store.event(t, 'TERMINALS_LOST', {}); });
    }
    // The same Job-close proof fences auxiliary sessions: a recorded RUNNING aux
    // cannot have survived the host, so it reconciles to CANCELLED, never resumed.
    for (const task of this.store.tasks()) {
      if (Object.values(task.auxSessions ?? {}).some((a: any) => a.state === 'RUNNING')) this.store.transaction(() => this.update(task.taskId, t => { for (const aux of Object.values(t.auxSessions ?? {}) as Row[]) if (aux.state === 'RUNNING') { aux.state = 'CANCELLED'; aux.reasonCode = 'HOST_RESTART_JOB_CLOSED'; aux.finishedAt = now(); this.store.event(t, 'aux-session.completed', { auxSessionId: aux.auxSessionId, state: 'CANCELLED', reasonCode: 'HOST_RESTART_JOB_CLOSED' }); if (aux.messageStreamId) { this.streamAppend(t, aux.messageStreamId, { child: aux.auxSessionId }, { kind: 'STATUS', text: 'CANCELLED', status: 'CANCELLED' }); this.streamAppend(t, aux.messageStreamId, { child: aux.auxSessionId }, { kind: 'RETURN', status: 'CANCELLED', text: 'HOST_RESTART_JOB_CLOSED' }); } } }));
    }
    // Collaboration children are equally fenced by the Job-close proof: RUNNING
    // children reconcile to CANCELLED and PENDING children to BLOCKED (scheduling
    // never resumes silently). Completed child history stays untouched (N19).
    for (const task of this.store.tasks()) {
      if (!Object.values(task.collaborations ?? {}).some((c: any) => c.state === 'RUNNING')) continue;
      this.store.transaction(() => this.update(task.taskId, t => {
        for (const collab of Object.values(t.collaborations ?? {}) as Row[]) {
          if (collab.state !== 'RUNNING') continue;
          for (const child of Object.values(collab.children) as Row[]) {
            if (child.state === 'RUNNING') { child.state = 'CANCELLED'; child.reasonCode = 'HOST_RESTART_JOB_CLOSED'; child.finishedAt = now(); this.store.event(t, 'collaboration.child.completed', { collaborationId: collab.collaborationId, childId: child.childId, state: 'CANCELLED', reasonCode: 'HOST_RESTART_JOB_CLOSED' }); if (child.messageStreamId) { this.streamAppend(t, child.messageStreamId, { collab: collab.collaborationId, child: child.childId }, { kind: 'STATUS', text: 'CANCELLED', status: 'CANCELLED' }); this.streamAppend(t, child.messageStreamId, { collab: collab.collaborationId, child: child.childId }, { kind: 'RETURN', status: 'CANCELLED', text: 'HOST_RESTART_JOB_CLOSED' }); } }
            else if (child.state === 'PENDING') { child.state = 'BLOCKED'; child.reasonCode = 'HOST_RESTART_JOB_CLOSED'; child.finishedAt = now(); this.store.event(t, 'collaboration.child.blocked', { collaborationId: collab.collaborationId, childId: child.childId, reasonCode: 'HOST_RESTART_JOB_CLOSED' }); }
          }
          collab.state = 'CANCELLED'; collab.reasonCode = 'HOST_RESTART_JOB_CLOSED'; collab.finishedAt = now();
          this.store.event(t, 'collaboration.stopped', { collaborationId: collab.collaborationId, reasonCode: 'HOST_RESTART_JOB_CLOSED' });
        }
      }));
    }
    // A QUEUED intent was never dispatched; re-evaluate admission so it dispatches exactly once.
    setImmediate(() => { if (!this.closed) this.admitQueue(); });
  }
  identity() { return { schemaVersion: 'worker-local/1', storeId: this.store.identity.storeId, bootId: this.bootId, storeGuard: 'HEADER_V1' }; }
  private executionAvailability() {
    if (this.config.childModule) return { available: true, availability: 'AVAILABLE', reasonCode: null };
    if (!process.env.MODEL_DEFAULT_MODEL || !process.env.MODEL_BASE_URL) return { available: false, availability: 'UNCONFIGURED', reasonCode: 'MODEL_BINDING_UNCONFIGURED' };
    if (!process.env.MODEL_API_KEY) return { available: false, availability: 'UNCONFIGURED', reasonCode: 'MODEL_AUTH_UNAVAILABLE' };
    let base: URL; try { base = new URL(process.env.MODEL_BASE_URL); if (!['http:', 'https:'].includes(base.protocol)) throw new Error(); } catch { return { available: false, availability: 'UNCONFIGURED', reasonCode: 'MODEL_BASE_URL_INVALID' }; }
    if (base.hostname === 'opencode.ai' && !process.env.LLM_HEADER_x_opencode_session) return { available: false, availability: 'UNCONFIGURED', reasonCode: 'MODEL_HEADER_UNCONFIGURED' };
    return { available: true, availability: 'AVAILABLE', reasonCode: null };
  }
  private assertModelReferences(task: Row) {
    if (this.config.childModule) return;
    if (!process.env.MODEL_API_KEY) throw new Error('MODEL_AUTH_UNAVAILABLE');
    if (task.modelBinding.headerEnvKeys.some((key: string) => !process.env[key])) throw new Error('MODEL_HEADER_UNCONFIGURED');
  }
  capabilities() { return { schemaVersion: 'worker-capabilities/1', items: [
    { id: 'P01', implementation: 'GAP', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Persistent native SDK session; explicit completed-session reopen after proven STOP/restart; public finalized history; fork remains GAP', evidenceRef: 'workers/pi/test/host.test.mjs', reasonCode: 'SESSION_FORK_GAP' },
    { id: 'P05', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'SQLite full-sync acceptance; original-key replay, conflict, unknown dispatch never reissued', evidenceRef: 'workers/pi/test/host.test.mjs', reasonCode: null },
    { id: 'P06', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: this.executionProfile === RUNNER_PROFILE ? 'STOP first closes all command contexts and proves activeCommands=0 and unknownCommands=0 for registered process groups, then proves the model process no-fork/exit/group-zero. Normal command forks are supported. Proof scope is MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS; arbitrary escaped processes and remote effects remain NOT_PROVEN. Unknown commands prevent sealing/reopen. PAUSE uses the existing awaited provider/tool boundary and does not interrupt in-flight commands.' : 'STOP proves Windows Job ActiveProcesses=0; macOS uses a pre-dispatch Darwin kqueue guardian and proves quiescence only if no fork was observed and the root exited with its process group absent. Any fork permanently vetoes whole-tree proof (UNKNOWN); group cleanup alone does not prove escaped writers stopped. PAUSE is BOUNDARY_CHECKPOINT: an inline SDK extension holds the next provider request inside awaited before_provider_request and holds new tool admission inside awaited tool_call (no synthetic error enters model context), and cancels session_before_compact; a durable checkpoint manifest (session-file sha256, bindings, pending inputs/effects, last event seq) is sealed only after a quiet boundary, else the receipt stays REQUESTED/BOUNDARY_NOT_REACHED. In-flight requests/tools finish before the boundary; network-level provider internals are not gated', evidenceRef: 'workers/pi/test/pause.test.mjs', reasonCode: null },
    { id: 'P07', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'PAUSED plus a valid checkpoint and platform-qualified writer teardown survives host restart as resumable; an abrupt macOS crash without a durable teardown proof stays UNKNOWN. RESUME releases a held gate in place (same native session, new segment) or reopens the sealed session file and dispatches a durable continuation prompt (INTERRUPT_CONTINUATION for the resume-after-restart path). Interrupted/unknown dispatch is never replayed; explicit completed-session follow-up is retained', evidenceRef: 'workers/pi/test/pause.test.mjs', reasonCode: null },
    { id: 'P09', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Managed ask_user confirm/select/input tool with generation/session/turn guards', evidenceRef: 'workers/pi/test/host.test.mjs', reasonCode: null },
    { id: 'P16', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Committed Git baseline cloned to task-owned workspace; writeScope gates file-mutating tool paths (workspace + declared extraWriteDirs minus denyPaths, OS-aware realpath) inside the awaited tool_call hook — denied calls return a real tool error, run stays alive; shell-command paths are not statically attributed; HOST is not an OS sandbox', evidenceRef: 'workers/pi/test/w4.test.mjs', reasonCode: 'OS_SANDBOX_NOT_CLAIMED' },
    { id: 'P17', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'environment binding at PREPARING admission: requiresCommands resolved on host PATH, requiresDirs must exist readable — any miss is terminal BINDING_UNAVAILABLE with no host-env fallback and no child spawned; envAllowlist copies named non-secret host vars only if present; binding re-validated on every relaunch', evidenceRef: 'workers/pi/test/w4.test.mjs', reasonCode: null },
    { id: 'P21', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'SOFT token budgets enforced at real boundaries inside the awaited provider/tool gate (post-hoc ENGINE_REPORTED usage; hard provider caps unavailable): exceed → terminal BUDGET_EXCEEDED + released child; EXTEND_BUDGET resumes via session reopen as a new segment; NWF_PI_MAX_ACTIVE_TASKS admission slots queue creates durably and admit strictly in create order', evidenceRef: 'workers/pi/test/w4.test.mjs', reasonCode: null },
    { id: 'P18', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Task-scoped pipe terminals: POST /tasks/{id}/terminals spawns argv with cwd=task workspace under a dedicated owned Windows Job (process group on POSIX); stdin bytes/base64 input, offset-tracked bounded output reads (ring buffer, cursorExpired on overrun), TERM/KILL signal via taskkill /T or Job teardown, DELETE, and task STOP kills every owned terminal with the Job ActiveProcesses=0 proof. pty:false is reported honestly — no real ConPTY exists — so terminal resize is NOT_SUPPORTED', evidenceRef: 'workers/pi/test/w4b.test.mjs', reasonCode: 'PTY_NOT_CLAIMED' },
    { id: 'P19', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Task create accepts mcpServers[{id,command,args,env?}] (stdio only; literal env values — no $VAR/${VAR}/!cmd indirection, secret-shaped names rejected). The SDK MCP extension connects them inside the managed session on session_start with fixed direct tool binding (mcp__<server>__*); mcp.bound records observed tool names per server, UNVERIFIED when none appear inside the bounded wait. MCP calls flow through the tool pipeline so the writeScope/budget gate applies. HTTP/OAuth servers, codemode/deferred exposure and per-tool exposure overrides are not wired', evidenceRef: 'workers/pi/test/w4b.test.mjs; workers/pi/test/model-w4b.mjs', reasonCode: null },
    { id: 'P20', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Task create accepts skills[{name,sourcePath}] and extensions[{name,sourcePath}]: the host copies each directory into the task-managed profile and records a content sha256 binding fact (verified again on every relaunch). Skills load through the real SDK loader into the system prompt; extensions load through the real SDK jiti loader with load errors and hook registrations recorded; skill.loaded/extension.loaded/extension.error facts are durable. ~/.pi and workspace .pi discovery stay disabled — only declared managed copies load', evidenceRef: 'workers/pi/test/w4b.test.mjs; workers/pi/test/model-w4b.mjs', reasonCode: null },
    { id: 'P11', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Task-scoped context packages: file sources are realpath-verified inside the task workspace (same scope logic as writeScope, denyPaths honored — outside is PACKAGE_SOURCE_OUT_OF_SCOPE), text/summary sources are literal; all sources are frozen byte copies at creation (bounded 256KiB/file, 1MiB/package, 20 files) with a canonical sha256 digest re-verified on read; durable packages never change after later workspace writes (N16). Observation snapshots/fixed-version material routing beyond aux-session fixed material remain narrower', evidenceRef: 'workers/pi/test/w5a.test.mjs', reasonCode: null },
    { id: 'P12', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Auxiliary read-only research sessions: a separate SDK child answers a question against a frozen snapshot COPY of the task workspace (isolation by copy — nothing it does reaches the real workspace; OS read-only enforcement is not claimed) plus declared context-package fixed material; independent SOFT budget ledger (never the parent task budget), max 4 per task and a small host-wide running bound (NWF_PI_MAX_ACTIVE_AUX, default 2); aux output never enters the main session until POST .../deliver injects it as a normal guarded NATIVE_FOLLOW_UP (N17). Writable collaboration is not claimed', evidenceRef: 'workers/pi/test/w5a.test.mjs; workers/pi/test/model-w5a.mjs', reasonCode: 'COPY_ISOLATION_NOT_OS_READONLY' },
    { id: 'P13', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Same-kernel assignment transition: compatibility-checked ({engineId} must be pi, model must equal the binding, toolAllowlist must name active tools — else INCOMPATIBLE_TRANSITION with no state change); the current owner quiesces via the W3 boundary checkpoint (or an already-sealed PAUSED/completed session), the owned child is Job-drained, then a new ownerEpoch/generation/segment is admitted on the SAME native session via sealed session-file reopen; stale-epoch requests stay fenced by the generation/session/command guards. Cross-engine hot migration is AMC territory and stays GAP', evidenceRef: 'workers/pi/test/w5a.test.mjs; workers/pi/test/model-w5a.mjs', reasonCode: 'CROSS_ENGINE_GAP' },
    { id: 'P14', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Writable collaboration DAG: POST /tasks/{id}/collaborations spawns bounded child executions (<=8 children, <=4 active collaborations per task, host-wide running bound NWF_PI_MAX_ACTIVE_COLLAB_CHILDREN default 4) as isolated snapshot COPIES under state/collab/. dependsOn gates scheduling — a child launches only after every dependency SUCCEEDED; a dependency failure/cancel marks dependents BLOCKED (partial failure, the collaboration itself completes). Each child has its own SOFT budget ledger and writeScope gated to its checkout; parent STOP and host close drain running children with the owned-Job proof; restart reconciles RUNNING children to CANCELLED and keeps completed child history. Copy-based isolation — no OS read-only enforcement; no AMC/cross-core wiring (P28 stays GAP)', evidenceRef: 'workers/pi/test/w5b.test.mjs', reasonCode: 'COPY_ISOLATION_NOT_OS_READONLY' },
    { id: 'P15', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Explicit result adoption: POST .../collaborations/{id}/adoptions {childId, mode:ALL|FILES, paths?} diffs a SUCCEEDED child checkout against its spawn baseline and writes only clean paths into the parent workspace. Parent drift under an adopted path since baseline returns a CONFLICT material (per-file sha256 list, nothing written); empty diffs and identical re-adoptions record NO_CHANGES; deletions adopt as removals; adoption facts record file sha256s — never contents. Adoption is file-level, not a git merge', evidenceRef: 'workers/pi/test/w5b.test.mjs; workers/pi/test/model-w5b.mjs', reasonCode: 'FILE_LEVEL_ADOPTION' },
    { id: 'P23', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Ordered audit view (subject, command receipts, dispatch marks, native turn ids, effect transitions, artifact writes, state transitions — no prompt bodies) plus diagnostics view replacing message/tool bodies with sha256+size refs and scrubbing secrets/absolute host paths', evidenceRef: 'workers/pi/test/w4.test.mjs', reasonCode: null },
    { id: 'P24', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'child-messages/1 durable per-stream capture for collaboration children and auxiliary sessions: ASSIGNMENT/STATUS/OUTPUT_DELTA/ASSISTANT_MESSAGE/TOOL_CALL/TOOL_RESULT/RETURN rows with decimal-string per-stream sequence (monotonic across restart), before/cursor pagination (exclusive, limit<=100); coverage is FULL_SINCE_CAPTURE only after durable capture begins (PARTIAL when page items carry truncated bodies) — legacy aggregate output is exposed as SUMMARY_ONLY text, never split into fake turns; streams never recorded report UNAVAILABLE', evidenceRef: 'workers/pi/test/w5c.test.mjs', reasonCode: null },
    { id: 'P25', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Collaboration preflight: POST .../collaborations/preflight runs the full create-validation segment (ids, goals, dependency DAG, per-task/host limits, packages, budget, writeScope) without creating a collaboration row or spawning a child; returns preflightId + planDigest + sourceBindings + allowedScopes + budgetTerms + expiresAt. Final create may bind expectedPreflightId/expectedPlanDigest — stale/divergent plans conflict PREFLIGHT_STALE; unbound create stays allowed for legacy clients. Records are capped (32/task, TTL-pruned)', evidenceRef: 'workers/pi/test/w5c.test.mjs', reasonCode: null },
    { id: 'P26', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Read-only adoption preview: POST .../collaborations/{id}/previews runs the identical candidate/plan/conflict classification as apply (per-path candidate/baseline/parent sha256, write/delete/noop/conflict actions) and freezes it under previewId + planDigest with a TTL — nothing is written to the parent workspace. Adopt may bind expectedPreviewId/expectedPreviewDigest; stale or divergent previews conflict PREVIEW_STALE and apply still re-verifies candidate bytes against the live parent immediately before writing', evidenceRef: 'workers/pi/test/w5c.test.mjs', reasonCode: null },
    { id: 'P27', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Source seal + handoff export: POST .../source-seals first requires the main task quiescent — durable terminal verdict or recorded quiescence proof with no dispatch left, else TASK_NOT_QUIESCENT — then drains collaboration children and aux sessions and raises a durable write fence (every mutation path rejects SOURCE_SEALED; STOP and reads stay open; seal proof binds taskId/generation/segmentId/ownerEpoch/target/material sha256). GET .../handoff-export is a paginated read-only projection (messages, aux answers, collaboration report refs, artifact manifest, factual events) with exportDigest and body textRefs readable via .../handoff-export/{part}/chunks/{sha256}/{offset} — part is a closed whitelist, never a path. targetStart is reported NOT_STARTED: export acceptance is not handoff completion', evidenceRef: 'workers/pi/test/w5c.test.mjs', reasonCode: null },
    { id: 'P29', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Project-scoped material uploads: POST /projects/{p}/material-uploads registers an upload (allowlisted image/png|jpeg and UTF-8 text/markdown/log/json plus GIT_BUNDLE/PATCH_BINARY for handoff delivery, per-kind size ceilings); raw 256KiB-bounded PUT chunks are idempotent by offset+digest and contiguous at nextOffset; seal verifies byte count + whole sha256 and issues materialRef; delete revokes only unbound uploads (BOUND/CONSUMED refuse). messages[] materials requires a SEALED ref with matching sha256 and task/createKey binding; REFERENCE_TEXT bytes are injected into the model input; IMAGE is refused MODEL_IMAGE_INPUT_UNSUPPORTED because the Pi image-input path is unverified; attachments on non-prompt intents refuse MATERIALS_UNSUPPORTED_INTENT', evidenceRef: 'workers/pi/test/w5c.test.mjs', reasonCode: 'MODEL_IMAGE_INPUT_UNVERIFIED' },
    { id: 'P30', implementation: 'IMPLEMENTED', qualification: 'PARTIAL', availability: 'AVAILABLE', semantics: 'Frozen execution-policy projection (executionPolicy on task detail + engines): profile, honest sandbox scope (native host = UNDECLARED, never an OS sandbox), approval/tool-admission facts, effective writeScope, environment binding, selected policyPreset, frozen:true. Preflight/create accept policyPresetId from the managed preset table (unknown ids refuse POLICY_PRESET_UNKNOWN); presets narrow the declared toolAllowlist at launch and never widen it; running-task policy changes are not implemented', evidenceRef: 'workers/pi/test/w5c.test.mjs', reasonCode: 'RUNNING_POLICY_IMMUTABLE' },
  ].map(item => ({ schemaVersion: 'worker-capability/1', ...item, ...(!this.executionAvailability().available && item.availability === 'AVAILABLE' && item.id !== 'P05' ? { availability: 'UNCONFIGURED', reasonCode: this.executionAvailability().reasonCode } : {}) })) }; }
  manifest() {
    const { files, ...provenance } = this.engineBinding.provenance;
    const summary = { ...provenance, fileCount: files.length, filesDigest: digest(files), nodeExecutableSha256: files.find(f => f.name === 'node/executable')!.sha256, sdkEntrySha256: files.find(f => f.name === 'sdk/dist/index.js')!.sha256, npmLockSha256: files.find(f => f.name === 'worker/package-lock.json')!.sha256 };
    return { ...this.store.identity, schemaVersion: 'worker-host/1', bootId: this.bootId, currentAvailability: this.executionAvailability().availability, reasonCode: this.executionAvailability().reasonCode, executionProfile: this.executionProfile, executionTools: this.executionTools, ...(this.executionProfile === RUNNER_PROFILE ? { executionScope: { modelProcess: 'DENY_PROCESS_FORK', commands: 'REGISTERED_PROCESS_GROUP', arbitraryEscapedProcesses: 'NOT_PROVEN', environment: 'TASK_EXPLICIT_NON_SECRET_ALLOWLIST', runnerState: 'TASK_BINDING_PRIVATE' } } : {}), engine: { id: 'pi', version: PI_VERSION, engineBindingDigest: this.engineBinding.digest, provenance: summary }, contracts: ['worker-host/1', 'worker-local/1'], capabilities: this.capabilities().items, supportedPaths: ['/local/v1', '/worker-host/v1/manifest', '/worker-host/v1/capabilities', '/worker-host/v1/operations/lookup', '/worker-host/v1/tasks/{taskId}/audit', '/worker-host/v1/tasks/{taskId}/diagnostics'] };
  }
  register(projectId: string, project: string) {
    const observed = inspectProject(id(projectId), project); const existing = this.store.getMeta('project:' + projectId);
    if (existing) { let same = false; try { same = realpathSync(existing.path) === observed.path; } catch {} if (!same) throw new Error('PROJECT_ALIAS_ALREADY_BOUND'); }
    this.store.setMeta('project:' + projectId, observed); return observed;
  }
  project(projectId: string) { const row = this.store.getMeta('project:' + id(projectId)); if (!row) throw new Error('PROJECT_NOT_REGISTERED'); return inspectProject(projectId, row.path); }
  projects() { return this.store.db.prepare("SELECT value FROM meta WHERE key LIKE 'project:%'").all().map(r => { const p = JSON.parse(r.value as string); return this.project(p.projectId); }); }
  /** Managed policy presets (P2): stable ids that freeze a launch-time parameter
   *  set. They only affect tasks created with them — a running task never
   *  mutates its frozen policy (§10.3: "下个任务生效"). */
  private policyPresets(): Record<string, Row> {
    const readOnly = this.executionTools.filter(t => ['read', 'ls', 'grep', 'glob', 'ask_user'].includes(t));
    return {
      'standard': { presetId: 'standard', revision: 'pi-policy-preset/1', engine: 'pi', projectScope: 'ANY', effectiveRange: 'NEW_TASKS', escalationNote: 'Managed default toolset for the execution profile; human approval via ask_user; file-mutating tools gated by declared writeScope; shell command paths are not statically attributed and the host is not an OS sandbox', revocable: false, frozenParameters: { toolAllowlist: null } },
      'read-only': { presetId: 'read-only', revision: 'pi-policy-preset/1', engine: 'pi', projectScope: 'ANY', effectiveRange: 'NEW_TASKS', escalationNote: 'Model-visible tools narrowed at session init to non-mutating tools only (' + readOnly.join(',') + '); the gate still enforces declared tools and this is not an OS sandbox', revocable: false, frozenParameters: { toolAllowlist: readOnly } },
    };
  }
  private policyPreset(spec: any): Row | null {
    if (spec === undefined) return null;
    if (typeof spec !== 'string') throw new Error('POLICY_PRESET_UNKNOWN');
    const preset = this.policyPresets()[spec]; if (!preset) throw new Error('POLICY_PRESET_UNKNOWN');
    return preset;
  }
  preflight(body: Row) {
    exact(body, ['projectId', 'engineId', 'policyPresetId']); if (body.engineId !== 'pi') throw new Error('ENGINE_NOT_REGISTERED'); const p = this.project(body.projectId);
    const policyPreset = this.policyPreset(body.policyPresetId);
    const result = { projectId: p.projectId, engineId: 'pi', engineVersion: PI_VERSION, executableDigest: this.engineBinding.digest, engineBindingDigest: this.engineBinding.digest, engineProvenanceKind: this.engineBinding.provenance.kind, profileId: this.profileId, executionProfile: this.executionProfile, profileHome: null, loadsUserExtensions: false, headRevision: p.headRevision, hasUncommittedChanges: p.hasUncommittedChanges, workingCopyPolicy: 'COMMITTED_BASE_ONLY', executionModes: ['WORKSPACE_TASK', 'CONTEXT_ONLY'], policyPreset };
    return { ...result, optionsRevision: digest(result) };
  }
  detail(task: Row) {
    const live = this.live.get(task.taskId); const connected = !!live && !live.stopping && live.child.connected;
    const queue = task.launchState === 'QUEUED' ? { ordinal: task.queueOrdinal, position: this.store.tasks().filter(t => t.launchState === 'QUEUED' && !t.intakeClosed).sort((a, b) => a.queueOrdinal - b.queueOrdinal).findIndex(t => t.taskId === task.taskId) + 1, activeTasks: this.store.tasks().filter(t => !t.quiescent).length, maxActiveTasks: this.maxActive() } : undefined;
    const lastEvent = this.store.lastEvent(task.taskId);
    const legacyDeadlineStop = !task.reasonCode && task.state === 'CANCELLED' && this.store.operation('deadline-' + task.taskId)?.state === 'APPLIED';
    return { ...task, reasonCode: task.reasonCode ?? (legacyDeadlineStop ? 'TASK_DEADLINE_EXCEEDED' : undefined), goal: undefined, output: undefined, inputRequests: undefined, publicMessages: undefined, pending: undefined, queue, storeId: this.store.identity.storeId, bootId: this.bootId, lastEventAt: lastEvent?.occurredAt ?? null, lastEventKind: lastEvent?.kind ?? null,
      contextPackages: Object.values(task.contextPackages ?? {}).map((p: any) => this.packageMeta(p)), auxiliarySessions: Object.values(task.auxSessions ?? {}).map((a: any) => this.auxPublic(a)), transitions: task.transitions ?? [], collaborations: Object.values(task.collaborations ?? {}).map((c: any) => this.collabPublic(c)),
      sourceCapabilities: this.sourceCapabilities(task), executionPolicy: this.executionPolicyProjection(task),
      capabilities: { stop: (task.launchState === 'QUEUED' && !task.intakeClosed) || (!task.intakeClosed && !task.quiescent && (connected || ['PREPARING', 'RESTORING'].includes(task.launchState))), steer: connected && task.state === 'RUNNING', followUp: (connected && task.state === 'SUCCEEDED') || this.canReopen(task), completedSessionFollowUpAfterRestart: this.canReopen(task), pause: this.canPause(task, live), resume: this.canResume(task, live), extendBudget: task.state === 'BUDGET_EXCEEDED', observeAfterRestart: true, continueAfterRestart: task.state === 'PAUSED' && this.checkpointValid(task) } };
  }
  /** source-capabilities/1: source facts only. The Host reports what the source
   *  itself can do — DENIED/mobile-grant verdicts stay with the PAC, which
   *  computes device permission independently (D §4.2). */
  private sourceCapabilities(task: Row) {
    const sealed = !!task.sealedFor || !!task.sealRequested;
    const contextOnly = task.executionMode === 'CONTEXT_ONLY';
    const blocked = sealed ? 'SOURCE_SEALED' : task.intakeClosed ? 'TASK_INTAKE_CLOSED' : null;
    const a = (state: string, reasonCode: string | null = null, extra: Row = {}) => ({ state, reasonCode, ...extra });
    const write = (available: Row) => contextOnly ? a('UNSUPPORTED', 'CONTEXT_ONLY_OPERATION_DENIED') : blocked ? a('BLOCKED_BY_STATE', blocked) : available;
    return { schemaVersion: 'source-capabilities/1', revision: digest({ generation: task.generation, segmentId: task.segmentId, ownerEpoch: String(task.ownerEpoch ?? '1') }), observedAt: now(), actions: {
      'sideQuestion.create': write(a('AVAILABLE', null, { limits: { maxQuestionBytes: AUX_MAX_QUESTION, maxSessionsPerTask: AUX_MAX_PER_TASK, maxContextPackages: AUX_MAX_PACKAGES, deadlineMs: AUX_DEADLINE_MS }, semantics: 'ISOLATED_SNAPSHOT_COPY_READ_ONLY_SINGLE_TURN' })),
      'collaboration.create': write(a('AVAILABLE', null, { limits: { maxChildren: COLLAB_MAX_CHILDREN, maxActivePerTask: COLLAB_MAX_PER_TASK, hostPool: this.maxCollabChildren(), maxGoalBytes: COLLAB_MAX_GOAL, deadlineMs: COLLAB_DEADLINE_MS }, semantics: 'ISOLATED_CHECKOUT_DAG_SOFT_BUDGET' })),
      'collaboration.preflight': write(a('AVAILABLE', null, { limits: { maxChildren: COLLAB_MAX_CHILDREN, ttlMs: COLLAB_PREFLIGHT_TTL_MS, maxPerTask: COLLAB_PREFLIGHT_MAX }, semantics: 'VALIDATE_ONLY_NO_SPAWN_FROZEN_PLAN' })),
      'childMessages.read': a('AVAILABLE', null, { limits: { defaultLimit: 50, maxLimit: 100 }, semantics: 'CAPTURED_PUBLIC_CHILD_STREAM_BEFORE_CURSOR_EXCLUSIVE' }),
      'adoption.preview': write(a('AVAILABLE', null, { limits: { maxPaths: COLLAB_MAX_PATHS, maxPerTask: COLLAB_PREVIEW_MAX, ttlMs: COLLAB_PREVIEW_TTL_MS }, semantics: 'READ_ONLY_CANDIDATE_DIFF_FROZEN_DIGEST' })),
      'adoption.apply': write(a('AVAILABLE', null, { limits: { maxPaths: COLLAB_MAX_PATHS, maxAdoptions: COLLAB_MAX_ADOPTIONS }, semantics: 'FILE_LEVEL_WRITE_CONFLICT_ZERO_WRITE' })),
      'handoff.export': a('AVAILABLE', null, { limits: { defaultLimit: 50, maxLimit: HANDOFF_EXPORT_MAX_LIMIT, chunkBytes: HANDOFF_EXPORT_CHUNK_MAX }, semantics: 'READ_ONLY_PUBLIC_PROJECTION_NO_ARBITRARY_PATHS' }),
      'handoff.seal': task.sealedFor ? a('BLOCKED_BY_STATE', 'SOURCE_ALREADY_SEALED') : this.sealQuiescent(task) ? a('AVAILABLE', null, { semantics: 'DRAINS_COLLAB_CHILDREN_AND_AUX_THEN_WRITE_FENCE' }) : a('BLOCKED_BY_STATE', 'TASK_NOT_QUIESCENT'),
      'materials.upload': blocked ? a('BLOCKED_BY_STATE', blocked) : a('AVAILABLE', null, { limits: this.materials.mediaLimits(), semantics: 'PROJECT_SCOPED_CHUNKED_SEAL' }),
      'message.materials': a('AVAILABLE', null, { limits: { maxPerMessage: this.materials.maxPerMessage(), roles: ['IMAGE', 'REFERENCE_TEXT'] }, semantics: 'REFERENCE_TEXT_INJECTED_IMAGE_REJECTED_MODEL_INPUT_UNVERIFIED' }),
      'voice.transcribe': a('NOT_INTEGRATED', 'TRANSCRIPTION_OWNED_BY_PAC', { semantics: 'TRANSCRIPTION_IS_A_PAC_SERVICE_NOT_A_SOURCE_CAPABILITY' }),
      'policy.read': a('AVAILABLE', null, { semantics: 'FROZEN_EFFECTIVE_PROJECTION' }),
      'policy.presets': a('AVAILABLE', null, { limits: { presets: Object.keys(this.policyPresets()) }, semantics: 'NEW_TASK_PRESET_ONLY_RUNNING_TASK_IMMUTABLE' }),
      'stop.affectedChildren': a('AVAILABLE', null, { semantics: 'PARENT_STOP_DRAINS_COLLAB_CHILDREN_AUX_LEFT_RUNNING_SOURCE_SEAL_DRAINS_ALL' }),
    } };
  }
  /** Read-only P1 execution-policy projection: what is actually frozen for this
   *  task. Sandbox is reported honestly — a native-profile host is not an OS
   *  sandbox; only a bound no-fork execution profile claims process denial. */
  private executionPolicyProjection(task: Row) {
    const sandbox = this.executionProfile === RUNNER_PROFILE ? 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS' : isNoForkProfile(this.executionProfile) ? 'DENY_PROCESS_FORK' : 'UNDECLARED';
    return { profile: this.executionProfile, profileId: this.profileId, sandbox, osSandboxClaimed: isNoForkProfile(this.executionProfile), approvalPolicy: 'HUMAN_VIA_ASK_USER_TOOL', toolAdmission: 'AWAITED_GATE_CHECK', writeScope: { effective: task.writeScope ?? null, default: 'WORKSPACE_ONLY', enforcement: 'FILE_MUTATING_TOOLS_GATE_SHELL_PATHS_NOT_ATTRIBUTED' }, environmentBinding: task.environmentBinding ?? null, toolAllowlist: task.toolAllowlist ?? null, policyPreset: task.policyPreset ?? null, frozen: true, semanticsNote: 'Frozen at launch; running-task policy changes are not implemented (new presets affect future tasks only)' };
  }
  private update(taskId: string, fn: (task: Row) => void) { const task = this.store.task(taskId); fn(task); this.store.save(task); return task; }
  /** Effective execution window for a task: a bounded per-task deadlineMs set
   *  at admission wins; otherwise the host --task-deadline-ms value applies
   *  (600000 preserves the original fixed window). Reused wherever a fresh
   *  execution window opens — create, reopen, resume, extend, transition. */
  private taskDeadlineMs(task?: Row) { return task?.deadlineMs ?? this.config.taskDeadlineMs ?? TASK_DEADLINE_DEFAULT_MS; }
  private state(task: Row, state: string) {
    task.state = state; task.turns[task.currentCommandId] = { state, nativeTurnId: task.nativeTurnId };
    // Timing fields carry real recorded facts only. The first terminal-state
    // transition of an execution episode stamps endedAt (completedAt for a
    // SUCCEEDED observation); a live-state transition clears both so a
    // reopened/resumed task never reports a stale end time. runStartedAt is
    // stamped separately at the first dispatched/observed turn and is
    // first-write-wins across the task's whole history.
    if (TASK_TERMINAL_STATES.includes(state)) { task.endedAt ??= now(); if (state === 'SUCCEEDED') task.completedAt ??= task.endedAt; }
    else { delete task.endedAt; delete task.completedAt; }
  }
  private replay(body: Row) { const previous = this.store.operation(id(body.requestKey)); if (previous && previous.bodyDigest !== digest(body)) throw new Error('IDEMPOTENCY_CONFLICT'); return previous; }
  create(body: Row) {
    exact(body, ['requestKey', 'projectId', 'engineId', 'optionsRevision', 'instruction', 'budget', 'writeScope', 'environment', 'mcpServers', 'skills', 'extensions', 'executionMode', 'policyPresetId', 'deadlineMs']); const prior = this.replay(body); if (prior) return prior;
    const mode = body.executionMode ?? 'WORKSPACE_TASK';
    if (!['WORKSPACE_TASK', 'CONTEXT_ONLY'].includes(mode)) throw new Error('EXECUTION_MODE_UNSUPPORTED');
    if (mode === 'CONTEXT_ONLY' && ['writeScope','environment','mcpServers','skills','extensions','policyPresetId'].some(k => body[k] !== undefined)) throw new Error('CONTEXT_ONLY_RESOURCE_DENIED');
    if (this.closed) throw new Error('LOCAL_HOST_CLOSED');
    if (typeof body.instruction !== 'string' || !body.instruction.trim() || Buffer.byteLength(body.instruction) > 16384) throw new Error('INVALID_INSTRUCTION');
    const checked = this.preflight({ projectId: body.projectId, engineId: body.engineId, policyPresetId: body.policyPresetId }); if (checked.optionsRevision !== body.optionsRevision) throw new Error('PREFLIGHT_CHANGED');
    const availability = this.executionAvailability(); if (!availability.available) throw new Error(availability.reasonCode!);
    const budget = this.validateBudget(body.budget);
    const deadlineMs = this.validateDeadlineMs(body.deadlineMs);
    const writeScope = this.validateWriteScope(body.writeScope);
    const environment = this.validateEnvironment(body.environment);
    const mcpServers = this.validateMcpServers(body.mcpServers);
    const skills = this.validateDeclaredDirs(body.skills, 'skill');
    const extensions = this.validateDeclaredDirs(body.extensions, 'extension');
    const model = { name: process.env.MODEL_DEFAULT_MODEL ?? 'fixture-model', baseUrl: process.env.MODEL_BASE_URL ?? 'http://127.0.0.1:1', headerEnvKeys: Object.keys(process.env).filter(k => k.startsWith('LLM_HEADER_')), api: 'openai-completions', contextWindow: 131072, maxTokens: 8192, parameterQualification: 'OPERATOR_CONFIGURED_COMPATIBILITY_ASSUMPTIONS' };
    if (!/^https?:\/\//.test(model.baseUrl)) throw new Error('MODEL_BASE_URL_REQUIRED');
    const taskId = 'native-' + randomUUID(); const queued = this.store.tasks().filter(t => !t.quiescent).length >= this.maxActive();
    const effectiveDeadlineMs = deadlineMs ?? this.taskDeadlineMs();
    const task: Row = { taskId, executionMode: mode, engineId: 'pi', engineVersion: PI_VERSION, projectId: body.projectId, baseCommit: checked.headRevision, profileId: this.profileId, executionProfile: this.executionProfile, workspace: join(this.store.directory, 'workspaces', taskId), sessionDir: join(this.store.directory, 'sessions', taskId), goal: body.instruction, modelBinding: model, configuredModel: model.name, configuredProvider: 'worker-model', generation: randomUUID(), nativeSessionId: null, nativeTurnId: null, segmentId: randomUUID(), currentCommandId: body.requestKey, ownerEpoch: '1', launchState: queued ? 'QUEUED' : 'PREPARING', state: queued ? 'QUEUED' : 'PREPARING', quiescent: queued, createdAt: now(), deadlineMs: effectiveDeadlineMs, deadlineEpochMillis: String(Date.now() + effectiveDeadlineMs), turns: {}, inputRequests: {}, publicMessages: [], output: '', usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, usageLedger: [], budget, writeScope, environment, mcpServers, declaredSkills: skills, declaredExtensions: extensions, bootId: this.bootId };
    if (checked.policyPreset) {
      task.policyPreset = { presetId: checked.policyPreset.presetId, revision: checked.policyPreset.revision, effectiveRange: checked.policyPreset.effectiveRange, frozenAt: now() };
      if (Array.isArray(checked.policyPreset.frozenParameters?.toolAllowlist)) task.toolAllowlist = checked.policyPreset.frozenParameters.toolAllowlist;
    }
    const macBinding = this.config.macTools?.binding(body.requestKey);
    if (macBinding) {
      if (mode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_RESOURCE_DENIED');
      if (!isNoForkProfile(this.executionProfile) || (task.environment?.envAllowlist ?? []).includes(macBinding.credentialEnvironment)) throw new Error('MAC_TOOL_PROFILE_REQUIRED');
      task.macToolBinding = macBinding;
    }
    task.engineBindingDigest = this.engineBinding.digest; let receipt: Row;
    this.store.transaction(() => {
      if (queued) { const ordinal = (this.store.getMeta('queue:ordinal') ?? 0) + 1; this.store.setMeta('queue:ordinal', ordinal); task.queueOrdinal = ordinal; }
      receipt = this.store.accept(body.requestKey, taskId, body, { state: 'ACCEPTED', launchState: task.launchState, engineId: 'pi', engineVersion: PI_VERSION, projectId: body.projectId });
      this.state(task, task.state); this.store.save(task); this.store.event(task, 'ACCEPTED');
      if (queued) this.store.event(task, 'QUEUED', { queueOrdinal: task.queueOrdinal });
    });
    if (!queued) setImmediate(() => { void this.launch(taskId); }); return receipt!;
  }
  private maxActive() { const n = this.config.maxActiveTasks ?? Number(process.env.NWF_PI_MAX_ACTIVE_TASKS ?? 4); return Number.isFinite(n) ? Math.max(1, Math.floor(n)) : 4; }
  private admitQueue() {
    if (this.closed) return;
    for (;;) {
      const tasks = this.store.tasks();
      if (tasks.filter(t => !t.quiescent).length >= this.maxActive()) return;
      const next = tasks.filter(t => t.launchState === 'QUEUED' && !t.intakeClosed && t.state === 'QUEUED').sort((a, b) => a.queueOrdinal - b.queueOrdinal)[0];
      if (!next) return;
      this.store.transaction(() => { const t = this.store.task(next.taskId); if (t.launchState !== 'QUEUED' || t.intakeClosed) return; t.launchState = 'PREPARING'; t.quiescent = false; t.deadlineEpochMillis = String(Date.now() + this.taskDeadlineMs(t)); this.state(t, 'PREPARING'); this.store.save(t); this.store.event(t, 'ADMITTED', { queueOrdinal: t.queueOrdinal }); });
      setImmediate(() => { void this.launch(next.taskId); });
    }
  }
  private validateBudget(spec: any) {
    if (spec === undefined) return undefined;
    exact(spec, ['maxTotalTokens', 'maxInputTokens', 'maxOutputTokens', 'maxCostMicroUsd', 'enforcement']);
    // SOFT only: usage is post-hoc ENGINE_REPORTED from SDK events; hard provider-side caps are unavailable.
    if (spec.enforcement !== 'SOFT') throw new Error('BUDGET_ENFORCEMENT_UNSUPPORTED');
    const caps: Row = {};
    for (const key of ['maxTotalTokens', 'maxInputTokens', 'maxOutputTokens', 'maxCostMicroUsd']) if (spec[key] !== undefined) { const v = budgetCap(spec[key]); if (v == null) throw new Error('INVALID_BUDGET'); caps[key] = v; }
    if (!Object.keys(caps).length) throw new Error('INVALID_BUDGET');
    return { caps, enforcement: 'SOFT', basis: 'ENGINE_REPORTED_POST_HOC', costCoverage: 'UNKNOWN', costEnforcement: caps.maxCostMicroUsd != null ? 'UNEVALUATED_COST_UNKNOWN' : undefined, reservedAt: now() };
  }
  /** Optional per-task execution window. Same contract convention as budget
   *  caps: a safe-integer millisecond count or its decimal string; bounded so
   *  a request can never mint a sub-minute or unbounded window. Wins over the
   *  host --task-deadline-ms default for this task's whole lifetime. */
  private validateDeadlineMs(spec: any) {
    if (spec === undefined) return undefined;
    const n = typeof spec === 'string' && /^(0|[1-9][0-9]*)$/.test(spec) ? Number(spec) : spec;
    if (!Number.isSafeInteger(n) || n < TASK_DEADLINE_MIN_MS || n > TASK_DEADLINE_MAX_MS) throw new Error('INVALID_DEADLINE_MS');
    return n;
  }
  private validateWriteScope(spec: any) {
    if (spec === undefined) return undefined;
    exact(spec, ['extraWriteDirs', 'denyPaths']);
    const readPaths = (value: any) => { if (value === undefined) return []; if (!Array.isArray(value) || value.length > 32 || value.some((p: any) => typeof p !== 'string' || !isAbsolute(p))) throw new Error('INVALID_WRITE_SCOPE'); return value.map((p: string) => this.realpathBestEffort(p)); };
    return { scopeId: 'wscope-' + randomUUID().slice(0, 8), extraWriteDirs: readPaths(spec.extraWriteDirs), denyPaths: readPaths(spec.denyPaths), declaredAt: now() };
  }
  private validateEnvironment(spec: any) {
    if (spec === undefined) return undefined;
    exact(spec, ['requiresCommands', 'requiresDirs', 'envAllowlist']);
    const out: Row = {};
    if (spec.requiresCommands !== undefined) { if (!Array.isArray(spec.requiresCommands) || spec.requiresCommands.length > 32 || spec.requiresCommands.some((c: any) => typeof c !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(c))) throw new Error('INVALID_ENVIRONMENT'); out.requiresCommands = spec.requiresCommands; }
    if (spec.requiresDirs !== undefined) { if (!Array.isArray(spec.requiresDirs) || spec.requiresDirs.length > 32 || spec.requiresDirs.some((d: any) => typeof d !== 'string' || !isAbsolute(d))) throw new Error('INVALID_ENVIRONMENT'); out.requiresDirs = spec.requiresDirs; }
    if (spec.envAllowlist !== undefined) {
      if (!Array.isArray(spec.envAllowlist) || spec.envAllowlist.length > 64) throw new Error('INVALID_ENVIRONMENT');
      for (const name of spec.envAllowlist) {
        if (typeof name !== 'string' || !/^[A-Z][A-Z0-9_]{0,63}$/.test(name)) throw new Error('INVALID_ENVIRONMENT');
        if (/KEY|TOKEN|SECRET|PASSWORD/.test(name) || /^(LLM|MODEL|WORKER|ACS|PAC)_/.test(name)) throw new Error('ENV_ALLOWLIST_DENIED');
      }
      out.envAllowlist = spec.envAllowlist;
    }
    return out;
  }
  private validateMcpServers(spec: any) {
    if (spec === undefined) return undefined;
    if (!Array.isArray(spec) || spec.length > 8) throw new Error('INVALID_MCP_SERVERS');
    const seen = new Set<string>();
    return spec.map((entry: any) => {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new Error('INVALID_MCP_SERVERS');
      exact(entry, ['id', 'command', 'args', 'env']);
      if (typeof entry.id !== 'string' || !/^[a-z][a-z0-9-]{0,63}$/.test(entry.id)) throw new Error('INVALID_MCP_SERVERS');
      const namespace = 'mcp__' + entry.id.replace(/[^A-Za-z0-9_]/g, '_');
      if (seen.has(namespace)) throw new Error('MCP_SERVER_NAME_CLASH'); seen.add(namespace);
      if (typeof entry.command !== 'string' || !entry.command.trim() || entry.command.length > 1024 || /[\x00\r\n]/.test(entry.command)) throw new Error('INVALID_MCP_SERVERS');
      if (entry.args !== undefined && (!Array.isArray(entry.args) || entry.args.length > 64 || entry.args.some((a: any) => typeof a !== 'string' || a.length > 1024 || a.includes('\x00')))) throw new Error('INVALID_MCP_SERVERS');
      const env: Row = {};
      for (const [key, value] of Object.entries(entry.env ?? {})) {
        if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(key) || /KEY|TOKEN|SECRET|PASSWORD/i.test(key) || /^(LLM|MODEL|WORKER|ACS|PAC)_/.test(key)) throw new Error('MCP_ENV_DENIED');
        // Literal values only: the SDK resolves $VAR/${VAR}/!cmd indirection, which
        // would smuggle host environment or command execution into the binding.
        if (typeof value !== 'string' || value.length > 4096 || value.startsWith('!') || /\$[A-Za-z_{]|\$\$/.test(value)) throw new Error('MCP_ENV_DENIED');
        env[key] = value;
      }
      return { id: entry.id, command: entry.command, args: entry.args ?? [], env };
    });
  }
  private validateDeclaredDirs(spec: any, kind: 'skill' | 'extension') {
    if (spec === undefined) return undefined;
    if (!Array.isArray(spec) || spec.length > 16) throw new Error(kind === 'skill' ? 'INVALID_SKILLS' : 'INVALID_EXTENSIONS');
    const seen = new Set<string>();
    return spec.map((entry: any) => {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new Error(kind === 'skill' ? 'INVALID_SKILLS' : 'INVALID_EXTENSIONS');
      exact(entry, ['name', 'sourcePath']);
      const namePattern = kind === 'skill' ? /^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$/ : /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
      if (typeof entry.name !== 'string' || !namePattern.test(entry.name) || entry.name.includes('--')) throw new Error(kind === 'skill' ? 'INVALID_SKILLS' : 'INVALID_EXTENSIONS');
      if (seen.has(entry.name)) throw new Error(kind === 'skill' ? 'INVALID_SKILLS' : 'INVALID_EXTENSIONS'); seen.add(entry.name);
      if (typeof entry.sourcePath !== 'string' || !isAbsolute(entry.sourcePath)) throw new Error(kind === 'skill' ? 'INVALID_SKILLS' : 'INVALID_EXTENSIONS');
      let source: string; try { source = realpathSync(entry.sourcePath); if (!statSync(source).isDirectory()) throw new Error('x'); } catch { throw new Error((kind === 'skill' ? 'SKILL' : 'EXTENSION') + '_SOURCE_UNAVAILABLE'); }
      return { name: entry.name, sourcePath: source };
    });
  }
  private realpathBestEffort(p: string) {
    let current = resolve(p); const tail: string[] = [];
    for (let n = 0; n < 64; n++) { try { const real = realpathSync(current); return tail.length ? join(real, ...tail.reverse()) : real; } catch { const parent = dirname(current); if (parent === current) return current; tail.push(basename(current)); current = parent; } }
    return current;
  }
  private pathKey(p: string) { const n = (process.platform === 'win32' ? p.replace(/[\\/]+/g, '/') : p).replace(/\/+$/, ''); return process.platform === 'win32' ? n.toLowerCase() : n; }
  private scopeDenial(task: Row, event: Row): string | null {
    const input = event.input ?? {};
    const candidates: string[] = [];
    for (const key of ['path', 'file', 'filePath', 'targetFile', 'file_path']) if (typeof input[key] === 'string') candidates.push(input[key]);
    if (Array.isArray(input.paths)) for (const p of input.paths) if (typeof p === 'string') candidates.push(p);
    if (!candidates.length) return null;
    const roots = [task.workspace, ...(task.writeScope?.extraWriteDirs ?? [])].map((p: string) => this.pathKey(this.realpathBestEffort(p)));
    const denied = (task.writeScope?.denyPaths ?? []).map((p: string) => this.pathKey(p));
    const within = (candidate: string, root: string) => candidate === root || candidate.startsWith(root + '/');
    for (const raw of candidates) {
      const real = this.realpathBestEffort(isAbsolute(raw) ? raw : resolve(task.workspace, raw));
      const c = this.pathKey(real);
      if (denied.some((d: string) => within(c, d)) || !roots.some((r: string) => within(c, r))) return real;
    }
    return null;
  }
  private budgetExceeded(task: Row): Row | null {
    const caps = task.budget?.caps; if (!caps) return null;
    const u = task.usage; const total = u.input + u.output + u.cacheRead + u.cacheWrite;
    // maxCostMicroUsd is recorded but never evaluated: cost coverage is UNKNOWN, never zero.
    if (caps.maxTotalTokens != null && total > caps.maxTotalTokens) return { field: 'maxTotalTokens', cap: caps.maxTotalTokens, observed: total };
    if (caps.maxInputTokens != null && u.input > caps.maxInputTokens) return { field: 'maxInputTokens', cap: caps.maxInputTokens, observed: u.input };
    if (caps.maxOutputTokens != null && u.output > caps.maxOutputTokens) return { field: 'maxOutputTokens', cap: caps.maxOutputTokens, observed: u.output };
    return null;
  }
  private gateCheck(taskId: string, live: Live, event: Row) {
    try {
      const task = this.store.task(taskId);
      if (task.executionMode === 'CONTEXT_ONLY' && event.kind === 'TOOL_ADMISSION') {
        this.store.event(task, 'context.only.tool.denied', {toolName: event.toolName, toolCallId: event.toolCallId});
        live.child.send({type:'gate_verdict',checkId:event.checkId,action:'block',reason:'CONTEXT_ONLY_TOOL_DENIED'}); return;
      }
      const exceeded = this.budgetExceeded(task);
      if (exceeded) {
        // Verdict decided; crash here leaves decision unwritten for honest reconcile.
        if (this.config.faultPoint === 'budget_decision') process.exit(91);
        this.store.transaction(() => {
          const t = this.store.task(taskId);
          if (t.state !== 'BUDGET_EXCEEDED') { this.state(t, 'BUDGET_EXCEEDED'); t.reasonCode = 'BUDGET_EXCEEDED'; t.budgetExceeded = { ...exceeded, usage: { ...t.usage }, at: now() }; this.store.save(t); this.store.event(t, 'budget.exceeded', { ...exceeded, usage: { ...t.usage }, enforcement: 'SOFT' }); }
        });
        live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'terminate', reason: 'BUDGET_EXCEEDED', ...exceeded });
        void this.budgetRelease(taskId, live);
        return;
      }
      if (event.kind === 'TOOL_ADMISSION' && !event.toolName?.startsWith('mac__') && !['read', 'bash', 'run_command', 'run_build', 'run_tests', 'ask_user', 'ls', 'grep', 'glob'].includes(event.toolName)) {
        // Scope gates only file-mutating tools (read/bash/shell paths are not
        // statically attributable). Undeclared writeScope means workspace-only.
        const denied = this.scopeDenial(task, event);
        if (denied) { this.store.event(task, 'write.scope.violation', { toolName: event.toolName, toolCallId: event.toolCallId, deniedPath: denied, scopeId: task.writeScope?.scopeId ?? 'workspace' }); live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'block', reason: 'WRITE_SCOPE_VIOLATION: ' + denied + ' is outside the task write scope' }); return; }
      }
      live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'allow' });
    } catch { try { live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'block', reason: 'GATE_EVALUATION_FAILED' }); } catch {} }
  }
  private async budgetRelease(taskId: string, live: Live) {
    live.stopping = true;
    await this.stopMacTools(taskId);
    const modelStopped = await this.terminate(live);
    const stopped = modelStopped && (!this.store.task(taskId).macToolBinding || this.store.task(taskId).macRemoteQuiescent === true);
    this.update(taskId, t => {
      if (stopped) { t.quiescent = true; t.stopProof = { ownerBootId: this.bootId, pid: live.child.pid, method: this.quiescenceMethod, observedAt: now(), ...(live.runnerProof ?? {}) }; }
      this.markEffectsUnknown(t);
      for (const q of Object.values(t.inputRequests) as Row[]) if (['PENDING', 'DISPATCH_STARTED'].includes(q.state)) q.state = 'EXPIRED';
      if (live.mac?.proof) t.processOwnership = live.mac.proof;
      this.store.event(t, stopped ? 'BUDGET_RELEASED' : 'BUDGET_RELEASE_UNKNOWN', {});
    });
    this.admitQueue();
  }
  private async launch(taskId: string) {
    let task = this.store.task(taskId);
    if (task.intakeClosed) return;
    if ((task.executionProfile ?? 'native') !== this.executionProfile) { this.update(taskId, t => { t.reasonCode = 'EXECUTION_PROFILE_CHANGED'; this.state(t, 'UNKNOWN'); }); return; }
    if (!this.bindEnvironment(task)) return;
    try {
      mkdirSync(resolve(task.workspace, '..'), { recursive: true }); mkdirSync(task.sessionDir, { recursive: true });
      if (task.launchState !== 'RESTORING' && task.executionMode === 'CONTEXT_ONLY') {
        mkdirSync(task.workspace, {recursive:true});
      } else if (task.launchState !== 'RESTORING') {
        await this.prepareGit(taskId, this.store.directory, ['clone', '--no-hardlinks', '--no-checkout', this.project(task.projectId).path, task.workspace]);
        if (this.store.task(taskId).intakeClosed) return;
        await this.prepareGit(taskId, task.workspace, ['checkout', '--detach', task.baseCommit]);
      }
      task = this.store.task(taskId); if (task.intakeClosed) return;
      this.assertModelReferences(task);
      this.prepareResources(taskId);
      const environment: NodeJS.ProcessEnv = {};
      for (const key of ['PATH', 'Path', 'SystemRoot', 'COMSPEC', 'PATHEXT', 'ProgramFiles', 'ProgramFiles(x86)', 'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'MODEL_API_KEY', ...task.modelBinding.headerEnvKeys]) if (process.env[key]) environment[key] = process.env[key];
      const envCopied: string[] = [];
      for (const key of task.environment?.envAllowlist ?? []) if (process.env[key]) { environment[key] = process.env[key]; envCopied.push(key); }
      if (envCopied.length) this.update(taskId, t => { t.environmentBinding = { ...t.environmentBinding, envCopied }; });
      environment.PI_CODING_AGENT_DIR = join(task.sessionDir, 'profile'); environment.PI_TELEMETRY = 'off';
      const module = this.config.childModule ?? fileURLToPath(new URL('../engine/bootstrap.js', import.meta.url));
      const profileOptions = isNoForkProfile(this.executionProfile) ? { execPath: SANDBOX_EXEC, execArgv: ['-p', NO_FORK_POLICY, process.execPath] } : {};
      const child = fork(module, [], { ...profileOptions, cwd: task.workspace, env: environment, detached: process.platform !== 'win32', stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
      child.on('error', () => {});
      const live: Live = { child, stopping: false, ready: false, pending: new Map(), pauseWaiters: new Map(), resumeWaiters: new Map() }; this.live.set(taskId, live);
      task = this.update(taskId, t => { t.pid = child.pid; t.ownerBootId = this.bootId; });
      if (process.platform === 'win32') {
        try { live.job = new WindowsJob(); live.job.assign(child.pid!); }
        catch { live.stopping = true; child.kill(); live.job?.close(); throw new Error('JOB_ASSIGN_FAILED'); }
      }
      if (process.platform === 'darwin') {
        try { live.mac = await MacOwnership.arm(child.pid!); } catch (error) { child.kill('SIGKILL'); throw error; }
        if (live.stopping || this.closed || this.store.task(taskId).intakeClosed) { live.stopping = true; await this.terminate(live); return; }
        this.update(taskId, t => { t.processOwnership = { method: MAC_QUIESCENCE_METHOD, guardianArmed: true }; });
      }
      child.on('message', msg => { this.event(taskId, live, msg as Row); });
      child.on('exit', () => {
        for (const w of live.pauseWaiters.values()) w({ type: 'pause_failed', code: 'NATIVE_PROCESS_EXITED_UNCONFIRMED' }); live.pauseWaiters.clear();
        for (const w of live.resumeWaiters.values()) w(null); live.resumeWaiters.clear();
        if (this.closed || this.live.get(taskId) !== live) return;
        this.update(taskId, t => { if (!live.stopping) { this.state(t, 'UNKNOWN'); t.reasonCode = 'NATIVE_PROCESS_EXITED_UNCONFIRMED'; this.markEffectsUnknown(t); } for (const q of Object.values(t.inputRequests) as Row[]) if (q.state === 'PENDING') q.state = 'EXPIRED'; this.store.event(t, 'CHILD_EXITED'); });
      });
      child.on('error', () => { this.update(taskId, t => { this.state(t, 'UNKNOWN'); t.reasonCode = 'NATIVE_START_UNCONFIRMED'; }); });
      const macCapabilities = task.macToolBinding ? await this.config.macTools!.capabilities(task.macToolBinding) : [];
      child.send({ type: 'init', executionMode: task.executionMode ?? 'WORKSPACE_TASK', macCapabilities, executionProfile: this.executionProfile, workspace: task.workspace, sessionDir: task.sessionDir, model: task.modelBinding, sessionFile: task.launchState === 'RESTORING' ? task.nativeSessionFile : undefined, probeEnv: task.environment?.envAllowlist ?? [], toolAllowlist: task.toolAllowlist,
        mcpServers: (task.mcpServers ?? []).map((s: Row) => ({ id: s.id, config: { command: s.command, args: s.args, env: s.env, exposure: 'direct', timeout: 30 } })),
        extensions: (task.declaredExtensions ?? []).map((e: Row) => ({ name: e.name })) });
    } catch (error) { if (this.closed) return; const reason = error instanceof Error && /^[A-Z][A-Z0-9_]+$/.test(error.message) ? error.message : 'LOCAL_LAUNCH_FAILED'; const freed = this.update(taskId, t => { if (t.intakeClosed) return; t.reasonCode = reason; if (t.launchState === 'PREPARING' && !t.pid) { this.state(t, 'FAILED'); t.quiescent = true; t.launchState = 'REJECTED'; } else this.state(t, 'UNKNOWN'); }); if (freed.quiescent) this.admitQueue(); }
  }
  /** Copy declared skills/extensions into the task-managed profile and seal a content
   *  sha256 binding; on relaunch the managed copies are re-hashed against it. */
  private prepareResources(taskId: string) {
    const task = this.store.task(taskId);
    const declared = [...(task.declaredSkills ?? []).map((s: any) => ({ kind: 'skill', ...s })), ...(task.declaredExtensions ?? []).map((s: any) => ({ kind: 'extension', ...s }))];
    const mcp = task.mcpServers ?? [];
    if (!declared.length && !mcp.length) return;
    const profile = join(task.sessionDir, 'profile'); const bindings: Row = {};
    for (const item of declared) {
      const dest = join(profile, item.kind === 'skill' ? 'skills' : 'extensions', item.name);
      const key = item.kind + ':' + item.name;
      if (!existsSync(dest)) {
        const copied = this.copyTree(item.sourcePath, dest);
        bindings[key] = { name: item.name, kind: item.kind, sha256: copied.sha256, fileCount: copied.fileCount, bytes: copied.bytes, sourcePathDigest: hash(item.sourcePath), boundAt: now() };
      } else {
        const digest = this.treeDigest(dest); const prior = task.resourceBindings?.[key];
        if (prior && prior.sha256 !== digest.sha256) throw new Error('RESOURCE_BINDING_CHANGED');
        bindings[key] = prior ?? { name: item.name, kind: item.kind, sha256: digest.sha256, fileCount: digest.fileCount, bytes: digest.bytes, boundAt: now() };
      }
    }
    this.update(taskId, t => {
      const fresh = new Set(Object.keys(bindings).filter(k => !t.resourceBindings?.[k]));
      t.resourceBindings = { ...(t.resourceBindings ?? {}), ...bindings };
      for (const key of fresh) { const b = bindings[key]; this.store.event(t, b.kind + '.bound', { name: b.name, sha256: b.sha256, fileCount: b.fileCount, bytes: b.bytes, sourcePathDigest: b.sourcePathDigest }); }
      if (mcp.length && !t.mcpDeclared) { t.mcpDeclared = true; for (const s of mcp) this.store.event(t, 'mcp.declared', { server: s.id, command: s.command, args: s.args, envKeys: Object.keys(s.env) }); }
    });
  }
  private copyTree(source: string, dest: string) {
    let files = 0, bytes = 0;
    const walk = (src: string, dst: string) => {
      mkdirSync(dst, { recursive: true });
      for (const entry of readdirSync(src, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
        const s = join(src, entry.name); const st = lstatSync(s);
        if (st.isSymbolicLink()) throw new Error('RESOURCE_SYMLINK_DENIED');
        if (st.isDirectory()) walk(s, join(dst, entry.name));
        else if (st.isFile()) {
          if (files >= 64 || st.size > 1048576 || bytes + st.size > 4194304) throw new Error('RESOURCE_TOO_LARGE');
          copyFileSync(s, join(dst, entry.name)); files++; bytes += st.size;
        }
      }
    };
    walk(source, dest); return { ...this.treeDigest(dest), fileCount: files, bytes };
  }
  private treeDigest(root: string) {
    const items: string[] = []; let fileCount = 0, bytes = 0;
    const walk = (dir: string, prefix: string) => {
      for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
        const p = join(dir, entry.name);
        if (entry.isDirectory()) walk(p, prefix + entry.name + '/');
        else if (entry.isFile()) { const content = readFileSync(p); items.push(prefix + entry.name + '\0' + hash(content)); fileCount++; bytes += content.length; }
      }
    };
    walk(root, ''); return { sha256: hash(items.join('\n')), fileCount, bytes };
  }
  private bindEnvironment(task: Row): boolean {
    const spec = task.environment; if (!spec) return true;
    const missing: Row[] = []; const resolved: Row = {};
    for (const name of spec.requiresCommands ?? []) {
      try { const found = execFileSync(process.platform === 'win32' ? 'where' : 'which', [name], { encoding: 'utf8', timeout: 10000, windowsHide: true }).split(/\r?\n/)[0].trim(); if (!found) throw new Error('x'); resolved['command:' + name] = found; }
      catch { missing.push({ kind: 'command', name }); }
    }
    for (const dir of spec.requiresDirs ?? []) {
      try { const real = realpathSync(dir); if (!statSync(real).isDirectory()) throw new Error('x'); accessSync(real, constants.R_OK); resolved['dir:' + dir] = real; }
      catch { missing.push({ kind: 'dir', name: dir }); }
    }
    if (missing.length) {
      this.store.transaction(() => { const t = this.store.task(task.taskId); if (t.intakeClosed) return; this.state(t, 'BINDING_UNAVAILABLE'); t.reasonCode = 'BINDING_UNAVAILABLE'; t.quiescent = true; t.launchState = 'REJECTED'; t.bindingUnavailable = { missing, at: now() }; this.store.save(t); this.store.event(t, 'BINDING_UNAVAILABLE', { missing }); });
      this.admitQueue(); return false;
    }
    this.update(task.taskId, t => { t.environmentBinding = { ...t.environmentBinding, resolved, boundAt: now(), bootId: this.bootId }; });
    return true;
  }
  private async prepareGit(taskId: string, cwd: string, args: string[]) {
    const child = fork(fileURLToPath(new URL('../engine/git-child.js', import.meta.url)), [], { detached: process.platform !== 'win32', stdio: ['ignore', 'ignore', 'ignore', 'ipc'], env: { ...Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH', 'Path', 'SystemRoot', 'COMSPEC', 'PATHEXT', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].includes(key))), GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: process.platform === 'win32' ? 'NUL' : '/dev/null' } });
    const live: Live = { child, stopping: false, ready: false, pending: new Map(), pauseWaiters: new Map(), resumeWaiters: new Map() }; this.live.set(taskId, live);
    child.on('error', () => {});
    if (process.platform === 'win32') {
      try { live.job = new WindowsJob(); live.job.assign(child.pid!); }
      catch { live.stopping = true; child.kill(); live.job?.close(); throw new Error('JOB_ASSIGN_FAILED'); }
    }
    if (process.platform === 'darwin') {
      try { live.mac = await MacOwnership.arm(child.pid!); } catch (error) { child.kill('SIGKILL'); throw error; }
      if (live.stopping || this.closed || this.store.task(taskId).intakeClosed) { await this.terminate(live); throw new Error('LOCAL_PREPARATION_STOPPED'); }
    }
    child.send({ cwd, args });
    const result = await new Promise<number | null>((done, reject) => { child.once('exit', done); child.once('error', reject); });
    if (live.job && live.job.active() !== 0) { await live.job.stop(); }
    live.job?.close(); live.mac?.close(); if (this.live.get(taskId) === live) this.live.delete(taskId);
    if (result !== 0) throw new Error('LOCAL_PROCESS_FAILED');
  }
  private dispatch(task: Row, live: Live, requestKey: string, content: string) {
    if (task.intakeClosed || live.stopping) return;
    if (engineBinding(this.executionProfile).digest !== this.engineBinding.digest) { this.update(task.taskId, t => { this.state(t, 'FAILED'); t.reasonCode = 'ENGINE_BINDING_CHANGED'; }); return; }
    task.engineBindingDigest = this.engineBinding.digest;
    this.store.transaction(() => {
      task.launchState = 'DISPATCH_STARTED'; this.state(task, 'UNKNOWN'); task.runStartedAt ??= now(); this.store.save(task);
      const op = this.store.operation(requestKey)!; op.state = 'DISPATCHING'; op.launchState = 'DISPATCH_STARTED'; op.dispatchStartedAt = now(); this.store.saveOperation(op); this.store.event(task, 'DISPATCHING');
    });
    if (this.config.faultPoint === 'dispatching') { process.exit(91); }
    live.pending.set(requestKey, content); live.child.send({ type: 'prompt', requestKey, content });
  }
  private event(taskId: string, live: Live, event: Row) {
    if (this.closed || this.live.get(taskId) !== live) return;
    let task = this.store.task(taskId);
    if (live.stopping && !['stopped'].includes(event.type)) return;
    switch (event.type) {
      case 'ready':
        if (live.ready) return; live.ready = true; if (task.reopenFrom && task.reopenFrom.nativeSessionId !== event.nativeSessionId) { this.update(taskId, t => { this.state(t, 'FAILED'); t.reasonCode = 'NATIVE_SESSION_IDENTITY_CHANGED'; }); return; } task.nativeSessionId = event.nativeSessionId; task.nativeSessionFile = event.sessionFile; task.activeTools = event.tools; if (event.envProbe) task.envProbe = event.envProbe; task.actualModel = event.model ?? task.modelBinding.name; task.modelBindingDigest = digest(task.modelBinding); this.store.save(task); if (event.model && event.model !== task.modelBinding.name) { this.update(taskId, t => { this.state(t, 'FAILED'); t.reasonCode = 'MODEL_BINDING_MISMATCH'; }); return; } if (task.executionMode === 'CONTEXT_ONLY' && (!Array.isArray(event.tools) || event.tools.length !== 0)) { this.update(taskId,t=>{t.reasonCode='CONTEXT_ONLY_TOOLS_EXPOSED';this.state(t,'FAILED');}); void this.stop(taskId,{requestKey:'context-only-reject-'+taskId,intent:'STOP',generation:task.generation,nativeSessionId:task.nativeSessionId,targetCommandId:task.currentCommandId});return;} this.bindResources(taskId, event); this.dispatch(task, live, task.currentCommandId, task.goal); return;
      // First observed turn start is a real run fact even when dispatch was
      // bypassed (steer/follow-up sent straight to a live child); ??= keeps
      // the earliest recorded fact.
      case 'started': task.runStartedAt ??= now(); if (!['PAUSED', 'PAUSE_REQUESTED'].includes(task.state)) this.state(task, 'RUNNING'); break;
      case 'assistant': {
        const text = publicText(event.text ?? ''); if (text) { task.output = (task.output + text + '\n').slice(-262144); task.publicMessages.push({ role: 'assistant', text, commandId: task.currentCommandId, nativeTurnId: task.nativeTurnId, occurredAt: now() }); }
        if (event.failed) task.nativeFailure = event.errorCode;
        if (event.usage) {
          for (const key of ['input', 'output', 'cacheRead', 'cacheWrite']) task.usage[key] += Number(event.usage[key] ?? 0);
          const u = { input: Number(event.usage.input ?? 0), output: Number(event.usage.output ?? 0), cacheRead: Number(event.usage.cacheRead ?? 0), cacheWrite: Number(event.usage.cacheWrite ?? 0) };
          task.usageLedger = [...(task.usageLedger ?? []).slice(-255), { segmentId: task.segmentId, commandId: task.currentCommandId, nativeTurnId: task.nativeTurnId, usage: { ...u, total: u.input + u.output + u.cacheRead + u.cacheWrite }, source: 'ENGINE_REPORTED', costMicroUsd: null, costCoverage: 'UNKNOWN', recordedAt: now() }];
        }
        this.store.event(task, 'ASSISTANT_MESSAGE', { text }); break;
      }
      case 'assistant_delta': {
        // worker-events/1 OUTPUT_DELTA spine: one durable row per native text_delta.
        // Best-effort — a store hiccup must not wedge the IPC dispatcher that still
        // owns lifecycle messages on the same channel.
        const fields = this.outputDeltaFields(event);
        if (fields) try { this.store.event(task, 'OUTPUT_DELTA', fields); } catch {}
        break;
      }
      case 'thinking_delta': {
        // worker-events/1 THINKING_DELTA spine: reasoning parts forward like the
        // output tail — same redaction bounds, same best-effort persistence.
        const fields = this.outputDeltaFields(event);
        if (fields) try { this.store.event(task, 'THINKING_DELTA', fields); } catch {}
        break;
      }
      case 'user_consumed': {
        if (event.requestKey && live.pending.has(event.requestKey)) { const op = this.store.operation(event.requestKey)!; op.state = ['RESUME', 'EXTEND_BUDGET', 'TRANSITION'].includes(op.kind) ? 'APPLIED' : 'CONSUMED'; op.consumedAt = now(); if (['RESUME', 'EXTEND_BUDGET'].includes(op.kind)) op.detail = 'Continuation prompt consumed as new segment'; if (op.kind === 'TRANSITION') op.detail = 'Transition continuation consumed as new segment under new owner epoch'; this.store.saveOperation(op); live.pending.delete(event.requestKey); this.store.event(task, 'INPUT_CONSUMED', { requestKey: event.requestKey }); }
        task.publicMessages.push({ role: 'user', text: publicText(event.text ?? ''), commandId: task.currentCommandId, nativeTurnId: task.nativeTurnId, occurredAt: now() }); break;
      }
      case 'settled': {
        const final = task.nativeFailure ? 'FAILED' : 'SUCCEEDED';
        // A run that ends while the gate is closed still settled at a boundary; the PAUSED
        // state and which terminal state RESUME restores are recorded, not overwritten.
        if (task.state === 'PAUSED' && task.checkpointManifest) task.checkpointManifest.pausedFromState = final;
        else if (task.state === 'PAUSE_REQUESTED') task.pauseFromState = final;
        else if (!['BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'].includes(task.state)) { this.state(task, final); if (final === 'FAILED') task.reasonCode = task.nativeFailure; }
        task.nativeIdle = true; this.store.event(task, 'NATIVE_SETTLED'); break;
      }
      case 'queued': { const op = this.store.operation(event.requestKey); if (op && op.state !== 'CONSUMED') { op.state = 'ACKNOWLEDGED'; op.disposition = event.disposition; this.store.saveOperation(op); } break; }
      case 'question': task.inputRequests[event.inputRequestId] = { inputRequestId: event.inputRequestId, method: event.method, title: publicText(event.title), message: publicText(event.message), options: event.options, state: 'PENDING', generation: task.generation, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, targetCommandId: task.currentCommandId }; this.store.event(task, 'INPUT_REQUESTED', { inputRequestId: event.inputRequestId }); break;
      case 'question_consumed': if (task.inputRequests[event.inputRequestId]) { task.inputRequests[event.inputRequestId].state = 'ANSWERED'; this.store.event(task, 'INPUT_RESPONSE_CONSUMED', { inputRequestId: event.inputRequestId }); } break;
      case 'answered': { const op = this.store.operation(event.requestKey); if (op) { op.state = 'CONSUMED'; op.consumedAt = now(); this.store.saveOperation(op); } break; }
      case 'failure': this.state(task, 'FAILED'); task.reasonCode = event.code; this.store.event(task, 'NATIVE_FAILURE', { reasonCode: event.code }); break;
      case 'tool_start': (task.effects ??= {})[event.toolCallId] = { toolName: event.toolName, status: 'STARTED', startedAt: now() }; this.store.event(task, 'TOOL_START', { toolName: event.toolName, toolCallId: event.toolCallId, isError: false, detail: event.detail ? publicText(String(event.detail).slice(0, 240)) : undefined }); break;
      case 'tool_end': { const entry = (task.effects ??= {})[event.toolCallId] ??= { toolName: event.toolName }; entry.status = 'SETTLED'; entry.settledAt = now(); entry.isError = event.isError ?? false; const preview = event.result != null ? publicText(String(event.result).slice(0, 4096)) : undefined; if (preview !== undefined) entry.resultPreview = preview; this.store.event(task, 'TOOL_END', { toolName: event.toolName, toolCallId: event.toolCallId, isError: event.isError ?? false, detail: event.detail ? publicText(String(event.detail).slice(0, 240)) : undefined, result: preview }); break; }
      case 'extension_error': this.store.event(task, 'EXTENSION_ERROR', { extensionPath: event.extensionPath, extensionEvent: event.event, error: publicText(String(event.error ?? '').slice(0, 500)) }); break;
      case 'gate_held':
        if (task.state === 'PAUSED' && task.checkpointManifest) {
          // Newly held request after progress inside PAUSED (for example an answered question
          // resuming into the closed gate): re-seal so the manifest still matches the file.
          const kind = event.kind ?? 'PROVIDER_REQUEST';
          const manifest = this.sealCheckpoint(task, live, { boundary: kind, idle: false });
          if (manifest) this.store.transaction(() => { task.checkpointManifest = manifest; this.store.save(task); this.store.event(task, 'CHECKPOINT_REFRESHED', { boundary: kind }); });
        } else if (task.state === 'PAUSE_REQUESTED') this.store.event(task, 'BOUNDARY_GATE_HELD', { boundary: event.kind ?? 'PROVIDER_REQUEST' });
        break;
      case 'paused': {
        const waiter = live.pauseWaiters.get(event.requestKey);
        if (waiter) { live.pauseWaiters.delete(event.requestKey); waiter(event); break; }
        // Late boundary confirmation after the command already ended, or a re-park while PAUSED.
        if (['PAUSE_REQUESTED', 'PAUSED'].includes(task.state)) {
          const manifest = this.sealCheckpoint(task, live, event);
          if (manifest) this.store.transaction(() => { task.checkpointManifest = manifest; this.state(task, 'PAUSED'); this.store.save(task); this.store.event(task, 'PAUSE_BOUNDARY_CHECKPOINT', { requestKey: event.requestKey, boundary: event.boundary, late: true }); });
          else this.store.event(task, 'PAUSE_BOUNDARY_NOT_REACHED', { requestKey: event.requestKey, detail: 'CHECKPOINT_SEAL_UNAVAILABLE' });
        }
        break;
      }
      case 'pause_failed': { const waiter = live.pauseWaiters.get(event.requestKey); if (waiter) { live.pauseWaiters.delete(event.requestKey); waiter(event); } else this.store.event(task, 'PAUSE_FAILED', { requestKey: event.requestKey, detail: event.code }); break; }
      case 'resumed': { const waiter = live.resumeWaiters.get(event.requestKey); if (waiter) { live.resumeWaiters.delete(event.requestKey); waiter(event); } break; }
      case 'mac_tool_request': void this.macToolRequest(taskId, live, event); return;
      case 'runner_request': void this.runnerRequest(taskId, live, event); return;
      case 'gate_check': this.gateCheck(taskId, live, event); return;
    }
    this.store.save(task);
  }
  /** Persist the child's ready-time resource report as durable facts and on the task row. */
  private bindResources(taskId: string, event: Row) {
    const r = event.resources; if (!r) return;
    this.update(taskId, task => {
      task.loadedResources = { skills: (r.skills ?? []).map((s: any) => s.name), skillDiagnostics: r.skillDiagnostics ?? [], extensions: r.extensions ?? [], extensionErrors: r.extensionErrors ?? [], mcp: r.mcp ?? [] };
      for (const s of r.skills ?? []) this.store.event(task, 'skill.loaded', { name: s.name });
      for (const d of r.skillDiagnostics ?? []) this.store.event(task, 'skill.diagnostic', { type: d.type, message: publicText(String(d.message ?? '').slice(0, 500)) });
      for (const e of r.extensions ?? []) this.store.event(task, 'extension.loaded', { extensionPath: e.path, handlers: e.handlers, tools: e.tools });
      for (const e of r.extensionErrors ?? []) this.store.event(task, 'extension.load_failed', { extensionPath: e.path, error: publicText(String(e.error ?? '').slice(0, 500)) });
      for (const s of r.mcp ?? []) this.store.event(task, s.state === 'BOUND' ? 'mcp.bound' : 'mcp.unbound', { server: s.id, toolCount: s.toolCount, tools: (s.tools ?? []).slice(0, 20) });
    });
  }
  private guard(task: Row, body: Row) {
    for (const field of ['generation', 'nativeSessionId', 'nativeTurnId']) if ((task[field] ?? null) !== (body[field] ?? null)) throw new Error('NATIVE_TARGET_CHANGED');
    if (task.currentCommandId !== body.targetCommandId) throw new Error('NATIVE_TARGET_CHANGED');
    // ownerEpoch is optional on requests; when supplied a stale epoch is fenced the same way.
    if (body.ownerEpoch !== undefined && String(task.ownerEpoch ?? '1') !== String(body.ownerEpoch)) throw new Error('NATIVE_TARGET_CHANGED');
  }
  /** A sealed source refuses every new write effect. STOP stays reachable (it
   *  is already idempotent and the seal receipt depends on it); plain reads are
   *  never fenced. Called inside each mutating handler after the task load. */
  private assertUnsealed(task: Row) { if (task.sealedFor || task.sealRequested) throw new Error('SOURCE_SEALED'); }
  /** 收口 quiescence gate for source-seal (design §9.4/F33): the write fence
   *  may only land once the MAIN task can no longer write — a durable terminal
   *  verdict, or recorded quiescence (stopProof / restart reconciliation) with
   *  no dispatch left behind it. A QUEUED row is quiescent-flagged for
   *  admission accounting only and can still launch, so it is not sealed
   *  source material until stopped out of the queue; a live RUNNING/PREPARING
   *  owner still has turn/tool writes in flight and refuses honestly. */
  private sealQuiescent(task: Row) {
    if (TASK_TERMINAL_STATES.includes(task.state)) return true;
    const schedulable = task.launchState === 'QUEUED' && !task.intakeClosed && task.state === 'QUEUED';
    return task.quiescent === true && !schedulable;
  }
  private taskCreateKey(taskId: string): string | null { return this.taskOperations(taskId).find(o => !o.kind || o.kind === 'TASK_CREATE')?.requestKey ?? null; }
  interact(taskId: string, body: Row) {
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    const task = this.store.task(taskId); this.guard(task, body); this.assertUnsealed(task); const live = this.live.get(taskId);
    if (task.state === 'BUDGET_EXCEEDED') throw new Error('BUDGET_EXCEEDED');
    if ((!live || !live.child.connected || live.stopping) && body.kind === 'NATIVE_FOLLOW_UP') {
      let reopenAttach: Row | null = null;
      if (body.materials !== undefined) reopenAttach = this.materials.attach(task, body.materials, this.taskCreateKey(taskId));
      return this.reopenCompleted(task, body, reopenAttach);
    }
    if (!live || !live.child.connected || live.stopping) throw new Error('NATIVE_TARGET_CHANGED');
    if (Date.now() >= Number(task.deadlineEpochMillis)) throw new Error('NATIVE_INTAKE_CLOSED');
    let attached: Row | null = null;
    if (body.materials !== undefined) {
      // Attachments only have meaning on prompt-shaped intents; an
      // INPUT_RESPONSE carries no attachment semantics at all.
      if (!['NATIVE_FOLLOW_UP', 'NATIVE_STEER'].includes(body.kind)) throw new Error('MATERIALS_UNSUPPORTED_INTENT');
      attached = this.materials.attach(task, body.materials, this.taskCreateKey(taskId));
    }
    if (body.kind === 'INPUT_RESPONSE') {
      const q = task.inputRequests[body.inputRequestId]; if (!q || q.state !== 'PENDING' || q.nativeTurnId !== task.nativeTurnId) throw new Error('NATIVE_INPUT_EXPIRED');
      exact(body.response, ['value', 'cancelled', 'confirmed', 'selected', 'text']);
      const value = body.response.value ?? body.response.text ?? body.response.selected ?? body.response.confirmed;
      if (!body.response.cancelled && (q.method === 'confirm' ? typeof value !== 'boolean' : typeof value !== 'string' || (q.method === 'select' && !q.options.includes(value)))) throw new Error('INVALID_NATIVE_RESPONSE');
      q.state = 'DISPATCH_STARTED';
    } else {
      if (engineBinding(this.executionProfile).digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
      if (typeof body.content !== 'string' || !body.content.trim() || Buffer.byteLength(body.content) > 16384) throw new Error('INVALID_NATIVE_CONTENT');
      if (body.kind === 'NATIVE_FOLLOW_UP' && task.state !== 'SUCCEEDED') throw new Error('NATIVE_NOT_IDLE');
      if (body.kind === 'NATIVE_STEER' && task.state !== 'RUNNING') throw new Error('NATIVE_NOT_RUNNING');
      if (!['NATIVE_FOLLOW_UP', 'NATIVE_STEER'].includes(body.kind)) throw new Error('NATIVE_ACTION_UNSUPPORTED');
    }
    const content = body.content + (attached?.injectedText ?? '');
    const op = this.store.transaction(() => {
      const op = this.store.accept(body.requestKey, taskId, body, { state: 'DISPATCHING', dispatchStartedAt: now(), kind: body.kind });
      if (attached) { op.materials = attached.refs; this.materials.mark(attached.refs, 'BOUND', { boundTaskId: taskId, boundRequestKey: body.requestKey }); }
      if (body.kind === 'NATIVE_FOLLOW_UP') { this.archiveEffects(task); task.currentCommandId = body.requestKey; task.segmentId = randomUUID(); task.nativeIdle = false; delete task.nativeFailure; delete task.reasonCode; this.state(task, 'UNKNOWN'); }
      this.store.save(task); this.store.event(task, 'INPUT_ACCEPTED', { requestKey: body.requestKey, inputKind: body.kind, ...(attached ? { materials: attached.refs.map((r: Row) => ({ materialRef: r.materialRef, sha256: r.sha256, role: r.role })) } : {}) }); return op;
    });
    if (body.kind === 'INPUT_RESPONSE') live.child.send({ type: 'answer', requestKey: body.requestKey, inputRequestId: body.inputRequestId, response: body.response });
    else {
      live.pending.set(body.requestKey, content); live.child.send({ type: body.kind === 'NATIVE_FOLLOW_UP' ? 'prompt' : 'steer', requestKey: body.requestKey, content });
      if (attached) { this.materials.mark(attached.refs, 'CONSUMED', { consumedTaskId: taskId, consumedRequestKey: body.requestKey }); this.store.event(this.store.task(taskId), 'message.materials', { requestKey: body.requestKey, materials: attached.refs.map((r: Row) => ({ materialRef: r.materialRef, sha256: r.sha256, role: r.role, assembledAs: r.assembledAs })) }); }
    }
    return op;
  }
  private canReopen(task: Row) {
    return (this.executionProfile !== RUNNER_PROFILE || task.completedSessionSeal?.runnerCommandsDigest === digest(task.runnerCommands ?? {})) && (task.executionProfile ?? 'native') === this.executionProfile && task.state === 'SUCCEEDED' && task.quiescent === true && task.nativeIdle === true && task.engineVersion === PI_VERSION && (process.platform === 'win32' ? ['WINDOWS_JOB_ACTIVE_ZERO'] : process.platform === 'darwin' ? [this.quiescenceMethod] : []).includes(task.completedSessionSeal?.quiescenceMethod) && (task.executionMode ?? 'WORKSPACE_TASK') === (task.completedSessionSeal?.executionMode ?? 'WORKSPACE_TASK') && task.completedSessionSeal?.nativeSessionId === task.nativeSessionId && !Object.values(task.inputRequests).some((q: any) => ['PENDING', 'DISPATCH_STARTED'].includes(q.state));
  }
  private sealCompleted(task: Row) {
    if (task.completedSessionSeal) return;
    if (Object.values(task.runnerCommands ?? {}).some((r: any) => r.status === 'UNKNOWN')) return;
    if (!task.quiescent || !(process.platform === 'win32' ? ['WINDOWS_JOB_ACTIVE_ZERO'] : process.platform === 'darwin' ? [this.quiescenceMethod] : []).includes(task.stopProof?.method) || task.state !== 'SUCCEEDED' || !task.nativeIdle || !task.nativeSessionFile) return;
    // Unknown effects block later "completed" claims; they do not block a new segment's own ledger.
    if (Object.values(task.effects ?? {}).some((e: any) => e.status === 'UNKNOWN')) return;
    try {
      const file = realpathSync(task.nativeSessionFile); if (!file.startsWith(realpathSync(task.sessionDir) + (process.platform === 'win32' ? '\\' : '/'))) return;
      task.completedSessionSeal = { executionMode:task.executionMode ?? 'WORKSPACE_TASK', nativeSessionId: task.nativeSessionId, sessionSha256: hash(readFileSync(file)), engineVersion: PI_VERSION, modelBindingDigest: digest(task.modelBinding), sealedAt: now(), segmentId: task.segmentId, ...(this.executionProfile === RUNNER_PROFILE ? { runnerCommandsDigest: digest(task.runnerCommands ?? {}) } : {}), quiescenceMethod: task.stopProof.method };
    } catch { task.completedSessionSeal = null; }
  }
  private reopenCompleted(task: Row, body: Row, attached: Row | null = null) {
    if (!this.canReopen(task)) throw new Error('COMPLETED_SESSION_REQUIRED');
    if (this.store.tasks().filter(t => t.taskId !== task.taskId && !t.quiescent).length >= this.maxActive()) throw new Error('LOCAL_TASK_BUSY');
    if (!process.env.MODEL_API_KEY) throw new Error('MODEL_AUTH_UNAVAILABLE');
    this.assertModelReferences(task);
    if (task.engineBindingDigest !== this.engineBinding.digest || engineBinding(this.executionProfile).digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
    if (process.env.MODEL_DEFAULT_MODEL !== task.modelBinding.name || process.env.MODEL_BASE_URL !== task.modelBinding.baseUrl || digest(task.modelBinding) !== task.completedSessionSeal.modelBindingDigest) throw new Error('MODEL_BINDING_CHANGED');
    if (typeof body.content !== 'string' || !body.content.trim() || Buffer.byteLength(body.content) > 16384) throw new Error('INVALID_NATIVE_CONTENT');
    if (realpathSync(task.workspace) !== realpathSync(join(this.store.directory, 'workspaces', task.taskId))) throw new Error('OWNED_WORKSPACE_REQUIRED');
    if (hash(readFileSync(task.nativeSessionFile)) !== task.completedSessionSeal.sessionSha256) throw new Error('NATIVE_SESSION_SEAL_CHANGED');
    const content = body.content + (attached?.injectedText ?? '');
    const op = this.store.transaction(() => {
      const op = this.store.accept(body.requestKey, task.taskId, body, { kind: body.kind, state: 'ACCEPTED', sourceSegmentId: task.segmentId, reopenedCompletedSession: true });
      if (attached) { op.materials = attached.refs; this.materials.mark(attached.refs, 'BOUND', { boundTaskId: task.taskId, boundRequestKey: body.requestKey }); }
      if (task.artifactManifest) { task.artifactHistory = [...(task.artifactHistory ?? []), task.artifactManifest]; delete task.artifactManifest; }
      this.archiveEffects(task);
      task.reopenFrom = task.completedSessionSeal; task.goal = content; task.currentCommandId = body.requestKey; task.segmentId = randomUUID(); task.generation = randomUUID(); task.intakeClosed = false; task.quiescent = false; task.nativeIdle = false; task.deadlineEpochMillis = String(Date.now() + this.taskDeadlineMs(task)); task.launchState = 'RESTORING'; delete task.pid; delete task.stopProof; delete task.nativeFailure; delete task.completedSessionSeal; delete task.reasonCode;
      this.state(task, 'UNKNOWN'); this.store.save(task); this.store.event(task, 'COMPLETED_SESSION_REOPEN_ACCEPTED', { requestKey: body.requestKey, sourceSegmentId: task.reopenFrom.segmentId, ...(attached ? { materials: attached.refs.map((r: Row) => ({ materialRef: r.materialRef, sha256: r.sha256, role: r.role })) } : {}) }); return op;
    });
    if (attached) this.materials.mark(attached.refs, 'CONSUMED', { consumedTaskId: task.taskId, consumedRequestKey: body.requestKey });
    setImmediate(() => { void this.launch(task.taskId); }); return op;
  }
  async command(taskId: string, body: Row) {
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    const task = this.store.task(taskId); this.guard(task, body);
    if (body.intent === 'STOP') return this.stop(taskId, body);
    if (body.intent === 'PAUSE') return this.pause(taskId, body, task);
    if (body.intent === 'RESUME') return this.resume(taskId, body, task);
    if (body.intent === 'EXTEND_BUDGET') return this.extendBudget(taskId, body, task);
    throw new Error('NATIVE_ACTION_UNSUPPORTED');
  }
  async stop(taskId: string, body: Row) {
    const prior = this.replay(body); if (prior) {
      if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT');
      const persisted = this.store.task(taskId);
      if (persisted.macToolBinding && persisted.macStopIntent && persisted.macRemoteQuiescent !== true) {
        await this.stopMacTools(taskId); // Query only the original Mac STOP key.
        const observed = this.store.task(taskId);
        if (observed.localQuiescent === true && observed.macRemoteQuiescent === true) {
          this.update(taskId, t => { t.quiescent = true; if (!['SUCCEEDED', 'FAILED', 'BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'].includes(t.state)) this.state(t, 'CANCELLED'); });
          prior.state = 'APPLIED'; prior.detail = 'Owned model and Mac child are both quiescent'; this.store.saveOperation(prior);
        }
      }
      return prior;
    }
    const task = this.store.task(taskId); this.guard(task, body); if (body.intent !== 'STOP') throw new Error('NATIVE_ACTION_UNSUPPORTED');
    const deadlineCause = String(body.requestKey ?? '').startsWith('deadline-');
    const op = this.store.accept(body.requestKey, taskId, body, { kind: 'STOP', state: 'UNKNOWN', detail: 'Owned writers not proven stopped' });
    this.update(taskId, t => { t.intakeClosed = true; });
    await this.stopMacTools(taskId);
    await this.killTaskTerminals(taskId, 'TASK_STOP');
    // Parent STOP propagates to writable collaboration children: each running
    // writer is Job-drained with proof; children never launched become BLOCKED.
    await this.stopTaskCollaborations(taskId, 'PARENT_STOP');
    // Aux sessions are deliberately NOT drained by a plain STOP (existing
    // semantics); the receipt states that honestly instead of implying all
    // children stopped. Only source-seal drains aux sessions.
    const affected = this.affectedChildren(taskId);
    const live = this.live.get(taskId);
    if (!live) {
      if (['PREPARING', 'RESTORING', 'QUEUED'].includes(task.launchState) && !task.pid && (!task.macToolBinding || this.store.task(taskId).macRemoteQuiescent === true)) { this.update(taskId, t => { t.quiescent = true; t.launchState = 'REJECTED'; this.state(t, 'CANCELLED'); if (deadlineCause) t.reasonCode = 'TASK_DEADLINE_EXCEEDED'; this.store.event(t, 'STOP_APPLIED', { requestKey: body.requestKey, proof: 'UNDISPATCHED_INTAKE_CLOSED', affectedChildren: affected }); }); op.state = 'APPLIED'; op.detail = 'Undispatched task intake durably closed'; op.affectedChildren = affected; this.store.saveOperation(op); this.admitQueue(); }
      else if (task.quiescent && (!task.macToolBinding || this.store.task(taskId).macRemoteQuiescent === true)) { this.update(taskId, t => { if (!['SUCCEEDED', 'FAILED', 'CANCELLED', 'BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'].includes(t.state)) { this.state(t, 'CANCELLED'); if (deadlineCause) t.reasonCode = 'TASK_DEADLINE_EXCEEDED'; } this.store.event(t, 'STOP_APPLIED', { requestKey: body.requestKey, proof: 'ALREADY_QUIESCENT', affectedChildren: affected }); }); op.state = 'APPLIED'; op.detail = 'Task already quiescent; intake durably closed'; op.affectedChildren = affected; this.store.saveOperation(op); }
      return op;
    }
    live.stopping = true;
    for (const w of live.pauseWaiters.values()) w({ type: 'pause_failed', code: 'STOP_SUPERSEDED' }); live.pauseWaiters.clear();
    for (const w of live.resumeWaiters.values()) w(null); live.resumeWaiters.clear();
    const modelStopped = await this.terminate(live);
    const stopped = modelStopped && (!task.macToolBinding || this.store.task(taskId).macRemoteQuiescent === true);
    this.update(taskId, t => {
      if (task.macToolBinding) t.localQuiescent = modelStopped;
      if (stopped) { t.quiescent = true; t.stopProof = { ownerBootId: this.bootId, pid: live.child.pid, method: this.quiescenceMethod, observedAt: now(), ...(live.runnerProof ?? {}) }; if (!['SUCCEEDED', 'FAILED', 'BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'].includes(t.state)) { this.state(t, 'CANCELLED'); if (deadlineCause) t.reasonCode = 'TASK_DEADLINE_EXCEEDED'; } }
      if (live.mac?.proof) { t.processOwnership = live.mac.proof; if (!stopped) { this.state(t, 'UNKNOWN'); t.reasonCode = live.mac.proof.forkObserved ? 'PROCESS_ESCAPE_NOT_EXCLUDED' : 'PROCESS_QUIESCENCE_UNPROVEN'; } }
      this.markEffectsUnknown(t);
      if (stopped) this.sealCompleted(t);
      for (const q of Object.values(t.inputRequests) as Row[]) if (['PENDING', 'DISPATCH_STARTED'].includes(q.state)) q.state = 'EXPIRED';
      this.store.event(t, stopped ? 'STOP_APPLIED' : 'STOP_UNKNOWN', { requestKey: body.requestKey, affectedChildren: affected });
    });
    op.state = stopped ? 'APPLIED' : 'UNKNOWN'; op.detail = stopped ? (this.executionProfile === RUNNER_PROFILE ? 'Model no-fork process exited; all registered command contexts closed with zero active/unknown commands; arbitrary escaped processes NOT_PROVEN' : 'Owned transport and process tree exited') : 'Owned writers not proven stopped'; op.affectedChildren = affected; this.store.saveOperation(op);
    if (stopped) this.admitQueue();
    return op;
  }
  /** Child-affect summary for STOP / source-seal receipts. Collab children are
   *  drained by both paths; aux sessions are left running by a plain STOP
   *  (reported, not hidden) and only drained by source-seal. */
  private affectedChildren(taskId: string) {
    const task = this.store.task(taskId);
    const aux = Object.values(task.auxSessions ?? {}).map((a: any) => ({ id: a.auxSessionId, state: a.state === 'RUNNING' ? 'LEFT_RUNNING' : a.state, reason: a.state === 'RUNNING' ? 'TASK_STOP_AUX_NOT_PROPAGATED' : (a.reasonCode ?? null) }));
    const collabChildren = Object.values(task.collaborations ?? {}).flatMap((c: any) => Object.values(c.children ?? {}).map((ch: any) => ({ collaborationId: c.collaborationId, childId: ch.childId, state: ch.state, reason: ch.reasonCode ?? null })));
    const terminals = Object.values(task.terminals ?? {}).map((t: any) => ({ terminalId: t.terminalId, state: t.state, reason: t.reasonCode ?? null }));
    return { aux, collabChildren, terminals };
  }
  private canPause(task: Row, live: Live | undefined) {
    return !!live && live.ready && !live.stopping && !live.stopped && !!live.child.connected && !task.intakeClosed && !task.quiescent && !['FAILED', 'CANCELLED'].includes(task.state);
  }
  private canResume(task: Row, live: Live | undefined) {
    if (task.state === 'PAUSED') return true;
    return task.state === 'PAUSE_REQUESTED' && !!live && !live.stopping && !live.stopped && !!live.child.connected;
  }
  private markEffectsUnknown(task: Row) { for (const r of Object.values(task.runnerCommands ?? {}) as Row[]) if (!['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'REJECTED'].includes(r.status)) { r.status = 'UNKNOWN'; r.reasonCode = 'SETTLEMENT_UNCONFIRMED'; } for (const e of Object.values(task.effects ?? {}) as Row[]) if (e.status === 'STARTED') { e.status = 'UNKNOWN'; e.detail = 'SETTLEMENT_UNCONFIRMED'; } this.projectSessionJournal(task); }
  /** The native session journal is the durable record of what the model
   *  actually said. When the live IPC feed produced no public messages before
   *  the task went UNKNOWN (e.g. PROCESS_ESCAPE_NOT_EXCLUDED), projecting the
   *  journal keeps finished work visible; the verification verdict in
   *  reasonCode/state stays separate and is not relabelled. */
  private journalMessages(task: Row): Row[] {
    try {
      const dir = realpathSync(task.sessionDir);
      const prefix = dir + (process.platform === 'win32' ? '\\' : '/');
      let file = '';
      try { if (task.nativeSessionFile) file = realpathSync(task.nativeSessionFile); } catch { file = ''; }
      if (!file.startsWith(prefix)) file = join(dir, readdirSync(dir).filter(f => f.endsWith('.jsonl')).sort().pop() ?? '');
      if (!file.startsWith(prefix)) return [];
      const out: Row[] = [];
      for (const line of readFileSync(file, 'utf8').split('\n')) {
        if (!line.includes('"type":"message"')) continue;
        try {
          const rec = JSON.parse(line); const msg = rec?.message;
          if (rec?.type !== 'message' || (msg?.role !== 'user' && msg?.role !== 'assistant')) continue;
          const parts = Array.isArray(msg.content) ? msg.content : [{ type: 'text', text: msg.content }];
          const text = publicText(parts.filter((p: any) => p?.type === 'text').map((p: any) => String(p?.text ?? '')).join('\n').trim());
          if (text) out.push({ role: msg.role, text, commandId: null, nativeTurnId: null, occurredAt: typeof rec.timestamp === 'string' ? rec.timestamp : now() });
        } catch { /* malformed journal lines are skipped, not fatal */ }
      }
      return out;
    } catch { return []; }
  }
  private projectSessionJournal(task: Row) {
    if ((task.publicMessages ?? []).length > 0) return;
    const recovered = this.journalMessages(task);
    if (!recovered.length) return;
    task.publicMessages = recovered;
    if (!task.output) task.output = recovered.filter(m => m.role === 'assistant').map(m => m.text).join('\n').slice(-262144);
  }
  private archiveEffects(task: Row) {
    const entries = task.effects ?? {};
    if (Object.keys(entries).length) task.effectSegments = [...(task.effectSegments ?? []).slice(-11), { segmentId: task.segmentId, effects: entries }];
    task.effects = {};
  }
  private sealCheckpoint(task: Row, live: Live, facts: Row): Row | null {
    if (!task.nativeSessionFile) return null;
    try {
      const file = realpathSync(task.nativeSessionFile);
      if (!file.startsWith(realpathSync(task.sessionDir) + (process.platform === 'win32' ? '\\' : '/'))) return null;
      const sessionSha256 = hash(readFileSync(file));
      const lastEventSequence = String(this.store.lastEventSeq(task.taskId));
      return { schemaVersion: 'worker-checkpoint/1', kind: 'BOUNDARY_CHECKPOINT', checkpointId: randomUUID(), taskId: task.taskId, segmentId: task.segmentId, generation: task.generation, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId,
        executionProfile: this.executionProfile, sessionFile: file, sessionSha256, sessionFileSha256: sessionSha256, engineVersion: PI_VERSION, engineBindingDigest: this.engineBinding.digest, modelBindingDigest: digest(task.modelBinding),
        boundary: facts.boundary ?? 'UNKNOWN', boundaryState: facts.boundary ?? 'UNKNOWN', idle: !!facts.idle, pausedFromState: task.pauseFromState ?? task.state,
        pendingInputs: Object.values(task.inputRequests ?? {}).filter((q: any) => q.state === 'PENDING').map((q: any) => ({ inputRequestId: q.inputRequestId, method: q.method, title: q.title, state: q.state })),
        unconsumedInputs: [...live.pending.keys()],
        runnerCommandsDigest: digest(task.runnerCommands ?? {}), pendingRunnerCommands: Object.values(task.runnerCommands ?? {}).filter((r: any) => !['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'REJECTED'].includes(r.status)).map((r: any) => ({ requestKey: r.requestKey, status: r.status, binding: r.binding })),
        pendingEffects: Object.entries(task.effects ?? {}).filter(([, e]: any) => e.status === 'STARTED').map(([toolCallId, e]: any) => ({ toolCallId, toolName: e.toolName, startedAt: e.startedAt })),
        usage: { ...task.usage }, writeScope: task.writeScope ?? null, budget: task.budget ?? null, lastEventSeq: Number(lastEventSequence), lastEventSequence, ownerEpoch: String(task.ownerEpoch ?? '1'), sealedAt: now(), ownerBootId: this.bootId };
    } catch { return null; }
  }
  private verifyCheckpoint(task: Row) {
    const manifest = task.checkpointManifest;
    if (!manifest || manifest.kind !== 'BOUNDARY_CHECKPOINT' || manifest.nativeSessionId !== task.nativeSessionId || manifest.segmentId !== task.segmentId) throw new Error('CHECKPOINT_INCOMPATIBLE');
    if (engineBinding(this.executionProfile).digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
    if ((task.executionProfile ?? 'native') !== this.executionProfile || (manifest.executionProfile ?? 'native') !== this.executionProfile) throw new Error('CHECKPOINT_INCOMPATIBLE');
    if (manifest.runnerCommandsDigest !== undefined && manifest.runnerCommandsDigest !== digest(task.runnerCommands ?? {})) throw new Error('CHECKPOINT_INCOMPATIBLE');
    if (manifest.engineBindingDigest !== this.engineBinding.digest || manifest.modelBindingDigest !== digest(task.modelBinding)) throw new Error('CHECKPOINT_INCOMPATIBLE');
    try { if (hash(readFileSync(realpathSync(manifest.sessionFile))) !== (manifest.sessionFileSha256 ?? manifest.sessionSha256)) throw new Error(); } catch { throw new Error('CHECKPOINT_INCOMPATIBLE'); }
  }
  private checkpointValid(task: Row) { try { this.verifyCheckpoint(task); return true; } catch { return false; } }
  private requestPause(live: Live, requestKey: string) {
    return new Promise<any>(resolve => {
      const timer = setTimeout(() => { live.pauseWaiters.delete(requestKey); resolve(null); }, this.config.pauseDeadlineMs ?? 30000);
      live.pauseWaiters.set(requestKey, reply => { clearTimeout(timer); resolve(reply); });
      live.child.send({ type: 'pause', requestKey });
    });
  }
  private requestResume(live: Live, requestKey: string) {
    return new Promise<any>(resolve => {
      const timer = setTimeout(() => { live.resumeWaiters.delete(requestKey); resolve(null); }, this.config.resumeDeadlineMs ?? 15000);
      live.resumeWaiters.set(requestKey, reply => { clearTimeout(timer); resolve(reply); });
      live.child.send({ type: 'resume', requestKey });
    });
  }
  private async pause(taskId: string, body: Row, task: Row) {
    const live = this.live.get(taskId);
    this.assertUnsealed(task);
    if (!this.canPause(task, live)) throw new Error('NATIVE_NOT_RUNNING');
    const fromState = task.state === 'PAUSED' ? task.checkpointManifest?.pausedFromState ?? 'UNKNOWN' : task.state === 'PAUSE_REQUESTED' ? task.pauseFromState : task.state;
    const op = this.store.transaction(() => {
      const op = this.store.accept(body.requestKey, taskId, body, { kind: 'PAUSE', state: 'ACCEPTED', fromState });
      task.pauseFromState = fromState; this.state(task, 'PAUSE_REQUESTED');
      this.store.save(task); this.store.event(task, 'PAUSE_ACCEPTED', { requestKey: body.requestKey }); return op;
    });
    op.state = 'DISPATCHING'; op.dispatchStartedAt = now(); this.store.saveOperation(op);
    const reply = await this.requestPause(live!, body.requestKey);
    if (reply?.type === 'paused') {
      this.store.transaction(() => {
        const fresh = this.store.task(taskId);
        const manifest = this.sealCheckpoint(fresh, live!, reply);
        if (manifest) { fresh.checkpointManifest = manifest; if (!['CANCELLED', 'FAILED'].includes(fresh.state)) this.state(fresh, 'PAUSED'); this.store.save(fresh); this.store.event(fresh, 'PAUSE_BOUNDARY_CHECKPOINT', { requestKey: body.requestKey, boundary: reply.boundary, lastEventSeq: manifest.lastEventSeq }); }
        else this.store.event(fresh, 'PAUSE_BOUNDARY_NOT_REACHED', { requestKey: body.requestKey, detail: 'CHECKPOINT_SEAL_UNAVAILABLE' });
      });
      const manifest = this.store.task(taskId).checkpointManifest;
      op.state = manifest ? 'APPLIED' : 'REQUESTED'; op.detail = manifest ? 'Quiet boundary reached; checkpoint sealed' : 'BOUNDARY_NOT_REACHED'; op.checkpointSegmentId = manifest?.segmentId; this.store.saveOperation(op);
    } else {
      op.state = 'REQUESTED'; op.detail = reply?.code ?? 'BOUNDARY_NOT_REACHED'; this.store.saveOperation(op);
      const fresh = this.store.task(taskId); this.store.event(fresh, 'PAUSE_BOUNDARY_NOT_REACHED', { requestKey: body.requestKey, detail: op.detail });
    }
    return op;
  }
  private async resume(taskId: string, body: Row, task: Row) {
    const live = this.live.get(taskId);
    this.assertUnsealed(task);
    const connected = !!live && live.ready && !live.stopping && !live.stopped && !!live.child.connected;
    if (task.state === 'PAUSE_REQUESTED') {
      if (!connected) throw new Error('NATIVE_TARGET_CHANGED');
      return this.resumeLive(taskId, body, live!, true);
    }
    if (task.state !== 'PAUSED' || !task.checkpointManifest) throw new Error('CHECKPOINT_INCOMPATIBLE');
    this.verifyCheckpoint(task);
    if (connected) return this.resumeLive(taskId, body, live!, false);
    return this.resumeRelaunch(taskId, body, task);
  }
  private async resumeLive(taskId: string, body: Row, live: Live, cancelled: boolean) {
    const op = this.store.transaction(() => {
      const op = this.store.accept(body.requestKey, taskId, body, { kind: 'RESUME', state: 'DISPATCHING', dispatchStartedAt: now(), resumeVia: 'HELD_GATE' });
      this.store.event(this.store.task(taskId), cancelled ? 'PAUSE_CANCEL_DISPATCHED' : 'RESUME_DISPATCHED', { requestKey: body.requestKey }); return op;
    });
    const reply = await this.requestResume(live, body.requestKey);
    if (reply?.type === 'resumed') {
      this.store.transaction(() => {
        const task = this.store.task(taskId);
        if (!cancelled) { this.archiveEffects(task); task.segmentId = randomUUID(); task.generation = randomUUID(); }
        task.deadlineEpochMillis = String(Date.now() + this.taskDeadlineMs(task));
        // A burst of IPC can deliver settled after resumed but before this awaited
        // continuation commits. Preserve that later terminal fact instead of
        // restoring RUNNING from the older resumed.idle snapshot.
        const settledDuringReply = task.nativeIdle && ['SUCCEEDED', 'FAILED'].includes(task.checkpointManifest?.pausedFromState);
        const restored = cancelled ? task.pauseFromState : reply.idle || settledDuringReply ? task.checkpointManifest?.pausedFromState : undefined;
        this.state(task, restored && restored !== 'PAUSE_REQUESTED' ? restored : 'RUNNING');
        // The same held question survives RESUME, but answers must bind to the
        // new owner generation just like every other live command. Preserve the
        // question identity and audit the rebinding; never revive expired rows.
        for (const q of Object.values(task.inputRequests ?? {}) as Row[]) {
          if (q.state !== 'PENDING' || q.nativeSessionId !== task.nativeSessionId || q.nativeTurnId !== task.nativeTurnId) continue;
          const previousGeneration = q.generation;
          q.generation = task.generation; q.targetCommandId = task.currentCommandId;
          this.store.event(task, 'INPUT_REQUEST_REBOUND', { inputRequestId: q.inputRequestId, previousGeneration, generation: task.generation });
        }
        delete task.pauseFromState;
        this.store.save(task); this.store.event(task, cancelled ? 'PAUSE_CANCELLED' : 'RESUME_APPLIED', { requestKey: body.requestKey, segmentId: task.segmentId, via: 'HELD_GATE' });
      });
      const task = this.store.task(taskId);
      op.state = 'APPLIED'; op.detail = cancelled ? 'Pause request cancelled; boundary gate released' : 'Held boundary released in place'; op.segmentId = task.segmentId; this.store.saveOperation(op);
    } else { op.state = 'REQUESTED'; op.detail = 'RESUME_UNCONFIRMED'; this.store.saveOperation(op); this.store.event(this.store.task(taskId), 'RESUME_UNCONFIRMED', { requestKey: body.requestKey }); }
    return op;
  }
  private resumeRelaunch(taskId: string, body: Row, task: Row) {
    const manifest = task.checkpointManifest;
    if (!this.config.childModule) {
      if (!process.env.MODEL_API_KEY) throw new Error('MODEL_AUTH_UNAVAILABLE');
      this.assertModelReferences(task);
      if (process.env.MODEL_DEFAULT_MODEL !== task.modelBinding.name || process.env.MODEL_BASE_URL !== task.modelBinding.baseUrl) throw new Error('MODEL_BINDING_CHANGED');
    }
    if (engineBinding(this.executionProfile).digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
    if (this.store.tasks().filter(t => t.taskId !== taskId && !t.quiescent).length >= this.maxActive()) throw new Error('LOCAL_TASK_BUSY');
    try { if (realpathSync(task.workspace) !== realpathSync(join(this.store.directory, 'workspaces', taskId))) throw new Error('x'); } catch { throw new Error('CHECKPOINT_INCOMPATIBLE'); }
    const sourceSegmentId = manifest.segmentId;
    const op = this.store.transaction(() => {
      const op = this.store.accept(body.requestKey, taskId, body, { kind: 'RESUME', state: 'ACCEPTED', sourceSegmentId, resumeVia: 'SESSION_REOPEN' });
      task.checkpointHistory = [...(task.checkpointHistory ?? []).slice(-11), manifest];
      task.resumedFrom = { segmentId: sourceSegmentId, sessionSha256: manifest.sessionSha256 };
      task.reopenFrom = { nativeSessionId: manifest.nativeSessionId, segmentId: sourceSegmentId, checkpoint: true };
      this.archiveEffects(task);
      task.goal = 'Execution paused at a verified boundary and was resumed by the worker host. Continue the original task.';
      task.currentCommandId = body.requestKey; task.segmentId = randomUUID(); task.generation = randomUUID(); task.ownerEpoch = String(Number(task.ownerEpoch ?? '1') + 1); task.launchState = 'RESTORING'; task.quiescent = false; task.nativeIdle = false; task.deadlineEpochMillis = String(Date.now() + this.taskDeadlineMs(task));
      delete task.pid; delete task.stopProof; delete task.nativeFailure; delete task.pauseFromState; delete task.reasonCode;
      this.state(task, 'UNKNOWN'); this.store.save(task); this.store.event(task, 'RESUME_CONTINUATION_ACCEPTED', { requestKey: body.requestKey, sourceSegmentId }); return op;
    });
    setImmediate(() => { void this.launch(taskId); }); return op;
  }
  private extendBudget(taskId: string, body: Row, task: Row) {
    this.assertUnsealed(task);
    if (task.state !== 'BUDGET_EXCEEDED' || !task.budget) throw new Error('BUDGET_NOT_EXCEEDED');
    if (!task.quiescent) throw new Error('BUDGET_RELEASE_PENDING');
    const caps = this.validateBudgetExtension(body.budget, task);
    if (!task.nativeSessionFile) throw new Error('NATIVE_SESSION_UNAVAILABLE');
    try { realpathSync(task.nativeSessionFile); } catch { throw new Error('NATIVE_SESSION_UNAVAILABLE'); }
    if (!this.config.childModule) {
      if (!process.env.MODEL_API_KEY) throw new Error('MODEL_AUTH_UNAVAILABLE');
      this.assertModelReferences(task);
      if (process.env.MODEL_DEFAULT_MODEL !== task.modelBinding.name || process.env.MODEL_BASE_URL !== task.modelBinding.baseUrl) throw new Error('MODEL_BINDING_CHANGED');
    }
    if (this.store.tasks().filter(t => t.taskId !== taskId && !t.quiescent).length >= this.maxActive()) throw new Error('LOCAL_TASK_BUSY');
    if (engineBinding(this.executionProfile).digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
    const sourceSegmentId = task.segmentId;
    const op = this.store.transaction(() => {
      const op = this.store.accept(body.requestKey, taskId, body, { kind: 'EXTEND_BUDGET', state: 'ACCEPTED', sourceSegmentId, resumeVia: 'SESSION_REOPEN', previousCaps: task.budget.caps, caps });
      task.budget = { ...task.budget, caps, history: [...(task.budget.history ?? []).slice(-11), { requestKey: body.requestKey, sourceSegmentId, caps, extendedAt: now() }] };
      task.extendedFrom = { segmentId: sourceSegmentId };
      task.reopenFrom = { nativeSessionId: task.nativeSessionId, segmentId: sourceSegmentId };
      this.archiveEffects(task);
      task.goal = 'The task budget was extended by the worker host. Continue the original task.';
      task.currentCommandId = body.requestKey; task.segmentId = randomUUID(); task.generation = randomUUID(); task.launchState = 'RESTORING'; task.quiescent = false; task.nativeIdle = false; task.deadlineEpochMillis = String(Date.now() + this.taskDeadlineMs(task));
      delete task.pid; delete task.stopProof; delete task.nativeFailure; delete task.budgetExceeded; delete task.reasonCode;
      this.state(task, 'UNKNOWN'); this.store.save(task); this.store.event(task, 'BUDGET_EXTENDED', { requestKey: body.requestKey, caps, sourceSegmentId }); return op;
    });
    setImmediate(() => { void this.launch(taskId); }); return op;
  }
  private validateBudgetExtension(spec: any, task: Row) {
    if (!spec || typeof spec !== 'object' || Array.isArray(spec)) throw new Error('INVALID_BUDGET');
    exact(spec, ['maxTotalTokens', 'maxInputTokens', 'maxOutputTokens', 'maxCostMicroUsd', 'enforcement']);
    if (spec.enforcement !== undefined && spec.enforcement !== 'SOFT') throw new Error('BUDGET_ENFORCEMENT_UNSUPPORTED');
    const caps = { ...task.budget.caps }; let extended = false;
    for (const key of ['maxTotalTokens', 'maxInputTokens', 'maxOutputTokens', 'maxCostMicroUsd']) if (spec[key] !== undefined) {
      const v = budgetCap(spec[key]); if (v == null) throw new Error('INVALID_BUDGET');
      if (caps[key] === undefined || v <= caps[key]) throw new Error('BUDGET_CAP_NOT_INCREASED');
      caps[key] = v; extended = true;
    }
    if (!extended) throw new Error('BUDGET_CAP_NOT_INCREASED');
    return caps;
  }
  private async stopMacTools(taskId: string) {
    const task = this.store.task(taskId);
    if (!task.macToolBinding) return;
    this.update(taskId, t => { t.macToolsClosed = true; });
    try {
      const original = task.macStopIntent;
      if (!original) this.update(taskId, t => { t.macStopIntent = { requested: true }; });
      const receipt = await this.config.macTools!.stop(task.macToolBinding, original);
      this.update(taskId, t => { t.macStopReceipt = receipt; t.macRemoteQuiescent = receipt.remoteQuiescent === true || receipt.quiescent === true; });
    } catch { this.update(taskId, t => { t.macRemoteQuiescent = false; t.quiescent = false; this.state(t, 'UNKNOWN'); t.macStopReason = 'MAC_STOP_UNCONFIRMED'; }); }
  }
  private async macToolRequest(taskId: string, live: Live, event: Row) {
    try {
      const task = this.store.task(taskId);
      if (!isNoForkProfile(this.executionProfile) || live.stopping || task.intakeClosed || !task.macToolBinding || !this.config.macTools || typeof event.toolCallId !== 'string' || event.toolCallId.length > 256) throw new Error('MAC_TOOLS_CLOSED');
      const receipt = await this.config.macTools.execute(task.macToolBinding, task, event, record => {
        this.update(taskId, t => { if (record.status === 'DISPATCHING' && (t.intakeClosed || t.macToolsClosed || live.stopping)) throw new Error('MAC_TOOLS_CLOSED'); (t.macOperations ??= {})[record.requestKey] = record; this.store.event(t, 'MAC_TOOL_OPERATION', { requestKey: record.requestKey, status: record.status }); });
      });
      if (live.child.connected && !live.stopping) live.child.send({ type: 'mac_tool_result', toolCallId: event.toolCallId, receipt });
    } catch {
      if (live.child.connected && !live.stopping) live.child.send({ type: 'mac_tool_result', toolCallId: event.toolCallId, error: 'MAC_OPERATION_UNCONFIRMED' });
    }
  }
  private async runnerRequest(taskId: string, live: Live, event: Row) {
    const reply = (fields: Row) => { if (live.child.connected && !live.stopping) live.child.send({ type: 'runner_result', toolCallId: event.toolCallId, ...fields }); };
    let requestKey: string | undefined;
    try {
      const task = this.store.task(taskId);
      if (this.executionProfile !== RUNNER_PROFILE || live.stopping || live.runnerAdmissionClosed || task.intakeClosed) throw new Error('RUNNER_CONTEXT_CLOSED');
      if (engineBinding(this.executionProfile).digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
      if (!['bash', 'run_command', 'run_build', 'run_tests'].includes(event.toolName) || typeof event.toolCallId !== 'string' || event.toolCallId.length > 256) throw new Error('RUNNER_INVALID_TOOL');
      const input = event.input; exact(input, ['command', 'cwd', 'timeoutMs']);
      if (typeof input.command !== 'string' || !input.command.trim() || Buffer.byteLength(input.command) > 65536) throw new Error('RUNNER_INVALID_COMMAND');
      if (input.cwd !== undefined && typeof input.cwd !== 'string') throw new Error('RUNNER_INVALID_CWD');
      if (input.timeoutMs !== undefined && (!Number.isInteger(input.timeoutMs) || input.timeoutMs < 100 || input.timeoutMs > 600000)) throw new Error('RUNNER_INVALID_TIMEOUT');
      const binding = { taskId, generation: task.generation, segmentId: task.segmentId, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, currentCommandId: task.currentCommandId, executionProfile: this.executionProfile, engineBindingDigest: this.engineBinding.digest };
      const contextId = 'ctx-' + digest(binding).slice(0, 48);
      requestKey = 'pi-' + digest({ binding, toolCallId: event.toolCallId }).slice(0, 56);
      const body = { requestKey, contextId, kind: event.toolName === 'run_build' ? 'build' : event.toolName === 'run_tests' ? 'test' : 'command', shell: { script: input.command, executable: '/bin/bash' }, cwd: input.cwd ?? '.', timeoutMs: input.timeoutMs ?? 120000 };
      const prior = task.runnerCommands?.[requestKey];
      if (prior) { if (prior.bodyDigest !== digest(body)) throw new Error('IDEMPOTENCY_CONFLICT'); if (prior.receipt) { reply({ receipt: prior.receipt }); return; } if (prior.status === 'REJECTED') throw new RunnerRequestRejected(prior.reasonCode); throw new Error('RUNNER_SETTLEMENT_UNCONFIRMED'); }
      // Durable binding and original key precede Runner admission, including its startup.
      this.update(taskId, t => { (t.runnerCommands ??= {})[requestKey!] = { requestKey, contextId, toolCallId: event.toolCallId, toolName: event.toolName, binding, bodyDigest: digest(body), status: 'DISPATCHING', acceptedAt: now() }; this.store.event(t, 'RUNNER_DISPATCHING', { requestKey, contextId, toolCallId: event.toolCallId, binding }); });
      const runners = live.runners ??= new Map();
      let pending = runners.get(contextId);
      if (!pending) { pending = CommandRunner.start(join(this.store.directory, 'command-runner', contextId), { contextId, workspace: task.workspace, binding, envAllowlist: task.environment?.envAllowlist ?? [] }); runners.set(contextId, pending); }
      const runner = await pending;
      if (live.stopping || live.runnerAdmissionClosed || this.closed || this.store.task(taskId).intakeClosed) throw new Error('RUNNER_CONTEXT_CLOSED');
      const receipt = await runner.execute(body, (accepted: Row) => this.update(taskId, t => { Object.assign(t.runnerCommands[requestKey!], { commandId: accepted.commandId, status: accepted.status }); this.store.event(t, 'RUNNER_ACCEPTED', { requestKey, commandId: accepted.commandId }); }));
      if (this.storageClosed) return;
      if (receipt.requestKey !== requestKey || receipt.contextId !== contextId || digest(receipt.binding) !== digest(binding)) throw new Error('RUNNER_RECEIPT_BINDING_CHANGED');
      receipt.stdout = publicText(String(receipt.stdout ?? '')); receipt.stderr = publicText(String(receipt.stderr ?? ''));
      this.update(taskId, t => { Object.assign(t.runnerCommands[requestKey!], { status: receipt.status, receipt, settledAt: now() }); this.store.event(t, 'RUNNER_SETTLED', { requestKey, commandId: receipt.commandId, status: receipt.status, exitCode: receipt.exitCode, buildSucceeded: receipt.buildSucceeded, tests: receipt.tests ?? null, stopProof: receipt.stopProof ?? null }); });
      if (receipt.status === 'UNKNOWN') throw new Error('RUNNER_SETTLEMENT_UNCONFIRMED');
      reply({ receipt });
    } catch (error) {
      const code = error instanceof Error && /^[A-Z_]+$/.test(error.message) ? error.message : 'RUNNER_UNAVAILABLE';
      reply({ error: code });
      if (error instanceof RunnerRequestRejected && requestKey && !this.storageClosed) {
        this.update(taskId, t => { const r = t.runnerCommands?.[requestKey!]; if (r) { r.status = 'REJECTED'; r.reasonCode = code; } this.store.event(t, 'RUNNER_REJECTED', { requestKey, reasonCode: code, dispatched: false }); });
        return;
      }
      if (requestKey && !live.stopping && !this.closed) {
        this.update(taskId, t => { const r = t.runnerCommands?.[requestKey!]; if (r && !r.receipt) { r.status = 'UNKNOWN'; r.reasonCode = code; } t.intakeClosed = true; this.state(t, 'UNKNOWN'); t.reasonCode = 'RUNNER_SETTLEMENT_UNCONFIRMED'; this.store.event(t, 'RUNNER_UNKNOWN', { requestKey, reasonCode: code }); });
        live.stopping = true; await this.terminate(live);
      }
    }
  }
  private async stopRunners(live: Live) {
    if (this.executionProfile !== RUNNER_PROFILE) return true;
    live.runnerProof = { scope: 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS', arbitraryEscapedProcesses: 'NOT_PROVEN', runnerContexts: [] };
    if (!live.runners) return true;
    live.runnerAdmissionClosed = true;
    const contexts: Row[] = []; let proven = true;
    for (const [contextId, pending] of live.runners) {
      try {
        const runner = await pending;
        const context = await runner.stop('stop-' + contextId);
        contexts.push(context);
        if (!context.closed || (!Array.isArray(context.activeCommands) || context.activeCommands.length !== 0) || (!Array.isArray(context.unknownCommands) || context.unknownCommands.length !== 0) || !context.quiescent || context.stopProof?.scope !== 'REGISTERED_PROCESS_GROUP') proven = false;
        await runner.close();
      } catch { proven = false; contexts.push({ contextId, quiescent: false, reasonCode: 'RUNNER_STOP_UNCONFIRMED' }); }
    }
    live.runnerProof = { scope: 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS', arbitraryEscapedProcesses: 'NOT_PROVEN', runnerContexts: contexts };
    return proven;
  }
  // ---- W5a: context packages (P11), auxiliary sessions (P12), assignment
  //      transitions (P13). Facts ride the same task row and event journal;
  //      mutating endpoints carry requestKey idempotency.
  /** Resolve a package file source inside the task workspace with the same
   *  realpath/deny logic as writeScope; the workspace itself is the only root
   *  (packages snapshot workspace content, not extraWriteDirs). */
  private packageSourcePath(task: Row, raw: any): string {
    if (typeof raw !== 'string' || !raw.trim() || raw.length > 1024 || raw.includes('\x00')) throw new Error('INVALID_PACKAGE_SOURCES');
    const root = this.pathKey(this.realpathBestEffort(task.workspace));
    const real = this.realpathBestEffort(isAbsolute(raw) ? raw : resolve(task.workspace, raw));
    const c = this.pathKey(real);
    const denied = (task.writeScope?.denyPaths ?? []).map((p: string) => this.pathKey(p));
    const within = (candidate: string, r: string) => candidate === r || candidate.startsWith(r + '/');
    if (denied.some((d: string) => within(c, d)) || !within(c, root)) throw new Error('PACKAGE_SOURCE_OUT_OF_SCOPE');
    return real;
  }
  private packageMeta(p: Row) { return { packageId: p.packageId, name: p.name ?? null, digest: p.digest, sources: p.sources.map((s: Row) => ({ kind: s.kind, ...(s.path !== undefined ? { path: s.path } : {}), sha256: s.sha256, size: s.size })), sourceCount: p.sources.length, fileCount: p.fileCount, size: p.size, createdAt: p.createdAt }; }
  private packageDigestOf(p: Row) { return digest(p.sources.map((s: Row) => ({ kind: s.kind, ...(s.path !== undefined ? { path: s.path } : {}), sha256: s.sha256, size: s.size }))); }
  createContextPackage(taskId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'name', 'sources']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); const p = this.store.task(taskId).contextPackages?.[prior.packageId]; if (!p) throw new Error('PACKAGE_NOT_FOUND'); return { ...this.packageMeta(p), replayed: true }; }
    const task = this.store.task(taskId); this.assertUnsealed(task);
    if (body.name !== undefined && (typeof body.name !== 'string' || !body.name.trim() || body.name.length > 128 || /[\x00-\x1f]/.test(body.name))) throw new Error('INVALID_PACKAGE_NAME');
    if (!Array.isArray(body.sources) || !body.sources.length || body.sources.length > PACKAGE_MAX_SOURCES) throw new Error('INVALID_PACKAGE_SOURCES');
    if (Object.keys(task.contextPackages ?? {}).length >= PACKAGE_LIMIT) throw new Error('PACKAGE_LIMIT');
    let workspaceReal: string; try { workspaceReal = realpathSync(task.workspace); } catch { throw new Error('PACKAGE_WORKSPACE_UNAVAILABLE'); }
    const sources: Row[] = []; let total = 0, files = 0;
    for (const src of body.sources) {
      if (!src || typeof src !== 'object' || Array.isArray(src)) throw new Error('INVALID_PACKAGE_SOURCES');
      if (src.kind === 'file') {
        exact(src, ['kind', 'path']);
        if (++files > PACKAGE_MAX_FILES) throw new Error('PACKAGE_SOURCE_TOO_LARGE');
        const real = this.packageSourcePath(task, src.path);
        let bytes: Buffer; try { if (!statSync(real).isFile()) throw 0; bytes = readFileSync(real); } catch { throw new Error('PACKAGE_SOURCE_UNAVAILABLE'); }
        if (bytes.length > PACKAGE_MAX_FILE_BYTES || total + bytes.length > PACKAGE_MAX_BYTES) throw new Error('PACKAGE_SOURCE_TOO_LARGE');
        // Frozen copy: bytes are hashed and stored now; later workspace writes
        // never mutate the package (N16).
        total += bytes.length;
        sources.push({ kind: 'file', path: relative(workspaceReal, real).split('\\').join('/'), sha256: hash(bytes), size: bytes.length, content: bytes.toString('base64') });
      } else if (src.kind === 'text' || src.kind === 'summary') {
        exact(src, ['kind', 'text']);
        if (typeof src.text !== 'string') throw new Error('INVALID_PACKAGE_SOURCES');
        if (Buffer.byteLength(src.text) > PACKAGE_MAX_TEXT || total + Buffer.byteLength(src.text) > PACKAGE_MAX_BYTES) throw new Error('PACKAGE_SOURCE_TOO_LARGE');
        total += Buffer.byteLength(src.text);
        sources.push({ kind: src.kind, sha256: hash(src.text), size: Buffer.byteLength(src.text), text: src.text });
      } else throw new Error('INVALID_PACKAGE_SOURCES');
    }
    const packageId = 'pkg-' + randomUUID();
    const pkg: Row = { schemaVersion: 'worker-context-package/1', packageId, taskId, name: body.name, digest: digest(sources.map(s => ({ kind: s.kind, ...(s.path !== undefined ? { path: s.path } : {}), sha256: s.sha256, size: s.size }))), sources, fileCount: files, size: total, createdAt: now() };
    let op: Row;
    this.store.transaction(() => {
      const t = this.store.task(taskId);
      (t.contextPackages ??= {})[packageId] = pkg; this.store.save(t);
      this.store.event(t, 'context-package.created', { packageId, name: body.name ?? null, digest: pkg.digest, size: total, sourceCount: sources.length });
      op = this.store.accept(body.requestKey, taskId, body, { kind: 'CONTEXT_PACKAGE_CREATE', state: 'APPLIED', packageId });
    });
    return this.packageMeta(pkg);
  }
  contextPackageDetail(task: Row, packageId: string) {
    const p = task.contextPackages?.[id(packageId)]; if (!p) throw new Error('PACKAGE_NOT_FOUND');
    return { ...this.packageMeta(p), digestVerified: this.packageDigestOf(p) === p.digest, sources: p.sources.map((s: Row) => s.kind === 'file' ? { kind: 'file', path: s.path, sha256: s.sha256, size: s.size, content: s.content } : { kind: s.kind, sha256: s.sha256, size: s.size, text: s.text }), coverage: 'FROZEN_AT_CREATION' };
  }
  // ---- Auxiliary sessions: one-shot read-only research children on a frozen
  //      workspace snapshot. They never share the parent budget ledger and their
  //      output only enters the main session through explicit deliver.
  private auxPublic(a: Row) { return { auxSessionId: a.auxSessionId, state: a.state, question: a.question, answer: a.answer ?? '', reasonCode: a.reasonCode ?? null, nativeSessionId: a.nativeSessionId ?? null, contextPackageIds: a.contextPackageIds ?? [], usage: a.usage, budget: a.budget ?? null, deliveredAt: a.deliveredAt ?? null, createdAt: a.createdAt, finishedAt: a.finishedAt ?? null }; }
  private maxAux() { const n = Number(process.env.NWF_PI_MAX_ACTIVE_AUX ?? 2); return Number.isFinite(n) ? Math.max(1, Math.floor(n)) : 2; }
  private validateAuxBudget(spec: any) {
    if (spec === undefined) return undefined;
    if (!spec || typeof spec !== 'object' || Array.isArray(spec)) throw new Error('INVALID_BUDGET');
    exact(spec, ['maxTotalTokens', 'enforcement']);
    if (spec.enforcement !== undefined && spec.enforcement !== 'SOFT') throw new Error('BUDGET_ENFORCEMENT_UNSUPPORTED');
    const v = budgetCap(spec.maxTotalTokens); if (v == null) throw new Error('INVALID_BUDGET');
    return { caps: { maxTotalTokens: v }, enforcement: 'SOFT', basis: 'ENGINE_REPORTED_POST_HOC', costCoverage: 'UNKNOWN', reservedAt: now() };
  }
  /** Frozen copy of the task workspace for the aux session: regular files only,
   *  `.git` and symlinks/junctions skipped, bounded. Isolation is by copy. */
  private snapshotWorkspace(workspace: string, dest: string) {
    let files = 0, bytes = 0, skipped = 0;
    const srcRoot = realpathSync(workspace);
    const walk = (src: string, dst: string) => {
      mkdirSync(dst, { recursive: true });
      for (const entry of readdirSync(src, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
        if (entry.name === '.git') { skipped++; continue; }
        const s = join(src, entry.name); const st = lstatSync(s);
        if (st.isSymbolicLink()) { skipped++; continue; }
        if (st.isDirectory()) walk(s, join(dst, entry.name));
        else if (st.isFile()) {
          if (++files > AUX_SNAPSHOT_MAX_FILES || st.size > AUX_SNAPSHOT_MAX_BYTES || bytes + st.size > AUX_SNAPSHOT_MAX_BYTES) throw new Error('AUX_SNAPSHOT_TOO_LARGE');
          bytes += st.size; copyFileSync(s, join(dst, entry.name));
        } else skipped++;
      }
    };
    walk(srcRoot, dest);
    return { files, bytes, skipped, sourceDigest: hash(srcRoot), snapshotAt: now() };
  }
  /** Frozen context-package bytes rendered as fixed instruction material; shared
   *  by read-only aux sessions and writable collaboration children. */
  private packageMaterial(task: Row, ids: string[]) {
    let material = '';
    for (const pid of ids) {
      const p = task.contextPackages?.[pid]; if (!p) continue;
      material += `--- context package ${p.name ?? p.packageId} (digest ${p.digest}) ---\n`;
      for (const s of p.sources) {
        if (s.kind === 'file') material += `[file ${s.path} sha256 ${s.sha256}]\n` + Buffer.from(s.content, 'base64').toString('utf8').slice(0, 32768) + '\n';
        else material += `[${s.kind} sha256 ${s.sha256}]\n` + String(s.text).slice(0, 32768) + '\n';
      }
    }
    return material;
  }
  private auxInstruction(task: Row, aux: Row) {
    const parts = ['You are an isolated read-only research session attached to a worker task. Your working directory is a frozen snapshot copy of the task workspace: nothing you do here affects the real task workspace. Use the fixed material below and the snapshot files to answer the question directly. Do not ask the user questions.'];
    parts.push('<<QUESTION>>\n' + aux.question + '\n<</QUESTION>>');
    parts.push('<<FIXED_MATERIAL>>\n' + this.packageMaterial(task, aux.contextPackageIds).slice(0, 131072) + '\n<</FIXED_MATERIAL>>');
    parts.push('Reply with the answer text only.');
    return parts.join('\n\n');
  }
  createAuxSession(taskId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'question', 'contextPackageIds', 'budget']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    const task = this.store.task(taskId); this.assertUnsealed(task);
    if (typeof body.question !== 'string' || !body.question.trim() || Buffer.byteLength(body.question) > AUX_MAX_QUESTION) throw new Error('INVALID_AUX_QUESTION');
    const ids = body.contextPackageIds ?? [];
    if (!Array.isArray(ids) || ids.length > AUX_MAX_PACKAGES || ids.some((p: any) => typeof p !== 'string')) throw new Error('INVALID_AUX_PACKAGES');
    for (const pid of ids) if (!task.contextPackages?.[pid]) throw new Error('PACKAGE_NOT_FOUND');
    const budget = this.validateAuxBudget(body.budget);
    if (Object.keys(task.auxSessions ?? {}).length >= AUX_MAX_PER_TASK) throw new Error('AUX_SESSION_LIMIT');
    if (this.auxLive.size >= this.maxAux()) throw new Error('AUX_SESSION_BUSY');
    const auxSessionId = 'aux-' + randomUUID();
    const auxDir = join(this.store.directory, 'aux', auxSessionId);
    const auxWorkspace = join(auxDir, 'workspace'); const auxSessionDir = join(auxDir, 'session');
    let snapshot: Row; try { snapshot = this.snapshotWorkspace(task.workspace, auxWorkspace); }
    catch (error) { throw new Error(error instanceof Error && /^[A-Z][A-Z0-9_]+$/.test(error.message) ? error.message : 'AUX_SNAPSHOT_FAILED'); }
    const aux: Row = { auxSessionId, taskId, requestKey: body.requestKey, state: 'RUNNING', question: body.question, contextPackageIds: ids, budget, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, answer: '', toolCalls: 0, workspace: auxWorkspace, sessionDir: auxSessionDir, snapshot, nativeSessionId: null, deadlineEpochMillis: String(Date.now() + AUX_DEADLINE_MS), createdAt: now() };
    let op: Row;
    this.store.transaction(() => {
      const t = this.store.task(taskId);
      (t.auxSessions ??= {})[auxSessionId] = aux; this.store.save(t);
      this.store.event(t, 'aux-session.started', { auxSessionId, contextPackageIds: ids, budget: budget ?? null });
      op = this.store.accept(body.requestKey, taskId, body, { kind: 'AUX_SESSION_CREATE', state: 'DISPATCHING', auxSessionId });
    });
    setImmediate(() => { void this.launchAux(taskId, auxSessionId); });
    return op!;
  }
  /** Child-stream helper: append a public message to the child's durable
   *  stream inside the caller's save transaction when one is open. */
  private streamAppend(task: Row, streamId: string | undefined | null, meta: Row, row: Row) {
    if (!streamId) return;
    this.store.appendChildMessage(streamId, { task: task.taskId, collab: meta.collab ?? null, child: meta.child ?? null }, row);
  }
  private streamText(text: string) { const t = publicText(String(text ?? '')); return t.length > CHILD_MSG_TEXT_MAX ? { text: t.slice(0, CHILD_MSG_TEXT_MAX), truncated: true } : { text: t }; }
  /** worker-events/1 OUTPUT_DELTA payload fields from an assistant_delta IPC
   *  event: the public-redacted delta plus any native coordinates the child
   *  actually reported. Null for empty deltas so no noise rows land. */
  private outputDeltaFields(event: Row): Row | null {
    const delta = publicText(String(event.delta ?? ''));
    if (!delta) return null;
    const fields: Row = { delta };
    if (Number.isSafeInteger(event.contentIndex)) fields.contentIndex = event.contentIndex;
    if (typeof event.nativeTurnId === 'string' && event.nativeTurnId) fields.nativeTurnId = event.nativeTurnId;
    if (typeof event.nativeItemId === 'string' && event.nativeItemId) fields.nativeItemId = event.nativeItemId;
    return fields;
  }
  /** child-messages variant: same payload fields, bounded like sibling rows. */
  private streamOutputDelta(task: Row, streamId: string | undefined | null, meta: Row, event: Row) {
    const fields = this.outputDeltaFields(event); if (!fields || !streamId) return;
    if ((fields.delta as string).length > CHILD_MSG_TEXT_MAX) { fields.truncated = true; fields.delta = (fields.delta as string).slice(0, CHILD_MSG_TEXT_MAX); }
    this.streamAppend(task, streamId, meta, { kind: 'OUTPUT_DELTA', ...fields });
  }
  private async launchAux(taskId: string, auxSessionId: string) {
    const task = this.store.task(taskId); const aux = task.auxSessions?.[auxSessionId];
    if (!aux || aux.state !== 'RUNNING') return;
    try {
      // Open the child-message stream before the first model byte: ASSIGNMENT
      // and the RUNNING status are capture anchors for everything that follows.
      const streamId = 'aux:' + auxSessionId;
      this.store.transaction(() => {
        this.update(taskId, t => {
          const a = t.auxSessions?.[auxSessionId]; if (!a || a.messageStreamId) return;
          a.messageStreamId = streamId;
          this.store.ensureChildStream(streamId, { taskId, auxSessionId });
          this.streamAppend(t, streamId, { child: auxSessionId }, { kind: 'ASSIGNMENT', ...this.streamText(aux.question ?? '') });
          this.streamAppend(t, streamId, { child: auxSessionId }, { kind: 'STATUS', text: 'RUNNING', status: 'RUNNING' });
        });
      });
      mkdirSync(aux.sessionDir, { recursive: true });
      const environment: NodeJS.ProcessEnv = {};
      for (const key of ['PATH', 'Path', 'SystemRoot', 'COMSPEC', 'PATHEXT', 'ProgramFiles', 'ProgramFiles(x86)', 'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'MODEL_API_KEY', ...task.modelBinding.headerEnvKeys]) if (process.env[key]) environment[key] = process.env[key];
      for (const key of task.environment?.envAllowlist ?? []) if (process.env[key]) environment[key] = process.env[key];
      environment.PI_CODING_AGENT_DIR = join(aux.sessionDir, 'profile'); environment.PI_TELEMETRY = 'off';
      const module = this.config.childModule ?? fileURLToPath(new URL('../engine/bootstrap.js', import.meta.url));
      const profileOptions = isNoForkProfile(this.executionProfile) ? { execPath: SANDBOX_EXEC, execArgv: ['-p', NO_FORK_POLICY, process.execPath] } : {};
      const child = fork(module, [], { ...profileOptions, cwd: aux.workspace, env: environment, detached: process.platform !== 'win32', stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
      child.on('error', () => {});
      const live: AuxLive = { child, stopping: false, ready: false, pending: new Map(), pauseWaiters: new Map(), resumeWaiters: new Map(), taskId, auxSessionId, instruction: this.auxInstruction(task, aux) };
      this.auxLive.set(auxSessionId, live);
      if (process.platform === 'win32') {
        try { live.job = new WindowsJob(); live.job.assign(child.pid!); }
        catch { live.stopping = true; child.kill(); live.job?.close(); throw new Error('JOB_ASSIGN_FAILED'); }
      }
      if (process.platform === 'darwin') {
        try { live.mac = await MacOwnership.arm(child.pid!); } catch (error) { child.kill('SIGKILL'); throw error; }
        if (live.stopping || this.closed || this.store.task(taskId).auxSessions?.[auxSessionId]?.state !== 'RUNNING') { live.stopping = true; await this.terminate(live); return; }
      }
      child.on('message', msg => { this.auxEvent(taskId, auxSessionId, live, msg as Row); });
      child.on('exit', () => {
        if (this.closed || this.auxLive.get(auxSessionId) !== live) return;
        const a = this.store.task(taskId).auxSessions?.[auxSessionId];
        if (a && a.state === 'RUNNING' && !live.stopping) void this.finishAux(taskId, auxSessionId, 'FAILED', 'AUX_PROCESS_EXITED_UNCONFIRMED', live);
        else this.auxLive.delete(auxSessionId);
      });
      child.send({ type: 'init', executionProfile: this.executionProfile, workspace: aux.workspace, sessionDir: aux.sessionDir, model: task.modelBinding, probeEnv: task.environment?.envAllowlist ?? [], mcpServers: [], extensions: [] });
    } catch { void this.finishAux(taskId, auxSessionId, 'FAILED', 'LOCAL_LAUNCH_FAILED', this.auxLive.get(auxSessionId)); }
  }
  private auxEvent(taskId: string, auxSessionId: string, live: AuxLive, event: Row) {
    if (this.closed || this.auxLive.get(auxSessionId) !== live) return;
    const task = this.store.task(taskId); const aux = task.auxSessions?.[auxSessionId]; if (!aux) return;
    switch (event.type) {
      case 'ready':
        aux.nativeSessionId = event.nativeSessionId; aux.nativeSessionFile = event.sessionFile; this.store.save(task);
        live.child.send({ type: 'prompt', requestKey: 'aux-prompt-' + auxSessionId, content: live.instruction, aux: true }); return;
      case 'started': case 'user_consumed': case 'question_consumed': case 'answered': case 'queued': case 'gate_held': case 'paused': case 'pause_failed': case 'resumed': case 'extension_error': return;
      case 'tool_start': aux.toolCalls++; this.store.transaction(() => { this.store.save(task); this.streamAppend(task, aux.messageStreamId, { child: auxSessionId }, { kind: 'TOOL_CALL', text: String(event.toolName ?? ''), ref: event.toolCallId }); }); return;
      case 'tool_end': this.store.transaction(() => { this.store.save(task); this.streamAppend(task, aux.messageStreamId, { child: auxSessionId }, { kind: 'TOOL_RESULT', text: String(event.toolName ?? ''), status: event.isError ? 'ERROR' : 'OK', ref: event.toolCallId }); }); return;
      case 'question':
        // Research sessions are autonomous: an ask_user is declined, never routed
        // into the parent task's input requests.
        try { live.child.send({ type: 'answer', requestKey: 'aux-decline-' + event.inputRequestId, inputRequestId: event.inputRequestId, response: { cancelled: true } }); } catch {}
        this.store.transaction(() => { this.store.save(task); this.streamAppend(task, aux.messageStreamId, { child: auxSessionId }, { kind: 'STATUS', text: 'INPUT_REQUEST_DECLINED', status: 'INPUT_DECLINED' }); });
        return;
      // OUTPUT_DELTA capture is best-effort: a store hiccup must not break the
      // child message flow that still owns this session's lifecycle events.
      case 'assistant_delta': try { this.streamOutputDelta(task, aux.messageStreamId, { child: auxSessionId }, event); } catch {} return;
      case 'assistant': {
        const text = publicText(event.text ?? ''); if (text) aux.answer = (aux.answer + text + '\n').slice(-AUX_ANSWER_MAX);
        if (event.failed) aux.nativeFailure = event.errorCode ?? 'MODEL_REQUEST_FAILED';
        if (event.usage) for (const key of ['input', 'output', 'cacheRead', 'cacheWrite']) aux.usage[key] += Number(event.usage[key] ?? 0);
        this.store.transaction(() => { this.store.save(task); if (text) this.streamAppend(task, aux.messageStreamId, { child: auxSessionId }, { kind: 'ASSISTANT_MESSAGE', ...this.streamText(event.text ?? '') }); });
        const exceeded = this.auxBudgetExceeded(aux);
        if (exceeded) { this.store.event(task, 'aux-session.budget_exceeded', { auxSessionId, ...exceeded, usage: { ...aux.usage } }); void this.finishAux(taskId, auxSessionId, 'FAILED', 'BUDGET_EXCEEDED', live); }
        return;
      }
      case 'gate_check': this.auxGateCheck(taskId, auxSessionId, live, event); return;
      case 'settled': void this.finishAux(taskId, auxSessionId, aux.nativeFailure ? 'FAILED' : 'SUCCEEDED', aux.nativeFailure, live); return;
      case 'failure': void this.finishAux(taskId, auxSessionId, 'FAILED', event.code ?? 'PI_EXECUTION_FAILED', live); return;
    }
  }
  private auxBudgetExceeded(aux: Row): Row | null {
    const caps = aux.budget?.caps; if (!caps) return null;
    const u = aux.usage; const total = u.input + u.output + u.cacheRead + u.cacheWrite;
    if (caps.maxTotalTokens != null && total > caps.maxTotalTokens) return { field: 'maxTotalTokens', cap: caps.maxTotalTokens, observed: total };
    return null;
  }
  /** Aux gate: its own SOFT budget verdict, plus workspace-only write gating so a
   *  file-mutating tool cannot escape the snapshot copy into the real workspace. */
  private auxGateCheck(taskId: string, auxSessionId: string, live: AuxLive, event: Row) {
    try {
      const aux = this.store.task(taskId).auxSessions?.[auxSessionId];
      const exceeded = aux ? this.auxBudgetExceeded(aux) : null;
      if (exceeded || !aux || aux.state !== 'RUNNING') {
        live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'terminate', reason: exceeded ? 'BUDGET_EXCEEDED' : 'AUX_SESSION_CLOSED' });
        if (aux && aux.state === 'RUNNING') void this.finishAux(taskId, auxSessionId, 'FAILED', 'BUDGET_EXCEEDED', live);
        return;
      }
      if (event.kind === 'TOOL_ADMISSION' && !['read', 'bash', 'ask_user', 'ls', 'grep', 'glob'].includes(event.toolName)) {
        const denied = this.scopeDenial({ workspace: aux.workspace }, event);
        if (denied) { this.store.event(this.store.task(taskId), 'aux-session.scope_violation', { auxSessionId, toolName: event.toolName, toolCallId: event.toolCallId, deniedPath: denied }); live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'block', reason: 'AUX_WORKSPACE_ONLY: ' + denied + ' is outside the isolated snapshot' }); return; }
      }
      live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'allow' });
    } catch { try { live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'block', reason: 'GATE_EVALUATION_FAILED' }); } catch {} }
  }
  private async finishAux(taskId: string, auxSessionId: string, state: string, reasonCode: string | null | undefined, live?: AuxLive) {
    if (live) { live.stopping = true; if (!live.stopped) await this.terminate(live); this.auxLive.delete(auxSessionId); }
    this.store.transaction(() => this.update(taskId, t => {
      const a = t.auxSessions?.[auxSessionId]; if (!a || ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(a.state)) return;
      a.state = state; a.finishedAt = now(); if (reasonCode) a.reasonCode = reasonCode;
      this.store.event(t, 'aux-session.completed', { auxSessionId, state, reasonCode: reasonCode ?? null, usage: { ...a.usage }, answerBytes: Buffer.byteLength(a.answer ?? ''), toolCalls: a.toolCalls ?? 0 });
      if (a.messageStreamId) {
        this.streamAppend(t, a.messageStreamId, { child: auxSessionId }, { kind: 'STATUS', text: state, status: state });
        this.streamAppend(t, a.messageStreamId, { child: auxSessionId }, { kind: 'RETURN', status: state, ...this.streamText((a.answer ?? '') || (reasonCode ?? state)) });
      }
    }));
    const aux = this.store.task(taskId).auxSessions?.[auxSessionId];
    if (aux?.requestKey) { const op = this.store.operation(aux.requestKey); if (op && op.state !== 'APPLIED') { op.state = 'APPLIED'; op.detail = 'Auxiliary session ' + state.toLowerCase() + (reasonCode ? ': ' + reasonCode : ''); op.auxSessionId = auxSessionId; op.auxState = state; this.store.saveOperation(op); } }
  }
  async auxStop(taskId: string, auxSessionId: string, body: Row) {
    exact(body, ['requestKey']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    const task = this.store.task(taskId); const aux = task.auxSessions?.[id(auxSessionId)];
    if (!aux) throw new Error('AUX_SESSION_NOT_FOUND');
    const op = this.store.accept(body.requestKey, taskId, body, { kind: 'AUX_SESSION_STOP', state: 'DISPATCHING', auxSessionId });
    const live = this.auxLive.get(auxSessionId);
    if (!live || aux.state !== 'RUNNING') { op.state = 'APPLIED'; op.detail = 'Auxiliary session already ' + String(aux.state).toLowerCase(); this.store.saveOperation(op); return op; }
    await this.finishAux(taskId, auxSessionId, 'CANCELLED', 'AUX_STOP', live);
    const final = this.store.task(taskId).auxSessions[auxSessionId];
    op.state = 'APPLIED'; op.detail = 'Auxiliary session stopped; owned child drained; parent task unaffected'; op.auxState = final.state; this.store.saveOperation(op);
    return op;
  }
  /** Explicit delivery (N17): the aux answer becomes a normal NATIVE_FOLLOW_UP on
   *  the main task through the existing guarded interact path — until then aux
   *  output never touches the main session. */
  auxDeliver(taskId: string, auxSessionId: string, body: Row) {
    exact(body, ['requestKey']);
    const task = this.store.task(taskId); this.assertUnsealed(task); const aux = task.auxSessions?.[id(auxSessionId)];
    if (!aux) throw new Error('AUX_SESSION_NOT_FOUND');
    const prior = this.store.operation(body.requestKey);
    if (prior) { if (prior.taskId !== taskId || prior.auxDelivery !== auxSessionId) throw new Error('IDEMPOTENCY_CONFLICT'); return { delivered: prior, auxSessionId, replayed: true }; }
    if (aux.deliveredAt) throw new Error('AUX_ALREADY_DELIVERED');
    if (aux.state !== 'SUCCEEDED') throw new Error('AUX_NOT_FINISHED');
    const content = ('Auxiliary session ' + auxSessionId + ' completed its read-only research.\nQuestion: ' + aux.question + '\n\nAnswer:\n' + (aux.answer ?? '').trim()).slice(0, 16000);
    const op = this.interact(taskId, { requestKey: body.requestKey, kind: 'NATIVE_FOLLOW_UP', content, generation: task.generation, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, targetCommandId: task.currentCommandId });
    op.auxDelivery = auxSessionId; this.store.saveOperation(op);
    this.update(taskId, t => { const a = t.auxSessions[auxSessionId]; a.deliveredAt = now(); a.deliveryRequestKey = body.requestKey; this.store.event(t, 'aux-session.delivered', { auxSessionId, requestKey: body.requestKey, targetCommandId: body.requestKey }); });
    return { delivered: op, auxSessionId };
  }
  sweepAuxDeadlines() { for (const [auxId, live] of this.auxLive) { const a = this.store.task(live.taskId).auxSessions?.[auxId]; if (a && a.state === 'RUNNING' && Date.now() > Number(a.deadlineEpochMillis ?? 0)) void this.finishAux(live.taskId, auxId, 'CANCELLED', 'AUX_DEADLINE', live); } }
  // ---- Assignment transitions (P13): same-kernel takeover. The current owner
  //      quiesces at a boundary checkpoint (or is already sealed PAUSED/completed),
  //      then a new ownerEpoch/generation/segment is admitted on the SAME native
  //      session via sealed session-file reopen. No cross-engine migration.
  private transitionCompatibility(task: Row, profile: any) {
    if (profile !== undefined) { if (!profile || typeof profile !== 'object' || Array.isArray(profile)) throw new Error('INVALID_TARGET_PROFILE'); exact(profile, ['engineId', 'model', 'toolAllowlist']); }
    if ((profile?.engineId ?? 'pi') !== 'pi' || task.engineId !== 'pi') throw new Error('INCOMPATIBLE_TRANSITION');
    if (profile?.model !== undefined && profile.model !== task.modelBinding.name) throw new Error('INCOMPATIBLE_TRANSITION');
    let toolAllowlist: string[] | null = null;
    if (profile?.toolAllowlist !== undefined) {
      if (!Array.isArray(profile.toolAllowlist) || !profile.toolAllowlist.length || profile.toolAllowlist.length > 64 || profile.toolAllowlist.some((t: any) => typeof t !== 'string' || !t.length || t.length > 128)) throw new Error('INVALID_TARGET_PROFILE');
      const active: string[] = task.activeTools ?? [];
      const honoured = profile.toolAllowlist.every((t: string) => active.includes(t) || (t.endsWith('*') && active.some(a => a.startsWith(t.slice(0, -1)))));
      if (!honoured) throw new Error('INCOMPATIBLE_TRANSITION');
      toolAllowlist = profile.toolAllowlist;
    }
    return { engineId: 'pi', model: task.modelBinding.name, toolAllowlist, sameEngine: true, crossEngine: false, sameNativeSession: true };
  }
  /** Admits the next command on the same native session under a new owner epoch:
   *  new generation/segment, durable transition record, relaunch as RESTORING. */
  private transitionAdmit(taskId: string, body: Row, transitionId: string, compatibility: Row, method: string, manifest: Row | null) {
    const op = this.store.transaction(() => {
      const task = this.store.task(taskId);
      const fromEpoch = String(task.ownerEpoch ?? '1'); const toEpoch = String(Number(fromEpoch) + 1);
      const fromSegment = task.segmentId; const toSegment = randomUUID();
      const op = this.store.operation(body.requestKey) ?? this.store.accept(body.requestKey, taskId, body, { kind: 'TRANSITION', state: 'ACCEPTED' });
      if (manifest) task.checkpointHistory = [...(task.checkpointHistory ?? []).slice(-11), manifest];
      task.transitionedFrom = { segmentId: fromSegment, transitionId, method };
      task.reopenFrom = { nativeSessionId: task.nativeSessionId, segmentId: fromSegment, checkpoint: method !== 'COMPLETED_SESSION', transition: transitionId };
      if (compatibility.toolAllowlist) task.toolAllowlist = compatibility.toolAllowlist;
      this.archiveEffects(task);
      task.goal = method === 'COMPLETED_SESSION'
        ? 'Assignment transitioned to a new owner epoch on the same native session (same engine pi). The previous owner stopped after proven completion. Continue the original task.'
        : 'Assignment transitioned to a new owner epoch on the same native session (same engine pi). The previous owner quiesced at a verified boundary. Continue the original task.';
      task.currentCommandId = body.requestKey; task.segmentId = toSegment; task.generation = randomUUID(); task.ownerEpoch = toEpoch;
      task.launchState = 'RESTORING'; task.intakeClosed = false; task.quiescent = false; task.nativeIdle = false; task.deadlineEpochMillis = String(Date.now() + this.taskDeadlineMs(task));
      delete task.pid; delete task.stopProof; delete task.nativeFailure; delete task.pauseFromState; delete task.completedSessionSeal;
      (task.transitions ??= []).push({ transitionId, fromEpoch, toEpoch, fromSegment, toSegment, compatibility: { ...compatibility, method }, createdAt: now() });
      task.transitions = task.transitions.slice(-32);
      this.state(task, 'UNKNOWN'); this.store.save(task);
      this.store.event(task, 'transition.admitted', { transitionId, fromEpoch, toEpoch, fromSegment, toSegment, via: 'SESSION_REOPEN', method });
      op.kind = 'TRANSITION'; op.transitionId = transitionId; op.fromEpoch = fromEpoch; op.toEpoch = toEpoch; op.state = 'DISPATCHING'; this.store.saveOperation(op);
      return op;
    });
    setImmediate(() => { void this.launch(taskId); });
    return op;
  }
  private transitionModelGate(task: Row) {
    if (!this.config.childModule) {
      if (!process.env.MODEL_API_KEY) throw new Error('MODEL_AUTH_UNAVAILABLE');
      this.assertModelReferences(task);
      if (process.env.MODEL_DEFAULT_MODEL !== task.modelBinding.name || process.env.MODEL_BASE_URL !== task.modelBinding.baseUrl) throw new Error('MODEL_BINDING_CHANGED');
    }
    if (engineBinding().digest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
  }
  private verifySealedFile(manifest: Row) {
    try { if (hash(readFileSync(realpathSync(manifest.sessionFile))) !== (manifest.sessionFileSha256 ?? manifest.sessionSha256)) throw 0; } catch { throw new Error('CHECKPOINT_INCOMPATIBLE'); }
  }
  async transition(taskId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    exact(body, ['requestKey', 'targetProfile', 'generation', 'nativeSessionId', 'nativeTurnId', 'targetCommandId', 'ownerEpoch']);
    const task = this.store.task(taskId); this.guard(task, body); this.assertUnsealed(task);
    const compatibility = this.transitionCompatibility(task, body.targetProfile);
    const transitionId = 'tr-' + randomUUID();
    const live = this.live.get(taskId);
    // Path A: a live task. PAUSE at a real boundary first, then drain the owned
    // child and reopen the sealed session under the new epoch.
    if (this.canPause(task, live)) {
      this.store.transaction(() => { this.store.accept(body.requestKey, taskId, body, { kind: 'TRANSITION', state: 'ACCEPTED', transitionId }); this.store.event(this.store.task(taskId), 'TRANSITION_ACCEPTED', { requestKey: body.requestKey, transitionId }); });
      const reply = await this.requestPause(live!, body.requestKey);
      const fail = (detail: string) => { const op = this.store.operation(body.requestKey)!; op.state = detail === 'TRANSITION_QUIESCE_FAILED' ? 'REQUESTED' : 'UNKNOWN'; op.detail = detail; this.store.saveOperation(op); this.store.event(this.store.task(taskId), 'transition.failed', { transitionId, reasonCode: detail }); return op; };
      if (reply?.type !== 'paused') { try { await this.requestResume(live!, 'transition-release-' + body.requestKey); } catch {} return fail('TRANSITION_QUIESCE_FAILED'); }
      const manifest = this.store.transaction(() => {
        const t = this.store.task(taskId); const m = this.sealCheckpoint(t, live!, reply);
        if (m) { t.checkpointManifest = m; if (!['CANCELLED', 'FAILED'].includes(t.state)) this.state(t, 'PAUSED'); this.store.save(t); this.store.event(t, 'transition.quiesced', { transitionId, boundary: reply.boundary ?? 'UNKNOWN', method: 'BOUNDARY_CHECKPOINT', checkpointId: m.checkpointId }); }
        return m;
      });
      if (!manifest) { try { await this.requestResume(live!, 'transition-release-' + body.requestKey); } catch {} return fail('TRANSITION_QUIESCE_FAILED'); }
      live!.stopping = true;
      const stopped = await this.terminate(live!);
      this.update(taskId, t => { this.markEffectsUnknown(t); for (const q of Object.values(t.inputRequests) as Row[]) if (['PENDING', 'DISPATCH_STARTED'].includes(q.state)) q.state = 'EXPIRED'; });
      if (!stopped) return fail('TRANSITION_QUIESCE_UNPROVEN');
      // Post-kill drift check: the held boundary must have kept the sealed bytes.
      try { this.verifySealedFile(manifest); } catch { return fail('CHECKPOINT_INCOMPATIBLE'); }
      this.transitionModelGate(this.store.task(taskId));
      return this.transitionAdmit(taskId, body, transitionId, compatibility, 'BOUNDARY_CHECKPOINT', manifest);
    }
    // Path B: task already PAUSED with a sealed checkpoint (possibly post-restart).
    if (task.state === 'PAUSED' && task.checkpointManifest) {
      this.verifyCheckpoint(task);
      if (live && live.child.connected && !live.stopping) {
        live.stopping = true;
        const stopped = await this.terminate(live);
        if (!stopped) { const op = this.store.accept(body.requestKey, taskId, body, { kind: 'TRANSITION', state: 'UNKNOWN', detail: 'TRANSITION_QUIESCE_UNPROVEN' }); return op; }
        this.verifyCheckpoint(task);
        this.update(taskId, t => { this.markEffectsUnknown(t); });
      }
      if (this.store.tasks().filter(t => t.taskId !== taskId && !t.quiescent).length >= this.maxActive()) throw new Error('LOCAL_TASK_BUSY');
      this.transitionModelGate(this.store.task(taskId));
      const manifest = task.checkpointManifest;
      this.store.event(this.store.task(taskId), 'transition.quiesced', { transitionId, method: 'BOUNDARY_CHECKPOINT', checkpointId: manifest.checkpointId, priorPause: true });
      return this.transitionAdmit(taskId, body, transitionId, compatibility, 'BOUNDARY_CHECKPOINT', manifest);
    }
    // Path C: proven completed+sealed session — stopped-session continuation.
    if (this.canReopen(task)) {
      if (this.store.tasks().filter(t => t.taskId !== taskId && !t.quiescent).length >= this.maxActive()) throw new Error('LOCAL_TASK_BUSY');
      this.transitionModelGate(task);
      if (task.engineBindingDigest !== this.engineBinding.digest) throw new Error('ENGINE_BINDING_CHANGED');
      if (!this.config.childModule && (process.env.MODEL_DEFAULT_MODEL !== task.modelBinding.name || process.env.MODEL_BASE_URL !== task.modelBinding.baseUrl || digest(task.modelBinding) !== task.completedSessionSeal.modelBindingDigest)) throw new Error('MODEL_BINDING_CHANGED');
      if (realpathSync(task.workspace) !== realpathSync(join(this.store.directory, 'workspaces', task.taskId))) throw new Error('OWNED_WORKSPACE_REQUIRED');
      if (hash(readFileSync(task.nativeSessionFile)) !== task.completedSessionSeal.sessionSha256) throw new Error('NATIVE_SESSION_SEAL_CHANGED');
      this.store.event(this.store.task(taskId), 'transition.quiesced', { transitionId, method: 'COMPLETED_SESSION', proof: task.completedSessionSeal.quiescenceMethod });
      return this.transitionAdmit(taskId, body, transitionId, compatibility, 'COMPLETED_SESSION', null);
    }
    throw new Error('TRANSITION_STATE_INCOMPATIBLE');
  }
  // ---- W5b: writable collaboration DAG (P14) and result adoption (P15).
  //      Children are WRITABLE executions on isolated snapshot copies — a
  //      different concept from read-only aux sessions: their checkouts are
  //      candidates the parent may adopt explicitly. Facts ride the same task
  //      row and event journal; mutating endpoints carry requestKey idempotency.
  //      No AMC/cross-core wiring (P28 stays GAP); copy isolation, not OS
  //      enforcement; file-level adoption, not a git merge.
  private maxCollabChildren() { const n = Number(process.env.NWF_PI_MAX_ACTIVE_COLLAB_CHILDREN ?? 4); return Number.isFinite(n) ? Math.max(1, Math.floor(n)) : 4; }
  private collabPublic(c: Row) { return { collaborationId: c.collaborationId, taskId: c.taskId, name: c.name ?? null, state: c.state, reasonCode: c.reasonCode ?? null, result: c.result ?? null, children: Object.values(c.children).map((ch: any) => this.collabChildPublic(ch)), createdAt: c.createdAt, finishedAt: c.finishedAt ?? null }; }
  private collabChildPublic(ch: Row) { return { childId: ch.childId, goal: ch.goal, state: ch.state, reasonCode: ch.reasonCode ?? null, dependsOn: ch.dependsOn ?? [], failedDependencies: ch.failedDependencies, contextPackageIds: ch.contextPackageIds ?? [], budget: ch.budget ?? null, nativeSessionId: ch.nativeSessionId ?? null, usage: ch.usage, toolCalls: ch.toolCalls ?? 0, stopProof: ch.stopProof ?? null, baseline: ch.baseline ? { digest: ch.baseline.digest, fileCount: ch.baseline.fileCount, bytes: ch.baseline.bytes, snapshotAt: ch.baseline.snapshotAt } : null, candidate: ch.candidate ?? null, adoptions: ch.adoptions ?? [], startedAt: ch.startedAt ?? null, finishedAt: ch.finishedAt ?? null, createdAt: ch.createdAt }; }
  /** Isolated checkout for a collaboration child: bounded regular-file copy of
   *  the task workspace (`.git`, symlinks/junctions skipped — same semantics as
   *  the aux snapshot) plus the per-file sha256 baseline map the adoption diff
   *  and the parent-drift conflict check are computed against. */
  private snapshotCollabCheckout(workspace: string, dest: string) {
    const files: Row = {}; let fileCount = 0, bytes = 0, skipped = 0;
    const srcRoot = realpathSync(workspace);
    const walk = (src: string, dst: string, prefix: string) => {
      mkdirSync(dst, { recursive: true });
      for (const entry of readdirSync(src, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
        if (entry.name === '.git') { skipped++; continue; }
        const s = join(src, entry.name); const st = lstatSync(s);
        if (st.isSymbolicLink()) { skipped++; continue; }
        if (st.isDirectory()) walk(s, join(dst, entry.name), prefix + entry.name + '/');
        else if (st.isFile()) {
          if (++fileCount > AUX_SNAPSHOT_MAX_FILES || st.size > AUX_SNAPSHOT_MAX_BYTES || bytes + st.size > AUX_SNAPSHOT_MAX_BYTES) throw new Error('COLLAB_SNAPSHOT_TOO_LARGE');
          const content = readFileSync(s); bytes += st.size; copyFileSync(s, join(dst, entry.name));
          files[prefix + entry.name] = hash(content);
        } else skipped++;
      }
    };
    walk(srcRoot, dest, '');
    return { files, digest: digest(files), fileCount, bytes, skipped, sourceDigest: hash(srcRoot), snapshotAt: now() };
  }
  private collabInstruction(task: Row, collab: Row, child: Row) {
    const parts = ['You are a writable child execution inside an isolated checkout of the parent task workspace. Nothing you write reaches the parent workspace directly: your finished checkout is a candidate the parent task may adopt explicitly, in whole or in part. Work only inside this checkout, write all result files here, then finish with a short summary. Do not ask the user questions.'];
    parts.push('<<GOAL>>\n' + child.goal + '\n<</GOAL>>');
    parts.push('<<FIXED_MATERIAL>>\n' + this.packageMaterial(task, child.contextPackageIds ?? []).slice(0, 131072) + '\n<</FIXED_MATERIAL>>');
    parts.push('When finished, list the files you wrote.');
    return parts.join('\n\n');
  }
  /** Shared collaboration-plan validation segment (create and preflight): name,
   *  children whitelist, goal/dependency/package/budget/writeScope checks, DAG
   *  acyclicity and the per-task running cap. Returns normalized specs and a
   *  stable canonical planDigest (volatile fields — scopeId, declaredAt,
   *  reservedAt — are excluded so the same plan digests identically). */
  private collabPlan(task: Row, body: Row) {
    // A STOP-closed intake also closes writable collaboration intake; a quiescent
    // (e.g. SUCCEEDED) task may still spawn children against its workspace.
    if (task.intakeClosed) throw new Error('COLLABORATION_TASK_INACTIVE');
    if (body.name !== undefined && (typeof body.name !== 'string' || !body.name.trim() || body.name.length > 128 || /[\x00-\x1f]/.test(body.name))) throw new Error('INVALID_COLLABORATION_NAME');
    if (!Array.isArray(body.children) || !body.children.length || body.children.length > COLLAB_MAX_CHILDREN) throw new Error('INVALID_COLLABORATION_CHILDREN');
    const declared = new Set<string>();
    for (const c of body.children) {
      if (!c || typeof c !== 'object' || Array.isArray(c)) throw new Error('INVALID_COLLABORATION_CHILDREN');
      exact(c, ['childId', 'goal', 'dependsOn', 'budget', 'writeScope', 'contextPackageIds']);
      if (c.childId !== undefined) { if (typeof c.childId !== 'string' || !COLLAB_CHILD_ID.test(c.childId) || declared.has(c.childId)) throw new Error('INVALID_COLLABORATION_CHILD_ID'); declared.add(c.childId); }
    }
    const used = new Set(declared); let auto = 0;
    const specs = body.children.map((c: any) => {
      let childId = c.childId;
      if (childId === undefined) { do { childId = 'child-' + (++auto); } while (used.has(childId)); used.add(childId); }
      if (typeof c.goal !== 'string' || !c.goal.trim() || Buffer.byteLength(c.goal) > COLLAB_MAX_GOAL) throw new Error('INVALID_COLLABORATION_GOAL');
      const dependsOn = c.dependsOn ?? [];
      if (!Array.isArray(dependsOn) || dependsOn.length > COLLAB_MAX_CHILDREN || new Set(dependsOn).size !== dependsOn.length || dependsOn.some((d: any) => typeof d !== 'string' || !COLLAB_CHILD_ID.test(d))) throw new Error('INVALID_COLLABORATION_DEPENDENCY');
      const contextPackageIds = c.contextPackageIds ?? [];
      if (!Array.isArray(contextPackageIds) || contextPackageIds.length > AUX_MAX_PACKAGES || contextPackageIds.some((p: any) => typeof p !== 'string')) throw new Error('INVALID_COLLABORATION_PACKAGES');
      for (const pid of contextPackageIds) if (!task.contextPackages?.[pid]) throw new Error('PACKAGE_NOT_FOUND');
      return { childId, goal: c.goal, dependsOn, budget: this.validateAuxBudget(c.budget), writeScope: this.validateWriteScope(c.writeScope), contextPackageIds };
    });
    // DAG checks: dependencies must name sibling children, no self-deps, acyclic.
    const idSet = new Set(specs.map(s => s.childId));
    for (const s of specs) for (const d of s.dependsOn) { if (!idSet.has(d)) throw new Error('COLLABORATION_DEPENDENCY_UNKNOWN'); if (d === s.childId) throw new Error('COLLABORATION_DEPENDENCY_CYCLE'); }
    const depMap = new Map(specs.map(s => [s.childId, s.dependsOn] as [string, string[]]));
    const visiting = new Set<string>(); const done = new Set<string>();
    const dfs = (n: string): void => { if (done.has(n)) return; if (visiting.has(n)) throw new Error('COLLABORATION_DEPENDENCY_CYCLE'); visiting.add(n); for (const d of depMap.get(n)!) dfs(d); visiting.delete(n); done.add(n); };
    for (const s of specs) dfs(s.childId);
    if (Object.values(task.collaborations ?? {}).filter((c: any) => c.state === 'RUNNING').length >= COLLAB_MAX_PER_TASK) throw new Error('COLLABORATION_LIMIT');
    const planDigest = digest({ name: body.name ?? null, children: specs.map(s => ({ childId: s.childId, goal: s.goal, dependsOn: s.dependsOn, budget: s.budget ? { caps: s.budget.caps, enforcement: s.budget.enforcement } : null, writeScope: s.writeScope ? { extraWriteDirs: s.writeScope.extraWriteDirs, denyPaths: s.writeScope.denyPaths } : null, contextPackageIds: s.contextPackageIds })) });
    return { specs, planDigest };
  }
  /** Validate-only collaboration plan (C3): identical checks to create, but no
   *  collaboration row, no child spawn, no write effect beyond the durable
   *  preflight record (≤32 per task, TTL-pruned). */
  collaborationPreflight(taskId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'name', 'children']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return { ...prior, replayed: true }; }
    const task = this.store.task(taskId); this.assertUnsealed(task);
    const { specs, planDigest } = this.collabPlan(task, body);
    const preflightId = 'pf-' + randomUUID();
    const expiresAt = new Date(Date.now() + COLLAB_PREFLIGHT_TTL_MS).toISOString();
    const record: Row = { schemaVersion: 'collaboration-preflight/1', preflightId, taskId, planDigest, childCount: specs.length,
      sourceBindings: { taskId, generation: task.generation, segmentId: task.segmentId, ownerEpoch: String(task.ownerEpoch ?? '1'), taskState: task.state, intakeClosed: !!task.intakeClosed, runningCollaborations: Object.values(task.collaborations ?? {}).filter((c: any) => c.state === 'RUNNING').length, engineBindingDigest: this.engineBinding.digest },
      allowedScopes: { workspace: 'ISOLATED_SNAPSHOT_COPY', writeScope: 'CHECKOUT_PLUS_DECLARED', contextPackageIds: 'TASK_OWNED', shells: 'CHILD_LOCAL_PROCESS' },
      budgetTerms: { enforcement: 'SOFT', basis: 'ENGINE_REPORTED_POST_HOC', fields: ['maxTotalTokens'], deadlineMs: COLLAB_DEADLINE_MS },
      createdAt: now(), expiresAt };
    this.store.transaction(() => {
      const t = this.store.task(taskId); this.assertUnsealed(t);
      const map = t.collabPreflights ??= {};
      for (const [k, v] of Object.entries(map) as [string, Row][]) if (Date.parse(v.expiresAt) <= Date.now()) delete map[k];
      const keys = Object.keys(map).sort((a, b) => String(map[a].createdAt).localeCompare(String(map[b].createdAt)));
      while (keys.length && Object.keys(map).length >= COLLAB_PREFLIGHT_MAX) delete map[keys.shift()!];
      map[preflightId] = record; this.store.save(t);
      this.store.event(t, 'collaboration.preflighted', { preflightId, planDigest, childCount: specs.length, expiresAt });
      this.store.accept(body.requestKey, taskId, body, { kind: 'COLLABORATION_PREFLIGHT', state: 'APPLIED', preflightId, planDigest });
    });
    return { preflightId, planDigest, sourceBindings: record.sourceBindings, allowedScopes: record.allowedScopes, budgetTerms: record.budgetTerms, expiresAt };
  }
  createCollaboration(taskId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'name', 'children', 'expectedPreflightId', 'expectedPlanDigest']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    if (this.closed) throw new Error('LOCAL_HOST_CLOSED');
    const task = this.store.task(taskId); this.assertUnsealed(task);
    const { specs, planDigest } = this.collabPlan(task, body);
    // Optional version guard: a caller that reviewed a preflight binds the same
    // plan. An unknown/expired/diverged preflight refuses with PREFLIGHT_STALE;
    // creating with no preflight stays allowed for legacy clients.
    if (body.expectedPreflightId !== undefined) {
      const pf = task.collabPreflights?.[String(body.expectedPreflightId)];
      if (!pf || Date.parse(pf.expiresAt) <= Date.now() || pf.planDigest !== planDigest) throw new Error('PREFLIGHT_STALE');
      // A transition rotates generation/segment/epoch; a preflight issued
      // before that no longer authorizes creation (same rule as Codex).
      const b = pf.sourceBindings;
      if (b && (b.generation !== task.generation || b.segmentId !== task.segmentId || String(b.ownerEpoch ?? '1') !== String(task.ownerEpoch ?? '1'))) throw new Error('PREFLIGHT_STALE');
    }
    if (body.expectedPlanDigest !== undefined && body.expectedPlanDigest !== planDigest) throw new Error('PREFLIGHT_STALE');
    const collaborationId = 'collab-' + randomUUID();
    const children: Row = {};
    for (const s of specs) {
      const dir = join(this.store.directory, 'collab', collaborationId, s.childId);
      children[s.childId] = { childId: s.childId, goal: s.goal, goalSha256: hash(s.goal), dependsOn: s.dependsOn, budget: s.budget, writeScope: s.writeScope, contextPackageIds: s.contextPackageIds, state: 'PENDING', reasonCode: null, workspace: join(dir, 'workspace'), sessionDir: join(dir, 'session'), baseline: null, candidate: null, adoptions: [], nativeSessionId: null, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, toolCalls: 0, output: '', deadlineEpochMillis: String(Date.now() + COLLAB_DEADLINE_MS), createdAt: now(), startedAt: null, finishedAt: null };
    }
    const collab: Row = { schemaVersion: 'worker-collaboration/1', collaborationId, taskId, name: body.name, requestKey: body.requestKey, state: 'RUNNING', children, createdAt: now() };
    let op: Row;
    this.store.transaction(() => {
      const t = this.store.task(taskId);
      if (body.expectedPreflightId !== undefined) { const pf = t.collabPreflights?.[String(body.expectedPreflightId)]; if (pf) pf.consumedAt = now(); }
      (t.collaborations ??= {})[collaborationId] = collab; this.store.save(t);
      this.store.event(t, 'collaboration.created', { collaborationId, name: body.name ?? null, childCount: specs.length, planDigest, preflightId: body.expectedPreflightId ?? null, children: specs.map(s => ({ childId: s.childId, dependsOn: s.dependsOn, goalSha256: hash(s.goal), budget: s.budget?.caps ?? null })) });
      op = this.store.accept(body.requestKey, taskId, body, { kind: 'COLLABORATION_CREATE', state: 'DISPATCHING', collaborationId, planDigest });
    });
    setImmediate(() => this.pumpCollab(taskId, collaborationId));
    return op!;
  }
  /** DAG scheduler: mark dependents of failed/cancelled children BLOCKED
   *  (partial failure — the collaboration itself still completes), launch every
   *  child whose dependencies all SUCCEEDED up to the host-wide running pool,
   *  and complete the collaboration once all children are terminal. */
  private pumpCollab(taskId: string, collabId: string) {
    if (this.closed) return;
    const launch: string[] = []; let completed = false;
    this.update(taskId, t => {
      const collab = t.collaborations?.[collabId]; if (!collab || collab.state !== 'RUNNING') return;
      for (const child of Object.values(collab.children) as Row[]) {
        if (child.state !== 'PENDING') continue;
        const failed = child.dependsOn.filter((d: string) => ['FAILED', 'CANCELLED', 'BLOCKED'].includes(collab.children[d]?.state));
        if (failed.length) { child.state = 'BLOCKED'; child.reasonCode = 'DEPENDENCY_UNSATISFIED'; child.failedDependencies = failed; child.finishedAt = now(); this.store.event(t, 'collaboration.child.blocked', { collaborationId: collabId, childId: child.childId, reasonCode: 'DEPENDENCY_UNSATISFIED', failedDependencies: failed }); }
      }
      for (const child of Object.values(collab.children) as Row[]) {
        if (child.state !== 'PENDING') continue;
        if (this.collabLive.size + launch.length >= this.maxCollabChildren()) break;
        if (!child.dependsOn.every((d: string) => collab.children[d]?.state === 'SUCCEEDED')) continue;
        child.state = 'RUNNING'; child.startedAt = now(); launch.push(child.childId);
        this.store.event(t, 'collaboration.child.started', { collaborationId: collabId, childId: child.childId });
      }
      const states = Object.values(collab.children).map((c: any) => c.state);
      if (states.every(s => ['SUCCEEDED', 'FAILED', 'CANCELLED', 'BLOCKED'].includes(s))) {
        collab.state = 'COMPLETED'; collab.finishedAt = now();
        collab.result = { succeeded: states.filter(s => s === 'SUCCEEDED').length, failed: states.filter(s => s === 'FAILED').length, cancelled: states.filter(s => s === 'CANCELLED').length, blocked: states.filter(s => s === 'BLOCKED').length };
        this.store.event(t, 'collaboration.completed', { collaborationId: collabId, ...collab.result });
        completed = true;
      }
    });
    if (completed) {
      const collab = this.store.task(taskId).collaborations?.[collabId]; const op = collab?.requestKey ? this.store.operation(collab.requestKey) : null;
      if (op && op.state !== 'APPLIED') { op.state = 'APPLIED'; op.detail = 'Collaboration completed ' + JSON.stringify(collab.result); op.result = collab.result; this.store.saveOperation(op); }
    }
    for (const childId of launch) setImmediate(() => { void this.launchCollabChild(taskId, collabId, childId); });
  }
  private async launchCollabChild(taskId: string, collabId: string, childId: string) {
    const task = this.store.task(taskId); const collab = task.collaborations?.[collabId]; const child = collab?.children?.[childId];
    if (!collab || !child || collab.state !== 'RUNNING' || child.state !== 'RUNNING') return;
    try {
      // Baseline is sealed at spawn, not at collaboration creation: a child that
      // waited on dependencies snapshots the parent tree it actually starts from.
      const baseline = this.snapshotCollabCheckout(task.workspace, child.workspace);
      const streamId = collabId + ':' + childId;
      this.store.transaction(() => this.update(taskId, t => {
        const c = t.collaborations?.[collabId]?.children?.[childId]; if (!c) return;
        c.baseline = baseline;
        if (!c.messageStreamId) {
          c.messageStreamId = streamId;
          this.store.ensureChildStream(streamId, { taskId, collaborationId: collabId, childId });
          this.streamAppend(t, streamId, { collab: collabId, child: childId }, { kind: 'ASSIGNMENT', ...this.streamText(child.goal ?? '') });
          this.streamAppend(t, streamId, { collab: collabId, child: childId }, { kind: 'STATUS', text: 'RUNNING', status: 'RUNNING' });
        }
      }));
      mkdirSync(child.sessionDir, { recursive: true });
      const environment: NodeJS.ProcessEnv = {};
      for (const key of ['PATH', 'Path', 'SystemRoot', 'COMSPEC', 'PATHEXT', 'ProgramFiles', 'ProgramFiles(x86)', 'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'MODEL_API_KEY', ...task.modelBinding.headerEnvKeys]) if (process.env[key]) environment[key] = process.env[key];
      for (const key of task.environment?.envAllowlist ?? []) if (process.env[key]) environment[key] = process.env[key];
      environment.PI_CODING_AGENT_DIR = join(child.sessionDir, 'profile'); environment.PI_TELEMETRY = 'off';
      const module = this.config.childModule ?? fileURLToPath(new URL('../engine/bootstrap.js', import.meta.url));
      const profileOptions = isNoForkProfile(this.executionProfile) ? { execPath: SANDBOX_EXEC, execArgv: ['-p', NO_FORK_POLICY, process.execPath] } : {};
      const proc = fork(module, [], { ...profileOptions, cwd: child.workspace, env: environment, detached: process.platform !== 'win32', stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
      proc.on('error', () => {});
      const live: CollabLive = { child: proc, stopping: false, ready: false, pending: new Map(), pauseWaiters: new Map(), resumeWaiters: new Map(), taskId, collaborationId: collabId, childId, instruction: this.collabInstruction(task, collab, child) };
      this.collabLive.set(collabId + '/' + childId, live);
      if (process.platform === 'win32') {
        try { live.job = new WindowsJob(); live.job.assign(proc.pid!); }
        catch { live.stopping = true; proc.kill(); live.job?.close(); throw new Error('JOB_ASSIGN_FAILED'); }
      }
      if (process.platform === 'darwin') {
        try { live.mac = await MacOwnership.arm(proc.pid!); } catch (error) { proc.kill('SIGKILL'); throw error; }
        if (live.stopping || this.closed || this.store.task(taskId).collaborations?.[collabId]?.children?.[childId]?.state !== 'RUNNING') { live.stopping = true; await this.terminate(live); return; }
      }
      proc.on('message', msg => { this.collabEvent(taskId, collabId, childId, live, msg as Row); });
      proc.on('exit', () => {
        if (this.closed || this.collabLive.get(collabId + '/' + childId) !== live) return;
        const c = this.store.task(taskId).collaborations?.[collabId]?.children?.[childId];
        if (c && c.state === 'RUNNING' && !live.stopping) void this.finishCollabChild(taskId, collabId, childId, 'FAILED', 'COLLAB_PROCESS_EXITED_UNCONFIRMED', live);
        else this.collabLive.delete(collabId + '/' + childId);
      });
      proc.send({ type: 'init', executionProfile: this.executionProfile, workspace: child.workspace, sessionDir: child.sessionDir, model: task.modelBinding, probeEnv: task.environment?.envAllowlist ?? [], mcpServers: [], extensions: [] });
    } catch (error) { const reason = error instanceof Error && /^[A-Z][A-Z0-9_]+$/.test(error.message) ? error.message : 'LOCAL_LAUNCH_FAILED'; void this.finishCollabChild(taskId, collabId, childId, 'FAILED', reason, this.collabLive.get(collabId + '/' + childId)); }
  }
  private collabEvent(taskId: string, collabId: string, childId: string, live: CollabLive, event: Row) {
    if (this.closed || this.collabLive.get(collabId + '/' + childId) !== live) return;
    const task = this.store.task(taskId); const child = task.collaborations?.[collabId]?.children?.[childId]; if (!child) return;
    switch (event.type) {
      case 'ready':
        child.nativeSessionId = event.nativeSessionId; child.nativeSessionFile = event.sessionFile; this.store.save(task);
        live.child.send({ type: 'prompt', requestKey: 'collab-prompt-' + collabId + '-' + childId, content: live.instruction, collab: true }); return;
      case 'started': case 'user_consumed': case 'question_consumed': case 'answered': case 'queued': case 'gate_held': case 'paused': case 'pause_failed': case 'resumed': case 'extension_error': return;
      case 'tool_start': child.toolCalls++; this.store.transaction(() => { this.store.save(task); this.streamAppend(task, child.messageStreamId, { collab: collabId, child: childId }, { kind: 'TOOL_CALL', text: String(event.toolName ?? ''), ref: event.toolCallId }); }); return;
      case 'tool_end': this.store.transaction(() => { this.store.save(task); this.streamAppend(task, child.messageStreamId, { collab: collabId, child: childId }, { kind: 'TOOL_RESULT', text: String(event.toolName ?? ''), status: event.isError ? 'ERROR' : 'OK', ref: event.toolCallId }); }); return;
      case 'question':
        // Writable children are autonomous: an ask_user is declined, never routed
        // into the parent task's input requests.
        try { live.child.send({ type: 'answer', requestKey: 'collab-decline-' + event.inputRequestId, inputRequestId: event.inputRequestId, response: { cancelled: true } }); } catch {}
        this.store.transaction(() => { this.store.save(task); this.streamAppend(task, child.messageStreamId, { collab: collabId, child: childId }, { kind: 'STATUS', text: 'INPUT_REQUEST_DECLINED', status: 'INPUT_DECLINED' }); });
        return;
      // OUTPUT_DELTA capture is best-effort: a store hiccup must not break the
      // child message flow that still owns this session's lifecycle events.
      case 'assistant_delta': try { this.streamOutputDelta(task, child.messageStreamId, { collab: collabId, child: childId }, event); } catch {} return;
      case 'assistant': {
        const text = publicText(event.text ?? ''); if (text) child.output = (child.output + text + '\n').slice(-AUX_ANSWER_MAX);
        if (event.failed) child.nativeFailure = event.errorCode ?? 'MODEL_REQUEST_FAILED';
        if (event.usage) for (const key of ['input', 'output', 'cacheRead', 'cacheWrite']) child.usage[key] += Number(event.usage[key] ?? 0);
        this.store.transaction(() => { this.store.save(task); if (text) this.streamAppend(task, child.messageStreamId, { collab: collabId, child: childId }, { kind: 'ASSISTANT_MESSAGE', ...this.streamText(event.text ?? '') }); });
        const exceeded = this.collabBudgetExceeded(child);
        if (exceeded) { this.store.event(task, 'collaboration.child.budget_exceeded', { collaborationId: collabId, childId, ...exceeded, usage: { ...child.usage } }); void this.finishCollabChild(taskId, collabId, childId, 'FAILED', 'BUDGET_EXCEEDED', live); }
        return;
      }
      case 'gate_check': this.collabGateCheck(taskId, collabId, childId, live, event); return;
      case 'settled': void this.finishCollabChild(taskId, collabId, childId, child.nativeFailure ? 'FAILED' : 'SUCCEEDED', child.nativeFailure, live); return;
      case 'failure': void this.finishCollabChild(taskId, collabId, childId, 'FAILED', event.code ?? 'PI_EXECUTION_FAILED', live); return;
    }
  }
  private collabBudgetExceeded(child: Row): Row | null {
    const caps = child.budget?.caps; if (!caps) return null;
    const u = child.usage; const total = u.input + u.output + u.cacheRead + u.cacheWrite;
    if (caps.maxTotalTokens != null && total > caps.maxTotalTokens) return { field: 'maxTotalTokens', cap: caps.maxTotalTokens, observed: total };
    return null;
  }
  /** Child gate: its own SOFT budget verdict plus writeScope resolved against
   *  the isolated checkout — the scope root is the child copy, never the parent. */
  private collabGateCheck(taskId: string, collabId: string, childId: string, live: CollabLive, event: Row) {
    try {
      const child = this.store.task(taskId).collaborations?.[collabId]?.children?.[childId];
      const exceeded = child ? this.collabBudgetExceeded(child) : null;
      if (exceeded || !child || child.state !== 'RUNNING') {
        live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'terminate', reason: exceeded ? 'BUDGET_EXCEEDED' : 'COLLAB_CHILD_CLOSED' });
        if (child && child.state === 'RUNNING') void this.finishCollabChild(taskId, collabId, childId, 'FAILED', 'BUDGET_EXCEEDED', live);
        return;
      }
      if (event.kind === 'TOOL_ADMISSION' && !['read', 'bash', 'ask_user', 'ls', 'grep', 'glob'].includes(event.toolName)) {
        const denied = this.scopeDenial({ workspace: child.workspace, writeScope: child.writeScope }, event);
        if (denied) { this.store.event(this.store.task(taskId), 'collaboration.child.scope_violation', { collaborationId: collabId, childId, toolName: event.toolName, toolCallId: event.toolCallId, deniedPath: denied }); live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'block', reason: 'COLLAB_CHECKOUT_ONLY: ' + denied + ' is outside the isolated checkout' }); return; }
      }
      live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'allow' });
    } catch { try { live.child.send({ type: 'gate_verdict', checkId: event.checkId, action: 'block', reason: 'GATE_EVALUATION_FAILED' }); } catch {} }
  }
  private async finishCollabChild(taskId: string, collabId: string, childId: string, state: string, reasonCode: string | null | undefined, live?: CollabLive) {
    let stopped = false;
    if (live) { live.stopping = true; stopped = live.stopped ? true : await this.terminate(live); this.collabLive.delete(collabId + '/' + childId); }
    this.store.transaction(() => this.update(taskId, t => {
      const c = t.collaborations?.[collabId]?.children?.[childId];
      if (!c || ['SUCCEEDED', 'FAILED', 'CANCELLED', 'BLOCKED'].includes(c.state)) return;
      c.state = state; c.finishedAt = now(); if (reasonCode) c.reasonCode = reasonCode;
      if (stopped && live) c.stopProof = { ownerBootId: this.bootId, pid: live.child.pid, method: this.quiescenceMethod, observedAt: now() };
      // The candidate digest is sealed at completion so later adoption reads the
      // same file set the completion event advertised.
      if (state === 'SUCCEEDED') { try { c.candidate = this.collabCandidate(c); } catch { c.candidate = { error: 'CANDIDATE_DIFF_FAILED' }; } }
      const kind = state === 'FAILED' ? 'collaboration.child.failed' : state === 'BLOCKED' ? 'collaboration.child.blocked' : 'collaboration.child.completed';
      this.store.event(t, kind, { collaborationId: collabId, childId, state, reasonCode: reasonCode ?? null, usage: { ...c.usage }, outputBytes: Buffer.byteLength(c.output ?? ''), toolCalls: c.toolCalls ?? 0, candidateFiles: c.candidate?.fileCount ?? null, candidateDigest: c.candidate?.digest ?? null });
      if (c.messageStreamId) {
        this.streamAppend(t, c.messageStreamId, { collab: collabId, child: childId }, { kind: 'STATUS', text: state, status: state });
        this.streamAppend(t, c.messageStreamId, { collab: collabId, child: childId }, { kind: 'RETURN', status: state, ...this.streamText((c.output ?? '') || (reasonCode ?? state)) });
      }
    }));
    this.pumpCollab(taskId, collabId);
  }
  /** Stop a whole collaboration: running children are Job-drained with proof,
   *  pending children become BLOCKED and the record turns terminal — the pump
   *  early-returns once state leaves RUNNING so no new child launches. */
  private async stopCollaboration(taskId: string, collabId: string, reason: string) {
    this.update(taskId, t => {
      const c = t.collaborations?.[collabId]; if (!c || c.state !== 'RUNNING') return;
      for (const child of Object.values(c.children) as Row[]) if (child.state === 'PENDING') { child.state = 'BLOCKED'; child.reasonCode = reason; child.finishedAt = now(); this.store.event(t, 'collaboration.child.blocked', { collaborationId: collabId, childId: child.childId, reasonCode: reason }); }
      c.state = 'CANCELLED'; c.reasonCode = reason; c.finishedAt = now();
      this.store.event(t, 'collaboration.stopped', { collaborationId: collabId, reasonCode: reason });
    });
    const collab = this.store.task(taskId).collaborations?.[collabId];
    for (const child of Object.values(collab?.children ?? {}) as Row[]) if (child.state === 'RUNNING') await this.finishCollabChild(taskId, collabId, child.childId, 'CANCELLED', reason, this.collabLive.get(collabId + '/' + child.childId));
  }
  /** Parent STOP / host close propagation: every still-running collaboration on
   *  the task is drained the same way; completed child history is kept (N19). */
  private async stopTaskCollaborations(taskId: string, reason: string) {
    const collabs = Object.values(this.store.task(taskId).collaborations ?? {}).filter((c: any) => c.state === 'RUNNING').map((c: any) => c.collaborationId);
    for (const collabId of collabs) await this.stopCollaboration(taskId, collabId, reason);
  }
  async collaborationStop(taskId: string, collabId: string, body: Row) {
    exact(body, ['requestKey']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return prior; }
    const task = this.store.task(taskId); const collab = task.collaborations?.[id(collabId)];
    if (!collab) throw new Error('COLLABORATION_NOT_FOUND');
    const op = this.store.accept(body.requestKey, taskId, body, { kind: 'COLLABORATION_STOP', state: 'DISPATCHING', collaborationId: collabId });
    if (collab.state !== 'RUNNING') { op.state = 'APPLIED'; op.detail = 'Collaboration already ' + String(collab.state).toLowerCase(); this.store.saveOperation(op); return op; }
    await this.stopCollaboration(taskId, collabId, 'COLLABORATION_STOP');
    op.state = 'APPLIED'; op.detail = 'Collaboration stopped; running children drained; pending children blocked'; this.store.saveOperation(op);
    return op;
  }
  sweepCollabDeadlines() { for (const live of this.collabLive.values()) { const c = this.store.task(live.taskId).collaborations?.[live.collaborationId]?.children?.[live.childId]; if (c && c.state === 'RUNNING' && Date.now() > Number(c.deadlineEpochMillis ?? 0)) void this.finishCollabChild(live.taskId, live.collaborationId, live.childId, 'CANCELLED', 'COLLAB_DEADLINE', live); } }
  /** Candidate diff of a finished child's checkout against its spawn baseline:
   *  added/modified files map to sha256+size, removed baseline paths list as
   *  deletions. Regular files only — symlinks are never adopted. */
  private collabCandidate(child: Row) {
    const current: Row = {}; let fileCount = 0;
    const root = realpathSync(child.workspace);
    const walk = (dir: string, prefix: string) => {
      for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
        if (entry.name === '.git') continue;
        const p = join(dir, entry.name); const st = lstatSync(p);
        if (st.isSymbolicLink() || (!st.isDirectory() && !st.isFile())) continue;
        if (st.isDirectory()) walk(p, prefix + entry.name + '/');
        else { if (++fileCount > COLLAB_CANDIDATE_MAX_FILES) throw new Error('CANDIDATE_TOO_LARGE'); const content = readFileSync(p); current[prefix + entry.name] = { sha256: hash(content), size: content.length }; }
      }
    };
    walk(root, '');
    const baseline = child.baseline?.files ?? {};
    const files: Row = {}; const deletions: string[] = [];
    for (const [rel, meta] of Object.entries(current) as [string, Row][]) if (baseline[rel] !== meta.sha256) files[rel] = meta;
    for (const rel of Object.keys(baseline)) if (!(rel in current)) deletions.push(rel);
    return { files, deletions, digest: digest({ files: Object.fromEntries(Object.entries(files).map(([k, v]: any) => [k, v.sha256])), deletions }), fileCount: Object.keys(files).length, deletionCount: deletions.length, computedAt: now() };
  }
  /** Normalize a client-supplied adoption path to a workspace-relative posix key. */
  private adoptRelPath(raw: any): string {
    if (typeof raw !== 'string' || !raw.trim() || raw.length > 1024 || raw.includes('\x00')) throw new Error('INVALID_ADOPTION_PATHS');
    const norm = raw.replace(/\\/g, '/');
    if (norm.startsWith('/') || /^[A-Za-z]:\//.test(norm)) throw new Error('INVALID_ADOPTION_PATHS');
    const parts = norm.split('/').filter(p => p && p !== '.');
    if (!parts.length || parts.some(p => p === '..')) throw new Error('INVALID_ADOPTION_PATHS');
    return parts.join('/');
  }
  /** Pure candidate→plan→conflict classification shared by adoption previews
   *  and apply: per-path review entries carry action (write/delete/noop/
   *  conflict), the three digests (candidate/baseline/parent) and a code
   *  (OK / ALREADY_APPLIED / PARENT_DRIFT / CANDIDATE_ESCAPE / PATH_ESCAPE /
   *  SCOPE_DENIED). No filesystem writes happen here. */
  private adoptionReview(task: Row, child: Row, candidate: Row, adopt: string[], checkoutReal: string, workspaceReal: string): Row[] {
    const candidates = new Set([...Object.keys(candidate.files), ...candidate.deletions]);
    for (const rel of adopt) if (!candidates.has(rel)) throw new Error('ADOPTION_PATH_NOT_CANDIDATE');
    const within = (c: string, r: string) => c === r || c.startsWith(r + '/');
    const denyPaths = (task.writeScope?.denyPaths ?? []).map((p: string) => this.pathKey(p));
    const review: Row[] = [];
    for (const rel of adopt) {
      const meta = candidate.files[rel] as Row | undefined; const candSha = meta?.sha256 ?? null;
      const baseSha = child.baseline?.files?.[rel] ?? null;
      if (meta && !within(this.pathKey(this.realpathBestEffort(join(child.workspace, rel))), this.pathKey(checkoutReal))) { review.push({ path: rel, action: 'conflict', code: 'CANDIDATE_ESCAPE', candidateSha256: candSha, baselineSha256: baseSha, parentSha256: null }); continue; }
      const targetReal = this.realpathBestEffort(join(task.workspace, rel));
      if (!within(this.pathKey(targetReal), this.pathKey(workspaceReal))) { review.push({ path: rel, action: 'conflict', code: 'PATH_ESCAPE', candidateSha256: candSha, baselineSha256: baseSha, parentSha256: null }); continue; }
      if (denyPaths.some((d: string) => within(this.pathKey(targetReal), d))) { review.push({ path: rel, action: 'conflict', code: 'SCOPE_DENIED', candidateSha256: candSha, baselineSha256: baseSha, parentSha256: null }); continue; }
      let parentSha: string | null = null; let parentIsFile = false; let parentOther = false;
      try { const real = realpathSync(join(task.workspace, rel)); const st = statSync(real); if (st.isFile()) { parentSha = hash(readFileSync(real)); parentIsFile = true; } else parentOther = true; } catch {}
      // Same bytes already in place → idempotent no-op, never a drift conflict.
      if ((parentIsFile && parentSha === candSha) || (!parentIsFile && !parentOther && candSha === null)) { review.push({ path: rel, action: 'noop', code: 'ALREADY_APPLIED', candidateSha256: candSha, baselineSha256: baseSha, parentSha256: parentSha }); continue; }
      const clean = parentIsFile ? parentSha === baseSha : (!parentOther && baseSha === null);
      if (clean) review.push({ path: rel, action: candSha ? 'write' : 'delete', code: 'OK', candidateSha256: candSha, baselineSha256: baseSha, parentSha256: parentSha });
      else review.push({ path: rel, action: 'conflict', code: 'PARENT_DRIFT', candidateSha256: candSha, baselineSha256: baseSha, parentSha256: parentSha });
    }
    return review;
  }
  private adoptionTruth = { verification: 'NOT_SUPPORTED' as const, verificationNote: 'Adoption performs no test execution', contextContinuation: 'NOT_SUPPORTED' as const, contextContinuationNote: 'Adopted content is not auto-injected into the parent session' };
  /** Read-only adoption preview (C4): same candidate/plan/conflict
   *  classification as apply, frozen under previewId + planDigest with a TTL.
   *  Nothing is written to the parent workspace. */
  collaborationPreview(taskId: string, collabId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'childId', 'paths']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return { ...prior, replayed: true }; }
    const task = this.store.task(taskId); this.assertUnsealed(task); const collab = task.collaborations?.[id(collabId)];
    if (!collab) throw new Error('COLLABORATION_NOT_FOUND');
    if (typeof body.childId !== 'string' || !COLLAB_CHILD_ID.test(body.childId)) throw new Error('INVALID_COLLABORATION_CHILD_ID');
    const child = collab.children?.[body.childId]; if (!child) throw new Error('COLLABORATION_CHILD_NOT_FOUND');
    if (child.state !== 'SUCCEEDED') throw new Error('COLLABORATION_CHILD_NOT_FINISHED');
    let selected: string[] | null = null;
    if (body.paths !== undefined) {
      if (!Array.isArray(body.paths) || !body.paths.length || body.paths.length > COLLAB_MAX_PATHS) throw new Error('INVALID_ADOPTION_PATHS');
      selected = body.paths.map((p: any) => this.adoptRelPath(p));
      if (new Set(selected).size !== selected.length) throw new Error('INVALID_ADOPTION_PATHS');
    }
    let candidate: Row; let checkoutReal: string; let workspaceReal: string;
    try { checkoutReal = realpathSync(child.workspace); workspaceReal = realpathSync(task.workspace); candidate = this.collabCandidate(child); }
    catch (error) { throw new Error(error instanceof Error && /^[A-Z][A-Z0-9_]+$/.test(error.message) ? error.message : 'ADOPTION_CHECKOUT_UNAVAILABLE'); }
    const adopt = selected ?? [...new Set([...Object.keys(candidate.files), ...candidate.deletions])];
    const review = this.adoptionReview(task, child, candidate, adopt, checkoutReal, workspaceReal);
    const previewId = 'pv-' + randomUUID();
    const expiresAt = new Date(Date.now() + COLLAB_PREVIEW_TTL_MS).toISOString();
    const files = review.map(e => ({ path: e.path, action: e.action, code: e.code, candidateSha256: e.candidateSha256, baselineSha256: e.baselineSha256, parentSha256: e.parentSha256 }));
    const planDigest = digest({ collaborationId: collabId, childId: body.childId, candidateDigest: candidate.digest, files });
    const record: Row = { schemaVersion: 'collaboration-preview/1', previewId, taskId, collaborationId: collabId, childId: body.childId, planDigest, candidateDigest: candidate.digest, files, writable: !review.some(e => e.action === 'conflict'), createdAt: now(), expiresAt };
    this.store.transaction(() => {
      const t = this.store.task(taskId); this.assertUnsealed(t);
      const map = t.adoptionPreviews ??= {};
      for (const [k, v] of Object.entries(map) as [string, Row][]) if (Date.parse(v.expiresAt) <= Date.now()) delete map[k];
      const keys = Object.keys(map).sort((a, b) => String(map[a].createdAt).localeCompare(String(map[b].createdAt)));
      while (keys.length && Object.keys(map).length >= COLLAB_PREVIEW_MAX) delete map[keys.shift()!];
      map[previewId] = record; this.store.save(t);
      this.store.event(t, 'collaboration.adoption.previewed', { collaborationId: collabId, childId: body.childId, previewId, planDigest, candidateDigest: candidate.digest, fileCount: files.length, conflicts: review.filter(e => e.action === 'conflict').length, expiresAt });
      this.store.accept(body.requestKey, taskId, body, { kind: 'COLLABORATION_PREVIEW', state: 'APPLIED', collaborationId: collabId, childId: body.childId, previewId, planDigest });
    });
    return { previewId, planDigest, candidateDigest: candidate.digest, childId: body.childId, files, writable: record.writable, expiresAt };
  }
  collaborationPreviewGet(task: Row, collabId: string, previewId: string) {
    const collab = task.collaborations?.[id(collabId)]; if (!collab) throw new Error('COLLABORATION_NOT_FOUND');
    const record = task.adoptionPreviews?.[id(previewId)];
    if (!record || record.collaborationId !== collabId) throw new Error('PREVIEW_NOT_FOUND');
    return { ...record, expired: Date.parse(record.expiresAt) <= Date.now() };
  }
  /** Explicit adoption (P15/N20): write a finished child's candidate paths into
   *  the parent workspace. A path adopts cleanly only when the parent still
   *  equals the baseline there; a parent that already equals the candidate is an
   *  idempotent no-op; any other drift — plus checkout escapes and denyPaths —
   *  produces a durable CONFLICT material and writes nothing. */
  collaborationAdopt(taskId: string, collabId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'childId', 'mode', 'paths', 'expectedPreviewId', 'expectedPreviewDigest']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return { outcome: prior.outcome, files: prior.files, conflicts: prior.conflicts, fileEffect: prior.fileEffect, verification: prior.verification, contextContinuation: prior.contextContinuation, replayed: true }; }
    const task = this.store.task(taskId); this.assertUnsealed(task); const collab = task.collaborations?.[id(collabId)];
    if (!collab) throw new Error('COLLABORATION_NOT_FOUND');
    if (typeof body.childId !== 'string' || !COLLAB_CHILD_ID.test(body.childId)) throw new Error('INVALID_COLLABORATION_CHILD_ID');
    const child = collab.children?.[body.childId]; if (!child) throw new Error('COLLABORATION_CHILD_NOT_FOUND');
    if (body.mode !== 'ALL' && body.mode !== 'FILES') throw new Error('INVALID_ADOPTION_MODE');
    if (child.state !== 'SUCCEEDED') throw new Error('COLLABORATION_CHILD_NOT_FINISHED');
    // Optional preview guard: the reviewed frozen plan must still be live —
    // unknown, expired, wrong-child or divergent previews refuse PREVIEW_STALE.
    if (body.expectedPreviewId !== undefined || body.expectedPreviewDigest !== undefined) {
      const pv = task.adoptionPreviews?.[String(body.expectedPreviewId ?? '')];
      if (!pv || pv.collaborationId !== collabId || pv.childId !== body.childId || Date.parse(pv.expiresAt) <= Date.now() || (body.expectedPreviewDigest !== undefined && body.expectedPreviewDigest !== pv.planDigest)) throw new Error('PREVIEW_STALE');
    }
    let selected: string[] | null = null;
    if (body.mode === 'FILES') {
      if (!Array.isArray(body.paths) || !body.paths.length || body.paths.length > COLLAB_MAX_PATHS) throw new Error('INVALID_ADOPTION_PATHS');
      selected = body.paths.map((p: any) => this.adoptRelPath(p));
      if (new Set(selected).size !== selected.length) throw new Error('INVALID_ADOPTION_PATHS');
    } else if (body.paths !== undefined) throw new Error('INVALID_ADOPTION_PATHS');
    let candidate: Row; let checkoutReal: string; let workspaceReal: string;
    try { checkoutReal = realpathSync(child.workspace); workspaceReal = realpathSync(task.workspace); candidate = this.collabCandidate(child); }
    catch (error) { throw new Error(error instanceof Error && /^[A-Z][A-Z0-9_]+$/.test(error.message) ? error.message : 'ADOPTION_CHECKOUT_UNAVAILABLE'); }
    const adopt = selected ?? [...new Set([...Object.keys(candidate.files), ...candidate.deletions])];
    const review = this.adoptionReview(task, child, candidate, adopt, checkoutReal, workspaceReal);
    const conflicts: Row[] = []; const plan: Row[] = []; let alreadyApplied = 0;
    for (const e of review) {
      if (e.action === 'conflict') { const c: Row = { path: e.path, kind: e.code, candidateSha256: e.candidateSha256 }; if (e.code === 'PARENT_DRIFT') { c.baselineSha256 = e.baselineSha256; c.parentSha256 = e.parentSha256; } conflicts.push(c); }
      else if (e.action === 'noop') alreadyApplied++;
      else plan.push({ path: e.path, action: e.action, sha256: e.candidateSha256 });
    }
    const record = (outcome: string, extra: Row = {}) => this.store.transaction(() => {
      const t = this.store.task(taskId); const c = t.collaborations?.[collabId]?.children?.[body.childId];
      if (c) c.adoptions = [...(c.adoptions ?? []).slice(-(COLLAB_MAX_ADOPTIONS - 1)), { requestKey: body.requestKey, mode: body.mode, outcome, files: (extra.files ?? []).map((f: Row) => ({ path: f.path, sha256: f.sha256, action: f.action })), conflicts: extra.conflicts, at: now() }];
      this.store.save(t);
      this.store.event(t, outcome === 'CONFLICT' ? 'collaboration.adoption.conflict' : 'collaboration.adopted', { collaborationId: collabId, childId: body.childId, requestKey: body.requestKey, mode: body.mode, outcome, fileEffect: extra.fileEffect, ...extra });
      return this.store.accept(body.requestKey, taskId, body, { kind: 'COLLABORATION_ADOPT', state: 'APPLIED', collaborationId: collabId, childId: body.childId, outcome, ...extra });
    });
    if (conflicts.length) { record('CONFLICT', { conflicts, fileEffect: 'NOT_STARTED' }); return { outcome: 'CONFLICT', conflicts, fileEffect: 'NOT_STARTED', ...this.adoptionTruth }; }
    if (!plan.length) { record('NO_CHANGES', { files: [], fileEffect: 'SUCCEEDED' }); return { outcome: 'NO_CHANGES', alreadyApplied, fileEffect: 'SUCCEEDED', ...this.adoptionTruth }; }
    // Preload source bytes and re-verify each sha before touching the parent;
    // candidate paths are realpath-pinned inside the child checkout above.
    const writes: Row[] = plan.filter(p => p.action === 'write').map((p: Row) => { const bytes = readFileSync(realpathSync(join(child.workspace, p.path))); if (hash(bytes) !== p.sha256) throw new Error('ADOPTION_CANDIDATE_CHANGED'); return { ...p, bytes }; });
    for (const w of writes) { const target = join(task.workspace, w.path); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, w.bytes); }
    for (const d of plan.filter(p => p.action === 'delete')) { try { rmSync(join(task.workspace, d.path)); } catch {} }
    const files = plan.map(p => ({ path: p.path, sha256: p.sha256, action: p.action }));
    record('ADOPTED', { files, fileEffect: 'SUCCEEDED' });
    return { outcome: 'ADOPTED', files, fileEffect: 'SUCCEEDED', ...this.adoptionTruth };
  }
  // ---- child-messages/1 (C2): bounded paginated reads over durable per-stream
  //      capture. before and cursor are mutually exclusive; with neither the
  //      latest page is returned. Coverage is honest: FULL_SINCE_CAPTURE only
  //      when a durable stream exists, SUMMARY_ONLY when only the legacy
  //      aggregate output survives (never split into fake turns), UNAVAILABLE
  //      when nothing was ever recorded.
  private childMessagesPage(streamId: string | undefined | null, params: URLSearchParams, legacyText: string) {
    if ([...params.keys()].some(k => !['before', 'cursor', 'limit'].includes(k)) || [...new Set(params.keys())].length !== [...params.keys()].length) throw new Error('INVALID_QUERY');
    const before = params.get('before'); const cursor = params.get('cursor');
    if (!streamId || !this.store.childStream(streamId)) {
      if (legacyText) return { schemaVersion: 'child-messages/1', streamId: streamId ?? null, coverage: 'SUMMARY_ONLY', summaryText: publicText(legacyText).slice(0, CHILD_MSG_TEXT_MAX), items: [], hasMoreBefore: false, hasMoreAfter: false, nextCursor: null, beforeCursor: null };
      return { schemaVersion: 'child-messages/1', streamId: streamId ?? null, coverage: 'UNAVAILABLE', items: [], hasMoreBefore: false, hasMoreAfter: false, nextCursor: null, beforeCursor: null };
    }
    const page = this.store.childMessages(streamId, before, cursor, Number(params.get('limit') ?? '50'));
    // Canonical child-messages/1 item vocabulary (design §7.2): `seq` decimal
    // string, `eventRef`, item-level `textTruncated` — the page-level
    // `truncated` stays the byte-cut flag, never an item property.
    const items = page.items.map(i => ({ id: i.id, seq: i.seq, kind: i.kind, createdAt: i.createdAt, status: i.status ?? null, eventRef: i.ref ?? null, text: i.text ?? '', textTruncated: !!i.truncated, ...(i.delta !== undefined ? { delta: i.delta } : {}), ...(i.contentIndex !== undefined ? { contentIndex: i.contentIndex } : {}), ...(i.nativeTurnId !== undefined ? { nativeTurnId: i.nativeTurnId } : {}), ...(i.nativeItemId !== undefined ? { nativeItemId: i.nativeItemId } : {}) }));
    const first = items[0]; const last = items.at(-1); const stream = this.store.childStream(streamId)!;
    // GAP-07: a page whose items carry truncated bodies degrades honestly to
    // PARTIAL — the flag was always on the row, it just never reached coverage.
    const degraded = items.some(i => i.textTruncated);
    return { schemaVersion: 'child-messages/1', streamId, coverage: degraded ? 'PARTIAL' : 'FULL_SINCE_CAPTURE', captureStartedAt: stream.captureStartedAt, items,
      hasMoreBefore: page.hasMoreBefore, hasMoreAfter: page.hasMoreAfter,
      nextCursor: page.hasMoreAfter && last ? String(last.seq) : null,
      beforeCursor: page.hasMoreBefore && first ? String(first.seq) : null };
  }
  collabChildMessages(task: Row, collabId: string, childId: string, params: URLSearchParams) {
    const collab = task.collaborations?.[id(collabId)]; if (!collab) throw new Error('COLLABORATION_NOT_FOUND');
    if (typeof childId !== 'string' || !COLLAB_CHILD_ID.test(childId)) throw new Error('COLLABORATION_CHILD_NOT_FOUND');
    const child = collab.children?.[childId]; if (!child) throw new Error('COLLABORATION_CHILD_NOT_FOUND');
    return { collaborationId: collabId, childId, childState: child.state, ...this.childMessagesPage(child.messageStreamId, params, child.output ?? '') };
  }
  auxMessages(task: Row, auxSessionId: string, params: URLSearchParams) {
    const aux = task.auxSessions?.[id(auxSessionId)]; if (!aux) throw new Error('AUX_SESSION_NOT_FOUND');
    return { auxSessionId, auxState: aux.state, ...this.childMessagesPage(aux.messageStreamId, params, aux.answer ?? '') };
  }
  // ---- C5 source seals: POST first proves the MAIN task quiescent — a
  //      durable terminal verdict or recorded quiescence with no dispatch
  //      left (F33: a live owner would keep writing under the fence) — then
  //      drains collaboration children and auxiliary sessions, then raises
  //      a durable write fence on the task row. Reads stay open; every
  //      mutation path rejects SOURCE_SEALED from the moment sealRequested
  //      lands. The seal record binds taskId, generation, segmentId, owner
  //      epoch, target and material digests.
  async sourceSeal(taskId: string, body: Row) {
    exact(body, ['requestKey', 'target', 'materials']);
    const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return { ...prior.seal, replayed: true, drained: prior.drained ?? null }; }
    const task = this.store.task(taskId);
    const target = body.target === undefined ? 'HANDOFF' : String(body.target);
    if (typeof target !== 'string' || !target.trim() || target.length > 128) throw new Error('INVALID_SEAL_TARGET');
    const materialRefs: Row[] = [];
    if (body.materials !== undefined) {
      if (!Array.isArray(body.materials) || !body.materials.length || body.materials.length > this.materials.maxPerMessage() * 4) throw new Error('MATERIALS_INVALID');
      for (const m of body.materials) {
        if (!m || typeof m !== 'object' || Array.isArray(m)) throw new Error('MATERIALS_INVALID');
        exact(m, ['materialRef', 'sha256']);
        const row = this.materials.resolve(m.materialRef);
        if (row.state !== 'SEALED' && row.state !== 'BOUND') throw new Error('MATERIAL_NOT_SEALED');
        if (typeof m.sha256 !== 'string' || m.sha256 !== row.sha256) throw new Error('MATERIAL_SHA256_MISMATCH');
        materialRefs.push({ materialRef: row.materialRef ?? row.uploadKey, uploadKey: row.uploadKey, sha256: row.sha256, mediaType: row.mediaType, sizeBytes: row.sizeBytes, displayName: row.displayName });
      }
    }
    if (task.sealedFor) {
      // A second seal against an already-fenced source is only meaningful if it
      // repeats the existing record; a different target/material set conflicts.
      const existing = Object.values(task.sourceSeals ?? {})[0] as Row | undefined;
      const same = existing && existing.target === target && digest(existing.materials) === digest(materialRefs);
      if (!same) throw new Error('SOURCE_ALREADY_SEALED');
      return { ...existing, replayed: true };
    }
    const sealId = 'seal-' + randomUUID();
    this.store.transaction(() => {
      const t = this.store.task(taskId); this.assertUnsealed(t);
      // Quiescence gate is atomic with the fence landing: a non-quiescent main
      // task refuses before sealRequested exists, so no drain/fence state can
      // half-apply over in-flight turn or tool writes.
      if (!this.sealQuiescent(t)) throw new Error('TASK_NOT_QUIESCENT');
      t.sealRequested = { sealId, requestedAt: now(), requestKey: body.requestKey };
      this.store.save(t); this.store.event(t, 'task.source.seal.requested', { sealId, requestKey: body.requestKey, target, materials: materialRefs.length });
    });
    // Drain owned child work so the sealed projection is final: collaboration
    // children are stopped like a parent STOP, aux sessions finish CANCELLED.
    // If the drain throws, roll back the fence — a half-sealed source with no
    // seal record is worse than an honest rejection.
    try {
      for (const c of Object.values(this.store.task(taskId).collaborations ?? {}) as Row[]) if (c.state === 'RUNNING') await this.stopCollaboration(taskId, c.collaborationId, 'SOURCE_SEALED');
      for (const [auxId, live] of [...this.auxLive]) if (live.taskId === taskId) await this.finishAux(taskId, auxId, 'CANCELLED', 'SOURCE_SEALED', live);
      // A live terminal keeps writing into the sealed workspace — drain it
      // like a STOP so the seal record covers every writer.
      await this.killTaskTerminals(taskId, 'SOURCE_SEALED');
    } catch (error) { this.store.transaction(() => { const t = this.store.task(taskId); if (t.sealRequested?.sealId === sealId) { delete t.sealRequested; this.store.save(t); } }); throw error; }
    // Recorded AFTER the drain: affectedChildren must describe the post-drain
    // durable states, not the pre-drain snapshot.
    const drained = this.affectedChildren(taskId);
    let seal: Row;
    this.store.transaction(() => {
      const t = this.store.task(taskId);
      const sealedAt = now();
      seal = { schemaVersion: 'source-seal/1', sealId, taskId, target, state: 'SEALED', materials: materialRefs, affectedChildren: drained, sealedAt,
        revision: { generation: t.generation, segmentId: t.segmentId, ownerEpoch: String(t.ownerEpoch ?? '1'), state: t.state, lastEventSequence: String(this.store.lastEventSeq(taskId)) },
        digest: digest({ taskId, generation: t.generation, segmentId: t.segmentId, ownerEpoch: String(t.ownerEpoch ?? '1'), target, materials: materialRefs.map(m => ({ materialRef: m.materialRef, sha256: m.sha256 })) }) };
      t.sealedFor = { sealId, target, sealedAt, digest: seal.digest }; delete t.sealRequested;
      (t.sourceSeals ??= {})[sealId] = seal; this.store.save(t);
      this.store.event(t, 'task.source.sealed', { sealId, target, digest: seal.digest, materials: materialRefs.length });
      this.store.accept(body.requestKey, taskId, body, { kind: 'SOURCE_SEAL', state: 'APPLIED', seal, drained });
    });
    return seal!;
  }
  sourceSealList(task: Row) { return { items: Object.values(task.sourceSeals ?? {}), sealedFor: task.sealedFor ?? null, sealRequested: task.sealRequested ?? null }; }
  sourceSealGet(task: Row, sealId: string) { const s = task.sourceSeals?.[id(sealId)]; if (!s) throw new Error('SEAL_NOT_FOUND'); return s; }
  /** Read-only bounded handoff export (C5): a deterministic public projection —
   *  task messages, aux answers, collaboration report refs, factual events and
   *  the artifact manifest. Items get a stable position; cursor pages forward.
   *  Text bodies inline up to HANDOFF_EXPORT_INLINE_TEXT, larger bodies are
   *  digest refs readable through the chunks route. Acceptance ≠ target start:
   *  targetStart stays NOT_STARTED unless a future integration reports more. */
  private handoffItems(task: Row, extra: Row[] = []): Row[] {
    const items: Row[] = []; const push = (kind: string, occurredAt: string, entry: Row) => items.push({ kind, occurredAt, ...entry });
    (task.publicMessages ?? []).forEach((m: any, i: number) => push('MESSAGE', m.occurredAt ?? task.createdAt, { id: 'message-' + i, role: m.role ?? 'assistant', commandId: m.commandId ?? null, nativeTurnId: m.nativeTurnId ?? null, body: this.handoffBody('message-' + i, m.text ?? '') }));
    Object.values(task.auxSessions ?? {}).forEach((a: any) => push('AUX_ANSWER', a.finishedAt ?? a.createdAt ?? task.createdAt, { id: 'aux-' + a.auxSessionId, auxSessionId: a.auxSessionId, state: a.state, reasonCode: a.reasonCode ?? null, body: this.handoffBody('aux-' + a.auxSessionId, a.answer ?? '') }));
    Object.values(task.collaborations ?? {}).forEach((c: any) => push('COLLABORATION_REPORT', c.createdAt ?? task.createdAt, { id: 'collab-' + c.collaborationId, collaborationId: c.collaborationId, name: c.name ?? null, state: c.state, children: Object.values(c.children ?? {}).map((ch: any) => ({ childId: ch.childId, state: ch.state, reasonCode: ch.reasonCode ?? null, candidateDigest: ch.candidate?.digest ?? null, outputDigest: ch.output ? hash(publicText(ch.output)) : null, messageStreamId: ch.messageStreamId ?? null })) }));
    const manifests = [...(task.artifactManifest ? [task.artifactManifest] : []), ...(task.artifactHistory ?? [])];
    manifests.forEach((m: any) => (m.items ?? []).forEach((i: any) => push('ARTIFACT', m.sealedAt ?? task.createdAt, { id: 'artifact-' + i.artifactId, artifactId: i.artifactId, path: i.path, sha256: i.sha256, size: i.size, status: i.status, segmentId: m.segmentId })));
    for (const e of this.taskEvents(task.taskId)) push('EVENT', e.occurredAt ?? task.createdAt, { id: 'event-' + e.seq, seq: e.seq, eventKind: e.kind, segmentId: e.segmentId ?? null });
    for (const it of extra) items.push(it);
    const order: Record<string, number> = { MESSAGE: 0, AUX_ANSWER: 1, COLLABORATION_REPORT: 2, ARTIFACT: 3, EVENT: 4, GIT_PART: 5 };
    items.sort((x, y) => String(x.occurredAt).localeCompare(String(y.occurredAt)) || (order[x.kind] - order[y.kind]) || String(x.id).localeCompare(String(y.id)));
    items.forEach((it, i) => { it.position = String(i + 1); });
    return items;
  }
  private handoffBody(part: string, text: string) {
    const clean = publicText(text); const size = Buffer.byteLength(clean);
    if (size <= HANDOFF_EXPORT_INLINE_TEXT) return { text: clean, sha256: hash(clean), size, truncated: false };
    return { textRef: { part, sha256: hash(clean), size, chunkBytes: HANDOFF_EXPORT_CHUNK_MAX }, truncated: false };
  }
  /** Materialized git payloads live under state/handoff-git/<taskId>/ so the
   *  chunk route re-reads the same bytes the manifest digest was computed
   *  over. Never a workspace path — the workspace stays agent-owned. */
  private gitBaselineDir(task: Row) { return join(this.store.directory, 'handoff-git', task.taskId); }
  /** GAP-04 gitBaseline producer (design §9.3): fact entries plus materialized
   *  binary payloads (layered patches, bundle) exported as GIT_PART items.
   *  Anything we cannot prove degrades to an honest state row instead of
   *  silently claiming coverage. Workspace-root containment matches the
   *  artifact sealer: only the owned task workspace is ever inspected. */
  private collectGitBaseline(task: Row): { entries: Row[]; payloadItems: Row[] } {
    const entries: Row[] = []; const payloadItems: Row[] = [];
    const at = task.sealedFor?.sealedAt ?? task.createdAt;
    const payload = (kind: string, part: string, sha256: string, size: number) => payloadItems.push({
      kind: 'GIT_PART', occurredAt: at, id: part, gitKind: kind, binary: true, sha256, sizeBytes: size,
      role: kind === 'BUNDLE' ? 'BUNDLE' : 'PATCH',
      body: { textRef: { part, sha256, size, chunkBytes: HANDOFF_EXPORT_CHUNK_MAX } },
    });
    let root: string;
    try { root = realpathSync(task.workspace); if (root !== realpathSync(join(this.store.directory, 'workspaces', task.taskId))) throw new Error('x'); }
    catch { return { entries: [{ kind: 'NON_GIT', reason: 'OWNED_WORKSPACE_REQUIRED' }], payloadItems }; }
    try { git(root, ['rev-parse', '--git-dir']); }
    catch { return { entries: [{ kind: 'NON_GIT', reason: 'NOT_A_REPOSITORY' }], payloadItems }; }
    try {
      const gitDir = git(root, ['rev-parse', '--absolute-git-dir']);
      const head = (() => { try { return git(root, ['rev-parse', '--verify', 'HEAD']); } catch { return null; } })();
      const branch = (() => { try { return git(root, ['symbolic-ref', '-q', '--short', 'HEAD']) || null; } catch { return null; } })();
      // In-flight markers: rebase records rebase-merge/ or rebase-apply/
      // DIRECTORIES (REBASE_HEAD only exists mid-pick); AUTO_MERGE is the ort
      // strategy's in-progress tree. FETCH_HEAD is not an operation.
      const inFlight = [['MERGE_HEAD', 'MERGE'], ['CHERRY_PICK_HEAD', 'CHERRY_PICK'], ['REVERT_HEAD', 'REVERT'], ['BISECT_HEAD', 'BISECT'], ['AUTO_MERGE', 'AUTO_MERGE']]
        .filter(([f]) => { try { return existsSync(join(gitDir, f)); } catch { return false; } }).map(([, k]) => k);
      try { if (existsSync(join(gitDir, 'rebase-merge')) || existsSync(join(gitDir, 'rebase-apply'))) inFlight.push('REBASE'); } catch { }
      const worktrees = (() => { try { return git(root, ['worktree', 'list', '--porcelain']).split('\n').filter(l => l.startsWith('worktree ')).length; } catch { return null; } })();
      entries.push({ kind: 'REPOSITORY', present: true, head, branch, detached: !branch, baseCommit: task.baseCommit ?? null, inFlight, linkedWorktrees: worktrees === null ? 'UNAVAILABLE' : Math.max(0, worktrees - 1) });
      // name-status -z pairs give path-level summaries so a target does not
      // have to parse a binary patch to learn which files the layer touches.
      const names = (args: string[]): { files: Row[]; truncated: boolean } | null => {
        try {
          const raw = git(root, args).split('\0'); const files: Row[] = [];
          for (let i = 0; i + 1 < raw.length && files.length < 512; i += 2) if (raw[i + 1]) files.push({ status: raw[i], path: raw[i + 1] });
          return { files, truncated: raw.length > 1025 };
        } catch { return null; }
      };
      for (const [kind, part, args, listArgs] of [
        ['PATCH_STAGED', 'git-staged', ['diff', '--cached', '--binary', '--full-index', '--no-color', '--no-ext-diff', '--no-textconv'], ['diff', '--cached', '--name-status', '-z', '--no-renames']],
        ['PATCH_UNSTAGED', 'git-unstaged', ['diff', '--binary', '--full-index', '--no-color', '--no-ext-diff', '--no-textconv'], ['diff', '--name-status', '-z', '--no-renames']],
      ] as const) {
        const listed = names(listArgs as unknown as string[]);
        try {
          const out = gitBytes(root, args as unknown as string[], GIT_PART_MAX);
          if (out.length === 0) { entries.push({ kind, state: 'EMPTY', files: listed?.files ?? [] }); continue; }
          // maxBuffer is GIT_PART_MAX+1, so this only triggers at the exact
          // boundary where the stream landed before the kill.
          if (out.length > GIT_PART_MAX) { entries.push({ kind, state: 'TOO_LARGE', sizeBytes: out.length, files: listed?.files ?? [] }); continue; }
          mkdirSync(this.gitBaselineDir(task), { recursive: true });
          writeFileSync(join(this.gitBaselineDir(task), part), out);
          const sha256 = hash(out);
          entries.push({ kind, state: 'AVAILABLE', part, sha256, sizeBytes: out.length, files: listed?.files ?? [], filesTruncated: listed?.truncated ?? false });
          payload(kind, part, sha256, out.length);
          // execFileSync ENOBUFS is the only TOO_LARGE proof we can get — the
          // stream never landed, so an exact byte size is unknowable here.
        } catch (error) { entries.push((error as NodeJS.ErrnoException)?.code === 'ENOBUFS' ? { kind, state: 'TOO_LARGE', files: listed?.files ?? [] } : { kind, state: 'UNAVAILABLE', reason: 'GIT_COMMAND_FAILED', files: listed?.files ?? [] }); }
      }
      let local = 0; try { local = Number(git(root, ['rev-list', '--count', 'HEAD', '--not', '--remotes'])) || 0; } catch { }
      if (head === null) entries.push({ kind: 'BUNDLE', state: 'EMPTY', reason: 'UNBORN_HEAD' });
      else if (local === 0) entries.push({ kind: 'BUNDLE', state: 'EMPTY', reason: 'NO_LOCAL_COMMITS', localCommits: 0 });
      else {
        try {
          mkdirSync(this.gitBaselineDir(task), { recursive: true });
          const file = join(this.gitBaselineDir(task), 'git-bundle');
          git(root, ['bundle', 'create', file, 'HEAD', '--not', '--remotes']);
          // stat before reading: the length check is only meaningful if it
          // happens before the whole bundle lands in memory.
          const size = statSync(file).size;
          if (size > GIT_PART_MAX) { rmSync(file, { force: true }); entries.push({ kind: 'BUNDLE', state: 'TOO_LARGE', sizeBytes: size, localCommits: local }); }
          else { const bytes = readFileSync(file); const sha256 = hash(bytes); entries.push({ kind: 'BUNDLE', state: 'AVAILABLE', part: 'git-bundle', sha256, sizeBytes: bytes.length, localCommits: local }); payload('BUNDLE', 'git-bundle', sha256, bytes.length); }
        } catch { entries.push({ kind: 'BUNDLE', state: 'UNAVAILABLE', reason: 'BUNDLE_CREATE_FAILED', localCommits: local }); }
      }
      // Stash objects are unreachable from HEAD, so the BUNDLE cannot carry
      // them — a non-empty stash list is a real dirty-workspace fact, not
      // something a later `rev-list` sweep would ever discover on its own.
      const stashes = (() => { try { return git(root, ['stash', 'list']).split('\n').filter(Boolean).slice(0, 32); } catch { return []; } })();
      if (stashes.length) entries.push({ kind: 'STASHES', items: stashes.map((line) => { const ref = line.split(':')[0]; let commit = null; try { commit = git(root, ['rev-parse', ref]); } catch { } return { ref, commit, summary: line.slice(line.indexOf(':') + 1).trim() }; }), note: 'Stash objects are not reachable from HEAD — not included in BUNDLE' });
      try { if (readFileSync(join(root, '.gitattributes')).subarray(0, 65536).toString('utf8').includes('filter=lfs')) entries.push({ kind: 'LFS', attributes: true, note: 'LFS objects live in the filter store — verify separately per §9.3' }); } catch { }
      let others: string[] | null = null;
      try { others = git(root, ['ls-files', '--others', '--exclude-standard', '-z']).split('\0').filter(Boolean).sort(); }
      catch { entries.push({ kind: 'UNTRACKED_MANIFEST', state: 'OVERSIZE', note: 'ls-files output exceeded the bounded read' }); }
      if (others !== null) {
        const manifests = [...(task.artifactManifest ? [task.artifactManifest] : []), ...(task.artifactHistory ?? [])];
        const byPath = new Map<string, Row>(manifests.flatMap((m: Row) => (m.items ?? []) as Row[]).map((i: Row) => [i.path, i] as [string, Row]));
        const files = others.slice(0, GIT_UNTRACKED_MAX).map((p) => { const a = byPath.get(p); return a ? { path: p, sizeBytes: a.size ?? null, sha256: a.sha256 ?? null, artifactId: a.artifactId ?? null, status: a.status ?? 'UNSEALED' } : { path: p, status: 'UNSEALED' }; });
        entries.push({ kind: 'UNTRACKED_MANIFEST', files, total: others.length, truncated: others.length > GIT_UNTRACKED_MAX });
      }
      const subs = (() => { try { return git(root, ['submodule', 'status', '--recursive']).split('\n').filter(Boolean).slice(0, GIT_SUBMODULE_MAX); } catch { return []; } })();
      if (subs.length) entries.push({ kind: 'SUBMODULES', items: subs.map((l) => l.trim()), note: 'Pointer facts only — nested objects are not bundled' });
    } catch { entries.push({ kind: 'UNAVAILABLE', reason: 'GIT_COMMAND_FAILED' }); }
    return { entries, payloadItems };
  }
  handoffExport(task: Row, params: URLSearchParams) {
    if ([...params.keys()].some(k => !['cursor', 'limit'].includes(k)) || [...new Set(params.keys())].length !== [...params.keys()].length) throw new Error('INVALID_QUERY');
    const cursor = params.get('cursor') ?? '0';
    if (!/^(0|[1-9][0-9]*)$/.test(cursor)) throw new Error('INVALID_QUERY');
    const limit = Number(params.get('limit') ?? '50');
    if (!Number.isInteger(limit) || limit < 1 || limit > HANDOFF_EXPORT_MAX_LIMIT) throw new Error('INVALID_QUERY');
    const baseline = this.collectGitBaseline(task);
    const all = this.handoffItems(task, baseline.payloadItems); const at = Number(cursor);
    const items = all.filter(i => Number(i.position) > at).slice(0, limit);
    const last = items.at(-1); const hasMore = !!last && Number(last.position) < all.length;
    return { schemaVersion: 'handoff-export/1', taskId: task.taskId, sealed: !!task.sealedFor, sealedFor: task.sealedFor ?? null,
      targetStart: 'NOT_STARTED', targetStartNote: 'Export acceptance is not handoff completion; the Host has no target-start integration',
      exportDigest: digest({ taskId: task.taskId, positions: all.map(i => i.id), bodies: all.map(i => i.body?.sha256 ?? i.body?.textRef?.sha256 ?? null) }),
      gitBaseline: baseline.entries,
      items, cursor: last ? String(last.position) : cursor, hasMore, coverage: 'PUBLIC_PROJECTION_BODIES_DIGEST_BOUND' };
  }
  /** Chunked read of one exported blob: part names are a closed whitelist
   *  (goal, output, message-N, aux-ID, child-COLLAB-CHILD, git-*) — never a
   *  path. git-* parts return the materialized binary payload untouched;
   *  text parts pass through publicText for the historical utf8 contract. */
  private handoffBlob(task: Row, part: string): string | Buffer | null {
    if (part === 'goal') return task.goal ?? '';
    if (part === 'output') return task.output ?? '';
    let m = part.match(/^message-(\d+)$/); if (m) return task.publicMessages?.[Number(m[1])]?.text ?? null;
    m = part.match(/^aux-([A-Za-z0-9][A-Za-z0-9._-]{0,127})$/); if (m) return task.auxSessions?.[m[1]]?.answer ?? null;
    m = part.match(/^child-([A-Za-z0-9][A-Za-z0-9._-]{0,127})-([A-Za-z0-9][A-Za-z0-9._-]{0,127})$/); if (m) return task.collaborations?.[m[1]]?.children?.[m[2]]?.output ?? null;
    if (/^git-(staged|unstaged|bundle)$/.test(part)) { try { return readFileSync(join(this.gitBaselineDir(task), part)); } catch { return null; } }
    return null;
  }
  handoffChunk(task: Row, part: string, sha256: string, offsetRaw: string) {
    const blob = this.handoffBlob(task, part); if (blob === null) throw new Error('HANDOFF_PART_NOT_FOUND');
    if (!/^[0-9a-f]{64}$/.test(sha256)) throw new Error('INVALID_QUERY');
    if (!/^(0|[1-9][0-9]*)$/.test(offsetRaw)) throw new Error('INVALID_QUERY');
    const bytes = Buffer.isBuffer(blob) ? blob : Buffer.from(publicText(blob));
    const sha = hash(bytes);
    if (sha !== sha256) throw new Error('HANDOFF_CHUNK_DIGEST_MISMATCH');
    const offset = Number(offsetRaw); const size = bytes.length;
    if (offset > size) throw new Error('HANDOFF_OFFSET_INVALID');
    const slice = bytes.subarray(offset, offset + HANDOFF_EXPORT_CHUNK_MAX);
    return { part, sha256: sha, offset, size, bytes: slice.toString('base64'), nextOffset: String(offset + slice.length) };
  }
  // ---- C6 material upload routes: project-scoped lifecycle over the durable
  //      ledger. Raw chunk PUT bodies arrive as Buffer; digest comes from the
  //      x-chunk-sha256 header so the JSON whitelist never carries bulk bytes.
  materialList(projectId: string) { const p = this.project(projectId); return { items: this.materials.list(p.projectId) }; }
  materialCreate(projectId: string, body: Row) {
    const p = this.project(projectId);
    if (body.target !== undefined) {
      if (!body.target || typeof body.target !== 'object' || Array.isArray(body.target)) throw new Error('MATERIAL_TARGET_INVALID');
      exact(body.target, ['taskId', 'createKey', 'handoff']);
      if (body.target.taskId !== undefined) { const t = this.store.task(id(body.target.taskId)); if (t.projectId !== p.projectId) throw new Error('MATERIAL_TARGET_INVALID'); }
      if (body.target.createKey !== undefined && typeof body.target.createKey !== 'string') throw new Error('MATERIAL_TARGET_INVALID');
    }
    return this.materials.create(p.projectId, body.target ?? {}, body);
  }
  materialGet(uploadKey: string) { return this.materials.public(this.materials.get(uploadKey)); }
  materialChunk(uploadKey: string, offsetRaw: string, bytes: Buffer, chunkDigest: string | undefined) {
    if (!/^(0|[1-9][0-9]*)$/.test(offsetRaw)) throw new Error('MATERIAL_OFFSET_INVALID');
    return this.materials.putChunk(this.materials.get(uploadKey), Number(offsetRaw), bytes, chunkDigest);
  }
  materialSeal(uploadKey: string, body: Row) { return this.materials.seal(this.materials.get(uploadKey), body); }
  materialDelete(uploadKey: string) { return this.materials.remove(this.materials.get(uploadKey)); }
  private async terminate(live: Live) {
    const child = live.child; if (!child.pid) return false;
    if (live.stopped) return true;
    // Close command intake and stop owned command groups before the model runtime.
    const runnerStopped = await this.stopRunners(live);
    // Terminate by retained Job handle, never by a PID that the OS could reuse.
    if (process.platform === 'win32') {
      if (!live.job) return false;
      live.stopped = await live.job.stop(); if (live.stopped) live.job.close(); return live.stopped;
    } else if (process.platform === 'darwin') {
      if (!live.mac) return false;
      live.stopped = await live.mac.stop() && runnerStopped; return live.stopped;
    } else { if (child.exitCode !== null || child.signalCode !== null) return false; try { process.kill(-child.pid, 'SIGKILL'); } catch {} }
    if (child.exitCode === null && child.signalCode === null) await Promise.race([new Promise<void>(done => child.once('exit', () => done())), new Promise<void>(done => setTimeout(done, 4000))]);
    live.stopped = (child.exitCode !== null || child.signalCode !== null) && !alive(child.pid); return live.stopped;
  }
  // ---- Task-scoped pipe terminals. Honest pipe sessions: pty is always false,
  //      output is a bounded offset-tracked ring, and every terminal dies with its Job.
  private terminalEnv() {
    return Object.fromEntries(Object.entries(process.env).filter(([k]) => !/KEY|TOKEN|SECRET|PASSWORD/i.test(k) && !/^(LLM|MODEL|WORKER|ACS|PAC)_/.test(k)));
  }
  private terminalMirror(t: Terminal, state: string, extra: Row = {}) { return { terminalId: t.terminalId, argv: t.argv, pty: false, pid: t.child.pid, state, createdAt: t.createdAt, exitedAt: t.exitedAt, exitCode: t.exitCode, signal: t.signal, ...extra }; }
  private terminalPersist(t: Terminal, state: string, extra: Row = {}) { this.update(t.taskId, task => { (task.terminals ??= {})[t.terminalId] = this.terminalMirror(t, state, extra); }); }
  private terminalPublic(t: Terminal) { return { terminalId: t.terminalId, argv: t.argv, pty: false, pid: t.child.pid ?? null, state: t.closed ? 'CLOSED' : t.exited ? 'EXITED' : 'RUNNING', createdAt: t.createdAt, exited: t.exited, exitCode: t.exitCode, signal: t.signal, cursor: String(t.written) }; }
  private liveTerminal(task: Row, terminalId: string) {
    const t = this.terminals.get(task.taskId + '/' + terminalId);
    if (!t) { const stored = task.terminals?.[terminalId]; if (!stored) throw new Error('TERMINAL_NOT_FOUND'); throw new Error(stored.state === 'LOST' ? 'TERMINAL_LOST' : 'TERMINAL_NOT_RUNNING'); }
    if (t.closed) throw new Error('TERMINAL_CLOSED');
    return t;
  }
  private terminalAppend(t: Terminal, chunk: Buffer) {
    t.chunks.push(chunk); t.buffered += chunk.length; t.written += chunk.length;
    while (t.buffered > TERMINAL_BUFFER && t.chunks.length > 1) t.buffered -= t.chunks.shift()!.length;
  }
  createTerminal(taskId: string, body: Row) {
    if (this.store.task(taskId).executionMode === 'CONTEXT_ONLY') throw new Error('CONTEXT_ONLY_OPERATION_DENIED');
    exact(body, ['requestKey', 'argv']);
    if (body.requestKey !== undefined) { const prior = this.replay(body); if (prior) { if (prior.taskId !== taskId) throw new Error('IDEMPOTENCY_CONFLICT'); return { terminalId: prior.terminalId, argv: prior.argv, pty: false, cursor: prior.cursor, replayed: true }; } }
    if (this.closed) throw new Error('LOCAL_HOST_CLOSED');
    const task = this.store.task(taskId); this.assertUnsealed(task);
    if (task.intakeClosed || ['CANCELLED', 'FAILED', 'BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'].includes(task.state)) throw new Error('TERMINAL_TASK_INACTIVE');
    if (!Array.isArray(body.argv) || !body.argv.length || body.argv.length > MAX_ARGV || body.argv.some((a: any) => typeof a !== 'string' || !a.length || a.length > 1024 || a.includes('\x00'))) throw new Error('INVALID_ARGV');
    if ([...this.terminals.values()].filter(t => t.taskId === taskId && !t.closed).length >= MAX_TERMINALS_PER_TASK) throw new Error('TERMINAL_LIMIT');
    const argv = body.argv as string[];
    const child = spawn(argv[0], argv.slice(1), { cwd: task.workspace, env: this.terminalEnv(), stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true, detached: process.platform !== 'win32' });
    const terminalId = 'term-' + randomUUID();
    const t: Terminal = { terminalId, taskId, child, argv, createdAt: now(), pty: false, written: 0, chunks: [], buffered: 0, exited: false, exitCode: null, signal: null, closed: false };
    child.stdout!.on('data', (c: Buffer) => this.terminalAppend(t, c));
    child.stderr!.on('data', (c: Buffer) => this.terminalAppend(t, c));
    child.on('error', () => {});
    if (process.platform === 'win32') {
      try { t.job = new WindowsJob(); t.job.assign(child.pid!); }
      catch { t.closed = true; try { child.kill(); } catch {} t.job?.close(); throw new Error('JOB_ASSIGN_FAILED'); }
    }
    child.on('exit', (code, signal) => {
      t.exited = true; t.exitCode = code; t.signal = signal; t.exitedAt = now();
      const stored = this.store.task(taskId)?.terminals?.[terminalId];
      if (stored && stored.state === 'RUNNING') this.terminalPersist(t, 'EXITED');
      if (stored?.state === 'RUNNING' || stored?.state === 'EXITED') { const task2 = this.store.task(taskId); if (task2) this.store.event(task2, 'TERMINAL_EXITED', { terminalId, exitCode: code, signal }); }
    });
    this.terminals.set(taskId + '/' + terminalId, t);
    this.update(taskId, task2 => { (task2.terminals ??= {})[terminalId] = this.terminalMirror(t, 'RUNNING'); this.store.event(task2, 'TERMINAL_CREATED', { terminalId, argv: argv.map(a => publicText(a)), pty: false, pid: child.pid }); });
    if (body.requestKey !== undefined) { const op = this.store.accept(body.requestKey, taskId, body, { kind: 'TERMINAL_CREATE', state: 'APPLIED' }); op.terminalId = terminalId; op.argv = argv; op.cursor = '0'; this.store.saveOperation(op); }
    return { terminalId, argv, pty: false, cursor: '0' };
  }
  terminalInput(task: Row, terminalId: string, body: Row) {
    exact(body, ['data']); this.assertUnsealed(task);
    const t = this.liveTerminal(task, terminalId);
    if (t.exited) throw new Error('TERMINAL_EXITED');
    if (typeof body.data !== 'string' || !/^[A-Za-z0-9+/]*={0,2}$/.test(body.data)) throw new Error('INVALID_BASE64');
    const bytes = Buffer.from(body.data, 'base64');
    if (bytes.length > 16384) throw new Error('TERMINAL_INPUT_TOO_LARGE');
    if (bytes.length) { try { t.child.stdin!.write(bytes); } catch { throw new Error('TERMINAL_STDIN_CLOSED'); } }
    this.store.event(task, 'TERMINAL_INPUT', { terminalId, bytes: bytes.length });
    return { terminalId, accepted: bytes.length };
  }
  async terminalOutput(task: Row, terminalId: string, params: URLSearchParams) {
    if ([...params.keys()].some(k => !['cursor', 'maxBytes', 'waitMs'].includes(k)) || [...new Set(params.keys())].length !== [...params.keys()].length) throw new Error('INVALID_QUERY');
    const stored = task.terminals?.[terminalId]; const t = this.terminals.get(task.taskId + '/' + terminalId);
    if (!t) { if (!stored) throw new Error('TERMINAL_NOT_FOUND'); throw new Error(stored.state === 'LOST' ? 'TERMINAL_LOST' : 'TERMINAL_NOT_RUNNING'); }
    const cursor = params.has('cursor') ? params.get('cursor')! : '0';
    if (!/^(0|[1-9][0-9]*)$/.test(cursor)) throw new Error('INVALID_QUERY');
    const maxBytes = Math.min(Number(params.get('maxBytes') ?? '65536'), 65536);
    const waitMs = Math.min(Number(params.get('waitMs') ?? '0'), 5000);
    const at = Number(cursor); if (at > t.written) throw new Error('TERMINAL_CURSOR_INVALID');
    const deadline = Date.now() + waitMs;
    for (;;) {
      const base = t.written - t.buffered;
      if (at < t.written || t.exited || t.closed || Date.now() >= deadline) {
        const start = Math.max(at, base); let skip = start - base; const parts: Buffer[] = []; let left = maxBytes;
        for (const chunk of t.chunks) {
          if (skip >= chunk.length) { skip -= chunk.length; continue; }
          const piece = skip ? chunk.subarray(skip) : chunk; skip = 0;
          if (piece.length <= left) { parts.push(piece); left -= piece.length; } else { parts.push(piece.subarray(0, left)); left = 0; break; }
        }
        const data = Buffer.concat(parts);
        return { terminalId, data: data.toString('base64'), bytes: data.length, cursor: String(start + data.length), end: String(t.written), truncated: at < base, exited: t.exited, exitCode: t.exitCode, signal: t.signal };
      }
      await new Promise<void>(done => { const onData = () => { cleanup(); done(); }; const onExit = () => { cleanup(); done(); }; const timer = setTimeout(() => { cleanup(); done(); }, Math.max(0, deadline - Date.now())); const cleanup = () => { clearTimeout(timer); t.child.stdout!.off('data', onData); t.child.stderr!.off('data', onData); t.child.off('exit', onExit); }; t.child.stdout!.on('data', onData); t.child.stderr!.on('data', onData); t.child.once('exit', onExit); });
    }
  }
  async terminalSignal(task: Row, terminalId: string, body: Row) {
    exact(body, ['signal']);
    const t = this.liveTerminal(task, terminalId);
    if (!['TERM', 'KILL', 'INT'].includes(body.signal)) throw new Error('INVALID_SIGNAL');
    if (t.exited) return { terminalId, delivered: false, exited: true, exitCode: t.exitCode, signal: t.signal };
    const pid = t.child.pid!;
    if (process.platform === 'win32') {
      // No POSIX signals exist here: TERM/INT request graceful tree exit, KILL tears the Job down.
      if (body.signal === 'KILL' && t.job) { await t.job.stop(); }
      else try { execFileSync('taskkill', ['/PID', String(pid), '/T', ...(body.signal === 'KILL' ? ['/F'] : [])], { timeout: 10000, windowsHide: true, stdio: 'ignore' }); } catch {}
    } else { try { process.kill(-pid, body.signal === 'KILL' ? 'SIGKILL' : body.signal === 'INT' ? 'SIGINT' : 'SIGTERM'); } catch {} }
    this.store.event(task, 'TERMINAL_SIGNAL', { terminalId, signal: body.signal });
    return { terminalId, delivered: true, signal: body.signal };
  }
  async terminalDelete(task: Row, terminalId: string) {
    const t = this.liveTerminal(task, terminalId);
    if (!t.exited) {
      if (process.platform === 'win32') { if (t.job) await t.job.stop(); else try { execFileSync('taskkill', ['/PID', String(t.child.pid!), '/T', '/F'], { timeout: 10000, windowsHide: true, stdio: 'ignore' }); } catch {} }
      else { try { process.kill(-t.child.pid!, 'SIGKILL'); } catch {} }
      await Promise.race([new Promise<void>(d => t.child.once('exit', () => d())), new Promise<void>(d => setTimeout(d, 3000))]);
    }
    t.closed = true; t.job?.close(); this.terminals.delete(task.taskId + '/' + terminalId);
    this.update(task.taskId, task2 => { (task2.terminals ??= {})[terminalId] = this.terminalMirror(t, 'CLOSED'); this.store.event(task2, 'TERMINAL_CLOSED', { terminalId }); });
    return { terminalId, closed: true };
  }
  /** Stop every terminal a task owns and prove each Job drained to zero processes. */
  private async killTaskTerminals(taskId: string, reason: string) {
    const proof: Row[] = [];
    for (const t of [...this.terminals.values()]) {
      if (t.taskId !== taskId || t.closed) continue;
      if (!t.exited) {
        if (process.platform === 'win32') { if (t.job) await t.job.stop(); else try { execFileSync('taskkill', ['/PID', String(t.child.pid!), '/T', '/F'], { timeout: 10000, windowsHide: true, stdio: 'ignore' }); } catch {} }
        else { try { process.kill(-t.child.pid!, 'SIGKILL'); } catch {} }
        await Promise.race([new Promise<void>(d => t.child.once('exit', () => d())), new Promise<void>(d => setTimeout(d, 3000))]);
      }
      t.closed = true; t.job?.close(); this.terminals.delete(taskId + '/' + t.terminalId);
      proof.push({ terminalId: t.terminalId, pid: t.child.pid, method: process.platform === 'win32' ? 'WINDOWS_JOB_ACTIVE_ZERO' : 'OWNED_PROCESS_GROUP', activeProcesses: 0, exitCode: t.exitCode });
      this.update(taskId, task => { (task.terminals ??= {})[t.terminalId] = this.terminalMirror(t, 'CLOSED', { reasonCode: reason }); });
    }
    if (proof.length) { const task = this.store.task(taskId); this.store.event(task, 'TERMINALS_STOPPED', { reason, terminals: proof }); }
    return proof;
  }
  private taskOperations(taskId: string): Row[] { return this.store.db.prepare('SELECT value FROM operations WHERE task=? ORDER BY rowid').all(taskId).map(r => JSON.parse(r.value as string) as Row); }
  private taskEvents(taskId: string): Row[] { return this.store.db.prepare('SELECT seq,value FROM events WHERE task=? ORDER BY seq LIMIT 500').all(taskId).map(r => ({ seq: String(r.seq), ...(JSON.parse(r.value as string) as Row) })); }
  private commandReceipt(o: Row) { return { requestKey: o.requestKey, kind: o.kind ?? 'TASK_CREATE', state: o.state, acceptedAt: o.acceptedAt, dispatchStartedAt: o.dispatchStartedAt, consumedAt: o.consumedAt, detail: o.detail, resumeVia: o.resumeVia, sourceSegmentId: o.sourceSegmentId }; }
  audit(task: Row) {
    const ops = this.taskOperations(task.taskId);
    const items = this.taskEvents(task.taskId).map(e => { const item: Row = { seq: e.seq, kind: e.kind, occurredAt: e.occurredAt, segmentId: e.segmentId, nativeSessionId: e.nativeSessionId, nativeTurnId: e.nativeTurnId, targetCommandId: e.targetCommandId }; for (const [k, v] of Object.entries(e)) if (!['seq', 'cursor', 'sequence', 'kind', 'occurredAt', 'taskId', 'generation', 'segmentId', 'nativeSessionId', 'nativeTurnId', 'targetCommandId', 'text', 'delta'].includes(k)) item[k] = v; return item; });
    const manifests = [...(task.artifactManifest ? [task.artifactManifest] : []), ...(task.artifactHistory ?? [])];
    return { schemaVersion: 'worker-audit/1', taskId: task.taskId,
      runnerCommands: Object.values(task.runnerCommands ?? {}).map((r: any) => ({ requestKey: r.requestKey, contextId: r.contextId, toolCallId: r.toolCallId, toolName: r.toolName, binding: r.binding, bodyDigest: r.bodyDigest, status: r.status, acceptedAt: r.acceptedAt, settledAt: r.settledAt, reasonCode: r.reasonCode, receipt: r.receipt ? { commandId: r.receipt.commandId, status: r.receipt.status, exitCode: r.receipt.exitCode, signal: r.receipt.signal, stdout: this.bodyRef(r.receipt.stdout ?? '', 'runner/' + r.requestKey + '/stdout'), stderr: this.bodyRef(r.receipt.stderr ?? '', 'runner/' + r.requestKey + '/stderr'), buildSucceeded: r.receipt.buildSucceeded, tests: r.receipt.tests, stopProof: r.receipt.stopProof } : undefined })),
      subject: { requestKey: ops.find(o => !o.kind)?.requestKey ?? null, client: 'worker-local/1', projectId: task.projectId, engineId: 'pi' },
      commands: ops.map(o => this.commandReceipt(o)), items,
      effects: [...Object.entries(task.effects ?? {}).map(([toolCallId, e]: any) => ({ toolCallId, segmentId: task.segmentId, ...e })), ...(task.effectSegments ?? []).flatMap((s: any) => Object.entries(s.effects).map(([toolCallId, e]: any) => ({ toolCallId, segmentId: s.segmentId, ...e })))],
      artifacts: manifests.flatMap((m: any) => m.items.map((i: any) => ({ artifactId: i.artifactId, path: i.path, sha256: i.sha256, size: i.size, status: i.status, segmentId: m.segmentId }))),
      stateTransitions: Object.entries(task.turns ?? {}).map(([commandId, t]: any) => ({ commandId, state: t.state, nativeTurnId: t.nativeTurnId })),
      usage: { ...task.usage, source: 'ENGINE_REPORTED' }, budget: task.budget ?? null, coverage: 'DURABLE_FACTS_NO_BODIES' };
  }
  private bodyRef(text: string, where: string) { return { ref: where, sha256: hash(text), size: Buffer.byteLength(text) }; }
  private redactDeep(value: any): any {
    if (typeof value === 'string') {
      let out = publicText(value);
      for (const root of [this.store.directory, ...this.store.tasks().map(t => t.workspace).filter(Boolean)]) { try { const real = realpathSync(root); if (real && out.includes(real)) out = out.split(real).join('<host-path>'); } catch {} }
      if (/^[A-Za-z]:[\\/]|^\//.test(out)) return { pathRef: hash(value), name: basename(out) };
      // Absolute paths outside the workspace must not leak from embedded fields either
      // (e.g. a write.scope.violation deniedPath); scrub drive-letter and multi-segment POSIX paths.
      out = out.replace(/[A-Za-z]:[\\/][^\s'"`<>|]+/g, '<host-path>').replace(/(?<![\w:/])\/(?:[^\s/'"`<>|]+\/)+[^\s/'"`<>|]*/g, '<host-path>');
      return out;
    }
    if (Array.isArray(value)) return value.map(v => this.redactDeep(v));
    if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, this.redactDeep(v)]));
    return value;
  }
  diagnostics(task: Row, searchParams: URLSearchParams) {
    if ([...searchParams.keys()].some(k => k !== 'redact') || [...new Set(searchParams.keys())].length !== [...searchParams.keys()].length || (searchParams.get('redact') ?? 'content') !== 'content') throw new Error('INVALID_QUERY');
    const ref = this.bodyRef;
    const events = this.taskEvents(task.taskId).map(e => { const item: Row = { seq: e.seq, kind: e.kind, occurredAt: e.occurredAt, segmentId: e.segmentId, nativeTurnId: e.nativeTurnId }; for (const [k, v] of Object.entries(e)) { if (['seq', 'cursor', 'sequence', 'kind', 'occurredAt', 'taskId', 'generation', 'segmentId', 'nativeSessionId', 'nativeTurnId', 'targetCommandId'].includes(k)) continue; item[k] = (k === 'text' || k === 'delta') && typeof v === 'string' ? ref.call(this, v, 'events/' + e.seq) : v; } return item; });
    return this.redactDeep({ schemaVersion: 'worker-diagnostics/1', taskId: task.taskId, redaction: 'content', bootId: this.bootId, storeId: this.store.identity.storeId,
      task: { taskId: task.taskId, engineId: 'pi', engineVersion: task.engineVersion, projectId: task.projectId, state: task.state, launchState: task.launchState, reasonCode: task.reasonCode, quiescent: task.quiescent, generation: task.generation, segmentId: task.segmentId, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, currentCommandId: task.currentCommandId, createdAt: task.createdAt, deadlineMs: task.deadlineMs, deadlineEpochMillis: task.deadlineEpochMillis, configuredModel: task.configuredModel, actualModel: task.actualModel, queueOrdinal: task.queueOrdinal, stopProof: task.stopProof, budgetExceeded: task.budgetExceeded, bindingUnavailable: task.bindingUnavailable, intakeClosed: task.intakeClosed },
      goal: ref.call(this, task.goal ?? '', 'task.goal'), output: ref.call(this, task.output ?? '', 'task.output'),
      usage: { ...task.usage, source: 'ENGINE_REPORTED' }, usageLedger: task.usageLedger ?? [], budget: task.budget ?? null, writeScope: task.writeScope ?? null, environment: task.environment ?? null, environmentBinding: task.environmentBinding ?? null, checkpointManifest: task.checkpointManifest ?? null,
      messages: (task.publicMessages ?? []).map((m: any, i: number) => ({ role: m.role, commandId: m.commandId, nativeTurnId: m.nativeTurnId, occurredAt: m.occurredAt, text: ref.call(this, m.text ?? '', 'publicMessages/' + i) })),
      inputRequests: Object.values(task.inputRequests ?? {}).map((q: any) => ({ inputRequestId: q.inputRequestId, method: q.method, state: q.state, nativeTurnId: q.nativeTurnId, targetCommandId: q.targetCommandId, title: q.title != null ? ref.call(this, q.title, 'inputRequests/' + q.inputRequestId + '/title') : undefined, message: q.message != null ? ref.call(this, q.message, 'inputRequests/' + q.inputRequestId + '/message') : undefined, options: q.options })),
      events, effects: { current: task.effects ?? {}, segments: task.effectSegments ?? [] },
      artifacts: { current: task.artifactManifest ?? null, history: task.artifactHistory ?? [] },
      stateTransitions: task.turns ?? {}, coverage: 'BODIES_REPLACED_BY_SHA256_REFS' });
  }
  /** worker-events/1 SSE surface (GET .../events with Accept: text/event-stream
   *  or ?follow=1): replay the durable events spine from a cursor to the current
   *  high-water, then keep pushing new rows on a short poll. Frames carry
   *  `id: <seq>`/`event: <kind>`/`data: <row JSON>` triples so a client can
   *  resume losslessly through cursor or Last-Event-ID. The client owns the
   *  disconnect; a consumer that lets ~1MiB back up is dropped rather than
   *  growing host memory. The first page doubles as cursor validation —
   *  identical rules to the JSON surface — so it runs BEFORE writeHead and any
   *  rejection still travels back as the normal JSON error. */
  private eventStream(taskId: string, cursor: string, pageLimit: number, req: IncomingMessage, res: ServerResponse) {
    const first = this.store.events(taskId, cursor, pageLimit);
    const pollMs = this.config.ssePollMs ?? SSE_POLL_MS; const heartbeatMs = this.config.sseHeartbeatMs ?? SSE_HEARTBEAT_MS;
    res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.flushHeaders();
    this.sseConnections.add(req.socket);
    let at = cursor, buffered = 0, closed = false, lastWrite = Date.now();
    const drop = () => { if (closed) return; closed = true; clearInterval(timer); this.sseConnections.delete(req.socket); try { res.end(); } catch {} };
    const write = (frame: string) => {
      if (closed) return;
      buffered += Buffer.byteLength(frame);
      const ok = res.write(frame, () => { buffered -= Buffer.byteLength(frame); });
      lastWrite = Date.now();
      // Backpressure: a single `false` is tolerable — the poll keeps producing
      // only one bounded page per tick — but a buffer that crossed ~1MiB means
      // the consumer is hopelessly behind; cut it loose.
      if (!ok && buffered > SSE_BUFFER_MAX) drop();
    };
    const push = (item: Row) => { write(`id: ${item.cursor}\nevent: ${item.kind}\ndata: ${JSON.stringify(item)}\n\n`); at = item.cursor; };
    try { for (const item of first.items) push(item); } catch { drop(); return; }
    const timer = setInterval(() => {
      if (closed) return;
      try {
        if (Date.now() - lastWrite >= heartbeatMs) write(`: heartbeat ${Date.now()}\n\n`);
        for (const item of this.store.events(taskId, at, pageLimit).items) push(item);
      } catch { drop(); }
    }, pollMs);
    timer.unref();
    req.on('close', drop); res.on('close', drop);
  }
  async listen(port = 0) { await new Promise<void>(done => this.server.listen(port, '127.0.0.1', done)); return (this.server.address() as any).port as number; }
  async close() { if (this.closed) return; this.closed = true; for (const socket of [...this.sseConnections]) socket.destroy(); await new Promise<void>(done => this.server.close(() => done())); for (const taskId of new Set([...this.terminals.values()].map(t => t.taskId))) await this.killTaskTerminals(taskId, 'HOST_CLOSE'); for (const [auxId, live] of this.auxLive) await this.finishAux(live.taskId, auxId, 'CANCELLED', 'HOST_CLOSE', live); for (const task of this.store.tasks()) if (Object.values(task.collaborations ?? {}).some((c: any) => c.state === 'RUNNING')) await this.stopTaskCollaborations(task.taskId, 'HOST_CLOSE'); for (const [taskId, live] of this.live) { live.stopping = true; await this.stopMacTools(taskId); const modelStopped = await this.terminate(live); const stopped = modelStopped && (!this.store.task(taskId).macToolBinding || this.store.task(taskId).macRemoteQuiescent === true); if (stopped) this.update(taskId, t => { t.quiescent = true; t.stopProof = { ownerBootId: this.bootId, pid: live.child.pid, method: this.quiescenceMethod, observedAt: now(), ...(live.runnerProof ?? {}) }; if (!['SUCCEEDED', 'FAILED', 'PAUSED', 'BUDGET_EXCEEDED', 'BINDING_UNAVAILABLE'].includes(t.state)) this.state(t, 'CANCELLED'); this.sealCompleted(t); this.store.event(t, 'HOST_CLOSE_STOPPED'); }); else this.update(taskId, t => { if (live.mac?.proof) t.processOwnership = live.mac.proof; if (!['BUDGET_EXCEEDED'].includes(t.state)) this.state(t, 'UNKNOWN'); t.reasonCode = 'PROCESS_QUIESCENCE_UNPROVEN'; this.markEffectsUnknown(t); this.store.event(t, 'HOST_CLOSE_STOP_UNKNOWN'); }); } this.storageClosed = true; this.store.close(); }
  private async http(req: IncomingMessage, res: ServerResponse) {
    const send = (status: number, body: any) => { res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(body)); };
    try {
      const wanted = Buffer.from('Bearer ' + this.config.token), auth = Buffer.from(req.headers.authorization ?? '');
      if (auth.length !== wanted.length || !timingSafeEqual(auth, wanted)) { send(401, { code: 'LOCAL_AUTH_REQUIRED' }); return; }
      if (!/^(127\.0\.0\.1|localhost):\d+$/.test(req.headers.host ?? '') || !['127.0.0.1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress ?? '')) throw new Error('LOOPBACK_REQUIRED');
      if (req.headers.origin) throw new Error('BROWSER_ORIGIN_DENIED');
      if (req.headers['x-worker-local-store'] && req.headers['x-worker-local-store'] !== this.store.identity.storeId) throw new Error('STORE_IDENTITY_CHANGED');
      const url = new URL(req.url!, 'http://127.0.0.1'); const method = req.method;
      let body: Row = {}; let rawBody: Buffer | null = null;
      if (method === 'POST') { const bodyLimit = url.pathname.includes('/context-packages') ? 2097152 : url.pathname.includes('/collaborations') ? 262144 : 65536; let buffer = ''; for await (const chunk of req) { buffer += chunk; if (Buffer.byteLength(buffer) > bodyLimit) throw new Error('BODY_TOO_LARGE'); } try { body = JSON.parse(buffer); } catch { throw new Error('INVALID_JSON'); } if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('INVALID_JSON'); if (body.requestKey && url.pathname !== '/worker-host/v1/operations/lookup' && req.headers['idempotency-key'] !== body.requestKey) throw new Error('KEY_MISMATCH'); }
      if (method === 'PUT') { const parts: Buffer[] = []; let total = 0; for await (const chunk of req) { const b = chunk as Buffer; total += b.length; if (total > MATERIAL_CHUNK_MAX) throw new Error('MATERIAL_CHUNK_TOO_LARGE'); parts.push(b); } rawBody = Buffer.concat(parts); }
      let result: any; let status = 200;
      if (url.pathname === '/worker-host/v1/manifest' && method === 'GET') result = this.manifest();
      if (url.pathname === '/worker-host/v1/capabilities' && method === 'GET') result = this.capabilities();
      if (url.pathname === '/worker-host/v1/operations/lookup' && method === 'POST') { exact(body, ['requestKey', 'taskId']); result = this.store.operation(body.requestKey); if (!result || (body.taskId && result.taskId !== body.taskId)) throw new Error('REQUEST_NOT_FOUND'); }
      const whTask = url.pathname.match(/^\/worker-host\/v1\/tasks\/([A-Za-z0-9][A-Za-z0-9._-]{0,127})\/(audit|diagnostics)$/);
      if (whTask && method === 'GET') { const task = this.store.task(whTask[1]); result = whTask[2] === 'audit' ? this.audit(task) : this.diagnostics(task, url.searchParams); }
      const path = url.pathname.startsWith('/local/v1/') ? url.pathname.slice('/local/v1/'.length).split('/') : [];
      if (method === 'GET' && path.length === 1) {
        if (path[0] === 'identity') result = this.identity();
        if (path[0] === 'projects') result = { ...this.identity(), items: this.projects() };
        if (path[0] === 'tasks') {
          if ([...url.searchParams.keys()].some(k => !['limit', 'cursor'].includes(k)) || [...url.searchParams.keys()].some(k => url.searchParams.getAll(k).length > 1)) throw new Error('INVALID_QUERY');
          const limit = Number(url.searchParams.get('limit') ?? '50');
          if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw new Error('INVALID_QUERY');
          const page = this.store.taskDirectory(url.searchParams.get('cursor'), limit);
          result = { ...this.identity(), nextCursor: page.nextCursor, items: page.items.map(t => ({ taskId: t.taskId, projectId: t.projectId, engineId: t.engineId, engineVersion: t.engineVersion, state: t.state })) };
        }
        if (path[0] === 'engines') result = { ...this.identity(), items: [{ engineId: 'pi', version: PI_VERSION, executableDigest: this.engineBinding.digest, engineBindingDigest: this.engineBinding.digest, engineProvenanceKind: this.engineBinding.provenance.kind, profileId: this.profileId, executionProfile: this.executionProfile, supportsPause: true, supportsResume: true, ...this.executionAvailability(), executionPolicy: { profile: this.executionProfile, profileId: this.profileId, sandbox: this.executionProfile === RUNNER_PROFILE ? 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS' : isNoForkProfile(this.executionProfile) ? 'DENY_PROCESS_FORK' : 'UNDECLARED', osSandboxClaimed: isNoForkProfile(this.executionProfile), approvalPolicy: 'HUMAN_VIA_ASK_USER_TOOL', toolAdmission: 'AWAITED_GATE_CHECK', toolAllowlist: this.executionTools, policyPresets: Object.keys(this.policyPresets()), frozen: false, semanticsNote: 'Engine-level defaults; a task freezes its policy at launch and never mutates it in-run' }, capabilities: { executionModes: ['WORKSPACE_TASK', 'CONTEXT_ONLY'], steer: true, followUp: true, stop: true, pause: true, resume: true, inputMethods: ['confirm', 'select', 'input'], environmentBinding: true, budget: true, writeScope: true, audit: true, diagnostics: true, contextPackages: true, auxiliarySessions: true, transitions: { sameEngine: true, crossEngine: false }, collaborations: { dag: true, isolatedCheckout: true, maxChildren: COLLAB_MAX_CHILDREN, maxActivePerTask: COLLAB_MAX_PER_TASK }, adoptions: { conflictCheck: true, partial: true, modes: ['ALL', 'FILES'] }, terminals: { pty: false, resize: false, maxPerTask: MAX_TERMINALS_PER_TASK, outputBuffer: TERMINAL_BUFFER, signals: ['TERM', 'KILL', 'INT'] }, mcpServers: { transport: 'stdio', exposure: 'direct', maxServers: 8, envIndirection: false }, skills: { managedProfileOnly: true, contentBinding: 'sha256' }, extensions: { managedProfileOnly: true, contentBinding: 'sha256' } } }] };
      }
      if (path.length === 1 && method === 'POST') {
        if (path[0] === 'task-preflights') result = this.preflight(body);
        if (path[0] === 'tasks') { result = this.create(body); status = 202; }
      }
      if (path[0] === 'task-requests' && path.length === 2 && method === 'GET') { result = this.store.operation(path[1]); if (!result || result.kind) throw new Error('REQUEST_NOT_FOUND'); }
      if (path[0] === 'tasks' && path.length >= 2) {
        const task = this.store.task(path[1]);
        if (method === 'GET' && path.length === 2) result = this.detail(task);
        if (method === 'GET' && path.length === 3) switch (path[2]) {
          case 'events': {
            const keys = [...url.searchParams.keys()];
            // worker-events/1 SSE: Accept: text/event-stream wins; ?follow=1
            // triggers the same surface for clients that cannot set headers.
            if ((req.headers.accept ?? '').includes('text/event-stream') || url.searchParams.get('follow') === '1') {
              if (keys.some(k => !['cursor', 'limit', 'follow'].includes(k)) || new Set(keys).size !== keys.length) throw new Error('INVALID_QUERY');
              const lastEventId = req.headers['last-event-id']; const queryCursor = url.searchParams.get('cursor');
              if (queryCursor !== null && lastEventId !== undefined && queryCursor !== String(lastEventId)) { send(400, { code: 'EVENT_CURSOR_CONFLICT', message: 'EVENT_CURSOR_CONFLICT' }); return; }
              const limitParam = url.searchParams.get('limit');
              if (limitParam !== null && (!/^\d+$/.test(limitParam) || Number(limitParam) < 1 || Number(limitParam) > 100)) throw new Error('INVALID_EVENT_LIMIT');
              this.eventStream(task.taskId, queryCursor ?? (lastEventId !== undefined ? String(lastEventId) : '0'), limitParam === null ? 100 : Number(limitParam), req, res);
              return;
            }
            if (keys.some(k => !['cursor', 'limit'].includes(k)) || new Set(keys).size !== keys.length) throw new Error('INVALID_QUERY'); result = this.store.events(task.taskId, url.searchParams.get('cursor') ?? '0', Number(url.searchParams.get('limit') ?? '30')); break;
          }
          case 'output': { const msgs = (task.publicMessages ?? []).length ? task.publicMessages : this.journalMessages(task); result = { text: task.output || msgs.filter((m: any) => m.role === 'assistant').map((m: any) => m.text).join('\n'), coverage: 'BOUNDED_PUBLIC_MESSAGES' }; break; }
          case 'messages': {
            // Same query contract as the sibling paged routes: cursor/before/
            // limit are optional decimals, mutually exclusive bounds, and any
            // unknown parameter is refused instead of silently ignored.
            const keys = [...url.searchParams.keys()];
            if (keys.some(k => !['cursor', 'before', 'limit'].includes(k)) || new Set(keys).size !== keys.length) throw new Error('INVALID_QUERY');
            const num = (k: string) => { const v = url.searchParams.get(k); if (v !== null && !/^[0-9]{1,18}$/.test(v)) throw new Error('INVALID_QUERY'); return v === null ? null : Number(v); };
            const cursor = num('cursor'), before = num('before'), limit = num('limit');
            if (cursor !== null && before !== null) throw new Error('INVALID_QUERY');
            if (limit !== null && (limit < 1 || limit > 100)) throw new Error('INVALID_QUERY');
            const all = ((task.publicMessages ?? []).length ? task.publicMessages : this.journalMessages(task)) as any[];
            let items = all.map((m, i) => ({ ...m, seq: i }));
            if (cursor !== null) items = items.filter(m => m.seq >= cursor);
            if (before !== null) items = items.filter(m => m.seq < before).slice(-(limit ?? 100));
            else items = items.slice(0, limit ?? 100);
            const nextSeq = items.length ? items[items.length - 1].seq + 1 : null;
            const hasMore = nextSeq !== null && all.length > nextSeq;
            result = { text: task.output || all.filter((m: any) => m.role === 'assistant').map((m: any) => m.text).join('\n'), turns: task.turns, items, coverage: 'BOUNDED_PUBLIC_MESSAGES', hasMore, cursor: hasMore ? String(nextSeq) : null };
            break;
          }
          case 'metrics': result = { cost: null, costCoverage: 'UNKNOWN', usageCoverage: 'NATIVE_TASK_TOTAL', configuredModel: task.configuredModel, ...Object.fromEntries(Object.entries(task.usage).map(([k, v]) => ['usage.' + k, String(v)])) }; break;
          case 'input-requests': result = { items: Object.values(task.inputRequests) }; break;
          case 'artifacts': result = artifacts(this.store, task); break;
          case 'terminals': { const items = new Map<string, Row>(); for (const s of Object.values(task.terminals ?? {}) as Row[]) items.set(s.terminalId, s); for (const t of [...this.terminals.values()].filter(t => t.taskId === task.taskId)) items.set(t.terminalId, this.terminalPublic(t)); result = { items: [...items.values()] }; break; }
          case 'context-packages': result = { items: Object.values(task.contextPackages ?? {}).map((p: any) => this.packageMeta(p)), coverage: 'FROZEN_AT_CREATION' }; break;
          case 'auxiliary-sessions': result = { items: Object.values(task.auxSessions ?? {}).map((a: any) => this.auxPublic(a)), coverage: 'ISOLATED_SNAPSHOT_READ_ONLY' }; break;
          case 'collaborations': result = { items: Object.values(task.collaborations ?? {}).map((c: any) => this.collabPublic(c)), coverage: 'ISOLATED_CHECKOUT_DAG' }; break;
          case 'transitions': result = { items: task.transitions ?? [] }; break;
          case 'source-seals': if ([...url.searchParams.keys()].length) throw new Error('INVALID_QUERY'); result = this.sourceSealList(task); break;
          case 'handoff-export': result = this.handoffExport(task, url.searchParams); break;
        }
        if (method === 'GET' && path.length === 4) {
          if (path[2] === 'artifacts') result = artifact(this.store, task, id(path[3]));
          if (path[2] === 'context-packages') result = this.contextPackageDetail(task, path[3]);
          if (path[2] === 'auxiliary-sessions') { const aux = task.auxSessions?.[id(path[3])]; if (!aux) throw new Error('AUX_SESSION_NOT_FOUND'); result = this.auxPublic(aux); }
          if (path[2] === 'collaborations') { const collab = task.collaborations?.[id(path[3])]; if (!collab) throw new Error('COLLABORATION_NOT_FOUND'); result = this.collabPublic(collab); }
          if (path[2] === 'source-seals') result = this.sourceSealGet(task, path[3]);
          if (['commands', 'interaction-requests'].includes(path[2])) { result = this.store.operation(path[3]); if (!result || result.taskId !== task.taskId) throw new Error('REQUEST_NOT_FOUND'); }
        }
        if (method === 'GET' && path.length === 5 && path[2] === 'terminals' && path[4] === 'output') result = await this.terminalOutput(task, id(path[3]), url.searchParams);
        if (method === 'GET' && path.length === 5 && path[2] === 'auxiliary-sessions' && path[4] === 'messages') result = this.auxMessages(task, path[3], url.searchParams);
        if (method === 'GET' && path.length === 5 && path[2] === 'collaborations' && path[4] === 'adoptions') { const collab = task.collaborations?.[id(path[3])]; if (!collab) throw new Error('COLLABORATION_NOT_FOUND'); result = { items: Object.values(collab.children ?? {}).flatMap((ch: any) => (ch.adoptions ?? []).map((a: any) => ({ childId: ch.childId, ...a }))) }; }
        if (method === 'GET' && path.length === 6 && path[2] === 'collaborations' && path[4] === 'previews') result = this.collaborationPreviewGet(task, path[3], path[5]);
        if (method === 'GET' && path.length === 7 && path[2] === 'collaborations' && path[4] === 'children' && path[6] === 'messages') result = this.collabChildMessages(task, path[3], path[5], url.searchParams);
        if (method === 'GET' && path.length === 7 && path[2] === 'handoff-export' && path[4] === 'chunks') result = this.handoffChunk(task, path[3], path[5], path[6]);
        if (method === 'POST' && path.length === 3) {
          if (path[2] === 'messages') { exact(body, ['requestKey', 'kind', 'content', 'materials', 'generation', 'nativeSessionId', 'nativeTurnId', 'targetCommandId']); result = this.interact(task.taskId, body); status = 202; }
          if (path[2] === 'commands') { const fields = ['requestKey', 'intent', 'generation', 'nativeSessionId', 'nativeTurnId', 'targetCommandId']; exact(body, body.intent === 'EXTEND_BUDGET' ? [...fields, 'budget'] : fields); result = { kind: 'WORKER_LOCAL', ...(await this.command(task.taskId, body)) }; status = 202; }
          if (path[2] === 'terminals') { result = this.createTerminal(task.taskId, body); status = 202; }
          if (path[2] === 'context-packages') { result = this.createContextPackage(task.taskId, body); status = 202; }
          if (path[2] === 'auxiliary-sessions') { result = this.createAuxSession(task.taskId, body); status = 202; }
          if (path[2] === 'collaborations') { result = this.createCollaboration(task.taskId, body); status = 202; }
          if (path[2] === 'transitions') { result = { kind: 'WORKER_LOCAL', ...(await this.transition(task.taskId, body)) }; status = 202; }
          if (path[2] === 'source-seals') { result = await this.sourceSeal(task.taskId, body); status = 202; }
        }
        if (method === 'POST' && path.length === 4 && path[2] === 'collaborations' && path[3] === 'preflight') { result = this.collaborationPreflight(task.taskId, body); status = 202; }
        if (method === 'POST' && path.length === 5 && path[2] === 'input-requests' && path[4] === 'responses') { exact(body, ['requestKey', 'response', 'generation', 'nativeSessionId', 'nativeTurnId', 'targetCommandId']); result = this.interact(task.taskId, { ...body, kind: 'INPUT_RESPONSE', inputRequestId: id(path[3]) }); status = 202; }
        if (method === 'POST' && path.length === 5 && path[2] === 'auxiliary-sessions') {
          if (path[4] === 'stop') result = await this.auxStop(task.taskId, path[3], body);
          if (path[4] === 'deliver') result = this.auxDeliver(task.taskId, path[3], body);
        }
        if (method === 'POST' && path.length === 5 && path[2] === 'collaborations') {
          if (path[4] === 'stop') { result = await this.collaborationStop(task.taskId, path[3], body); status = 202; }
          if (path[4] === 'adoptions') { result = this.collaborationAdopt(task.taskId, path[3], body); status = result?.outcome === 'CONFLICT' ? 409 : 202; }
          if (path[4] === 'previews') { result = this.collaborationPreview(task.taskId, path[3], body); status = 202; }
        }
        if (method === 'POST' && path.length === 5 && path[2] === 'terminals') {
          if (path[4] === 'input') result = this.terminalInput(task, id(path[3]), body);
          if (path[4] === 'signal') result = await this.terminalSignal(task, id(path[3]), body);
          if (path[4] === 'resize') throw new Error('NOT_SUPPORTED');
        }
        if (method === 'DELETE' && path.length === 4 && path[2] === 'terminals') result = await this.terminalDelete(task, id(path[3]));
      }
      // Project-scoped material uploads (C6). Chunk bytes arrive as the raw PUT
      // body with the chunk digest in x-chunk-sha256; every other mutation is
      // JSON + requestKey idempotent like the rest of the surface.
      if (path[0] === 'projects' && path[2] === 'material-uploads' && path.length === 3) {
        if (method === 'GET') result = this.materialList(id(path[1]));
        if (method === 'POST') { result = this.materialCreate(id(path[1]), body); status = 202; }
      }
      if (path[0] === 'material-uploads') {
        if (path.length === 2 && method === 'GET') result = this.materialGet(path[1]);
        if (path.length === 2 && method === 'DELETE') result = this.materialDelete(path[1]);
        if (path.length === 3 && path[2] === 'seal' && method === 'POST') result = this.materialSeal(path[1], body);
        if (path.length === 4 && path[2] === 'chunks' && method === 'PUT') result = this.materialChunk(path[1], path[3], rawBody ?? Buffer.alloc(0), req.headers['x-chunk-sha256'] as string | undefined);
      }
      if (result === undefined) throw new Error('FIXED_LOCAL_ROUTE_REQUIRED'); send(status, result);
    } catch (error) { let code = error instanceof Error ? error.message : ''; if (!/^[A-Z][A-Z0-9_]+$/.test(code)) code = 'LOCAL_SOURCE_UNAVAILABLE'; send(/NOT_FOUND|NOT_AVAILABLE|FIXED_LOCAL_ROUTE/.test(code) ? 404 : code === 'LOCAL_SOURCE_UNAVAILABLE' ? 502 : 409, { code, message: code }); }
  }
}
