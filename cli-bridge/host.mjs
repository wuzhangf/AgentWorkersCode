#!/usr/bin/env node
/**
 * cli-bridge — thin worker-local/1 host driving a stock vendor CLI.
 *
 * One process = one engine = one backendStoreId. Tasks spawn the installed CLI
 * (`claude -p --output-format stream-json`, `codex exec --json`) in the bound
 * project directory; follow-ups resume the recorded CLI session; STOP kills
 * the child. Deliberately implements only the conversation slice — no budget,
 * collaboration, terminals, materials or pause/resume — matching what the
 * phone's native-task surface needs to create/observe/message/stop a task.
 *
 *   host.mjs serve --engine claude --state <dir> --port 4397 \
 *     --project <dir> --project-id <id>
 *
 * Token comes from WORKER_LOCAL_TOKEN (≥24 chars). Loopback only; every route
 * requires bearer auth; non-identity routes pin X-Worker-Local-Store.
 */
import http from 'node:http';
import { spawn } from 'node:child_process';
import { randomUUID, createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync, renameSync, existsSync, readdirSync, lstatSync, realpathSync } from 'node:fs';
import { join, resolve, relative, sep } from 'node:path';
import { execFileSync } from 'node:child_process';

const args = process.argv.slice(2);
const cmd = args[0];
const opt = {};
for (let i = 1; i < args.length; i += 2) opt[args[i].replace(/^--/, '')] = args[i + 1];

const ENGINE = opt.engine ?? '';
const STATE_DIR = opt.state ?? '';
const PORT = Number(opt.port ?? 0);
const PROJECT_DIR = opt.project ? resolve(opt.project) : '';
const PROJECT_ID = opt['project-id'] ?? 'workspace';
const PROJECT_NAME = opt['project-name'] ?? PROJECT_ID;
const BINARY = opt.executable;
const TOKEN = process.env.WORKER_LOCAL_TOKEN ?? '';
// Stock CLIs carry account-bound defaults that may not exist on the signed-in
// plan — an explicit --model overrides the vendor config for spawned turns.
const MODEL = opt.model ?? process.env.CLI_BRIDGE_MODEL ?? '';
const MAX_BODY = 1 << 20;
const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED']);

const fail = (code, status) => { const e = new Error(code); e.code = code; e.status = status; throw e; };

// ---- stock-CLI engine adapters -------------------------------------------------

const ENGINES = {
  claude: {
    argv: (sessionId) => [
      '--print', '--output-format', 'stream-json', '--verbose',
      '--permission-mode', 'default',
      ...(MODEL ? ['--model', MODEL] : []),
      ...(sessionId ? ['--resume', sessionId] : []),
    ],
    bin: 'claude',
    /** stream-json: init carries session_id; assistant rows carry text blocks;
     * result carries the terminal verdict. */
    onEvent: (row, task, addMessage) => {
      if (row.type === 'system' && row.subtype === 'init' && typeof row.session_id === 'string') {
        task.nativeSessionId = row.session_id;
      }
      if (row.type === 'assistant' && row.message && Array.isArray(row.message.content)) {
        const text = row.message.content
          .filter((b) => b && b.type === 'text' && typeof b.text === 'string')
          .map((b) => b.text).join('\n');
        if (text.trim()) addMessage('assistant', text);
      }
      if (row.type === 'result') {
        if (typeof row.session_id === 'string') task.nativeSessionId = row.session_id;
        if (typeof row.result === 'string' && row.result.trim()) task.output = row.result;
        return row.subtype === 'success' && row.is_error !== true ? 'SUCCEEDED' : 'FAILED';
      }
      return null;
    },
  },
  codex: {
    argv: (sessionId) => [
      '--ask-for-approval', 'never', 'exec', '--sandbox', 'workspace-write',
      '--json', '--skip-git-repo-check', ...(MODEL ? ['--model', MODEL] : []),
      ...(sessionId ? ['resume', sessionId] : []), '-',
    ],
    bin: 'codex',
    /** codex exec --json: thread.started carries thread_id; item.completed rows
     * carry agent_message text; turn.completed finishes the turn. */
    onEvent: (row, task, addMessage) => {
      if (row.type === 'thread.started' && typeof row.thread_id === 'string') {
        task.nativeSessionId = row.thread_id;
      }
      const item = row.item;
      if ((row.type === 'item.completed' || row.type === 'item.done') && item) {
        if ((item.type === 'agent_message' || item.type === 'assistant_message' || item.type === 'message')
            && typeof item.text === 'string' && item.text.trim()) {
          addMessage('assistant', item.text);
        }
      }
      if (row.type === 'turn.completed') return 'SUCCEEDED';
      if (row.type === 'turn.failed' || row.type === 'error') return 'FAILED';
      return null;
    },
  },
};

if (!ENGINES[ENGINE]) { console.error(`unknown --engine ${ENGINE}`); process.exit(2); }
if (!STATE_DIR || !PORT || !PROJECT_DIR) { console.error('serve requires --state --port --project'); process.exit(2); }
if (TOKEN.length < 24) { console.error('WORKER_LOCAL_TOKEN must be at least 24 chars'); process.exit(2); }
if (cmd !== 'serve') { console.error('usage: host.mjs serve ...'); process.exit(2); }

const engine = ENGINES[ENGINE];
const executable = BINARY ?? engine.bin;
const childEnvironment = { ...process.env };
delete childEnvironment.WORKER_LOCAL_TOKEN;
delete childEnvironment.CLI_BRIDGE_MODEL;
let engineVersion = 'unknown';
try { engineVersion = execFileSync(executable, ['--version'], { encoding: 'utf8', timeout: 8000, env: childEnvironment }).trim(); } catch { console.error(`${engine.bin} not on PATH`); process.exit(2); }

// ---- durable ledger ------------------------------------------------------------

mkdirSync(STATE_DIR, { recursive: true });
const LEDGER = join(STATE_DIR, 'ledger.json');
const ledger = existsSync(LEDGER)
  ? JSON.parse(readFileSync(LEDGER, 'utf8'))
  : { storeId: randomUUID(), tasks: {}, requests: {} };
if (!ledger.storeId) ledger.storeId = randomUUID();
if (!ledger.tasks) ledger.tasks = {};
if (!ledger.requests) ledger.requests = {};

const save = () => {
  const tmp = LEDGER + '.tmp';
  writeFileSync(tmp, JSON.stringify(ledger), { mode: 0o600 });
  renameSync(tmp, LEDGER);
};

// A task left RUNNING by a dead process is unrecoverable — mark it so the
// phone sees a terminal verdict instead of a frozen spinner.
for (const t of Object.values(ledger.tasks)) {
  if (!TERMINAL.has(t.state)) { t.state = 'FAILED'; t.reasonCode = 'HOST_RESTARTED_UNCONFIRMED'; t.quiescent = false; }
}
save();

const live = new Map(); // taskId -> ChildProcess
const now = () => new Date().toISOString();
const msgId = () => 'msg-' + randomUUID();

const task = (id) => Object.hasOwn(ledger.tasks, id) ? ledger.tasks[id] : fail('REQUEST_NOT_FOUND', 404);

const addMessage = (t, role, text) => {
  const seq = t.publicMessages.length;
  t.publicMessages.push({ role, text, messageId: msgId(), seq, occurredAt: now(), textComplete: true });
};

/** Spawn the stock CLI for one turn; stream its events into the task row. */
const dispatch = (t, instruction, requestKey) => {
  const child = spawn(executable, engine.argv(t.nativeSessionId), {
    cwd: PROJECT_DIR,
    env: childEnvironment,
    detached: process.platform !== 'win32',
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  const run = { child, stopKeys: [], timeout: undefined, spawnFailed: false };
  live.set(t.taskId, run);
  child.on('error', () => { run.spawnFailed = true; });
  child.stdin.on('error', () => { /* Early CLI exit is handled by close. */ });
  child.stdin.end(instruction);
  const firstMessage = t.publicMessages.length;
  t.output = '';
  t.pendingVerdict = undefined;
  t.artifactManifest = undefined;
  t.segmentId = randomUUID();
  t.state = 'RUNNING';
  t.launchState = 'RUNNING';
  t.quiescent = false;
  t.nativeTurnId = randomUUID();
  t.currentCommandId = requestKey;
  t.updatedAt = now();
  addMessage(t, 'user', instruction);
  child.stderr.resume();
  let buf = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (d) => {
    buf += d;
    let i;
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i); buf = buf.slice(i + 1);
      if (!line.trim()) continue;
      let row;
      try { row = JSON.parse(line); } catch { continue; }
      const verdict = engine.onEvent(row, t, (role, text) => addMessage(t, role, text));
      if (verdict) t.pendingVerdict = verdict;
      t.updatedAt = now();
    }
    if (buf.length > (1 << 20)) buf = buf.slice(-(1 << 20));
  });
  child.on('close', async (code) => {
    if (run.stopKeys.length || shuttingDown) signalRun(run, 'SIGKILL');
    clearTimeout(run.timeout);
    // The CLI can exit before descendants have released the process group.
    for (let attempt = 0; attempt < 30 && groupAlive(run); attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    t.quiescent = !groupAlive(run);
    t.updatedAt = now();
    if (!t.output) {
      t.output = t.publicMessages.slice(firstMessage)
        .filter((m) => m.role === 'assistant').map((m) => m.text).join('\n');
    }
    if (!t.quiescent) {
      t.state = 'FAILED'; t.reasonCode = 'PROCESS_EXIT_UNCONFIRMED';
    } else if (run.stopKeys.length || shuttingDown) {
      t.state = 'CANCELLED'; t.reasonCode = 'STOPPED_BY_OWNER';
    } else if (code === 0 && (t.pendingVerdict === 'SUCCEEDED'
        || (t.pendingVerdict !== 'FAILED' && t.output))) {
      t.state = 'SUCCEEDED'; t.reasonCode = '';
    } else {
      t.state = 'FAILED';
      t.reasonCode = run.spawnFailed ? 'ENGINE_SPAWN_FAILED'
        : t.pendingVerdict === 'FAILED' ? 'ENGINE_FAILED' : 'ENGINE_EXIT_' + code;
    }
    // Keep stderr on the host side. Vendor diagnostics can contain account data.
    t.pendingVerdict = undefined;
    for (const key of run.stopKeys) {
      const row = ledger.requests[key];
      row.state = t.quiescent ? 'APPLIED' : 'FAILED';
      row.result = { taskId: t.taskId, state: t.state, quiescent: t.quiescent };
      if (!t.quiescent) row.code = t.reasonCode;
    }
    live.delete(t.taskId);
    save();
    if (shuttingDown && live.size === 0) process.exit(0);
  });
  save();
};

function groupAlive({ child }) {
  if (!child.pid) return false;
  if (process.platform === 'win32') return child.exitCode === null && child.signalCode === null;
  try { process.kill(-child.pid, 0); return true; }
  catch (error) { return error.code !== 'ESRCH'; }
}

function signalRun({ child }, signal) {
  if (!child.pid) return;
  try {
    if (process.platform === 'win32') child.kill(signal);
    else process.kill(-child.pid, signal);
  } catch { /* The process may have already exited. */ }
}

function requestStop(run) {
  if (run.timeout) return;
  signalRun(run, 'SIGTERM');
  run.timeout = setTimeout(() => signalRun(run, 'SIGKILL'), 3000);
}

let shuttingDown = false;
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    shuttingDown = true;
    if (live.size === 0) process.exit(0);
    for (const run of live.values()) requestStop(run);
  });
}

function checkTarget(t, body) {
  for (const [bf, tf] of [['generation', 'generation'], ['nativeSessionId', 'nativeSessionId'],
                         ['nativeTurnId', 'nativeTurnId'], ['targetCommandId', 'currentCommandId']]) {
    if ((body[bf] ?? null) !== (t[tf] ?? null)) fail('NATIVE_TARGET_CHANGED', 409);
  }
}

const facts = (t) => ({
  schemaVersion: 'worker-local/1',
  taskId: t.taskId, projectId: t.projectId, engineId: t.engineId, engineVersion,
  profileId: 'USER_PROFILE', storeId: ledger.storeId,
  state: t.state, launchState: t.launchState ?? t.state, reasonCode: t.reasonCode ?? '',
  quiescent: t.quiescent === true,
  generation: t.generation, segmentId: t.segmentId,
  nativeSessionId: t.nativeSessionId ?? null, nativeTurnId: t.nativeTurnId ?? null,
  currentCommandId: t.currentCommandId ?? null,
  createdAt: t.createdAt, updatedAt: t.updatedAt,
  goal: t.instruction, displayName: t.instruction.split(/\r?\n/, 1)[0].slice(0, 128),
  instruction: t.instruction,
  output: t.output ?? '',
  result: t.output ?? '',
  capabilities: { stop: live.has(t.taskId), pause: false, resume: false,
                  steer: false, followUp: t.quiescent === true && !!t.nativeSessionId },
  configuredModel: 'vendor-cli',
});

const receipt = (r) => ({
  schemaVersion: 'worker-local-op/1',
  requestKey: r.requestKey, taskId: r.taskId ?? null, operation: r.operation ?? 'LAUNCH',
  ...(r.operation === 'NATIVE_FOLLOW_UP' || r.operation === 'STOP' ? { kind: r.operation } : {}),
  state: r.state, result: r.result ?? null, code: r.code ?? null,
});

const digest = (v) => createHash('sha256').update(JSON.stringify(v)).digest('hex');

// ---- artifacts --------------------------------------------------------------
// Mirrors the pi host's seal-on-quiescent contract without git: a bounded
// inventory snapshot at task creation is diffed against the current tree.
const ART_DIR = join(STATE_DIR, 'artifacts');
const SKIP_DIRS = new Set(['.git', 'node_modules', '.idea', '.vscode', 'target', 'dist', 'build', '__pycache__', '.next', '.cache', '.Trash']);
const SECRET_RE = /(?:sk-[A-Za-z0-9_-]{20,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:api[_-]?key|password|access[_-]?token)\s*[:=]\s*["']?[^\s"']{8,})/i;
const knownSecrets = Object.entries(process.env).filter(([k, v]) => /(?:KEY|PASSWORD|TOKEN|SECRET)$/.test(k) && v && v.length >= 8).map(([, v]) => v);

const inventory = () => {
  const root = realpathSync(PROJECT_DIR);
  const map = new Map();
  const walk = (dir, depth) => {
    if (depth > 6 || map.size > 2000) return;
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      const p = join(dir, e.name);
      const rel = relative(root, p);
      if (e.isDirectory()) { if (!SKIP_DIRS.has(e.name)) walk(p, depth + 1); continue; }
      if (!e.isFile()) continue;
      try { const s = lstatSync(p); map.set(rel, { s: s.size, m: s.mtimeMs }); } catch {}
    }
  };
  try { walk(root, 0); } catch {}
  return map;
};

const hashBytes = (b) => createHash('sha256').update(b).digest('hex');

const sealArtifacts = (t) => {
  if (t.artifactManifest) return t.artifactManifest;
  if (!t.quiescent) return { items: [], reasonCode: 'STOP_TO_SEAL_RESULTS' };
  const root = realpathSync(PROJECT_DIR);
  const current = inventory();
  const before = new Map((t.baseline ?? []).map((r) => [r.p, r]));
  const names = new Map();
  for (const [rel, cur] of current) {
    const old = before.get(rel);
    if (!old || old.s !== cur.s || Math.abs(old.m - cur.m) > 0.001) names.set(rel, old ? 'M' : 'A');
  }
  for (const rel of before.keys()) if (!current.has(rel)) names.set(rel, 'D');
  const items = []; let total = 0;
  for (const [rel, status] of [...names.entries()].sort().slice(0, 100)) {
    const row = { artifactId: 'file-' + hashBytes(rel + '\0' + t.segmentId), path: rel };
    items.push(row);
    if (/[\x00-\x1f\x7f]/.test(rel)) { row.status = 'WITHHELD_PATH'; continue; }
    if (status === 'D') { row.status = 'DELETED'; continue; }
    const candidate = resolve(root, rel);
    const relCheck = relative(root, candidate);
    if (relCheck === '..' || relCheck.startsWith('..' + sep) || relCheck === '') { row.status = 'WITHHELD_PATH'; continue; }
    try {
      const s = lstatSync(candidate);
      if (!s.isFile() || s.isSymbolicLink() || !realpathSync(candidate).startsWith(root + sep)) { row.status = 'WITHHELD_PATH'; continue; }
      if (s.size > 524288 || total + s.size > 4194304) { row.status = 'WITHHELD_SIZE'; continue; }
      const bytes = readFileSync(candidate);
      const after = lstatSync(candidate);
      if (after.mtimeMs !== s.mtimeMs || after.size !== s.size || after.ino !== s.ino) { row.status = 'WITHHELD_CHANGED'; continue; }
      const text = bytes.toString('utf8');
      if (knownSecrets.some((x) => text.includes(x)) || SECRET_RE.test(text)) { row.status = 'WITHHELD_SECRET'; continue; }
      total += bytes.length;
      row.status = 'AVAILABLE'; row.size = bytes.length; row.sha256 = hashBytes(bytes); row.mediaType = 'application/octet-stream';
      mkdirSync(join(ART_DIR, t.taskId), { recursive: true });
      writeFileSync(join(ART_DIR, t.taskId, row.artifactId), bytes);
    } catch { row.status = 'WITHHELD_UNREADABLE'; }
  }
  const manifest = { items, bounded: names.size > items.length, coverage: 'CHANGED_OWNED_FILES', segmentId: t.segmentId };
  t.artifactManifest = manifest; save(); return manifest;
};

const preflight = () => ({
  schemaVersion: 'worker-local-preflight/1',
  projectId: PROJECT_ID, engineId: ENGINE, engineVersion,
  optionsRevision: digest([PROJECT_ID, ENGINE, engineVersion, PROJECT_DIR, 'REGISTERED_TARGET_ONLY']),
  profileId: 'USER_PROFILE', executionProfile: 'vendor-cli-v1',
  loadsUserExtensions: true, headRevision: null, hasUncommittedChanges: null,
  workingCopyPolicy: 'REGISTERED_TARGET_ONLY',
  executionModes: ['WORKSPACE_TASK'], policyPreset: null,
});

const identity = () => ({
  schemaVersion: 'worker-host/1', storeId: ledger.storeId, storeGuard: 'HEADER_V1',
  bootId: BOOT_ID, currentAvailability: 'AVAILABLE',
  engine: { id: ENGINE, version: engineVersion },
  contracts: ['worker-host/1', 'worker-local/1'],
  supportedPaths: ['/local/v1', '/worker-host/v1/manifest'],
  capabilities: { items: [] },
});
const BOOT_ID = randomUUID();

// ---- HTTP ---------------------------------------------------------------------

const routes = (method, seg, taskId, body, query) => {
  // /local/v1/* and /worker-host/v1/* both arrive with the full path segments.
  if (seg[0] === 'worker-host' && seg[1] === 'v1') {
    if (method === 'GET' && seg[2] === 'manifest') return identity();
    if (method === 'GET' && seg[2] === 'capabilities') return { ...identity(), items: [] };
    fail('NOT_FOUND', 404);
  }
  if (seg[0] !== 'local' || seg[1] !== 'v1') fail('NOT_FOUND', 404);
  const rest = seg.slice(2);
  if (method === 'GET' && rest[0] === 'identity') return identity();
  if (method === 'GET' && rest[0] === 'projects' && rest.length === 1) {
    return { ...identity(), items: [{ projectId: PROJECT_ID, displayName: PROJECT_NAME,
      availability: 'AVAILABLE', headRevision: null, hasUncommittedChanges: null, kind: 'WORKSPACE' }] };
  }
  if (method === 'GET' && rest[0] === 'engines' && rest.length === 1) {
    return { ...identity(), items: [{ engineId: ENGINE, engineVersion, version: engineVersion,
      availability: 'AVAILABLE', profileId: 'USER_PROFILE',
      capabilities: { executionModes: ['WORKSPACE_TASK'], steer: false, followUp: true,
                      stop: true, pause: false, resume: false } }] };
  }
  if (method === 'GET' && rest[0] === 'tasks' && rest.length === 1) {
    return { ...identity(), items: Object.values(ledger.tasks).map((t) => ({
      taskId: t.taskId, projectId: t.projectId, engineId: t.engineId,
      engineVersion, state: t.state })) };
  }
  if (method === 'GET' && rest[0] === 'task-requests' && rest.length === 2) {
    const r = Object.hasOwn(ledger.requests, rest[1]) ? ledger.requests[rest[1]] : undefined;
    if (!r || r.operation !== 'LAUNCH') fail('REQUEST_NOT_FOUND', 404);
    return receipt(r);
  }
  if (method === 'POST' && rest[0] === 'task-preflights' && rest.length === 1) {
    if (!body || body.projectId !== PROJECT_ID || body.engineId !== ENGINE) fail('INVALID_SCOPE', 400);
    return preflight();
  }
  if (method === 'POST' && rest[0] === 'tasks' && rest.length === 1) {
    if (!body || typeof body.requestKey !== 'string' || body.projectId !== PROJECT_ID
        || body.engineId !== ENGINE || typeof body.instruction !== 'string' || !body.instruction.trim()) {
      fail('INVALID_REQUEST', 400);
    }
    if (body.optionsRevision !== undefined && body.optionsRevision !== preflight().optionsRevision) {
      fail('PREFLIGHT_CHANGED', 409);
    }
    const prior = ledger.requests[body.requestKey];
    if (prior) {
      if (prior.hash !== digest({ i: body.instruction, p: body.projectId, e: body.engineId })) {
        fail('IDEMPOTENCY_PAYLOAD_MISMATCH', 409);
      }
      return receipt(prior);
    }
    const t = {
      taskId: 'native-' + randomUUID(), requestKey: body.requestKey,
      projectId: PROJECT_ID, engineId: ENGINE,
      state: 'PREPARING', launchState: 'PREPARING', reasonCode: '', quiescent: false,
      generation: randomUUID(), segmentId: randomUUID(),
      nativeSessionId: null, nativeTurnId: null, currentCommandId: body.requestKey,
      createdAt: now(), updatedAt: now(),
      instruction: body.instruction, output: '', publicMessages: [], inputRequests: {},
      baseline: [...inventory()].map(([p, v]) => ({ p, s: v.s, m: v.m })),
    };
    ledger.tasks[t.taskId] = t;
    ledger.requests[body.requestKey] = {
      requestKey: body.requestKey, taskId: t.taskId, operation: 'LAUNCH',
      state: 'ACCEPTED', result: { taskId: t.taskId, engineId: ENGINE, engineVersion, state: 'ACCEPTED' },
      hash: digest({ i: body.instruction, p: body.projectId, e: body.engineId }),
    };
    save();
    dispatch(t, body.instruction, body.requestKey);
    return { ...receipt(ledger.requests[body.requestKey]), taskId: t.taskId, engineId: ENGINE, engineVersion, state: 'ACCEPTED' };
  }
  if (rest[0] === 'tasks' && rest.length >= 2) {
    const t = task(rest[1]);
    const sub = rest[2];
    if (method === 'GET' && rest.length === 2) return facts(t);
    if (method === 'GET' && rest.length === 3) {
      if (sub === 'output') {
        const text = t.output || t.publicMessages.filter((m) => m.role === 'assistant').map((m) => m.text).join('\n');
        return { text, coverage: 'BOUNDED_PUBLIC_MESSAGES' };
      }
      if (sub === 'messages') {
        const all = t.publicMessages.map((m, i) => ({ ...m, seq: i }));
        const num = (k) => (query.has(k) ? Number(query.get(k)) : null);
        const cursor = num('cursor'), before = num('before'), limit = num('limit') ?? 100;
        let items = all;
        if (cursor !== null) items = items.filter((m) => m.seq >= cursor);
        if (before !== null) items = items.filter((m) => m.seq < before).slice(-limit);
        else items = items.slice(0, limit);
        const nextSeq = items.length ? items[items.length - 1].seq + 1 : null;
        const hasMore = nextSeq !== null && all.length > nextSeq;
        return { messageSchemaVersion: 'native-messages/1', taskId: t.taskId, storeId: ledger.storeId,
          text: t.output || all.filter((m) => m.role === 'assistant').map((m) => m.text).join('\n'),
          turns: {}, items, coverage: 'BOUNDED_PUBLIC_MESSAGES', hasMore,
          cursor: hasMore ? String(nextSeq) : null };
      }
      if (sub === 'input-requests') return { items: Object.values(t.inputRequests ?? {}) };
      if (sub === 'artifacts') return sealArtifacts(t);
      if (sub === 'events') return { items: [], nextCursor: null };
      if (sub === 'metrics') return { cost: null, costCoverage: 'UNKNOWN', usageCoverage: 'NATIVE_TASK_TOTAL', configuredModel: 'vendor-cli' };
      fail('NOT_FOUND', 404);
    }
    if (method === 'GET' && rest.length === 4 && sub === 'artifacts') {
      const manifest = sealArtifacts(t);
      const row = (manifest.items ?? []).find((r) => r.artifactId === rest[3] && r.status === 'AVAILABLE');
      if (!row) fail('ARTIFACT_NOT_AVAILABLE', 404);
      const staged = join(ART_DIR, t.taskId, row.artifactId);
      if (!existsSync(staged)) fail('ARTIFACT_NOT_AVAILABLE', 404);
      const bytes = readFileSync(staged);
      if (hashBytes(bytes) !== row.sha256) fail('ARTIFACT_CHANGED', 409);
      return { artifactId: row.artifactId, path: row.path, sha256: row.sha256, encoding: 'base64', bytes: bytes.toString('base64') };
    }
    if (method === 'GET' && rest.length === 3 && sub === 'commands') fail('NOT_FOUND', 404);
    if (method === 'GET' && rest.length === 3 && sub === 'interaction-requests') fail('NOT_FOUND', 404);
    if (method === 'GET' && rest.length === 4 && sub === 'interaction-requests') {
      const r = Object.hasOwn(ledger.requests, rest[3]) ? ledger.requests[rest[3]] : undefined;
      if (!r || r.taskId !== t.taskId) fail('REQUEST_NOT_FOUND', 404);
      return receipt(r);
    }
    if (method === 'POST' && rest.length === 3 && sub === 'messages') {
      if (!body || typeof body.requestKey !== 'string') fail('INVALID_REQUEST', 400);
      if (body.kind !== 'NATIVE_FOLLOW_UP') fail('OPERATION_NOT_ALLOWED', 403);
      if (typeof body.content !== 'string' || !body.content.trim()) fail('INVALID_REQUEST', 400);
      const prior = ledger.requests[body.requestKey];
      if (prior) {
        if (prior.taskId !== t.taskId || prior.hash !== digest({ c: body.content, k: body.kind })) {
          fail('IDEMPOTENCY_PAYLOAD_MISMATCH', 409);
        }
        return receipt(prior);
      }
      checkTarget(t, body);
      if (live.has(t.taskId)) fail('TASK_BUSY', 409);
      if (!t.quiescent || !t.nativeSessionId) fail('NATIVE_SESSION_UNAVAILABLE', 409);
      ledger.requests[body.requestKey] = {
        requestKey: body.requestKey, taskId: t.taskId, operation: body.kind,
        state: 'DISPATCHING', hash: digest({ c: body.content, k: body.kind }),
      };
      save();
      dispatch(t, body.content, body.requestKey);
      return receipt(ledger.requests[body.requestKey]);
    }
    if (method === 'POST' && rest.length === 3 && sub === 'commands') {
      if (!body || typeof body.requestKey !== 'string' || body.intent !== 'STOP') {
        fail(body && body.intent ? 'NATIVE_ACTION_UNSUPPORTED' : 'INVALID_REQUEST',
             body && body.intent ? 409 : 400);
      }
      const prior = ledger.requests[body.requestKey];
      if (prior) {
        if (prior.taskId !== t.taskId || prior.hash !== 'stop') fail('IDEMPOTENCY_PAYLOAD_MISMATCH', 409);
        return receipt(prior);
      }
      checkTarget(t, body);
      const run = live.get(t.taskId);
      if (!run) fail('NATIVE_ACTION_UNSUPPORTED', 409);
      ledger.requests[body.requestKey] = {
        requestKey: body.requestKey, taskId: t.taskId, operation: 'STOP',
        state: 'DISPATCHING', hash: 'stop',
      };
      run.stopKeys.push(body.requestKey);
      requestStop(run);
      save();
      return receipt(ledger.requests[body.requestKey]);
    }
    if (method === 'POST' && rest.length === 5 && sub === 'input-requests' && rest[4] === 'responses') {
      fail('REQUEST_NOT_FOUND', 404);
    }
  }
  fail('NOT_FOUND', 404);
};

http.createServer((req, res) => {
  const send = (status, obj) => {
    const data = JSON.stringify(obj);
    res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) });
    res.end(data);
  };
  (async () => {
    if (req.headers.authorization !== 'Bearer ' + TOKEN) return send(401, { code: 'LOCAL_AUTH_REQUIRED' });
    const url = new URL(req.url, 'http://127.0.0.1');
    const seg = url.pathname.split('/').filter(Boolean);
    const isIdentity = seg.join('/') === 'local/v1/identity';
    if (!isIdentity) {
      const store = req.headers['x-worker-local-store'];
      if (store !== ledger.storeId && seg[0] === 'local') return send(409, { code: 'LOCAL_SOURCE_IDENTITY_CHANGED' });
    }
    let body;
    if (req.method === 'POST') {
      let raw = '';
      for await (const c of req) { raw += c; if (raw.length > MAX_BODY) return send(413, { code: 'REQUEST_TOO_LARGE' }); }
      try { body = raw ? JSON.parse(raw) : {}; } catch { return send(422, { code: 'INVALID_REQUEST' }); }
      if (shuttingDown) return send(503, { code: 'HOST_STOPPING' });
      if (body && 'requestKey' in Object(body)
          && (typeof body.requestKey !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(body.requestKey)
              || Object.hasOwn(Object.prototype, body.requestKey))) return send(400, { code: 'INVALID_REQUEST' });
      const key = req.headers['idempotency-key'];
      if (key !== undefined && body.requestKey !== undefined && key !== body.requestKey) {
        return send(400, { code: 'IDEMPOTENCY_KEY_MISMATCH' });
      }
    }
    const result = routes(req.method, seg, null, body, url.searchParams);
    send(200, result);
  })().catch((e) => {
    send(e.status ?? 500, { code: e.code ?? 'LOCAL_SOURCE_UNAVAILABLE' });
  });
}).listen(PORT, '127.0.0.1', () => {
  console.log(`cli-bridge ${ENGINE} store=${ledger.storeId} on 127.0.0.1:${PORT} project=${PROJECT_DIR}`);
});
