import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync, spawn } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { canonical, hash } from '../dist/host/store.js';

const token = 'deterministic-test-token-not-a-real-secret';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn) { for (let n = 0; n < 200; n++) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup() { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); assert.equal(result.status, 202); return { request: body, taskId: result.body.taskId, receipt: result.body }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
function confirmGolden(vectors) { assert.equal(vectors.schemaVersion, 'worker-host-golden/1'); assert.ok(vectors.items.length > 0); const ids = new Set(); for (const vector of vectors.items) { assert.ok(!ids.has(vector.id), `duplicate golden vector id: ${vector.id}`); ids.add(vector.id); assert.equal(canonical(vector.input), vector.canonical, vector.id); assert.equal(hash(canonical(vector.input)), vector.sha256, vector.id); } }

test('canonical Unicode code point order, integer-only values', () => { assert.equal(canonical({ '\ue000': 1, '𐀀': 2, '10': 'ten', '2': 'two' }), '{"10":"ten","2":"two","":1,"𐀀":2}'); assert.throws(() => canonical({ value: 1.1 })); });
test('shared EDC canonical golden vectors', () => { const vectors = JSON.parse(readFileSync(new URL('../../../contracts/worker-host/v1/golden-vectors.json', import.meta.url), 'utf8')); confirmGolden(vectors); });
test('project aliases cannot rebind and execution is UNCONFIGURED while ledger reads remain available', async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-config-')); const p = project(); const other = project();
  const names = ['MODEL_DEFAULT_MODEL', 'MODEL_BASE_URL', 'MODEL_API_KEY', 'LLM_HEADER_x_opencode_session']; const saved = Object.fromEntries(names.map(k => [k, process.env[k]])); for (const key of names) delete process.env[key];
  const host = new PiHost({ state, token }); const call = client(await host.listen());
  try {
    host.register('bound', p); host.register('bound', p); assert.throws(() => host.register('bound', other), /PROJECT_ALIAS_ALREADY_BOUND/); assert.equal(host.project('bound').path, realpathSync(p));
    const manifest = (await call('/worker-host/v1/manifest')).body; assert.equal(manifest.currentAvailability, 'UNCONFIGURED'); assert.equal(manifest.engine.provenance.nodeExecutableSha256, hash(readFileSync(process.execPath)));
    assert.equal(manifest.engine.provenance.sdkEntrySha256, hash(readFileSync(fileURLToPath(import.meta.resolve('@earendil-works/pi-coding-agent')))));
    assert.equal((await call('/engines')).body.items[0].available, false); assert.equal(manifest.capabilities.find(c => c.id === 'P09').availability, 'UNCONFIGURED'); assert.equal(manifest.capabilities.find(c => c.id === 'P05').availability, 'AVAILABLE');
    const pf = (await call('/task-preflights', { engineId: 'pi', projectId: 'bound' })).body; assert.equal(pf.executableDigest, manifest.engine.engineBindingDigest);
    const denied = await call('/tasks', { requestKey: 'unconfigured', projectId: 'bound', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'must not dispatch' }); assert.equal(denied.body.code, 'MODEL_BINDING_UNCONFIGURED'); assert.equal(host.store.tasks().length, 0);
    process.env.MODEL_DEFAULT_MODEL = 'explicit-model'; process.env.MODEL_BASE_URL = 'https://opencode.ai/zen/go/v1'; process.env.MODEL_API_KEY = 'benign-sentinel-not-credential'; assert.equal(host.manifest().reasonCode, 'MODEL_HEADER_UNCONFIGURED');
    delete process.env.MODEL_API_KEY; assert.equal(host.manifest().reasonCode, 'MODEL_AUTH_UNAVAILABLE'); assert.equal((await call('/identity')).body.storeId, host.store.identity.storeId);
  } finally { await host.close(); for (const key of names) if (saved[key] === undefined) delete process.env[key]; else process.env[key] = saved[key]; }
});
test('compatible API durable same-key replay, followup, stop, sealed byte artifacts, restart', async () => {
  const s = await setup(); try {
    const identity = (await s.call('/identity')).body; assert.equal(identity.storeGuard, 'HEADER_V1');
    assert.equal((await s.call('/identity', undefined, { Authorization: 'wrong' })).status, 401);
    assert.equal((await s.call('/identity', undefined, { 'X-Worker-Local-Store': 'wrong' })).body.code, 'STORE_IDENTITY_CHANGED');
    assert.equal((await s.call('/worker-host/v1/manifest')).body.schemaVersion, 'worker-host/1');
    const c = await create(s.call); let d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal(c.receipt.engineVersion, '1.4.0'); assert.equal(d.nativeTurnId, null);
    assert.equal((await s.call('/tasks', c.request)).body.taskId, c.taskId);
    assert.equal((await s.call('/tasks', { ...c.request, instruction: 'different' })).body.code, 'IDEMPOTENCY_CONFLICT');
    assert.equal((await s.call(`/tasks/${c.taskId}/artifacts`)).body.reasonCode, 'STOP_TO_SEAL_RESULTS');
    const f = { requestKey: 'follow', kind: 'NATIVE_FOLLOW_UP', content: 'second', ...guard(d) };
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, f)).status, 202);
    d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d.currentCommandId === 'follow' && d; });
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, f)).body.requestKey, 'follow');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { ...f, requestKey: 'old', targetCommandId: c.request.requestKey })).body.code, 'NATIVE_TARGET_CHANGED');
    const stop = { requestKey: 'stop', intent: 'STOP', ...guard(d) };
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, stop)).body.state, 'APPLIED');
    const unpublishedId = 'file-' + hash('result.txt\0' + s.host.store.task(c.taskId).segmentId);
    s.host.store.db.prepare('INSERT INTO artifacts VALUES (?,?,?)').run(c.taskId, unpublishedId, Buffer.from('unpublished orphan bytes'));
    const index = (await s.call(`/tasks/${c.taskId}/artifacts`)).body; const row = index.items.find(r => r.path === 'result.txt'); assert.equal(row.status, 'AVAILABLE');
    const bytes = Buffer.from((await s.call(`/tasks/${c.taskId}/artifacts/${row.artifactId}`)).body.bytes, 'base64'); assert.equal(bytes.toString(), 'second'); assert.equal(hash(bytes), row.sha256);
    const events = (await s.call(`/tasks/${c.taskId}/events?limit=100`)).body; assert(events.items.some(e => e.kind === 'INPUT_CONSUMED')); assert(events.items.some(e => e.kind === 'STOP_APPLIED'));
    assert.equal((await s.call(`/tasks/${c.taskId}/metrics`)).body['usage.input'], '6');
    await s.host.close();
    const restarted = new PiHost({ state: s.state, token, childModule: fixture }); const call = client(await restarted.listen());
    assert.equal((await call('/identity')).body.storeId, identity.storeId); assert.notEqual((await call('/identity')).body.bootId, identity.bootId);
    assert.equal((await call('/tasks', c.request)).body.taskId, c.taskId); assert.equal((await call(`/tasks/${c.taskId}/output`)).body.text.includes('second'), true);
    assert.equal(readFileSync(join(d.workspace, 'dispatch-count.txt'), 'utf8').trim().split('\n').length, 2); await restarted.close();
  } finally { await s.host.close(); }
});
test('pending questions bind original generation and selection; steer has consumption evidence', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'question'); const q = await wait(async () => (await s.call(`/tasks/${c.taskId}/input-requests`)).body.items[0]);
    const d = (await s.call(`/tasks/${c.taskId}`)).body;
    const answer = { requestKey: 'answer', response: { value: 'yes' }, ...guard(d) };
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, { ...answer, generation: 'wrong' })).body.code, 'NATIVE_TARGET_CHANGED');
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, { ...answer, response: { value: 'invalid' } })).body.code, 'INVALID_NATIVE_RESPONSE');
    assert.equal(q.method, 'select'); assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, answer)).status, 202);
    await wait(async () => (await s.call(`/tasks/${c.taskId}`)).body.state === 'SUCCEEDED');
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, answer)).body.requestKey, 'answer');
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, { ...answer, requestKey: 'late' })).body.code, 'NATIVE_INPUT_EXPIRED');
    let idle = (await s.call(`/tasks/${c.taskId}`)).body;
    await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'waiting-again', kind: 'NATIVE_FOLLOW_UP', content: 'waiting', ...guard(idle) });
    const running = await wait(async () => { const d = (await s.call(`/tasks/${c.taskId}`)).body; return d.state === 'RUNNING' && d; });
    await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'same-text-steer', kind: 'NATIVE_STEER', content: 'waiting', ...guard(running) });
    await wait(async () => (await s.call(`/tasks/${c.taskId}/interaction-requests/same-text-steer`)).body.state === 'ACKNOWLEDGED');
    assert.equal((await s.call(`/tasks/${c.taskId}/interaction-requests/same-text-steer`)).body.consumedAt, undefined, 'equal text is not native consumption proof');
  } finally { await s.host.close(); }
});
test('STOP accepted before dispatch durably closes intake', async () => {
  const s = await setup(); try { const pf = s.host.preflight({ projectId: 'demo', engineId: 'pi' }); const c = s.host.create({ requestKey: 'early', engineId: 'pi', projectId: 'demo', optionsRevision: pf.optionsRevision, instruction: 'waiting' }); const d = s.host.store.task(c.taskId); const stopped = await s.host.stop(c.taskId, { requestKey: 'early-stop', intent: 'STOP', ...guard(d) }); assert.equal(stopped.state, 'APPLIED'); await new Promise(r => setTimeout(r, 100)); assert.equal(s.host.store.task(c.taskId).state, 'CANCELLED'); assert.equal(s.host.store.task(c.taskId).pid, undefined); } finally { await s.host.close(); }
});
test('source seal requires main-task quiescence: RUNNING refuses, terminal or stop-proof seals', async () => {
  const s = await setup(); try {
    // A RUNNING owner still has turn/tool writes in flight — the fence refuses honestly.
    const c = await create(s.call, 'waiting-never');
    const d = await wait(async () => { const v = (await s.call('/tasks/' + c.taskId)).body; return v.state === 'RUNNING' && v.nativeSessionId && v; });
    const refused = await s.call(`/tasks/${c.taskId}/source-seals`, { requestKey: 'seal-running', target: 'HANDOFF' });
    assert.equal(refused.status, 409); assert.equal(refused.body.code, 'TASK_NOT_QUIESCENT');
    let t = s.host.store.task(c.taskId);
    assert.equal(t.sealedFor, undefined); assert.equal(t.sealRequested, undefined, 'rejection leaves no half-landed fence');
    assert.equal(Object.keys(t.sourceSeals ?? {}).length, 0);
    // Reads are never gated: the seal list and the capability projection stay readable.
    const openList = await s.call(`/tasks/${c.taskId}/source-seals`);
    assert.equal(openList.status, 200); assert.equal(openList.body.items.length, 0);
    const caps = (await s.call(`/tasks/${c.taskId}`)).body.sourceCapabilities.actions['handoff.seal'];
    assert.equal(caps.state, 'BLOCKED_BY_STATE'); assert.equal(caps.reasonCode, 'TASK_NOT_QUIESCENT');
    // A proven stop lands the durable quiescence receipt; the seal then applies.
    const stop = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'seal-stop', intent: 'STOP', ...guard(d) });
    assert.equal(stop.body.state, 'APPLIED');
    t = s.host.store.task(c.taskId);
    assert.equal(t.state, 'CANCELLED'); assert.equal(t.quiescent, true); assert.ok(t.stopProof?.method, 'recorded quiescence proof');
    const sealed = await s.call(`/tasks/${c.taskId}/source-seals`, { requestKey: 'seal-stopped', target: 'HANDOFF' });
    assert.equal(sealed.status, 202, JSON.stringify(sealed.body));
    assert.equal(s.host.store.task(c.taskId).sealedFor.sealId, sealed.body.sealId);
    assert.equal((await s.call(`/tasks/${c.taskId}/source-seals/${sealed.body.sealId}`)).body.sealId, sealed.body.sealId);
    // A terminal SUCCEEDED task seals while children still run — the drain is recorded.
    const c2 = await create(s.call, 'seal-drain');
    await wait(async () => { const v = (await s.call('/tasks/' + c2.taskId)).body; return v.state === 'SUCCEEDED' && v; });
    const made = await s.call(`/tasks/${c2.taskId}/collaborations`, { requestKey: 'cb-drain', children: [{ childId: 'hang', goal: 'child-hang' }] });
    const cid = made.body.collaborationId;
    await wait(async () => s.host.store.task(c2.taskId).collaborations[cid]?.children.hang.state === 'RUNNING');
    const aux = await s.call(`/tasks/${c2.taskId}/auxiliary-sessions`, { requestKey: 'ax-drain', question: 'aux-hang now' });
    await wait(async () => s.host.store.task(c2.taskId).auxSessions[aux.body.auxSessionId]?.state === 'RUNNING');
    const sealed2 = await s.call(`/tasks/${c2.taskId}/source-seals`, { requestKey: 'seal-succeeded', target: 'HANDOFF' });
    assert.equal(sealed2.status, 202, JSON.stringify(sealed2.body));
    const t2 = s.host.store.task(c2.taskId);
    assert.equal(t2.sealedFor.sealId, sealed2.body.sealId);
    assert.equal(t2.collaborations[cid].children.hang.state, 'CANCELLED');
    assert.equal(t2.collaborations[cid].children.hang.reasonCode, 'SOURCE_SEALED');
    assert.equal(t2.auxSessions[aux.body.auxSessionId].state, 'CANCELLED');
    assert.equal(sealed2.body.affectedChildren.collabChildren[0].state, 'CANCELLED');
    assert.equal(sealed2.body.affectedChildren.collabChildren[0].reason, 'SOURCE_SEALED');
    assert.equal(sealed2.body.affectedChildren.aux[0].state, 'CANCELLED');
  } finally { await s.host.close(); }
});
test('source seal refuses a queued task: admission-quiescent is still dispatchable', async () => {
  // QUEUED rows are quiescent only for admission accounting — admitQueue can
  // still launch them, so sealing one would fence nothing. STOP removes the
  // task from the queue (CANCELLED), then the seal applies.
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-sealq-')); const p = project();
  const host = new PiHost({ state, token, childModule: fixture, maxActiveTasks: 1 }); host.register('demo', p);
  try {
    const pf = host.preflight({ projectId: 'demo', engineId: 'pi' });
    const busy = host.create({ requestKey: 'q-busy', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'waiting-never' });
    const queued = host.create({ requestKey: 'q-queued', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello' });
    const qt = host.store.task(queued.taskId);
    assert.equal(qt.state, 'QUEUED'); assert.equal(qt.launchState, 'QUEUED'); assert.equal(qt.quiescent, true);
    await assert.rejects(() => host.sourceSeal(queued.taskId, { requestKey: 'q-seal', target: 'HANDOFF' }), /TASK_NOT_QUIESCENT/);
    assert.equal(host.store.task(queued.taskId).sealRequested, undefined);
    await host.stop(queued.taskId, { requestKey: 'q-stop', intent: 'STOP', ...guard(qt) });
    const stopped = host.store.task(queued.taskId); assert.equal(stopped.state, 'CANCELLED'); assert.equal(stopped.launchState, 'REJECTED');
    const seal = await host.sourceSeal(queued.taskId, { requestKey: 'q-seal-2', target: 'HANDOFF' });
    assert.equal(seal.state, 'SEALED'); assert.equal(host.store.task(queued.taskId).sealedFor.sealId, seal.sealId);
    await wait(() => { const v = host.store.task(busy.taskId); return v.state === 'RUNNING' && v; });
  } finally { await host.close(); }
});
test('create deadlineMs is optional, bounded and keeps the exact-field whitelist', async () => {
  const s = await setup(); try {
    const pf = (await s.call('/task-preflights', { projectId: 'demo', engineId: 'pi' })).body;
    const mk = extra => ({ requestKey: 'dl-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello', ...extra });
    for (const deadlineMs of [59999, 86400001, 'abc', 1.5, -1, true, null, {}]) assert.equal((await s.call('/tasks', mk({ deadlineMs }))).body.code, 'INVALID_DEADLINE_MS', JSON.stringify(deadlineMs));
    assert.equal((await s.call('/tasks', mk({ bogus: 1 }))).body.code, 'UNEXPECTED_FIELD');
    assert.equal(s.host.store.tasks().length, 0, 'rejected admissions create no task');
    const bounded = await s.call('/tasks', mk({ deadlineMs: 120000 })); assert.equal(bounded.status, 202);
    const task = s.host.store.task(bounded.body.taskId);
    assert.equal(task.deadlineMs, 120000);
    assert.ok(Math.abs(Number(task.deadlineEpochMillis) - (Date.now() + 120000)) < 30000, 'epoch is create-time + task window');
    const asString = await s.call('/tasks', mk({ deadlineMs: '180000' })); assert.equal(asString.status, 202);
    assert.equal(s.host.store.task(asString.body.taskId).deadlineMs, 180000, 'contract decimal strings accepted like budget caps');
    const detail = (await s.call('/tasks/' + bounded.body.taskId)).body;
    assert.equal(detail.deadlineMs, 120000); assert.equal(detail.deadlineEpochMillis, task.deadlineEpochMillis);
    await wait(async () => { const d = (await s.call('/tasks/' + bounded.body.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    await wait(async () => { const d = (await s.call('/tasks/' + asString.body.taskId)).body; return d.state === 'SUCCEEDED' && d; });
  } finally { await s.host.close(); }
});
test('host taskDeadlineMs option applies at admission and per-task deadlineMs wins', async () => {
  const s = await setup(); try { const c = await create(s.call); assert.equal(s.host.store.task(c.taskId).deadlineMs, 600000, 'host default preserves the historical window'); } finally { await s.host.close(); }
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-deadline-')); const p = project();
  const host = new PiHost({ state, token, childModule: fixture, taskDeadlineMs: 120000 }); host.register('demo', p);
  try {
    const pf = host.preflight({ projectId: 'demo', engineId: 'pi' });
    const c = host.create({ requestKey: 'd1', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello' });
    const task = host.store.task(c.taskId); assert.equal(task.deadlineMs, 120000);
    assert.ok(Math.abs(Number(task.deadlineEpochMillis) - (Date.now() + 120000)) < 30000);
    const c2 = host.create({ requestKey: 'd2', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello', deadlineMs: 300000 });
    assert.equal(host.store.task(c2.taskId).deadlineMs, 300000, 'task-level value wins over the host option');
    assert.throws(() => host.create({ requestKey: 'd3', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello', deadlineMs: 30 }), /INVALID_DEADLINE_MS/);
    assert.equal(host.store.tasks().length, 2, 'rejected admissions create no task');
    // Let the queued setImmediate launches settle so no launch touches a closed store.
    for (const id of [c.taskId, c2.taskId]) await wait(() => { const t = host.store.task(id); return t.state === 'SUCCEEDED' && t; });
  } finally { await host.close(); }
  for (const bad of [1000, 1.5, 'abc', 86400001]) assert.throws(() => new PiHost({ state: mkdtempSync(join(tmpdir(), 'nwf-pi-baddeadline-')), token, taskDeadlineMs: bad }), /INVALID_TASK_DEADLINE_MS/, JSON.stringify(bad));
});
test('serve --task-deadline-ms reaches task creation over real CLI plumbing', async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-cli-')); const p = project();
  const main = fileURLToPath(new URL('../dist/host/main.js', import.meta.url));
  const env = { ...process.env, PI_TEST_WORKER_TOKEN: token, MODEL_DEFAULT_MODEL: 'fixture-model', MODEL_BASE_URL: 'http://127.0.0.1:1', MODEL_API_KEY: 'cli-test-key-not-a-secret' };
  execFileSync(process.execPath, [main, 'init', '--state', state, '--project-id', 'demo', '--project', p], { env });
  const child = spawn(process.execPath, [main, 'serve', '--state', state, '--token-env', 'PI_TEST_WORKER_TOKEN', '--task-deadline-ms', '240000', '--port', '0'], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    const port = await new Promise((resolve, reject) => { let buf = ''; const timer = setTimeout(() => reject(new Error('READY_TIMEOUT')), 15000); child.stdout.on('data', d => { buf += d; const m = buf.match(/127\.0\.0\.1:(\d+)/); if (m) { clearTimeout(timer); resolve(Number(m[1])); } }); child.once('exit', () => reject(new Error('SERVE_EXITED'))); });
    const call = client(port);
    const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'pi' })).body;
    const c = await call('/tasks', { requestKey: 'cli-1', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello' });
    assert.equal(c.status, 202);
    assert.equal((await call('/tasks/' + c.body.taskId)).body.deadlineMs, 240000, 'serve-level option stamps the task window');
    const c2 = await call('/tasks', { requestKey: 'cli-2', projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction: 'hello', deadlineMs: 90000 });
    assert.equal(c2.status, 202); assert.equal((await call('/tasks/' + c2.body.taskId)).body.deadlineMs, 90000);
  } finally { child.kill('SIGTERM'); await Promise.race([new Promise(d => child.once('exit', d)), new Promise(d => setTimeout(d, 5000))]); }
});
test('recorded task deadline drives sweepTaskDeadlines, not a hard-coded window', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting-never', { deadlineMs: 60000 });
    await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'RUNNING' && d; });
    await s.host.sweepTaskDeadlines(Date.now() + 30000);
    assert.equal(s.host.store.task(c.taskId).state, 'RUNNING', 'a future check before the recorded epoch does not kill');
    await s.host.sweepTaskDeadlines(Date.now() + 120000);
    assert.equal(s.host.store.task(c.taskId).state, 'CANCELLED', 'a check past the recorded epoch stops the task');
    const t = s.host.store.task(c.taskId); assert.ok(t.endedAt); assert.equal(t.completedAt, undefined);
  } finally { await s.host.close(); }
});
test('detail timing fields are honest recorded facts only', async () => {
  const s = await setup(); try {
    const c = await create(s.call);
    const created = (await s.call('/tasks/' + c.taskId)).body;
    const d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.ok(Date.parse(d.runStartedAt), 'first dispatched/observed turn stamped');
    assert.ok(Date.parse(d.endedAt) && Date.parse(d.completedAt), 'terminal observations stamped');
    assert.equal(d.endedAt, d.completedAt, 'one recorded terminal observation');
    assert.ok(Date.parse(d.runStartedAt) >= Date.parse(created.createdAt) - 2000 && Date.parse(d.endedAt) >= Date.parse(d.runStartedAt));
    // A follow-up opens a new episode: the stale end time clears until the next terminal fact.
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'fu-timing', kind: 'NATIVE_FOLLOW_UP', content: 'waiting', ...guard(d) })).status, 202);
    const running = await wait(async () => { const x = (await s.call('/tasks/' + c.taskId)).body; return x.state === 'RUNNING' && x.currentCommandId === 'fu-timing' && x; });
    assert.equal(running.endedAt, undefined); assert.equal(running.completedAt, undefined);
    // A later terminal observation stamps a fresh end time for the new episode.
    const stop = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'timing-stop', intent: 'STOP', ...guard(running) });
    assert.equal(stop.body.state, 'APPLIED');
    const d2 = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(d2.state, 'CANCELLED');
    assert.ok(Date.parse(d2.endedAt) >= Date.parse(d.endedAt), 'the new episode records its own terminal observation');
    assert.equal(d2.completedAt, undefined, 'a cancelled episode records no completion');
    assert.equal(d2.runStartedAt, d.runStartedAt, 'run start is first-write-wins across episodes');
    // A native failure records an end observation but never a completion.
    const f = await create(s.call, 'fail-model');
    const fd = await wait(async () => { const x = (await s.call('/tasks/' + f.taskId)).body; return x.state === 'FAILED' && x; });
    assert.ok(Date.parse(fd.runStartedAt)); assert.ok(Date.parse(fd.endedAt)); assert.equal(fd.completedAt, undefined, 'no fabricated completion');
    // A task cancelled before any turn keeps no run start and no completion.
    const pf = s.host.preflight({ projectId: 'demo', engineId: 'pi' });
    const early = s.host.create({ requestKey: 'early-timing', engineId: 'pi', projectId: 'demo', optionsRevision: pf.optionsRevision, instruction: 'waiting' });
    const d0 = s.host.store.task(early.taskId);
    await s.host.stop(early.taskId, { requestKey: 'early-timing-stop', intent: 'STOP', ...guard(d0) });
    await new Promise(r => setTimeout(r, 100));
    const t = s.host.store.task(early.taskId);
    assert.equal(t.state, 'CANCELLED'); assert.ok(t.endedAt, 'the cancellation observation is real');
    assert.equal(t.runStartedAt, undefined); assert.equal(t.completedAt, undefined);
  } finally { await s.host.close(); }
});
test('owned Windows Job stops actual descendant writer and proves ActiveProcesses zero', { skip: process.platform !== 'win32' }, async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'descendant'); const d = await wait(async () => { const d = (await s.call(`/tasks/${c.taskId}`)).body; return d.state === 'SUCCEEDED' && d; });
    await wait(() => { try { return readFileSync(join(d.workspace, 'heartbeat.txt')).length > 0; } catch { return false; } });
    const op = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-tree', intent: 'STOP', ...guard(d) }); assert.equal(op.body.state, 'APPLIED');
    const proof = (await s.call(`/tasks/${c.taskId}`)).body.stopProof; assert.equal(proof.method, 'WINDOWS_JOB_ACTIVE_ZERO');
    const size = readFileSync(join(d.workspace, 'heartbeat.txt')).length; await new Promise(r => setTimeout(r, 100)); assert.equal(readFileSync(join(d.workspace, 'heartbeat.txt')).length, size);
  } finally { await s.host.close(); }
});
test('SQLite owner exclusion and DISPATCHING process crash never reissues original intent', async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-crash-')); const p = project();
  const child = spawn(process.execPath, [fileURLToPath(new URL('./crash-host.mjs', import.meta.url)), state, p, token], { stdio: ['ignore', 'pipe', 'pipe'] });
  const port = await new Promise(resolve => child.stdout.once('data', data => resolve(Number(String(data).trim())))); const call = client(port);
  const c = await create(call); await new Promise(resolve => child.once('exit', resolve));
  const restarted = new PiHost({ state, token, childModule: fixture }); try {
    assert.throws(() => new PiHost({ state, token, childModule: fixture }), /LOCAL_HOST_ALREADY_OWNS_STATE/);
    assert.equal(restarted.store.operation(c.request.requestKey).state, 'DISPATCHING'); assert.equal(restarted.store.task(c.taskId).state, 'UNKNOWN');
    assert.equal(restarted.create(c.request).taskId, c.taskId); await new Promise(r => setTimeout(r, 100)); assert.equal(restarted.store.task(c.taskId).reasonCode, 'RESTART_RECONCILIATION_REQUIRED');
  } finally { await restarted.close(); }
});

for (const escaped of [false, true]) test(`macOS real ${escaped ? 'setsid escape' : 'descendant'} vetoes quiescence even after group cleanup`, { skip: process.platform !== 'darwin' }, async () => {
  const s = await setup(); let pid;
  try {
    const c = await create(s.call, escaped ? 'escaped-descendant' : 'descendant');
    const d = await wait(async () => { const d = (await s.call(`/tasks/${c.taskId}`)).body; return d.state === 'SUCCEEDED' && d; });
    pid = Number(readFileSync(join(d.workspace, 'descendant-pid.txt'), 'utf8'));
    await wait(() => { try { return readFileSync(join(d.workspace, 'heartbeat.txt')).length > 0; } catch { return false; } });
    const stop = (await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'mac-stop-tree', intent: 'STOP', ...guard(d) })).body;
    assert.equal(stop.state, 'UNKNOWN');
    const stopped = (await s.call(`/tasks/${c.taskId}`)).body;
    assert.equal(stopped.state, 'UNKNOWN'); assert.equal(stopped.quiescent, false);
    assert.equal(stopped.processOwnership.forkObserved, true); assert.equal(stopped.completedSessionSeal, undefined);
    assert.equal(stopped.capabilities.completedSessionFollowUpAfterRestart, false);
    assert.equal((await s.call(`/tasks/${c.taskId}/artifacts`)).body.reasonCode, 'STOP_TO_SEAL_RESULTS');
    const size = readFileSync(join(d.workspace, 'heartbeat.txt')).length;
    await new Promise(r => setTimeout(r, 120));
    const later = readFileSync(join(d.workspace, 'heartbeat.txt')).length;
    if (escaped) assert(later > size, 'setsid writer really escapes the PGID; UNKNOWN is required');
    else assert.equal(later, size, 'ordinary descendant writer really stops');
  } finally { if (pid) try { process.kill(pid, 'SIGKILL'); } catch {} await s.host.close(); }
});

test('macOS host SIGKILL guardian stops a real writer while restart keeps uncertain dispatch UNKNOWN', { skip: process.platform !== 'darwin' }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-mac-parent-')); const p = project(); let descendantPid; let restarted;
  const parent = spawn(process.execPath, [fileURLToPath(new URL('./pause-host.mjs', import.meta.url)), state, p, token], { stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    const port = await new Promise((done, reject) => { parent.stdout.once('data', data => done(Number(String(data).trim()))); parent.once('error', reject); });
    const call = client(port), c = await create(call, 'descendant');
    const d = await wait(async () => { const d = (await call(`/tasks/${c.taskId}`)).body; return d.state === 'SUCCEEDED' && d; });
    descendantPid = Number(readFileSync(join(d.workspace, 'descendant-pid.txt'), 'utf8'));
    await wait(() => { try { return readFileSync(join(d.workspace, 'heartbeat.txt')).length > 0; } catch { return false; } });
    const exited = new Promise(done => parent.once('exit', done)); parent.kill('SIGKILL'); await exited;
    await new Promise(r => setTimeout(r, 200)); const size = readFileSync(join(d.workspace, 'heartbeat.txt')).length;
    await new Promise(r => setTimeout(r, 150)); assert.equal(readFileSync(join(d.workspace, 'heartbeat.txt')).length, size);
    restarted = new PiHost({ state, token, childModule: fixture }); const t = restarted.store.task(c.taskId);
    assert.equal(t.state, 'UNKNOWN'); assert.equal(t.quiescent, false); assert.equal(restarted.create(c.request).taskId, c.taskId);
    assert.equal(readFileSync(join(d.workspace, 'dispatch-count.txt'), 'utf8').trim().split('\n').length, 1);
  } finally { if (parent.exitCode === null && parent.signalCode === null) parent.kill('SIGKILL'); if (descendantPid) try { process.kill(descendantPid, 'SIGKILL'); } catch {} await restarted?.close(); }
});

for (const action of ['STOP', 'CLOSE']) test(`macOS ${action} during native guardian arm never releases late init`, { skip: process.platform !== 'darwin' }, async () => {
  const { MacOwnership } = await import('../dist/control/macos-ownership.js');
  const original = MacOwnership.arm; let count = 0, release, armed;
  const blocked = new Promise(done => { release = done; }); const arrived = new Promise(done => { armed = done; });
  MacOwnership.arm = async pid => { const owner = await original.call(MacOwnership, pid); if (++count === 3) { armed(pid); await blocked; } return owner; };
  const s = await setup(); let pid;
  try {
    const c = await create(s.call, 'must-never-dispatch'); pid = await arrived;
    if (action === 'CLOSE') await s.host.close();
    else { const d = s.host.store.task(c.taskId); const result = await s.host.stop(c.taskId, { requestKey: 'arm-race-stop', intent: 'STOP', ...guard(d) }); assert.equal(result.state, 'UNKNOWN', 'the ownership handshake has not completed'); }
    release();
    await wait(() => { try { process.kill(pid, 0); return false; } catch { return true; } });
    if (action === 'STOP') assert.equal(s.host.store.operation(c.request.requestKey).state, 'ACCEPTED', 'late init never dispatches');
  } finally { release(); MacOwnership.arm = original; if (pid) try { process.kill(pid, 'SIGKILL'); } catch {} await s.host.close(); }
});

test('owner task discovery is bounded, read only and omits private task data', async () => {
 const s=await setup();try {
  const created=await create(s.call,'private-fixture-instruction');
  const result=await s.call('/tasks?limit=1');assert.equal(result.status,200);assert.equal(result.body.items.length,1);
  assert.equal(result.body.items[0].taskId,created.taskId);
  assert.deepEqual(Object.keys(result.body.items[0]).sort(),['engineId','engineVersion','projectId','state','taskId']);
  assert.equal(JSON.stringify(result.body).includes('private-fixture-instruction'),false);
  assert.equal((await s.call('/tasks?limit=0')).status,409);
  assert.equal((await s.call('/tasks?limit=51')).status,409);
  assert.equal((await s.call('/tasks?limit=1&limit=2')).status,409);
  assert.equal((await s.call('/tasks',undefined,{Authorization:'Bearer wrong'})).status,401);
 }finally{await s.host.close();}
});


test('owner task pages keep stable order through updates and new tasks', async () => {
 const s=await setup(); try {
  const created=await create(s.call,'directory-seed');
  const seed=s.host.store.task(created.taskId);
  for(let n=0;n<4;n++)s.host.store.save({...seed,taskId:'directory-'+n});
  const first=(await s.call('/tasks?limit=2')).body;
  assert.deepEqual(first.items.map(x=>x.taskId),['directory-3','directory-2']);
  assert.match(first.nextCursor,/^[0-9]+$/);
  s.host.store.save({...seed,taskId:'directory-0',state:'SUCCEEDED'});
  s.host.store.save({...seed,taskId:'directory-new'});
  const second=(await s.call('/tasks?limit=2&cursor='+first.nextCursor)).body;
  assert.deepEqual(second.items.map(x=>x.taskId),['directory-1','directory-0']);
  const last=(await s.call('/tasks?limit=2&cursor='+second.nextCursor)).body;
  assert.deepEqual(last.items.map(x=>x.taskId),[created.taskId]);assert.equal(last.nextCursor,null);
  assert.equal((await s.call('/tasks?cursor=-1')).status,409);
  assert.equal((await s.call('/tasks?cursor=1&cursor=2')).status,409);
  assert.deepEqual(s.host.store.taskDirectory(first.nextCursor,2).items.map(x=>x.taskId),['directory-1','directory-0']);
 }finally{await s.host.close();}
});
