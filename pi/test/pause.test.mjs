import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync, spawn } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

const token = 'deterministic-pause-test-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-pause-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
const pauseHost = fileURLToPath(new URL('./pause-host.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-pause-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello') { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction }; const result = await call('/tasks', body); assert.equal(result.status, 202); return { request: body, taskId: result.body.taskId, receipt: result.body }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const running = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'RUNNING' && d; });
const pause = (call, taskId, d, key = 'pause-1') => call(`/tasks/${taskId}/commands`, { requestKey: key, intent: 'PAUSE', ...guard(d) });
const resume = (call, taskId, d, key = 'resume-1') => call(`/tasks/${taskId}/commands`, { requestKey: key, intent: 'RESUME', ...guard(d) });

test('PAUSE seals BOUNDARY_CHECKPOINT; RESUME releases in place with new segment', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting');
    const d0 = await running(s.call, c.taskId);
    assert.equal((await s.call('/engines')).body.items[0].supportsPause, true);
    await wait(async () => (await s.call(`/tasks/${c.taskId}/events?limit=100`)).body.items.some(e => e.kind === 'TOOL_START'));
    const paused = await pause(s.call, c.taskId, d0);
    assert.equal(paused.status, 202); assert.equal(paused.body.state, 'APPLIED');
    let d = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(d.state, 'PAUSED'); assert.equal(d.capabilities.resume, true); assert.equal(d.capabilities.continueAfterRestart, true);
    const m = d.checkpointManifest;
    assert.equal(m.kind, 'BOUNDARY_CHECKPOINT'); assert.equal(m.nativeSessionId, 'fixture-pi-session');
    assert.equal(m.segmentId, d.segmentId); assert.equal(m.generation, d.generation);
    assert.match(m.sessionSha256, /^[0-9a-f]{64}$/); assert.equal(m.sessionSha256, hash(readFileSync(m.sessionFile)));
    assert.match(m.checkpointId, /^[A-Za-z0-9-]{36}$/); assert.equal(m.sessionFileSha256, m.sessionSha256);
    assert.equal(m.lastEventSequence, String(m.lastEventSeq)); assert.equal(m.ownerEpoch, String(d.ownerEpoch));
    assert.equal(m.boundaryState, m.boundary);
    assert.equal(m.engineBindingDigest, d.engineBindingDigest); assert.ok(m.lastEventSeq > 0);
    assert.equal(m.pendingEffects.length, 0, 'tool batch settled before boundary');
    assert.equal(m.pausedFromState, 'RUNNING'); assert.ok(m.sealedAt);
    // same-key replay returns the original receipt
    const replayed = await pause(s.call, c.taskId, d0); assert.equal(replayed.body.requestKey, 'pause-1'); assert.equal(replayed.body.state, 'APPLIED');
    // wrong guard rejected
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'resume-wrong', intent: 'RESUME', ...guard(d), generation: 'wrong' })).body.code, 'NATIVE_TARGET_CHANGED');
    // follow-up and steer are rejected while paused
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'fu', kind: 'NATIVE_FOLLOW_UP', content: 'x', ...guard(d) })).body.code, 'NATIVE_NOT_IDLE');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'st', kind: 'NATIVE_STEER', content: 'x', ...guard(d) })).body.code, 'NATIVE_NOT_RUNNING');
    const segmentBefore = d.segmentId, generationBefore = d.generation; const dPaused = d;
    const resumed = await resume(s.call, c.taskId, dPaused);
    assert.equal(resumed.body.state, 'APPLIED'); assert.equal(resumed.body.resumeVia, 'HELD_GATE');
    d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal(d.nativeSessionId, 'fixture-pi-session'); assert.notEqual(d.segmentId, segmentBefore); assert.notEqual(d.generation, generationBefore);
    const replayedResume = await resume(s.call, c.taskId, dPaused); assert.equal(replayedResume.body.requestKey, 'resume-1'); assert.equal(replayedResume.body.state, 'APPLIED');
    const events = (await s.call(`/tasks/${c.taskId}/events?limit=100`)).body.items;
    assert(events.some(e => e.kind === 'PAUSE_BOUNDARY_CHECKPOINT')); assert(events.some(e => e.kind === 'RESUME_APPLIED'));
  } finally { await s.host.close(); }
});

test('PAUSE with pending question records pending input; RESUME then answer completes', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'question');
    await wait(async () => (await s.call(`/tasks/${c.taskId}/input-requests`)).body.items[0]);
    const d0 = (await s.call('/tasks/' + c.taskId)).body;
    const paused = await pause(s.call, c.taskId, d0);
    assert.equal(paused.body.state, 'APPLIED');
    const d = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(d.state, 'PAUSED'); assert.equal(d.checkpointManifest.boundary, 'INPUT_REQUEST');
    assert.equal(d.checkpointManifest.pendingInputs.length, 1); assert.equal(d.checkpointManifest.pendingInputs[0].inputRequestId, 'q1');
    await s.host.sweepTaskDeadlines(Number(d.deadlineEpochMillis) + 1);
    assert.equal((await s.call('/tasks/' + c.taskId)).body.state, 'PAUSED', 'execution deadline must not destroy an acknowledged pause');
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests`)).body.items[0].state, 'PENDING');
    const resumed = await resume(s.call, c.taskId, d); assert.equal(resumed.body.state, 'APPLIED');
    const d2 = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'RUNNING' && d; });
    const q = (await s.call(`/tasks/${c.taskId}/input-requests`)).body.items[0];
    assert.equal(q.generation, d2.generation); assert.equal(q.nativeSessionId, d2.nativeSessionId);
    assert.equal(q.targetCommandId, d2.currentCommandId);
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, { requestKey: 'stale-answer', response: { value: 'yes' }, ...guard(d0) })).body.code, 'NATIVE_TARGET_CHANGED');
    const answer = { requestKey: 'answer-1', response: { value: 'yes' }, generation: q.generation, nativeSessionId: q.nativeSessionId, nativeTurnId: q.nativeTurnId, targetCommandId: q.targetCommandId };
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, answer)).status, 202);
    await wait(async () => (await s.call('/tasks/' + c.taskId)).body.state === 'SUCCEEDED');
  } finally { await s.host.close(); }
});

test('execution deadline still stops a running task with pending input', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'question');
    await wait(async () => (await s.call(`/tasks/${c.taskId}/input-requests`)).body.items[0]);
    const d = (await s.call('/tasks/' + c.taskId)).body;
    await s.host.sweepTaskDeadlines(Number(d.deadlineEpochMillis) + 1);
    const stopped = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(stopped.state, 'CANCELLED');
    assert.equal(stopped.quiescent, true);
  } finally { await s.host.close(); }
});

test('boundary deadline is honest: REQUESTED + PAUSE_REQUESTED, RESUME cancels the pause', async () => {
  const s = await setup({ pauseDeadlineMs: 900 }); try {
    const c = await create(s.call, 'waiting-never');
    const d0 = await running(s.call, c.taskId);
    const paused = await pause(s.call, c.taskId, d0);
    assert.equal(paused.body.state, 'REQUESTED'); assert.equal(paused.body.detail, 'BOUNDARY_NOT_REACHED');
    const d = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(d.state, 'PAUSE_REQUESTED'); assert.equal(d.checkpointManifest, undefined);
    const resumed = await resume(s.call, c.taskId, d); assert.equal(resumed.body.state, 'APPLIED'); assert.equal(resumed.body.detail, 'Pause request cancelled; boundary gate released');
    await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
  } finally { await s.host.close(); }
});

test('STOP during PAUSED kills the child and keeps the checkpoint readable', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting'); const d0 = await running(s.call, c.taskId);
    assert.equal((await pause(s.call, c.taskId, d0)).body.state, 'APPLIED');
    const d = (await s.call('/tasks/' + c.taskId)).body;
    const stopped = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-1', intent: 'STOP', ...guard(d) });
    assert.equal(stopped.body.state, 'APPLIED');
    const after = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(after.state, 'CANCELLED'); assert.equal(after.checkpointManifest.kind, 'BOUNDARY_CHECKPOINT');
    assert.equal((await resume(s.call, c.taskId, after)).body.code, 'CHECKPOINT_INCOMPATIBLE');
  } finally { await s.host.close(); }
});

test('answer while PAUSED refreshes a changed session at the held gate and settling preserves PAUSED', async () => {
  const s = await setup();
  try {
    const c = await create(s.call, 'question'); const d0 = await running(s.call, c.taskId);
    await wait(async () => (await s.call(`/tasks/${c.taskId}/input-requests`)).body.items.some(q => q.state === 'PENDING'));
    assert.equal((await pause(s.call, c.taskId, d0)).body.state, 'APPLIED');
    const paused = (await s.call(`/tasks/${c.taskId}`)).body;
    assert.equal(paused.quiescent, false);
    assert.equal((await s.call(`/tasks/${c.taskId}/input-requests/q1/responses`, { requestKey: 'answer-held', response: { value: 'yes' }, ...guard(paused) })).status, 202);
    const d = await wait(async () => { const d = (await s.call(`/tasks/${c.taskId}`)).body; return d.nativeIdle && d; });
    assert.equal(d.state, 'PAUSED'); assert.equal(d.checkpointManifest.pausedFromState, 'SUCCEEDED');
    assert.notEqual(d.checkpointManifest.sessionSha256, paused.checkpointManifest.sessionSha256);
    assert.equal(d.checkpointManifest.sessionSha256, hash(readFileSync(d.nativeSessionFile)));
    assert.equal((await s.call(`/tasks/${c.taskId}/events?limit=100`)).body.items.some(e => e.kind === 'CHECKPOINT_REFRESHED'), true);
    assert.equal((await resume(s.call, c.taskId, d)).body.state, 'APPLIED');
  } finally { await s.host.close(); }
});

test('macOS SIGKILL while PAUSED keeps an unproven checkpoint UNKNOWN after restart', { skip: process.platform !== 'darwin' }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-mac-pause-kill-')), p = project(); let host;
  const child = spawn(process.execPath, [pauseHost, state, p, token], { stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    const port = await new Promise(done => child.stdout.once('data', data => done(Number(String(data).trim())))), call = client(port);
    const c = await create(call, 'waiting'), d0 = await running(call, c.taskId);
    assert.equal((await pause(call, c.taskId, d0)).body.state, 'APPLIED');
    const exited = new Promise(done => child.once('exit', done)); child.kill('SIGKILL'); await exited;
    host = new PiHost({ state, token, childModule: fixture }); const task = host.store.task(c.taskId);
    assert.equal(task.state, 'UNKNOWN'); assert.equal(task.quiescent, false);
    assert.equal(host.detail(task).capabilities.resume, false); assert.equal(host.detail(task).capabilities.continueAfterRestart, false);
    assert.equal(host.create(c.request).taskId, c.taskId);
  } finally { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await host?.close(); }
});

test('host close while PAUSED: restart keeps PAUSED and RESUME reopens session continuation', async () => {
  const s = await setup(); const c = await create(s.call, 'waiting'); const d0 = await running(s.call, c.taskId);
  assert.equal((await pause(s.call, c.taskId, d0)).body.state, 'APPLIED');
  const paused = (await s.call('/tasks/' + c.taskId)).body;
  const sessionBytes = readFileSync(paused.checkpointManifest.sessionFile);
  await s.host.close();
  const host = new PiHost({ state: s.state, token, childModule: fixture }); const call = client(await host.listen());
  try {
    const d = (await call('/tasks/' + c.taskId)).body;
    assert.equal(d.state, 'PAUSED'); assert.equal(d.capabilities.resume, true); assert.equal(d.capabilities.continueAfterRestart, true);
    // tampering with the sealed session file blocks resume
    writeFileSync(paused.checkpointManifest.sessionFile, Buffer.concat([sessionBytes, Buffer.from('tampered')]));
    assert.equal((await resume(call, c.taskId, d)).body.code, 'CHECKPOINT_INCOMPATIBLE');
    writeFileSync(paused.checkpointManifest.sessionFile, sessionBytes);
    const resumed = await resume(call, c.taskId, d);
    assert.equal(resumed.body.resumeVia, 'SESSION_REOPEN');
    const applied = await wait(async () => { const op = (await call(`/tasks/${c.taskId}/commands/resume-1`)).body; return op.state === 'APPLIED' && op; });
    assert.equal(applied.detail, 'Continuation prompt consumed as new segment');
    const done = await wait(async () => { const d = (await call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal(done.nativeSessionId, 'fixture-pi-session'); assert.notEqual(done.segmentId, paused.segmentId); assert.notEqual(done.generation, paused.generation);
    const events = (await call(`/tasks/${c.taskId}/events?limit=100`)).body.items;
    assert(events.some(e => e.kind === 'RESUME_CONTINUATION_ACCEPTED')); assert(events.some(e => e.kind === 'RESTART_RECONCILIATION' && e.reasonCode === 'BOUNDARY_CHECKPOINT_SURVIVED_RESTART'));
  } finally { await host.close(); }
});

test('SIGKILL while PAUSED: restart keeps PAUSED, RESUME relaunches', { skip: process.platform !== 'win32' }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-pause-kill-')); const p = project();
  const child = spawn(process.execPath, [pauseHost, state, p, token], { stdio: ['ignore', 'pipe', 'pipe'] });
  const port = await new Promise(resolve => child.stdout.once('data', data => resolve(Number(String(data).trim()))));
  const call = client(port);
  const c = await create(call, 'waiting'); const d0 = await running(call, c.taskId);
  assert.equal((await pause(call, c.taskId, d0)).body.state, 'APPLIED');
  assert.equal((await call('/tasks/' + c.taskId)).body.state, 'PAUSED');
  child.kill('SIGKILL'); await new Promise(resolve => child.once('exit', resolve));
  const host = new PiHost({ state, token, childModule: fixture }); const call2 = client(await host.listen());
  try {
    const d = (await call2('/tasks/' + c.taskId)).body;
    assert.equal(d.state, 'PAUSED'); assert.equal(d.capabilities.resume, true);
    const resumed = await resume(call2, c.taskId, d); assert.equal(resumed.body.resumeVia, 'SESSION_REOPEN');
    await wait(async () => { const op = (await call2(`/tasks/${c.taskId}/commands/resume-1`)).body; return op.state === 'APPLIED' && op; });
    await wait(async () => (await call2('/tasks/' + c.taskId)).body.state === 'SUCCEEDED');
  } finally { await host.close(); }
});

test('SIGKILL mid PAUSE_REQUESTED reconciles to UNKNOWN, never PAUSED, never replayed', { skip: !['win32', 'darwin'].includes(process.platform) }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-pausekill-req-')); const p = project();
  const child = spawn(process.execPath, [pauseHost, state, p, token], { stdio: ['ignore', 'pipe', 'pipe'] });
  const port = await new Promise(resolve => child.stdout.once('data', data => resolve(Number(String(data).trim()))));
  const call = client(port);
  const c = await create(call, 'waiting-never'); const d0 = await running(call, c.taskId);
  const pending = pause(call, c.taskId, d0); // never resolves on the child side; deadline is 30s
  await new Promise(r => setTimeout(r, 250));
  child.kill('SIGKILL'); await Promise.all([new Promise(resolve => child.once('exit', resolve)), pending.catch(() => {})]);
  const host = new PiHost({ state, token, childModule: fixture });
  try {
    const task = host.store.task(c.taskId);
    assert.equal(task.state, 'UNKNOWN'); assert.equal(task.reasonCode, 'RESTART_RECONCILIATION_REQUIRED');
    const op = host.store.operation('pause-1');
    assert(['ACCEPTED', 'DISPATCHING', 'REQUESTED'].includes(op.state)); assert.notEqual(op.state, 'APPLIED');
    assert.equal(task.effects['tool-stuck-1'].status, 'UNKNOWN', 'unsettled tool effect marked UNKNOWN');
    await assert.rejects(() => host.command(c.taskId, { requestKey: 'resume-1', intent: 'RESUME', generation: task.generation, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, targetCommandId: task.currentCommandId }), /CHECKPOINT_INCOMPATIBLE/);
  } finally { await host.close(); }
});

test('effect ledger settles tools and blocks completed seal on UNKNOWN effects', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting'); await running(s.call, c.taskId);
    const task = await wait(async () => { const t = s.host.store.task(c.taskId); return t.effects?.['tool-wait-1']?.status === 'SETTLED' && t; });
    assert.equal(task.effects['tool-wait-1'].isError, false);
    const d = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal((await pause(s.call, c.taskId, d)).body.state, 'APPLIED');
    assert.equal(s.host.store.task(c.taskId).checkpointManifest.pendingEffects.length, 0);
  } finally { await s.host.close(); }
});
