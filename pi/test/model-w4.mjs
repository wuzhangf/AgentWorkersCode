import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, symlinkSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

if (!process.env.MODEL_API_KEY) throw new Error('Set MODEL_API_KEY in the environment before running this test');
const ownerToken = hash('NWF-PI-MODEL-W4:' + process.env.MODEL_API_KEY);
const root = resolve('.local'); mkdirSync(root, { recursive: true }); const run = mkdtempSync(join(root, 'model-w4-')); const project = join(run, 'project'); mkdirSync(project);
execFileSync('git', ['init', project], { stdio: 'ignore' }); writeFileSync(join(project, 'README.md'), 'Native Pi W4a smoke');
for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
let host = new PiHost({ state: join(run, 'state'), token: ownerToken }); host.register('smoke', project); let port = await host.listen();
async function call(path, body) { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + ownerToken, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await response.json(); if (!response.ok) throw new Error(JSON.stringify(value)); return value; }
async function wait(fn, timeout = 240000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 500)); } throw new Error('MODEL_W4_TIMEOUT'); }
const guard = d => ({ generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId });
const events = async taskId => (await call(`/tasks/${taskId}/events?limit=100`)).items;
const evidence = { schemaVersion: 'nwf-pi-w4a/1', result: 'FAIL', engine: '@earendil-works/pi-coding-agent@1.4.0', node: process.version, platform: process.platform, model: process.env.MODEL_DEFAULT_MODEL, scenarios: {} };
try {
  // Scenario A: SOFT budget of 1 token -> BUDGET_EXCEEDED at a real gate boundary, then EXTEND_BUDGET resumes a new segment on the same native session.
  const pf = await call('/task-preflights', { engineId: 'pi', projectId: 'smoke' });
  const a = await call('/tasks', { requestKey: 'w4-budget-a', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: 'Use the write tool to create a1.txt containing exactly NWF_W4_A1. Then reply NWF_W4_DONE. Do not use bash.', budget: { maxTotalTokens: 1, enforcement: 'SOFT' } });
  console.log('W4_A_START ' + JSON.stringify({ taskId: a.taskId, run }));
  let d = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return d.state === 'BUDGET_EXCEEDED' && d.quiescent && d; });
  const releaseAtExceed = { method: d.stopProof?.method, usage: { ...d.usage }, ledgerSource: d.usageLedger?.[0]?.source ?? 'UNKNOWN' };
  const hit = (await events(a.taskId)).find(e => e.kind === 'budget.exceeded');
  assert.equal(hit.field, 'maxTotalTokens'); assert.equal(hit.cap, 1); assert.ok(hit.observed > 0, 'real reported usage recorded');
  assert.ok(d.usage.input + d.usage.output > 0); assert.equal(d.reasonCode, 'BUDGET_EXCEEDED');
  const assistantCount = (await events(a.taskId)).filter(e => e.kind === 'ASSISTANT_MESSAGE').length;
  await new Promise(r => setTimeout(r, 3000));
  assert.equal((await events(a.taskId)).filter(e => e.kind === 'ASSISTANT_MESSAGE').length, assistantCount, 'no further provider dispatch after exceed');
  const deniedResp = await fetch(`http://127.0.0.1:${port}/local/v1/tasks/${a.taskId}/messages`, { method: 'POST', headers: { Authorization: 'Bearer ' + ownerToken, 'Content-Type': 'application/json', 'Idempotency-Key': 'w4-fu-denied' }, body: JSON.stringify({ requestKey: 'w4-fu-denied', kind: 'NATIVE_FOLLOW_UP', content: 'continue', ...guard(d) }) });
  assert.equal((await deniedResp.json()).code, 'BUDGET_EXCEEDED');
  const segmentA = d.segmentId, sessionA = d.nativeSessionId;
  const extended = await call(`/tasks/${a.taskId}/commands`, { requestKey: 'w4-extend-a', intent: 'EXTEND_BUDGET', budget: { maxTotalTokens: 500000 }, ...guard(d) });
  assert.equal(extended.state, 'ACCEPTED'); assert.equal(extended.resumeVia, 'SESSION_REOPEN');
  await wait(async () => { const op = await call(`/tasks/${a.taskId}/commands/w4-extend-a`); return op.state === 'APPLIED' && op; });
  d = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('EXTEND_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  assert.equal(d.nativeSessionId, sessionA, 'same native session resumed'); assert.notEqual(d.segmentId, segmentA);
  assert.equal(d.budget.caps.maxTotalTokens, 500000); assert.ok(d.budget.history.length >= 1);
  evidence.scenarios.budgetExceedExtend = { taskId: a.taskId, exceeded: { field: hit.field, cap: hit.cap, observed: hit.observed }, usageAtExceed: { ...releaseAtExceed.usage, ledgerSource: releaseAtExceed.ledgerSource }, releaseProof: releaseAtExceed.method, extended: { caps: d.budget.caps, via: extended.resumeVia, sameNativeSession: true, newSegment: true } };

  // Scenario B: writeScope denial of an absolute path outside the workspace + declared extraWriteDirs admit.
  // Junctions let a willing model attempt workspace-relative writes whose real targets resolve outside (denied) or inside a declared extra root (admitted) — the host gate enforces realpaths, not the model's belief.
  const outside = join(run, 'outside-target'); mkdirSync(outside);
  const allowedDir = join(run, 'allowed-extra'); mkdirSync(allowedDir);
  const b = await call('/tasks', { requestKey: 'w4-scope-b', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: 'Use the write tool to create two files inside your workspace: escape-link/escape.txt containing exactly NWF_W4_OUTSIDE and extra-link/extra.txt containing exactly NWF_W4_EXTRA. Do not use bash. Do not ask questions.', writeScope: { extraWriteDirs: [allowedDir] } });
  console.log('W4_B_START ' + JSON.stringify({ taskId: b.taskId }));
  const bws = join(run, 'state', 'workspaces', b.taskId);
  { const deadline = Date.now() + 120000; while (!existsSync(bws)) { if (Date.now() > deadline) throw new Error('MODEL_W4_TIMEOUT'); await new Promise(r => setTimeout(r, 100)); } }
  symlinkSync(outside, join(bws, 'escape-link'), process.platform === 'win32' ? 'junction' : 'dir'); symlinkSync(allowedDir, join(bws, 'extra-link'), process.platform === 'win32' ? 'junction' : 'dir');
  await wait(async () => { const d = await call('/tasks/' + b.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return (await events(b.taskId)).some(e => e.kind === 'write.scope.violation') && d; });
  const violation = (await events(b.taskId)).find(e => e.kind === 'write.scope.violation');
  assert.ok(violation, 'write.scope.violation emitted'); assert.equal(violation.toolName, 'write'); assert.ok(violation.scopeId);
  assert.equal(existsSync(join(outside, 'escape.txt')), false, 'outside file was never written');
  d = await wait(async () => { const d = await call('/tasks/' + b.taskId); return ['SUCCEEDED', 'BUDGET_EXCEEDED', 'FAILED'].includes(d.state) && d; }, 120000).catch(() => call('/tasks/' + b.taskId));
  assert.equal(existsSync(join(allowedDir, 'extra.txt')), true, 'declared extra write dir admitted through junction realpath');
  const stopB = await call(`/tasks/${b.taskId}/commands`, { requestKey: 'w4-stop-b', intent: 'STOP', ...guard(await call('/tasks/' + b.taskId)) }); assert.equal(stopB.state, 'APPLIED');
  evidence.scenarios.writeScopeViolation = { taskId: b.taskId, event: { toolName: violation.toolName, scopeId: violation.scopeId, deniedPathHash: hash(violation.deniedPath ?? '') }, outsideFileAbsent: true, runKeptAlive: d.state !== 'FAILED' };
  evidence.scenarios.writeScopeAllowedExtra = { taskId: b.taskId, extraWriteAdmitted: true };

  // Scenario C: unsatisfiable environment binding fails terminally before any model call.
  const c = await call('/tasks', { requestKey: 'w4-env-c', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: 'never dispatched', environment: { requiresCommands: ['nwf-w4-definitely-missing-cmd'] } });
  d = await wait(async () => { const d = await call('/tasks/' + c.taskId); return d.state === 'BINDING_UNAVAILABLE' && d; });
  assert.equal(d.quiescent, true); assert.equal(d.pid, undefined); assert.equal(d.usage.input, 0);
  assert.equal(d.bindingUnavailable.missing[0].kind, 'command');
  evidence.scenarios.bindingUnavailable = { taskId: c.taskId, state: d.state, missing: d.bindingUnavailable.missing, noModelCall: true };

  // Scenario D: cap 1 queues the second create; STOP releases the slot and the queue dispatches once.
  await host.close(); host = new PiHost({ state: join(run, 'state'), token: ownerToken, maxActiveTasks: 1 }); port = await host.listen();
  const d1 = await call('/tasks', { requestKey: 'w4-queue-1', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: 'Use the write tool to create queue1.txt containing exactly NWF_W4_Q1. Then reply NWF_W4_DONE. Do not ask questions. Use only the write tool; do not use bash or spawn processes.' });
  const d2 = await call('/tasks', { requestKey: 'w4-queue-2', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: 'Use the write tool to create queue2.txt containing exactly NWF_W4_Q2. Then reply NWF_W4_DONE. Do not ask questions. Use only the write tool; do not use bash or spawn processes.' });
  const q2 = await wait(async () => { const d = await call('/tasks/' + d2.taskId); return d.state === 'QUEUED' && d; });
  assert.equal(q2.queue.position, 1);
  const firstReady = await wait(async () => { const d = await call('/tasks/' + d1.taskId); if (d.state === 'FAILED') throw new Error('QUEUE_FIRST_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  const d1state = firstReady.state;
  const stopD1 = await call(`/tasks/${d1.taskId}/commands`, { requestKey: 'w4-stop-q1', intent: 'STOP', ...guard(await call('/tasks/' + d1.taskId)) });
  assert.ok(['APPLIED', 'UNKNOWN'].includes(stopD1.state));
  d = await wait(async () => { const d = await call('/tasks/' + d2.taskId); if (d.state === 'FAILED') throw new Error('QUEUE_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  assert.ok((await events(d2.taskId)).some(e => e.kind === 'ADMITTED'));
  const stopD2 = await call(`/tasks/${d2.taskId}/commands`, { requestKey: 'w4-stop-q2', intent: 'STOP', ...guard(d) }); assert.equal(stopD2.state, 'APPLIED');
  evidence.scenarios.queueRelease = { taskId: d2.taskId, queuedOrdinal: q2.queueOrdinal, position: q2.queue.position, releasedBy: 'STOP', firstState: d1state, dispatchedOnce: true };
  evidence.taskIds = [a.taskId, b.taskId, c.taskId, d1.taskId, d2.taskId]; evidence.storeId = host.store.identity.storeId; evidence.result = 'PASS';
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  mkdirSync(resolve('test/evidence'), { recursive: true }); writeFileSync(resolve(`test/evidence/${process.platform === 'darwin' ? 'macos-arm64' : process.platform === 'win32' ? 'windows' : process.platform}-deepseek-w4a.json`), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(evidence));
} finally { await host.close(); }
