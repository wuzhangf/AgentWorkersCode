import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

if (!process.env.MODEL_API_KEY) throw new Error('Set MODEL_API_KEY in the environment before running this test');
const executionProfile = process.argv[2] ?? 'native';
const ownerToken = hash('NWF-PI-MODEL-SMOKE:' + process.env.MODEL_API_KEY);
const root = resolve('.local'); mkdirSync(root, { recursive: true }); const run = mkdtempSync(join(root, 'model-')); const project = join(run, 'project'); mkdirSync(project);
execFileSync('git', ['init', project], { stdio: 'ignore' }); writeFileSync(join(project, 'README.md'), 'Native Pi smoke');
for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
let host = new PiHost({ executionProfile, state: join(run, 'state'), token: ownerToken }); host.register('smoke', project); let port = await host.listen();
async function call(path, body) { const response = await fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + ownerToken, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await response.json(); if (!response.ok) throw new Error(JSON.stringify(value)); return value; }
async function wait(fn, timeout = 180000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 500)); } throw new Error('MODEL_SMOKE_TIMEOUT'); }
const guard = d => ({ generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId });
try {
  const pf = await call('/task-preflights', { engineId: 'pi', projectId: 'smoke' });
  const request = { requestKey: 'real-start', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: 'Use write tool to create result.txt containing exactly NWF_PI_REAL_1. Then reply NWF_PI_DONE_1. Use no shell for this first step.' };
  const c = await call('/tasks', request); console.log('MODEL_START ' + JSON.stringify({ taskId: c.taskId, run }));
  let d = await wait(async () => { const d = await call('/tasks/' + c.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  assert.equal((await call('/tasks', request)).taskId, c.taskId);
  await call(`/tasks/${c.taskId}/messages`, { requestKey: 'real-follow', kind: 'NATIVE_FOLLOW_UP', content: 'Read result.txt, then use write tool to replace it with exactly NWF_PI_REAL_2. Then reply NWF_PI_DONE_2. Use no shell.', ...guard(d) });
  d = await wait(async () => { const d = await call('/tasks/' + c.taskId); if (d.state === 'FAILED') throw new Error('FOLLOWUP_FAILED'); return d.state === 'SUCCEEDED' && d.currentCommandId === 'real-follow' && d; });
  const output = await call(`/tasks/${c.taskId}/output`); assert(output.text.includes('NWF_PI_DONE_2'));
  await call(`/tasks/${c.taskId}/messages`, { requestKey: 'real-question', kind: 'NATIVE_FOLLOW_UP', content: 'Call ask_user with method select, title Native decision, and options [BLUE, GREEN]. Wait for its answer, then write answer.txt containing exactly the selected value. Do not choose on behalf of the user.', ...guard(d) });
  const question = await wait(async () => (await call(`/tasks/${c.taskId}/input-requests`)).items.find(q => q.state === 'PENDING'));
  d = await call('/tasks/' + c.taskId);
  await call(`/tasks/${c.taskId}/messages`, { requestKey: 'real-steer', kind: 'NATIVE_STEER', content: 'After receiving the answer, also use write to create steer.txt containing exactly NWF_PI_STEER. Then finish.', ...guard(d) });
  await call(`/tasks/${c.taskId}/input-requests/${question.inputRequestId}/responses`, { requestKey: 'real-answer', response: { value: 'GREEN' }, ...guard(d) });
  d = await wait(async () => { const d = await call('/tasks/' + c.taskId); if (d.state === 'FAILED') throw new Error('QUESTION_FAILED'); return d.state === 'SUCCEEDED' && d.currentCommandId === 'real-question' && d; });
  const steerReceipt = await call(`/tasks/${c.taskId}/interaction-requests/real-steer`); assert(['ACKNOWLEDGED', 'CONSUMED'].includes(steerReceipt.state));
  const stop = await call(`/tasks/${c.taskId}/commands`, { requestKey: 'real-stop', intent: 'STOP', ...guard(d) }); assert.equal(stop.state, 'APPLIED');
  const manifest = await call(`/tasks/${c.taskId}/artifacts`); const row = manifest.items.find(item => item.path === 'result.txt'); assert.equal(row.status, 'AVAILABLE');
  const data = await call(`/tasks/${c.taskId}/artifacts/${row.artifactId}`); const bytes = Buffer.from(data.bytes, 'base64'); assert.equal(hash(bytes), row.sha256); assert.equal(bytes.toString().trim(), 'NWF_PI_REAL_2');
  for (const [path, expected] of [['answer.txt', 'GREEN'], ['steer.txt', 'NWF_PI_STEER']]) { const item = manifest.items.find(i => i.path === path); assert.equal(item?.status, 'AVAILABLE'); const file = await call(`/tasks/${c.taskId}/artifacts/${item.artifactId}`); assert.equal(Buffer.from(file.bytes, 'base64').toString().trim(), expected); }
  const previousSession = d.nativeSessionId; const previousGeneration = d.generation;
  await host.close(); host = new PiHost({ executionProfile, state: join(run, 'state'), token: ownerToken }); port = await host.listen();
  d = await call('/tasks/' + c.taskId); assert.equal(d.capabilities.completedSessionFollowUpAfterRestart, true);
  await call(`/tasks/${c.taskId}/messages`, { requestKey: 'real-reopen', kind: 'NATIVE_FOLLOW_UP', content: 'From the prior conversation, use write to create reopened.txt containing exactly the color selected by the user earlier. Do not read files and do not use shell. Then reply NWF_PI_REOPEN_DONE.', ...guard(d) });
  d = await wait(async () => { const d = await call('/tasks/' + c.taskId); if (d.state === 'FAILED') throw new Error('REOPEN_FAILED'); return d.state === 'SUCCEEDED' && d.currentCommandId === 'real-reopen' && d; });
  assert.equal(d.nativeSessionId, previousSession); assert.notEqual(d.generation, previousGeneration);
  const reopenedStop = await call(`/tasks/${c.taskId}/commands`, { requestKey: 'real-reopened-stop', intent: 'STOP', ...guard(d) }); assert.equal(reopenedStop.state, 'APPLIED');
  const reopenedManifest = await call(`/tasks/${c.taskId}/artifacts`); const reopenedRow = reopenedManifest.items.find(item => item.path === 'reopened.txt'); assert.equal(reopenedRow?.status, 'AVAILABLE');
  const reopenedBytes = Buffer.from((await call(`/tasks/${c.taskId}/artifacts/${reopenedRow.artifactId}`)).bytes, 'base64'); assert.equal(reopenedBytes.toString().trim(), 'GREEN');
  assert.equal((await call(`/tasks/${c.taskId}/artifacts/${row.artifactId}`)).bytes, data.bytes);
  const evidence = { schemaVersion: 'nwf-pi-smoke/1', result: 'PASS', executionProfile, engine: '@earendil-works/pi-coding-agent@1.4.0', node: process.version, platform: process.platform, model: d.configuredModel, taskId: c.taskId, storeId: host.store.identity.storeId, artifact: { path: row.path, sha256: row.sha256, size: row.size }, question: { method: question.method, state: 'ANSWERED', answerFile: 'answer.txt' }, steer: { receipt: steerReceipt.state, artifact: 'steer.txt', consumptionQualification: 'No native input ID: queued ACK only; actual resultant file verified' }, reopen: { sameNativeSession: true, newGeneration: true, persistedSelectedValue: 'GREEN', artifactSha256: reopenedRow.sha256, oldArtifactStillImmutable: true }, stop: reopenedStop.state, stopProof: (await call('/tasks/' + c.taskId)).stopProof, metrics: await call(`/tasks/${c.taskId}/metrics`) };
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2)); console.log(JSON.stringify(evidence));
} finally { await host.close(); }
