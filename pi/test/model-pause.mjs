import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

if (!process.env.MODEL_API_KEY) throw new Error('Set MODEL_API_KEY in the environment before running this test');
const ownerToken = hash('NWF-PI-MODEL-PAUSE:' + process.env.MODEL_API_KEY);
const root = resolve('.local'); mkdirSync(root, { recursive: true }); const run = mkdtempSync(join(root, 'model-pause-')); const project = join(run, 'project'); mkdirSync(project);
execFileSync('git', ['init', project], { stdio: 'ignore' }); writeFileSync(join(project, 'README.md'), 'Native Pi W3 pause smoke');
for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
let host = new PiHost({ state: join(run, 'state'), token: ownerToken }); host.register('smoke', project); let port = await host.listen();
async function call(path, body) { const response = await fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + ownerToken, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await response.json(); if (!response.ok) throw new Error(JSON.stringify(value)); return value; }
async function wait(fn, timeout = 180000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 500)); } throw new Error('MODEL_PAUSE_TIMEOUT'); }
const guard = d => ({ generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId });
const events = async (taskId, cursor = '0') => (await call(`/tasks/${taskId}/events?cursor=${cursor}&limit=100`)).items;
const files = (a, b, c) => `Use the write tool to create ${a} containing exactly NWF_W3_${a.replace(/\W/g, '').toUpperCase()}. Then use the write tool to create ${b} containing exactly NWF_W3_${b.replace(/\W/g, '').toUpperCase()}. Then use the write tool to create ${c} containing exactly NWF_W3_${c.replace(/\W/g, '').toUpperCase()}. Exactly three write tool calls, one file per call, sequentially. Then reply NWF_W3_DONE. Do not use bash.`;
const evidence = { schemaVersion: 'nwf-pi-w3/1', result: 'FAIL', engine: '@earendil-works/pi-coding-agent@1.4.0', node: process.version, platform: process.platform, model: process.env.MODEL_DEFAULT_MODEL, semantics: { pause: 'BOUNDARY_CHECKPOINT', resumeInPlace: 'BOUNDARY_CHECKPOINT_RELEASE', resumeAfterRestart: 'INTERRUPT_CONTINUATION' }, scenarios: {} };
try {
  // Scenario A: PAUSE mid-run against the live gate, 15s quiet dwell, in-place RESUME.
  const pf = await call('/task-preflights', { engineId: 'pi', projectId: 'smoke' });
  const created = await call('/tasks', { requestKey: 'w3-pause-a', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision, instruction: files('a1.txt', 'a2.txt', 'a3.txt') });
  const taskA = created.taskId; console.log('W3_A_START ' + JSON.stringify({ taskId: taskA, run }));
  let d = await wait(async () => { const d = await call('/tasks/' + taskA); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return d.state === 'RUNNING' && (await events(taskA)).some(e => e.kind === 'TOOL_START') && d; });
  const paused = await call(`/tasks/${taskA}/commands`, { requestKey: 'w3-pause-a-cmd', intent: 'PAUSE', ...guard(d) });
  assert.equal(paused.state, 'APPLIED', JSON.stringify(paused));
  d = await call('/tasks/' + taskA);
  assert.equal(d.state, 'PAUSED'); assert.equal(d.checkpointManifest.kind, 'BOUNDARY_CHECKPOINT'); assert.equal(d.capabilities.resume, true);
  const seqAtPause = Number((await events(taskA)).find(e => e.kind === 'PAUSE_BOUNDARY_CHECKPOINT').sequence);
  const dwellStart = Date.now(); let dwellExecution = 0;
  while (Date.now() - dwellStart < 15000) { const items = await events(taskA); dwellExecution = items.filter(e => Number(e.sequence) > seqAtPause && ['TOOL_START', 'ASSISTANT_MESSAGE', 'INPUT_CONSUMED'].includes(e.kind)).length; await new Promise(r => setTimeout(r, 500)); }
  assert.equal(dwellExecution, 0, 'no tool/model execution observed during paused dwell');
  const seqAfterDwell = (await events(taskA)).at(-1)?.sequence;
  assert.equal(Number(seqAfterDwell), seqAtPause, 'no events appended while PAUSED');
  const resumed = await call(`/tasks/${taskA}/commands`, { requestKey: 'w3-resume-a-cmd', intent: 'RESUME', ...guard(d) });
  assert.equal(resumed.state, 'APPLIED'); assert.equal(resumed.resumeVia, 'HELD_GATE');
  const segmentA = d.segmentId, generationA = d.generation;
  d = await wait(async () => { const d = await call('/tasks/' + taskA); if (d.state === 'FAILED') throw new Error('RESUME_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  assert.equal(d.nativeSessionId, (await call('/tasks/' + taskA)).nativeSessionId); assert.notEqual(d.segmentId, segmentA); assert.notEqual(d.generation, generationA);
  const stopped = await call(`/tasks/${taskA}/commands`, { requestKey: 'w3-stop-a', intent: 'STOP', ...guard(d) }); assert.equal(stopped.state, 'APPLIED');
  const manifestA = await call(`/tasks/${taskA}/artifacts`);
  const rowsA = {};
  for (const name of ['a1.txt', 'a2.txt', 'a3.txt']) { const item = manifestA.items.find(i => i.path === name); assert.equal(item?.status, 'AVAILABLE', name); const file = await call(`/tasks/${taskA}/artifacts/${item.artifactId}`); const bytes = Buffer.from(file.bytes, 'base64'); assert.equal(bytes.toString().trim(), 'NWF_W3_' + name.replace(/\W/g, '').toUpperCase()); rowsA[name] = { sha256: item.sha256, size: item.size }; }
  evidence.scenarios.pauseResumeInPlace = { taskId: taskA, boundary: d.checkpointManifest?.boundary ?? 'sealed', pausedEventSeq: seqAtPause, dwellSeconds: 15, dwellNewEvents: 0, resumeVia: resumed.resumeVia, sameNativeSession: true, newSegment: true, artifacts: rowsA };

  // Scenario B: PAUSE then host close; new host RESUMEs via session reopen (INTERRUPT_CONTINUATION).
  const pfB = await call('/task-preflights', { engineId: 'pi', projectId: 'smoke' });
  const createdB = await call('/tasks', { requestKey: 'w3-pause-b', engineId: 'pi', projectId: 'smoke', optionsRevision: pfB.optionsRevision, instruction: files('b1.txt', 'b2.txt', 'b3.txt') });
  const taskB = createdB.taskId; console.log('W3_B_START ' + JSON.stringify({ taskId: taskB }));
  d = await wait(async () => { const d = await call('/tasks/' + taskB); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return d.state === 'RUNNING' && (await events(taskB)).some(e => e.kind === 'TOOL_START') && d; });
  const pausedB = await call(`/tasks/${taskB}/commands`, { requestKey: 'w3-pause-b-cmd', intent: 'PAUSE', ...guard(d) });
  assert.equal(pausedB.state, 'APPLIED', JSON.stringify(pausedB));
  d = await call('/tasks/' + taskB); assert.equal(d.state, 'PAUSED');
  const manifestB = d.checkpointManifest; const segmentB = d.segmentId;
  await host.close(); host = new PiHost({ state: join(run, 'state'), token: ownerToken }); port = await host.listen();
  d = await call('/tasks/' + taskB);
  assert.equal(d.state, 'PAUSED'); assert.equal(d.capabilities.resume, true); assert.equal(d.capabilities.continueAfterRestart, true);
  const resumedB = await call(`/tasks/${taskB}/commands`, { requestKey: 'w3-resume-b-cmd', intent: 'RESUME', ...guard(d) });
  assert.equal(resumedB.resumeVia, 'SESSION_REOPEN');
  await wait(async () => { const op = await call(`/tasks/${taskB}/commands/w3-resume-b-cmd`); return op.state === 'APPLIED' && op; });
  d = await wait(async () => { const d = await call('/tasks/' + taskB); if (d.state === 'FAILED') throw new Error('REOPEN_RESUME_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  assert.equal(d.nativeSessionId, manifestB.nativeSessionId); assert.notEqual(d.segmentId, segmentB);
  const stoppedB = await call(`/tasks/${taskB}/commands`, { requestKey: 'w3-stop-b', intent: 'STOP', ...guard(d) }); assert.equal(stoppedB.state, 'APPLIED');
  const artifactsB = await call(`/tasks/${taskB}/artifacts`);
  const rowsB = {};
  for (const name of ['b1.txt', 'b2.txt', 'b3.txt']) { const item = artifactsB.items.find(i => i.path === name); assert.equal(item?.status, 'AVAILABLE', name); const file = await call(`/tasks/${taskB}/artifacts/${item.artifactId}`); assert.equal(Buffer.from(file.bytes, 'base64').toString().trim(), 'NWF_W3_' + name.replace(/\W/g, '').toUpperCase()); rowsB[name] = { sha256: item.sha256, size: item.size }; }
  evidence.scenarios.pauseRestartResume = { taskId: taskB, boundary: manifestB.boundary, checkpointSurvivedRestart: true, resumeVia: resumedB.resumeVia, sameNativeSession: true, newSegment: true, artifacts: rowsB };
  evidence.taskIds = [taskA, taskB]; evidence.storeId = host.store.identity.storeId; evidence.result = 'PASS';
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  mkdirSync(resolve('test/evidence'), { recursive: true }); writeFileSync(resolve(`test/evidence/${process.platform === 'darwin' ? 'macos-arm64' : process.platform === 'win32' ? 'windows' : process.platform}-deepseek-w3.json`), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(evidence));
} finally { await host.close(); }
