import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

if (!process.env.MODEL_API_KEY) throw new Error('Set MODEL_API_KEY in the environment before running this test');
const ownerToken = hash('NWF-PI-MODEL-W5A:' + process.env.MODEL_API_KEY);
const root = resolve('.local'); mkdirSync(root, { recursive: true }); const run = mkdtempSync(join(root, 'model-w5a-')); const project = join(run, 'project'); mkdirSync(project);
execFileSync('git', ['init', project], { stdio: 'ignore' });
mkdirSync(join(project, 'notes'));
writeFileSync(join(project, 'README.md'), 'Native Pi W5a smoke');
writeFileSync(join(project, 'notes', 'marker.txt'), 'The frozen marker is NWF_W5A_PKG_MARKER_7749.\n');
for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
const host = new PiHost({ state: join(run, 'state'), token: ownerToken }); host.register('smoke', project); const port = await host.listen();
async function call(path, body, method) { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + ownerToken, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await response.json(); if (!response.ok) throw new Error(JSON.stringify(value)); return value; }
const errBody = (path, body) => fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: 'POST', headers: { Authorization: 'Bearer ' + ownerToken, 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey }, body: JSON.stringify(body) }).then(r => r.json());
async function wait(fn, timeout = 240000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 1000)); } throw new Error('MODEL_W5A_TIMEOUT'); }
const guard = d => ({ generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId });
const events = async taskId => (await call(`/tasks/${taskId}/events?limit=100`)).items;
const workspaceFiles = rootDir => { const out = []; const walk = (dir, prefix) => { for (const e of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) { if (e.name === '.git') continue; const p = join(dir, e.name); if (e.isDirectory()) walk(p, prefix + e.name + '/'); else if (e.isFile()) out.push(prefix + e.name + ':' + hash(readFileSync(p))); } }; walk(rootDir, ''); return out; };
const evidence = { schemaVersion: 'nwf-pi-w5a/1', result: 'FAIL', engine: '@earendil-works/pi-coding-agent@1.4.0', node: process.version, platform: process.platform, model: process.env.MODEL_DEFAULT_MODEL, scenarios: {} };
try {
  // Scenario A: real task succeeds, then a context package freezes workspace bytes.
  const pf = await call('/task-preflights', { engineId: 'sage-pi', projectId: 'smoke' });
  const a = await call('/tasks', { requestKey: 'w5a-main', engineId: 'sage-pi', projectId: 'smoke', optionsRevision: pf.optionsRevision,
    instruction: 'Reply with exactly NWF_W5A_MAIN_OK and nothing else. Do not use tools and do not ask questions.' });
  console.log('W5A_A_START ' + JSON.stringify({ taskId: a.taskId, run }));
  const d0 = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return ['SUCCEEDED', 'BUDGET_EXCEEDED'].includes(d.state) && d; });
  assert.equal(d0.state, 'SUCCEEDED');
  const pkg = await call(`/tasks/${a.taskId}/context-packages`, { requestKey: 'w5a-pkg-1', name: 'frozen-notes', sources: [{ kind: 'file', path: 'notes/marker.txt' }, { kind: 'summary', text: 'W5a model verification bundle' }] });
  assert.match(pkg.packageId, /^pkg-/); assert.match(pkg.digest, /^[0-9a-f]{64}$/); assert.equal(pkg.sourceCount, 2);
  const detail = await call(`/tasks/${a.taskId}/context-packages/${pkg.packageId}`);
  assert.equal(detail.digestVerified, true);
  assert.ok(Buffer.from(detail.sources.find(s => s.kind === 'file').content, 'base64').toString('utf8').includes('NWF_W5A_PKG_MARKER_7749'));
  const filesBefore = workspaceFiles(d0.workspace);
  const outputBefore = (await call(`/tasks/${a.taskId}/output`)).text;
  assert.ok(!outputBefore.includes('NWF_W5A_PKG_MARKER_7749'), 'answer material not in main output yet');
  const caps = (await call('/engines')).items[0].capabilities;
  assert.equal(caps.contextPackages, true); assert.equal(caps.auxiliarySessions, true); assert.deepEqual(caps.transitions, { sameEngine: true, crossEngine: false });
  evidence.scenarios.contextPackage = { packageId: pkg.packageId, digest: pkg.digest, digestVerified: detail.digestVerified, capabilities: { contextPackages: caps.contextPackages, auxiliarySessions: caps.auxiliarySessions, transitions: caps.transitions } };

  // Scenario B: isolated aux session answers from the frozen package on the real
  // model; the parent workspace and output stay untouched until explicit deliver.
  const aux = await call(`/tasks/${a.taskId}/auxiliary-sessions`, { requestKey: 'w5a-aux-1', question: 'What is the frozen marker text inside notes/marker.txt? Reply with only the marker.', contextPackageIds: [pkg.packageId] });
  const aid = aux.auxSessionId;
  const auxDone = await wait(async () => { const x = await call(`/tasks/${a.taskId}/auxiliary-sessions/${aid}`); if (x.state === 'FAILED') throw new Error('AUX_FAILED ' + x.reasonCode); return x.state === 'SUCCEEDED' && x; });
  assert.ok(auxDone.answer.includes('NWF_W5A_PKG_MARKER_7749'), 'real model answered from the frozen package material');
  assert.notEqual(auxDone.nativeSessionId, d0.nativeSessionId, 'aux runs its own native session');
  assert.deepEqual(workspaceFiles(d0.workspace), filesBefore, 'parent workspace byte-identical after aux');
  assert.ok(!(await call(`/tasks/${a.taskId}/output`)).text.includes('NWF_W5A_PKG_MARKER_7749'), 'undelivered answer absent from main output');
  const usageNow = (await call('/tasks/' + a.taskId)).usage;
  assert.deepEqual(usageNow, d0.usage, 'aux never drains the parent ledger');
  const evs1 = await events(a.taskId);
  assert.ok(evs1.some(e => e.kind === 'aux-session.started')); assert.ok(evs1.some(e => e.kind === 'aux-session.completed' && e.state === 'SUCCEEDED'));
  evidence.scenarios.auxResearch = { auxSessionId: aid, auxNativeSessionId: auxDone.nativeSessionId, parentNativeSessionId: d0.nativeSessionId, markerInAnswer: true, parentWorkspaceUnchanged: true, parentLedgerUntouched: true, auxUsage: auxDone.usage };

  // Scenario C: explicit deliver injects the answer through the guarded follow-up
  // path; the real model consumes it on the same native session.
  const deliver = await call(`/tasks/${a.taskId}/auxiliary-sessions/${aid}/deliver`, { requestKey: 'w5a-dlv-1' });
  assert.equal(deliver.delivered.kind, 'NATIVE_FOLLOW_UP');
  const dAfterDeliver = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d.currentCommandId === 'w5a-dlv-1' && d; });
  const outputAfter = (await call(`/tasks/${a.taskId}/output`)).text;
  assert.ok(outputAfter.length > outputBefore.length, 'delivery produced a new real assistant turn');
  const dlvEvent = (await events(a.taskId)).find(e => e.kind === 'aux-session.delivered');
  assert.equal(dlvEvent.auxSessionId, aid); assert.equal(dlvEvent.requestKey, 'w5a-dlv-1');
  evidence.scenarios.deliver = { followUpAccepted: true, newOutputBytes: outputAfter.length - outputBefore.length, event: 'aux-session.delivered' };

  // Scenario D: same-engine transition quiesces the owner, then admits a new
  // owner epoch on the SAME native session with a real continuation prompt.
  const tr = await call(`/tasks/${a.taskId}/transitions`, { requestKey: 'w5a-tr-1', ...guard(dAfterDeliver) });
  assert.equal(tr.kind, 'TRANSITION');
  const d1 = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return d.state === 'SUCCEEDED' && d; });
  assert.equal(d1.ownerEpoch, '2'); assert.equal(d1.nativeSessionId, d0.nativeSessionId, 'same native session across transition');
  assert.notEqual(d1.generation, d0.generation);
  const trs = (await call(`/tasks/${a.taskId}/transitions`)).items;
  assert.equal(trs.length, 1); assert.equal(trs[0].fromEpoch, '1'); assert.equal(trs[0].toEpoch, '2');
  const evs2 = await events(a.taskId);
  assert.ok(evs2.some(e => e.kind === 'transition.quiesced')); assert.ok(evs2.some(e => e.kind === 'transition.admitted' && e.toEpoch === '2'));
  const stale = await errBody(`/tasks/${a.taskId}/commands`, { requestKey: 'w5a-stale', intent: 'STOP', ...guard(d0) });
  assert.equal(stale.code, 'NATIVE_TARGET_CHANGED', 'stale epoch fenced after transition');
  const incompatible = await errBody(`/tasks/${a.taskId}/transitions`, { requestKey: 'w5a-tr-x', targetProfile: { engineId: 'codex' }, ...guard(d1) });
  assert.equal(incompatible.code, 'INCOMPATIBLE_TRANSITION');
  evidence.scenarios.transition = { transitionId: trs[0].transitionId, fromEpoch: '1', toEpoch: '2', sameNativeSession: true, method: trs[0].compatibility.method, staleEpochFenced: true, crossEngineRefused: 'INCOMPATIBLE_TRANSITION' };
  evidence.taskIds = [a.taskId]; evidence.storeId = host.store.identity.storeId; evidence.result = 'PASS';
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  mkdirSync(resolve('test/evidence'), { recursive: true }); writeFileSync(resolve('test/evidence/windows-deepseek-w5a.json'), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(evidence));
} finally { await host.close(); }
