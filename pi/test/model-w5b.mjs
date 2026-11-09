import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

if (!process.env.MODEL_API_KEY) throw new Error('Set MODEL_API_KEY in the environment before running this test');
const ownerToken = hash('NWF-PI-MODEL-W5B:' + process.env.MODEL_API_KEY);
const root = resolve('.local'); mkdirSync(root, { recursive: true }); const run = mkdtempSync(join(root, 'model-w5b-')); const project = join(run, 'project'); mkdirSync(project);
execFileSync('git', ['init', project], { stdio: 'ignore' });
writeFileSync(join(project, 'README.md'), 'Native Pi W5b smoke\n');
for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
const host = new PiHost({ state: join(run, 'state'), token: ownerToken }); host.register('smoke', project); const port = await host.listen();
async function call(path, body, method) { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + ownerToken, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await response.json(); if (!response.ok) throw new Error(JSON.stringify(value)); return value; }
const raw = (path, body) => fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: 'POST', headers: { Authorization: 'Bearer ' + ownerToken, 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, body: await r.json() }));
async function wait(fn, timeout = 240000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 1000)); } throw new Error('MODEL_W5B_TIMEOUT'); }
const events = async taskId => (await call(`/tasks/${taskId}/events?limit=100`)).items;
const collabGet = async (taskId, cid) => call(`/tasks/${taskId}/collaborations/${cid}`);
const evidence = { schemaVersion: 'nwf-pi-w5b/1', result: 'FAIL', engine: '@earendil-works/pi-coding-agent@1.4.0', node: process.version, platform: process.platform, model: process.env.MODEL_DEFAULT_MODEL, scenarios: {} };
try {
  // Scenario A: a normal task succeeds; its workspace is the collaboration root.
  const pf = await call('/task-preflights', { engineId: 'pi', projectId: 'smoke' });
  const a = await call('/tasks', { requestKey: 'w5b-main', engineId: 'pi', projectId: 'smoke', optionsRevision: pf.optionsRevision,
    instruction: 'Reply with exactly NWF_W5B_MAIN_OK and nothing else. Do not use tools and do not ask questions.' });
  console.log('W5B_A_START ' + JSON.stringify({ taskId: a.taskId, run }));
  const d0 = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return ['SUCCEEDED', 'BUDGET_EXCEEDED'].includes(d.state) && d; });
  assert.equal(d0.state, 'SUCCEEDED');
  const workspace = d0.workspace;
  const caps = (await call('/engines')).items[0].capabilities;
  assert.deepEqual(caps.collaborations, { dag: true, isolatedCheckout: true, maxChildren: 8, maxActivePerTask: 4 });
  assert.deepEqual(caps.adoptions, { conflictCheck: true, partial: true, modes: ['ALL', 'FILES'] });

  // Scenario B: a 2-child DAG on the real model — child B launches only after
  // child A finishes, and each child writes inside its own isolated checkout.
  const made = await call(`/tasks/${a.taskId}/collaborations`, { requestKey: 'w5b-collab-1', name: 'model-dag', children: [
    { childId: 'writer-a', goal: 'Write a file named collab-out-a.txt whose entire content is exactly NWF_W5B_CHILD_A_771 and nothing else, then stop.' },
    { childId: 'writer-b', goal: 'Write a file named collab-out-b.txt whose entire content is exactly NWF_W5B_CHILD_B_772 and nothing else, then stop.', dependsOn: ['writer-a'] },
  ] });
  const cid = made.collaborationId;
  const collab = await wait(async () => { const g = await collabGet(a.taskId, cid); return g.state === 'COMPLETED' && g; });
  const byId = Object.fromEntries(collab.children.map(ch => [ch.childId, ch]));
  assert.equal(byId['writer-a'].state, 'SUCCEEDED'); assert.equal(byId['writer-b'].state, 'SUCCEEDED');
  assert.ok(byId['writer-b'].startedAt >= byId['writer-a'].finishedAt, 'DAG: B started only after A finished');
  assert.notEqual(byId['writer-a'].nativeSessionId, d0.nativeSessionId, 'child runs its own native session');
  const aCheckout = join(run, 'state', 'collab', cid, 'writer-a', 'workspace');
  const aFile = join(aCheckout, 'collab-out-a.txt'); const bFile = join(run, 'state', 'collab', cid, 'writer-b', 'workspace', 'collab-out-b.txt');
  assert.ok(existsSync(aFile), 'child A wrote collab-out-a.txt inside its own checkout');
  assert.ok(existsSync(bFile), 'child B wrote collab-out-b.txt inside its own checkout');
  assert.equal(existsSync(join(workspace, 'collab-out-a.txt')), false, 'child file absent from parent until adoption');
  assert.equal(existsSync(join(workspace, 'collab-out-b.txt')), false);
  const aContent = readFileSync(aFile, 'utf8');
  assert.ok(aContent.includes('NWF_W5B_CHILD_A_771'), 'real model wrote the requested marker: ' + aContent.slice(0, 120));
  const evs = await events(a.taskId);
  assert.ok(evs.some(e => e.kind === 'collaboration.created' && e.childCount === 2));
  assert.ok(evs.some(e => e.kind === 'collaboration.child.started' && e.childId === 'writer-a'));
  assert.ok(evs.some(e => e.kind === 'collaboration.child.completed' && e.childId === 'writer-a'));
  assert.ok(evs.some(e => e.kind === 'collaboration.completed' && e.succeeded === 2));
  evidence.scenarios.dag = { collaborationId: cid, orderingProof: byId['writer-b'].startedAt >= byId['writer-a'].finishedAt, childANativeSession: byId['writer-a'].nativeSessionId, parentNativeSession: d0.nativeSessionId, childAWroteInCheckout: true, parentUntouchedUntilAdopt: true };

  // Scenario C: explicit adoption lands the child file in the parent workspace;
  // same-key replay and an identical second adoption are NO_CHANGES-idempotent.
  const adopt = await call(`/tasks/${a.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'w5b-ad-1', childId: 'writer-a', mode: 'FILES', paths: ['collab-out-a.txt'] });
  assert.equal(adopt.outcome, 'ADOPTED');
  assert.equal(adopt.files[0].path, 'collab-out-a.txt');
  assert.match(adopt.files[0].sha256, /^[0-9a-f]{64}$/);
  const adoptedContent = readFileSync(join(workspace, 'collab-out-a.txt'), 'utf8');
  assert.equal(adoptedContent, aContent, 'adopted bytes identical to the child checkout bytes');
  const replay = await call(`/tasks/${a.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'w5b-ad-1', childId: 'writer-a', mode: 'FILES', paths: ['collab-out-a.txt'] });
  assert.equal(replay.replayed, true); assert.equal(replay.outcome, 'ADOPTED');
  const again = await call(`/tasks/${a.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'w5b-ad-2', childId: 'writer-a', mode: 'FILES', paths: ['collab-out-a.txt'] });
  assert.equal(again.outcome, 'NO_CHANGES', 'identical re-adoption is an idempotent NO_CHANGES fact');
  const evs2 = await events(a.taskId);
  assert.ok(evs2.some(e => e.kind === 'collaboration.adopted' && e.outcome === 'ADOPTED' && e.files.some(f => f.path === 'collab-out-a.txt')));
  assert.ok(evs2.some(e => e.kind === 'collaboration.adopted' && e.outcome === 'NO_CHANGES'));
  const audit = await call(`/worker-host/v1/tasks/${a.taskId}/audit`);
  assert.ok(audit.commands.some(o => o.kind === 'COLLABORATION_ADOPT' && o.requestKey === 'w5b-ad-1'));
  evidence.scenarios.adoption = { outcome: 'ADOPTED', file: 'collab-out-a.txt', sha256: adopt.files[0].sha256, byteIdentical: adoptedContent === aContent, sameKeyReplay: true, secondAdoptionNoChanges: true };
  evidence.taskIds = [a.taskId]; evidence.collaborationId = cid; evidence.storeId = host.store.identity.storeId; evidence.result = 'PASS';
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  mkdirSync(resolve('test/evidence'), { recursive: true }); writeFileSync(resolve('test/evidence/windows-deepseek-w5b.json'), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(evidence));
} finally { await host.close(); }
