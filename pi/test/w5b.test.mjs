import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

const token = 'deterministic-w5b-test-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w5b-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w5b-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, receipt: result.body, status: result.status, code: result.body.code }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const events = async (call, taskId) => (await call(`/tasks/${taskId}/events?limit=100`)).body.items;
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });
const running = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'RUNNING' && d; });
const collabDone = async (call, taskId, collabId) => wait(async () => { const c = (await call(`/tasks/${taskId}/collaborations/${collabId}`)).body; return ['COMPLETED', 'CANCELLED'].includes(c.state) && c; });
const collabList = async (call, taskId) => (await call(`/tasks/${taskId}/collaborations`)).body.items;
const collabGet = async (call, taskId, collabId) => (await call(`/tasks/${taskId}/collaborations/${collabId}`)).body;

test('collaboration DAG: dependency ordering, isolated checkouts, durable facts, projection', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'writefile seed.txt');
    const d = await succeeded(s.call, c.taskId);
    const workspace = d.workspace;
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-1', name: 'chain', children: [
      { childId: 'a', goal: 'child-write out-a.txt' },
      { childId: 'b', goal: 'child-write out-b.txt', dependsOn: ['a'] },
      { childId: 'c', goal: 'child-write out-c.txt', dependsOn: ['b'] },
    ] });
    assert.equal(made.status, 202); const cid = made.body.collaborationId; assert.match(cid, /^collab-/);
    const collab = await collabDone(s.call, c.taskId, cid);
    assert.equal(collab.state, 'COMPLETED');
    assert.deepEqual(collab.result, { succeeded: 3, failed: 0, cancelled: 0, blocked: 0 });
    for (const ch of collab.children) assert.equal(ch.state, 'SUCCEEDED', ch.childId);
    const byId = Object.fromEntries(collab.children.map(ch => [ch.childId, ch]));
    // DAG ordering: a dependent child only starts after its dependency finished
    assert.ok(byId.b.startedAt >= byId.a.finishedAt, 'b started only after a finished');
    assert.ok(byId.c.startedAt >= byId.b.finishedAt, 'c started only after b finished');
    assert.ok(byId.a.baseline.digest, 'baseline digest sealed at spawn');
    assert.ok(byId.b.baseline.snapshotAt >= byId.a.finishedAt, 'b baseline reflects parent tree at its own spawn');
    assert.ok(byId.a.candidate.fileCount >= 1 && byId.a.candidate.digest, 'candidate sealed at completion');
    assert.ok(byId.a.nativeSessionId, 'child runs its own native session');
    // durable event order: started(a) < completed(a) < started(b) < completed(b) < started(c) < completed(c)
    const evs = await events(s.call, c.taskId);
    const seq = (kind, childId) => evs.findIndex(e => e.kind === kind && e.childId === childId);
    const created = evs.find(e => e.kind === 'collaboration.created'); assert.equal(created.collaborationId, cid); assert.equal(created.childCount, 3);
    assert.ok(!('goal' in created.children[0]) && created.children[0].goalSha256, 'creation event carries goal digests, not bodies');
    assert.ok(seq('collaboration.child.started', 'a') < seq('collaboration.child.completed', 'a'));
    assert.ok(seq('collaboration.child.completed', 'a') < seq('collaboration.child.started', 'b'));
    assert.ok(seq('collaboration.child.completed', 'b') < seq('collaboration.child.started', 'c'));
    assert.ok(evs.some(e => e.kind === 'collaboration.completed' && e.succeeded === 3));
    // isolation: child output exists only inside its own checkout, never the parent
    for (const ch of collab.children) {
      const name = 'out-' + ch.childId + '.txt';
      assert.equal(existsSync(join(workspace, name)), false, name + ' absent from parent until adopt');
      assert.equal(readFileSync(join(s.state, 'collab', cid, ch.childId, 'workspace', name), 'utf8'), 'collab write ' + name);
    }
    // the create operation reached APPLIED and the task detail projects children
    const op = (await s.call(`/tasks/${c.taskId}/interaction-requests/cb-1`)).body;
    assert.equal(op.state, 'APPLIED'); assert.equal(op.kind, 'COLLABORATION_CREATE');
    const detail = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(detail.collaborations[0].collaborationId, cid);
    assert.equal(detail.collaborations[0].children.length, 3);
    const caps = (await s.call('/engines')).body.items[0].capabilities;
    assert.deepEqual(caps.collaborations, { dag: true, isolatedCheckout: true, maxChildren: 8, maxActivePerTask: 4 });
    assert.deepEqual(caps.adoptions, { conflictCheck: true, partial: true, modes: ['ALL', 'FILES'] });
    for (const pid of ['P14', 'P15']) {
      const row = (await s.call('/worker-host/v1/capabilities')).body.items.find(i => i.id === pid);
      assert.ok(row, pid + ' row'); assert.equal(row.implementation, 'IMPLEMENTED'); assert.equal(row.qualification, 'PARTIAL');
    }
    // same requestKey replays the durable operation; a colliding key refuses
    const replay = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-1', name: 'chain', children: [{ childId: 'a', goal: 'child-write out-a.txt' }, { childId: 'b', goal: 'child-write out-b.txt', dependsOn: ['a'] }, { childId: 'c', goal: 'child-write out-c.txt', dependsOn: ['b'] }] });
    assert.equal(replay.body.collaborationId, cid);
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-1', children: [{ goal: 'x' }] })).body.code, 'IDEMPOTENCY_CONFLICT');
  } finally { await s.host.close(); }
});

test('collaboration DAG: dependency failure blocks dependents; partial failure still completes', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); await succeeded(s.call, c.taskId);
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-fail', children: [
      { childId: 'bad', goal: 'child-fail' },
      { childId: 'dep', goal: 'child-write dep-out.txt', dependsOn: ['bad'] },
      { childId: 'ok', goal: 'child-write ok-out.txt' },
    ] });
    const collab = await collabDone(s.call, c.taskId, made.body.collaborationId);
    assert.equal(collab.state, 'COMPLETED', 'partial failure does not fail the collaboration');
    assert.deepEqual(collab.result, { succeeded: 1, failed: 1, cancelled: 0, blocked: 1 });
    const byId = Object.fromEntries(collab.children.map(ch => [ch.childId, ch]));
    assert.equal(byId.bad.state, 'FAILED'); assert.equal(byId.bad.reasonCode, 'MODEL_REQUEST_FAILED');
    assert.equal(byId.dep.state, 'BLOCKED'); assert.equal(byId.dep.reasonCode, 'DEPENDENCY_UNSATISFIED');
    assert.deepEqual(byId.dep.failedDependencies, ['bad']);
    assert.equal(byId.dep.startedAt, null, 'blocked child never launched');
    assert.equal(byId.ok.state, 'SUCCEEDED');
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'collaboration.child.failed' && e.childId === 'bad'));
    assert.ok(evs.some(e => e.kind === 'collaboration.child.blocked' && e.childId === 'dep' && e.reasonCode === 'DEPENDENCY_UNSATISFIED'));
    assert.equal(existsSync(join(s.state, 'collab', made.body.collaborationId, 'dep', 'workspace', 'dep-out.txt')), false);
  } finally { await s.host.close(); }
});

test('collaboration stop and parent STOP: running children drained with proof, pending blocked', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting'); const d = await running(s.call, c.taskId);
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-stop', children: [
      { childId: 'hang', goal: 'child-hang' },
      { childId: 'behind', goal: 'child-write late.txt', dependsOn: ['hang'] },
    ] });
    const cid = made.body.collaborationId;
    await wait(async () => { const g = await collabGet(s.call, c.taskId, cid); const h = g.children.find(x => x.childId === 'hang'); return h.state === 'RUNNING' && h.nativeSessionId && g; });
    const stop = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/stop`, { requestKey: 'cb-stop-1' });
    assert.equal(stop.status, 202); assert.equal(stop.body.state, 'APPLIED');
    const collab = await collabGet(s.call, c.taskId, cid);
    assert.equal(collab.state, 'CANCELLED'); assert.equal(collab.reasonCode, 'COLLABORATION_STOP');
    const byId = Object.fromEntries(collab.children.map(ch => [ch.childId, ch]));
    assert.equal(byId.hang.state, 'CANCELLED'); assert.equal(byId.hang.reasonCode, 'COLLABORATION_STOP');
    assert.equal(byId.hang.stopProof.method, process.platform === 'win32' ? 'WINDOWS_JOB_ACTIVE_ZERO' : process.platform === 'darwin' ? 'DARWIN_KQUEUE_NO_FORK_GROUP_ZERO' : 'OWNED_PROCESS_GROUP', 'running child drained with owned-Job proof');
    assert.equal(byId.behind.state, 'BLOCKED'); assert.equal(byId.behind.reasonCode, 'COLLABORATION_STOP');
    // idempotent: same key replays, second stop reports the terminal state
    const replay = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/stop`, { requestKey: 'cb-stop-1' });
    assert.equal(replay.body.state, 'APPLIED'); assert.equal(replay.body.requestKey, 'cb-stop-1');
    const again = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/stop`, { requestKey: 'cb-stop-2' });
    assert.equal(again.body.detail, 'Collaboration already cancelled');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/collab-nope/stop`, { requestKey: 'cb-stop-3' })).body.code, 'COLLABORATION_NOT_FOUND');
    // parent STOP propagates: a fresh running child is drained with proof
    const made2 = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-stop-p', children: [{ childId: 'hang2', goal: 'child-hang' }] });
    await wait(async () => { const g = await collabGet(s.call, c.taskId, made2.body.collaborationId); const h = g.children[0]; return h.state === 'RUNNING' && h.nativeSessionId && g; });
    const stopTask = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-task', intent: 'STOP', ...guard(d) });
    assert.equal(stopTask.body.state, 'APPLIED');
    const collab2 = await collabGet(s.call, c.taskId, made2.body.collaborationId);
    assert.equal(collab2.state, 'CANCELLED'); assert.equal(collab2.reasonCode, 'PARENT_STOP');
    assert.equal(collab2.children[0].state, 'CANCELLED'); assert.equal(collab2.children[0].reasonCode, 'PARENT_STOP');
    assert.equal(collab2.children[0].stopProof.method, process.platform === 'win32' ? 'WINDOWS_JOB_ACTIVE_ZERO' : process.platform === 'darwin' ? 'DARWIN_KQUEUE_NO_FORK_GROUP_ZERO' : 'OWNED_PROCESS_GROUP');
    const after = (await s.call('/tasks/' + c.taskId)).body; assert.equal(after.state, 'CANCELLED');
    // intake closed: no new collaborations on a stopped task
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-dead', children: [{ goal: 'child-write z.txt' }] })).body.code, 'COLLABORATION_TASK_INACTIVE');
  } finally { await s.host.close(); }
});

test('adoption: ALL/FILES adoption, idempotent replay, NO_CHANGES, deletions, conflict material', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'writefile base.txt'); const d = await succeeded(s.call, c.taskId);
    const workspace = d.workspace;
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-adopt', children: [
      { childId: 'w1', goal: 'child-write new-dir/new-file.txt child-write second.txt' },
      { childId: 'w2', goal: 'child-write README.md' },
      { childId: 'w3', goal: 'child-noop' },
      { childId: 'w4', goal: 'child-delete base.txt' },
    ] });
    const cid = made.body.collaborationId;
    const collab = await collabDone(s.call, c.taskId, cid);
    assert.equal(collab.state, 'COMPLETED');
    // FILES partial adoption: only the listed path lands
    const a1 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-1', childId: 'w1', mode: 'FILES', paths: ['new-dir/new-file.txt'] });
    assert.equal(a1.status, 202); assert.equal(a1.body.outcome, 'ADOPTED');
    assert.deepEqual(a1.body.files, [{ path: 'new-dir/new-file.txt', sha256: hash('collab write new-dir/new-file.txt'), action: 'write' }]);
    assert.equal(readFileSync(join(workspace, 'new-dir', 'new-file.txt'), 'utf8'), 'collab write new-dir/new-file.txt');
    assert.equal(existsSync(join(workspace, 'second.txt')), false, 'unlisted candidate path not adopted');
    // same requestKey replays; a second identical adoption records NO_CHANGES
    const r1 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-1', childId: 'w1', mode: 'FILES', paths: ['new-dir/new-file.txt'] });
    assert.equal(r1.body.replayed, true); assert.equal(r1.body.outcome, 'ADOPTED');
    const a2 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-2', childId: 'w1', mode: 'FILES', paths: ['new-dir/new-file.txt'] });
    assert.equal(a2.body.outcome, 'NO_CHANGES', 'identical re-adoption is an idempotent no-op');
    // remaining candidate paths adopt via ALL
    const a3 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-3', childId: 'w1', mode: 'ALL' });
    assert.equal(a3.body.outcome, 'ADOPTED');
    assert.deepEqual(a3.body.files.map(f => f.path), ['second.txt']);
    assert.equal(readFileSync(join(workspace, 'second.txt'), 'utf8'), 'collab write second.txt');
    const a4 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-4', childId: 'w1', mode: 'ALL' });
    assert.equal(a4.body.outcome, 'NO_CHANGES', 'everything already applied');
    // empty candidate diff → NO_CHANGES
    const a5 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-5', childId: 'w3', mode: 'ALL' });
    assert.equal(a5.body.outcome, 'NO_CHANGES');
    // deletion adoption removes the parent file only when it still equals baseline
    const a6 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-6', childId: 'w4', mode: 'ALL' });
    assert.equal(a6.body.outcome, 'ADOPTED');
    assert.deepEqual(a6.body.files, [{ path: 'base.txt', sha256: null, action: 'delete' }]);
    assert.equal(existsSync(join(workspace, 'base.txt')), false, 'deletion adopted as parent removal');
    // N20: parent drifted under an adopted baseline path → CONFLICT, nothing written
    writeFileSync(join(workspace, 'README.md'), 'parent drifted after baseline');
    const a7 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-7', childId: 'w2', mode: 'ALL' });
    assert.equal(a7.status, 409); assert.equal(a7.body.outcome, 'CONFLICT');
    assert.equal(a7.body.conflicts.length, 1); assert.equal(a7.body.conflicts[0].path, 'README.md'); assert.equal(a7.body.conflicts[0].kind, 'PARENT_DRIFT');
    assert.equal(a7.body.conflicts[0].candidateSha256, hash('collab write README.md'));
    assert.equal(a7.body.conflicts[0].baselineSha256, hash('baseline'));
    assert.equal(readFileSync(join(workspace, 'README.md'), 'utf8'), 'parent drifted after baseline', 'conflict writes nothing');
    // new-file collision: parent created the same path with different content
    const a8 = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-adopt-2', children: [{ childId: 'w5', goal: 'child-write collide.txt' }] });
    const cid2 = a8.body.collaborationId; await collabDone(s.call, c.taskId, cid2);
    writeFileSync(join(workspace, 'collide.txt'), 'parent-owned content');
    const a9 = await s.call(`/tasks/${c.taskId}/collaborations/${cid2}/adoptions`, { requestKey: 'ad-9', childId: 'w5', mode: 'ALL' });
    assert.equal(a9.status, 409); assert.equal(a9.body.conflicts[0].kind, 'PARENT_DRIFT'); assert.equal(a9.body.conflicts[0].baselineSha256, null);
    assert.equal(readFileSync(join(workspace, 'collide.txt'), 'utf8'), 'parent-owned content');
    // durable facts: adopted/conflict events and child adoption records, sha256s only
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'collaboration.adopted' && e.outcome === 'ADOPTED' && e.files.some(f => f.path === 'new-dir/new-file.txt' && f.sha256 === hash('collab write new-dir/new-file.txt'))));
    assert.ok(evs.some(e => e.kind === 'collaboration.adopted' && e.outcome === 'NO_CHANGES'));
    assert.ok(evs.some(e => e.kind === 'collaboration.adoption.conflict' && e.conflicts[0].path === 'README.md'));
    const w1 = (await collabGet(s.call, c.taskId, cid)).children.find(x => x.childId === 'w1');
    assert.deepEqual(w1.adoptions.map(a => a.outcome), ['ADOPTED', 'NO_CHANGES', 'ADOPTED', 'NO_CHANGES']);
    // adoption guards
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-x1', childId: 'w1', mode: 'BOGUS' })).body.code, 'INVALID_ADOPTION_MODE');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-x2', childId: 'nope', mode: 'ALL' })).body.code, 'COLLABORATION_CHILD_NOT_FOUND');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-x3', childId: 'w1', mode: 'FILES', paths: ['README.md'] })).body.code, 'ADOPTION_PATH_NOT_CANDIDATE');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-x4', childId: 'w1', mode: 'FILES', paths: ['../escape.txt'] })).body.code, 'INVALID_ADOPTION_PATHS');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-x5', childId: 'w1', mode: 'FILES', paths: [] })).body.code, 'INVALID_ADOPTION_PATHS');
  } finally { await s.host.close(); }
});

test('adoption on unfinished child refused; scope gate keeps writes inside the checkout', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); await succeeded(s.call, c.taskId);
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-guard', children: [
      { childId: 'hang', goal: 'child-hang' },
      { childId: 'escape', goal: 'child-write ../escape.txt' },
    ] });
    const cid = made.body.collaborationId;
    await wait(async () => { const g = await collabGet(s.call, c.taskId, cid); return g.children.find(x => x.childId === 'escape').state === 'SUCCEEDED' && g; });
    // a still-running child is not a finished candidate
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-run', childId: 'hang', mode: 'ALL' })).body.code, 'COLLABORATION_CHILD_NOT_FINISHED');
    // the escape write was scope-blocked inside the awaited gate; nothing landed
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'collaboration.child.scope_violation' && e.childId === 'escape'));
    assert.equal(existsSync(join(s.state, 'collab', cid, 'escape.txt')), false, 'blocked write never escaped the checkout');
    await s.call(`/tasks/${c.taskId}/collaborations/${cid}/stop`, { requestKey: 'cb-guard-stop' });
  } finally { await s.host.close(); }
});

test('host-wide running bound, validation, per-task limit, child budget', async () => {
  process.env.NWF_PI_MAX_ACTIVE_COLLAB_CHILDREN = '1';
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); await succeeded(s.call, c.taskId);
    // pool size 1: the second independent child waits for the first to finish
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-pool', children: [
      { childId: 'first', goal: 'child-write first.txt' },
      { childId: 'second', goal: 'child-write second.txt' },
    ] });
    const cid = made.body.collaborationId;
    const collab = await collabDone(s.call, c.taskId, cid);
    const evs = await events(s.call, c.taskId);
    const seq = (kind, childId) => evs.findIndex(e => e.kind === kind && e.childId === childId);
    assert.ok(seq('collaboration.child.started', 'second') > seq('collaboration.child.completed', 'first'), 'second child launched only after the pool slot freed');
    assert.deepEqual(collab.result, { succeeded: 2, failed: 0, cancelled: 0, blocked: 0 });
    // child SOFT budget: fixture reports 4 tokens; cap 1 exceeds at the gate
    const mb = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-budget', children: [{ childId: 'bb', goal: 'child-write b.txt', budget: { maxTotalTokens: '1' } }] });
    const cb = await collabDone(s.call, c.taskId, mb.body.collaborationId);
    assert.equal(cb.children[0].state, 'FAILED'); assert.equal(cb.children[0].reasonCode, 'BUDGET_EXCEEDED');
    assert.ok((await events(s.call, c.taskId)).some(e => e.kind === 'collaboration.child.budget_exceeded' && e.observed === 4));
    // validation
    for (const [body, code] of [
      [{ children: Array.from({ length: 9 }, (_, i) => ({ goal: 'g' + i })) }, 'INVALID_COLLABORATION_CHILDREN'],
      [{ children: [] }, 'INVALID_COLLABORATION_CHILDREN'],
      [{ children: [{ childId: 'BAD!', goal: 'x' }] }, 'INVALID_COLLABORATION_CHILD_ID'],
      [{ children: [{ childId: 'a', goal: 'x' }, { childId: 'a', goal: 'y' }] }, 'INVALID_COLLABORATION_CHILD_ID'],
      [{ children: [{ goal: '' }] }, 'INVALID_COLLABORATION_GOAL'],
      [{ children: [{ goal: 'x'.repeat(17000) }] }, 'INVALID_COLLABORATION_GOAL'],
      [{ children: [{ childId: 'a', goal: 'x', dependsOn: ['ghost'] }] }, 'COLLABORATION_DEPENDENCY_UNKNOWN'],
      [{ children: [{ childId: 'a', goal: 'x', dependsOn: ['b'] }, { childId: 'b', goal: 'y', dependsOn: ['a'] }] }, 'COLLABORATION_DEPENDENCY_CYCLE'],
      [{ children: [{ childId: 'a', goal: 'x', dependsOn: ['a'] }] }, 'COLLABORATION_DEPENDENCY_CYCLE'],
      [{ children: [{ goal: 'x', contextPackageIds: ['pkg-nope'] }] }, 'PACKAGE_NOT_FOUND'],
      [{ children: [{ goal: 'x', budget: { maxTotalTokens: '5', enforcement: 'HARD' } }] }, 'BUDGET_ENFORCEMENT_UNSUPPORTED'],
      [{ children: [{ goal: 'x' }], extra: 1 }, 'UNEXPECTED_FIELD'],
      [{ children: [{ goal: 'x', unexpected: 1 }] }, 'UNEXPECTED_FIELD'],
    ]) assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'bad-' + Math.random().toString(36).slice(2), ...body })).body.code, code, JSON.stringify(body).slice(0, 80));
    // per-task active collaboration limit: four RUNNING collabs, fifth refused
    for (const key of ['l1', 'l2', 'l3', 'l4']) await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-' + key, children: [{ goal: 'child-hang' }] });
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`)).body.items.filter(x => x.state === 'RUNNING').length, 4);
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-l5', children: [{ goal: 'x' }] })).body.code, 'COLLABORATION_LIMIT');
    for (const coll of (await s.call(`/tasks/${c.taskId}/collaborations`)).body.items.filter(x => x.state === 'RUNNING')) await s.call(`/tasks/${c.taskId}/collaborations/${coll.collaborationId}/stop`, { requestKey: 'cleanup-' + coll.collaborationId });
  } finally { delete process.env.NWF_PI_MAX_ACTIVE_COLLAB_CHILDREN; await s.host.close(); }
});

test('restart keeps completed child history and reconciles running children; adoption survives restart', async () => {
  const s = await setup();
  const c = await create(s.call, 'hello');
  await succeeded(s.call, c.taskId);
  const done = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-r1', children: [{ childId: 'keep', goal: 'child-write keep-me.txt' }] });
  const cid1 = done.body.collaborationId;
  await collabDone(s.call, c.taskId, cid1);
  const hang = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-r2', children: [{ childId: 'hang', goal: 'child-hang' }, { childId: 'pend', goal: 'child-write p.txt', dependsOn: ['hang'] }] });
  const cid2 = hang.body.collaborationId;
  await wait(async () => { const g = await collabGet(s.call, c.taskId, cid2); const h = g.children.find(x => x.childId === 'hang'); return h.state === 'RUNNING' && h.nativeSessionId && g; });
  await s.host.close();
  const host = new PiHost({ state: s.state, token, childModule: fixture }); const call = client(await host.listen());
  try {
    // completed child history intact: states, candidate and the checkout itself
    const g1 = await collabGet(call, c.taskId, cid1);
    assert.equal(g1.state, 'COMPLETED'); assert.equal(g1.children[0].state, 'SUCCEEDED');
    assert.equal(readFileSync(join(s.state, 'collab', cid1, 'keep', 'workspace', 'keep-me.txt'), 'utf8'), 'collab write keep-me.txt');
    // the interrupted collaboration reconciled: running child CANCELLED, pending BLOCKED
    const g2 = await collabGet(call, c.taskId, cid2);
    assert.equal(g2.state, 'CANCELLED');
    const byId = Object.fromEntries(g2.children.map(ch => [ch.childId, ch]));
    assert.equal(byId.hang.state, 'CANCELLED'); assert.ok(['HOST_CLOSE', 'HOST_RESTART_JOB_CLOSED'].includes(byId.hang.reasonCode));
    assert.equal(byId.pend.state, 'BLOCKED'); assert.ok(['HOST_CLOSE', 'HOST_RESTART_JOB_CLOSED'].includes(byId.pend.reasonCode));
    const evs = (await call(`/tasks/${c.taskId}/events?limit=100`)).body.items;
    assert.ok(evs.some(e => e.kind === 'collaboration.stopped' && e.collaborationId === cid2));
    // adoption still works against the persisted baseline/checkout after restart
    const adopt = await call(`/tasks/${c.taskId}/collaborations/${cid1}/adoptions`, { requestKey: 'ad-restart', childId: 'keep', mode: 'ALL' });
    assert.equal(adopt.body.outcome, 'ADOPTED');
    const workspace = (await call('/tasks/' + c.taskId)).body.workspace;
    assert.equal(readFileSync(join(workspace, 'keep-me.txt'), 'utf8'), 'collab write keep-me.txt');
  } finally { await host.close(); }
});

test('audit projection carries collaboration facts without bodies', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-au', name: 'audit', children: [{ childId: 'a', goal: 'SECRET-GOAL-BODY-991 child-write a.txt' }] });
    const cid = made.body.collaborationId;
    await collabDone(s.call, c.taskId, cid);
    await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-au', childId: 'a', mode: 'ALL' });
    const audit = (await s.call(`/worker-host/v1/tasks/${c.taskId}/audit`)).body;
    const kinds = audit.items.map(i => i.kind);
    for (const kind of ['collaboration.created', 'collaboration.child.started', 'collaboration.child.completed', 'collaboration.completed', 'collaboration.adopted'])
      assert.ok(kinds.includes(kind), 'audit missing ' + kind);
    assert.ok(audit.commands.some(o => o.kind === 'COLLABORATION_CREATE' && o.requestKey === 'cb-au'));
    assert.ok(audit.commands.some(o => o.kind === 'COLLABORATION_ADOPT' && o.requestKey === 'ad-au'));
    const adopted = audit.items.find(i => i.kind === 'collaboration.adopted');
    assert.ok(adopted.files.every(f => typeof f.sha256 === 'string' || f.sha256 === null), 'adoption audit lists sha256s');
    assert.ok(JSON.stringify(audit).includes('SECRET-GOAL-BODY-991') === false, 'audit carries no goal body');
    const diagnostics = (await s.call(`/worker-host/v1/tasks/${c.taskId}/diagnostics?redact=content`)).body;
    assert.ok(JSON.stringify(diagnostics).includes('SECRET-GOAL-BODY-991') === false, 'diagnostics carries no goal body');
  } finally { await s.host.close(); }
});
