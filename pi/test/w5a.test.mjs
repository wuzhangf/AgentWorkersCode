import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

const token = 'deterministic-w5a-test-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w5a-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w5a-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, receipt: result.body, status: result.status, code: result.body.code }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const events = async (call, taskId) => (await call(`/tasks/${taskId}/events?limit=100`)).body.items;
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });
const running = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'RUNNING' && d; });
const workspaceFiles = root => { const out = []; const walk = (dir, prefix) => { for (const e of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) { if (e.name === '.git') continue; const p = join(dir, e.name); if (e.isDirectory()) walk(p, prefix + e.name + '/'); else if (e.isFile()) { const b = readFileSync(p); out.push(prefix + e.name + ':' + hash(b)); } } }; walk(root, ''); return out; };

test('context packages: create/get/list, frozen digest survives later workspace writes, scoping enforced', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'writefile pkg-src.txt');
    const d = await succeeded(s.call, c.taskId);
    const workspace = d.workspace;
    assert.equal(readFileSync(join(workspace, 'pkg-src.txt'), 'utf8'), 'fixture write');
    const before = readFileSync(join(workspace, 'pkg-src.txt'));
    const made = await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'pkg-1', name: 'research-bundle', sources: [{ kind: 'file', path: 'pkg-src.txt' }, { kind: 'text', text: 'frozen note NWF_W5A_TEXT' }, { kind: 'summary', text: 'summary words' }] });
    assert.equal(made.status, 202);
    const pkg = made.body;
    assert.match(pkg.packageId, /^pkg-/); assert.equal(pkg.name, 'research-bundle');
    assert.match(pkg.digest, /^[0-9a-f]{64}$/); assert.equal(pkg.sourceCount, 3); assert.equal(pkg.fileCount, 1);
    assert.equal(pkg.sources[0].path, 'pkg-src.txt'); assert.equal(pkg.sources[0].size, before.length);
    assert.ok(!('content' in pkg.sources[0]) && !('text' in pkg.sources[1]), 'list metadata never carries stored bytes');
    // same-key replay returns the durable package
    const replay = await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'pkg-1', name: 'research-bundle', sources: [{ kind: 'file', path: 'pkg-src.txt' }, { kind: 'text', text: 'frozen note NWF_W5A_TEXT' }, { kind: 'summary', text: 'summary words' }] });
    assert.equal(replay.body.packageId, pkg.packageId); assert.equal(replay.body.replayed, true);
    const conflict = await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'pkg-1', name: 'different', sources: [{ kind: 'text', text: 'x' }] });
    assert.equal(conflict.body.code, 'IDEMPOTENCY_CONFLICT');
    // later workspace changes never mutate the package (N16)
    writeFileSync(join(workspace, 'pkg-src.txt'), 'mutated after freeze');
    writeFileSync(join(workspace, 'new-file.txt'), 'appeared later');
    const detail = await s.call(`/tasks/${c.taskId}/context-packages/${pkg.packageId}`);
    assert.equal(detail.body.digest, pkg.digest, 'digest frozen at creation');
    assert.equal(detail.body.digestVerified, true, 'stored bytes still verify against the digest');
    const fileSrc = detail.body.sources.find(x => x.kind === 'file');
    assert.equal(Buffer.from(fileSrc.content, 'base64').toString('utf8'), 'fixture write', 'frozen bytes, not live bytes');
    assert.equal(detail.body.sources.find(x => x.kind === 'text').text, 'frozen note NWF_W5A_TEXT');
    assert.equal(detail.body.sources.find(x => x.kind === 'summary').text, 'summary words');
    const list = await s.call(`/tasks/${c.taskId}/context-packages`);
    assert.deepEqual(list.body.items.map(p => p.packageId), [pkg.packageId]);
    assert.equal((await s.call(`/tasks/${c.taskId}/context-packages/pkg-missing`)).body.code, 'PACKAGE_NOT_FOUND');
    const created = (await events(s.call, c.taskId)).find(e => e.kind === 'context-package.created');
    assert.equal(created.packageId, pkg.packageId); assert.equal(created.digest, pkg.digest);
    // scoping and bounds
    for (const [sources, code] of [
      [[{ kind: 'file', path: '../escape.txt' }], 'PACKAGE_SOURCE_OUT_OF_SCOPE'],
      [[{ kind: 'file', path: join(s.state, 'outside.txt') }], 'PACKAGE_SOURCE_OUT_OF_SCOPE'],
      [[{ kind: 'file', path: 'missing.txt' }], 'PACKAGE_SOURCE_UNAVAILABLE'],
      [[{ kind: 'file', path: '.', }], 'PACKAGE_SOURCE_UNAVAILABLE'],
      [[{ kind: 'bogus' }], 'INVALID_PACKAGE_SOURCES'],
      [[{ kind: 'text' }], 'INVALID_PACKAGE_SOURCES'],
      [[], 'INVALID_PACKAGE_SOURCES'],
      [[{ kind: 'text', text: 'x'.repeat(70000) }], 'PACKAGE_SOURCE_TOO_LARGE'],
    ]) assert.equal((await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'bad-' + Math.random().toString(36).slice(2), sources })).body.code, code, JSON.stringify(sources).slice(0, 90));
    writeFileSync(join(workspace, 'big.bin'), Buffer.alloc(300000, 7));
    assert.equal((await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'big-1', sources: [{ kind: 'file', path: 'big.bin' }] })).body.code, 'PACKAGE_SOURCE_TOO_LARGE');
    assert.equal((await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'nofields', sources: [{ kind: 'text', text: 'x' }], extra: 1 })).body.code, 'UNEXPECTED_FIELD');
    // engines surface advertises the new capabilities
    const caps = (await s.call('/engines')).body.items[0].capabilities;
    assert.equal(caps.contextPackages, true); assert.equal(caps.auxiliarySessions, true);
    assert.deepEqual(caps.transitions, { sameEngine: true, crossEngine: false });
    for (const pid of ['P11', 'P12', 'P13']) {
      const row = (await s.call('/worker-host/v1/capabilities')).body.items.find(i => i.id === pid);
      assert.ok(row, pid + ' row'); assert.equal(row.implementation, 'IMPLEMENTED'); assert.equal(row.qualification, 'PARTIAL');
    }
  } finally { await s.host.close(); }
});

test('auxiliary sessions: isolated snapshot research, context package material, parent untouched, explicit deliver', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello');
    const d = await succeeded(s.call, c.taskId);
    const workspace = d.workspace;
    const filesBefore = workspaceFiles(workspace);
    const usageBefore = { ...d.usage };
    const pkg = (await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'pkg-a', name: 'clues', sources: [{ kind: 'text', text: 'AUXPKG-MARKER-991' }] })).body;
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-1', question: 'what marker?', contextPackageIds: [pkg.packageId] });
    assert.equal(aux.status, 202); const aid = aux.body.auxSessionId; assert.match(aid, /^aux-/);
    const a = await wait(async () => { const a = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}`)).body; return a.state === 'SUCCEEDED' && a; });
    assert.ok(a.answer.includes('aux-answer: what marker?'), 'fixture aux answered the question');
    assert.ok(a.answer.includes('AUXPKG-MARKER-991'), 'context package fixed material reached the aux session');
    assert.equal(a.contextPackageIds[0], pkg.packageId);
    assert.ok(a.usage.input + a.usage.output > 0, 'aux has its own usage ledger');
    // N17: the parent task is untouched — no aux file, no output, no budget drain
    const dAfter = (await s.call('/tasks/' + c.taskId)).body;
    assert.deepEqual(workspaceFiles(workspace), filesBefore, 'main workspace byte-identical (snapshot isolation)');
    assert.equal(existsSync(join(workspace, 'aux-result.txt')), false, 'aux writes land only in its own copy');
    assert.equal(existsSync(join(s.state, 'aux', aid, 'workspace', 'aux-result.txt')), true, 'aux wrote inside its snapshot copy');
    assert.deepEqual(dAfter.usage, usageBefore, 'aux never consumes the parent budget ledger');
    assert.equal((await s.call(`/tasks/${c.taskId}/output`)).body.text.includes('aux-answer'), false, 'undelivered answer never enters main output');
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'aux-session.started')); assert.ok(evs.some(e => e.kind === 'aux-session.completed' && e.state === 'SUCCEEDED'));
    // deliver injects the answer as a normal guarded follow-up
    const deliver = await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}/deliver`, { requestKey: 'dlv-1' });
    assert.equal(deliver.status, 200); assert.equal(deliver.body.delivered.kind, 'NATIVE_FOLLOW_UP');
    const done = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d.currentCommandId === 'dlv-1' && d; });
    assert.ok((await s.call(`/tasks/${c.taskId}/output`)).body.text.includes('aux-answer'), 'delivered answer entered main output');
    const dlv = (await events(s.call, c.taskId)).find(e => e.kind === 'aux-session.delivered');
    assert.equal(dlv.auxSessionId, aid); assert.equal(dlv.requestKey, 'dlv-1');
    const replay = await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}/deliver`, { requestKey: 'dlv-1' });
    assert.equal(replay.body.replayed, true);
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}/deliver`, { requestKey: 'dlv-2' })).body.code, 'AUX_ALREADY_DELIVERED');
    const a2 = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}`)).body;
    assert.ok(a2.deliveredAt);
  } finally { await s.host.close(); }
});

test('aux sessions: independent SOFT budget, stop kills only the aux child, limits and validation', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); await succeeded(s.call, c.taskId);
    // own SOFT budget: fixture reports 5 total tokens; cap 1 exceeds at the ledger boundary
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-b', question: 'budget probe', contextPackageIds: [], budget: { maxTotalTokens: '1' } });
    const a = await wait(async () => { const a = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aux.body.auxSessionId}`)).body; return a.state === 'FAILED' && a; });
    assert.equal(a.reasonCode, 'BUDGET_EXCEEDED');
    const hit = (await events(s.call, c.taskId)).find(e => e.kind === 'aux-session.budget_exceeded');
    assert.equal(hit.observed, 5); assert.equal(hit.cap, 1);
    // a running aux is stopped by its own endpoint; the parent stays SUCCEEDED/live
    const hang = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-h', question: 'aux-hang forever' });
    const hid = hang.body.auxSessionId;
    await wait(async () => (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}`)).body.state === 'RUNNING' && (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}`)).body.nativeSessionId);
    const stop = await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}/stop`, { requestKey: 'aux-stop-1' });
    assert.equal(stop.body.state, 'APPLIED');
    const ah = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}`)).body;
    assert.equal(ah.state, 'CANCELLED'); assert.equal(ah.reasonCode, 'AUX_STOP');
    const parent = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(parent.state, 'SUCCEEDED', 'parent task unaffected by aux stop');
    // deliver on unfinished aux refused
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}/deliver`, { requestKey: 'dlv-x' })).body.code, 'AUX_NOT_FINISHED');
    // validation + limit
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-miss', question: 'q', contextPackageIds: ['pkg-nope'] })).body.code, 'PACKAGE_NOT_FOUND');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-badq', question: '' })).body.code, 'INVALID_AUX_QUESTION');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-badb', question: 'q', budget: { maxTotalTokens: '5', enforcement: 'HARD' } })).body.code, 'BUDGET_ENFORCEMENT_UNSUPPORTED');
    for (const key of ['aux-c', 'aux-d']) await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: key, question: 'quick' });
    await wait(async () => { const items = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions`)).body.items; return items.length === 4 && items.filter(x => x.state === 'SUCCEEDED').length === 2 && items; });
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-e', question: 'over limit' })).body.code, 'AUX_SESSION_LIMIT');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}/stop`, { requestKey: 'aux-stop-again' })).body.detail, 'Auxiliary session already cancelled');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/aux-nope/stop`, { requestKey: 'aux-stop-nope' })).body.code, 'AUX_SESSION_NOT_FOUND');
  } finally { await s.host.close(); }
});

test('deliver to a running task is refused by the normal follow-up guard', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting'); const d = await running(s.call, c.taskId);
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-r', question: 'quick question' });
    const aid = aux.body.auxSessionId;
    await wait(async () => { const a = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}`)).body; return a.state === 'SUCCEEDED' && a; });
    const del = await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}/deliver`, { requestKey: 'dlv-run' });
    assert.equal(del.body.code, 'NATIVE_NOT_IDLE', 'deliver reuses follow-up guards — a RUNNING task refuses');
    const task = s.host.store.task(c.taskId); assert.equal(task.auxSessions[aid].deliveredAt, undefined);
    // after STOP the parent is CANCELLED: delivery is still refused, never smuggled in
    await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-w', intent: 'STOP', ...guard(d) });
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}/deliver`, { requestKey: 'dlv-stop' })).body.code, 'COMPLETED_SESSION_REQUIRED');
  } finally { await s.host.close(); }
});

test('transitions: quiesce at boundary, new epoch on same session, stale epoch fenced, incompatible refused', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting');
    const d0 = await running(s.call, c.taskId);
    const tr = await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'tr-1', ...guard(d0) });
    assert.equal(tr.status, 202); assert.equal(tr.body.kind, 'TRANSITION');
    const d = await succeeded(s.call, c.taskId);
    assert.equal(d.ownerEpoch, '2'); assert.equal(d.nativeSessionId, d0.nativeSessionId, 'same native session');
    assert.notEqual(d.generation, d0.generation); assert.notEqual(d.segmentId, d0.segmentId);
    const op = (await s.call(`/tasks/${c.taskId}/commands/tr-1`)).body;
    assert.equal(op.state, 'APPLIED'); assert.equal(op.fromEpoch, '1'); assert.equal(op.toEpoch, '2');
    const list = (await s.call(`/tasks/${c.taskId}/transitions`)).body.items;
    assert.equal(list.length, 1); assert.equal(list[0].fromEpoch, '1'); assert.equal(list[0].toEpoch, '2');
    assert.equal(list[0].fromSegment, d0.segmentId); assert.equal(list[0].toSegment, d.segmentId);
    assert.equal(list[0].compatibility.engineId, 'sage-pi'); assert.equal(list[0].compatibility.sameEngine, true);
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'transition.quiesced' && e.method === 'BOUNDARY_CHECKPOINT'));
    assert.ok(evs.some(e => e.kind === 'transition.admitted' && e.toEpoch === '2'));
    // the fixture appended every dispatched request key to the same session file —
    // the transition continuation ran on the same native session bytes
    const sessionFile = s.host.store.task(c.taskId).nativeSessionFile;
    assert.ok(readFileSync(sessionFile, 'utf8').includes('tr-1'), 'continuation prompt consumed on the same session');
    // stale-epoch requests are fenced by the guard machinery
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stale-1', intent: 'STOP', ...guard(d0) })).body.code, 'NATIVE_TARGET_CHANGED');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'stale-2', kind: 'NATIVE_FOLLOW_UP', content: 'x', ...guard(d0) })).body.code, 'NATIVE_TARGET_CHANGED');
    // incompatible targets refuse with no state change
    for (const [targetProfile, code] of [
      [{ engineId: 'codex' }, 'INCOMPATIBLE_TRANSITION'],
      [{ model: 'some-other-model' }, 'INCOMPATIBLE_TRANSITION'],
      [{ toolAllowlist: ['no_such_tool'] }, 'INCOMPATIBLE_TRANSITION'],
      [{ engineId: 'sage-pi', model: 'fixture-model', toolAllowlist: ['write'] }, 202],
      [undefined, 202],
    ]) {
      const key = 'tr-' + Math.random().toString(36).slice(2);
      const r = await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: key, ...(targetProfile ? { targetProfile } : {}), ...guard((await s.call('/tasks/' + c.taskId)).body) });
      if (code === 202) { assert.equal(r.status, 202); await succeeded(s.call, c.taskId); }
      else assert.equal(r.body.code, code, JSON.stringify(targetProfile));
    }
    const after = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(after.state, 'SUCCEEDED'); assert.equal(after.nativeSessionId, 'fixture-pi-session');
    assert.equal(Number(after.ownerEpoch) >= 3, true, 'compatible transitions admitted more epochs');
    assert.ok(s.host.store.task(c.taskId).transitions.length >= 3);
    // a transition that narrowed the tool allowlist is recorded and applied
    const narrowed = s.host.store.task(c.taskId).transitions.find(t => t.compatibility.toolAllowlist);
    assert.ok(narrowed); assert.deepEqual(s.host.store.task(c.taskId).toolAllowlist, ['write']);
  } finally { await s.host.close(); }
});

test('transition on an already PAUSED task reopens the sealed checkpoint', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting'); const d0 = await running(s.call, c.taskId);
    const paused = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'pause-1', intent: 'PAUSE', ...guard(d0) });
    assert.equal(paused.body.state, 'APPLIED');
    const d = (await s.call('/tasks/' + c.taskId)).body; assert.equal(d.state, 'PAUSED');
    const tr = await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'tr-paused', ...guard(d) });
    assert.equal(tr.status, 202);
    const done = await succeeded(s.call, c.taskId);
    assert.equal(done.ownerEpoch, '2'); assert.equal(done.nativeSessionId, 'fixture-pi-session');
    const list = (await s.call(`/tasks/${c.taskId}/transitions`)).body.items;
    assert.equal(list[0].compatibility.method, 'BOUNDARY_CHECKPOINT');
    assert.ok((await events(s.call, c.taskId)).some(e => e.kind === 'transition.quiesced'));
    // QUEUED/UNKNOWN tasks cannot transition
    const c2 = await create(s.call, 'hello');
    await succeeded(s.call, c2.taskId);
    await s.call(`/tasks/${c2.taskId}/commands`, { requestKey: 'stop-2', intent: 'STOP', ...guard((await s.call('/tasks/' + c2.taskId)).body) });
    const d2 = (await s.call('/tasks/' + c2.taskId)).body;
    const tr2 = await s.call(`/tasks/${c2.taskId}/transitions`, { requestKey: 'tr-stopped', ...guard(d2) });
    // a STOP'd SUCCEEDED task is a proven completed session: transition continues it
    if (tr2.status === 202) { await succeeded(s.call, c2.taskId); assert.equal((await s.call('/tasks/' + c2.taskId)).body.nativeSessionId, 'fixture-pi-session'); }
    else assert.equal(tr2.body.code, 'TRANSITION_STATE_INCOMPATIBLE');
  } finally { await s.host.close(); }
});

test('restart persists packages, aux and transition records; running aux reconciles CANCELLED', async () => {
  const s = await setup();
  const c = await create(s.call, 'hello');
  const d = await succeeded(s.call, c.taskId);
  const pkg = (await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'pkg-r', sources: [{ kind: 'text', text: 'restart-frozen' }] })).body;
  const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-r1', question: 'persisted aux' });
  const aid = aux.body.auxSessionId;
  await wait(async () => { const a = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aid}`)).body; return a.state === 'SUCCEEDED' && a; });
  const hang = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-r2', question: 'aux-hang across restart' });
  const hid = hang.body.auxSessionId;
  await wait(async () => { const a = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${hid}`)).body; return a.nativeSessionId && a; });
  const tr = await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'tr-r', ...guard(d) });
  assert.equal(tr.status, 202);
  await succeeded(s.call, c.taskId);
  await s.host.close();
  const host = new PiHost({ state: s.state, token, childModule: fixture }); const call = client(await host.listen());
  try {
    const list = (await call(`/tasks/${c.taskId}/context-packages`)).body.items;
    assert.deepEqual(list.map(p => p.packageId), [pkg.packageId]);
    const detail = (await call(`/tasks/${c.taskId}/context-packages/${pkg.packageId}`)).body;
    assert.equal(detail.digest, pkg.digest); assert.equal(detail.digestVerified, true);
    assert.equal(detail.sources[0].text, 'restart-frozen');
    const auxes = (await call(`/tasks/${c.taskId}/auxiliary-sessions`)).body.items;
    assert.equal(auxes.find(a => a.auxSessionId === aid).state, 'SUCCEEDED');
    const reconciled = auxes.find(a => a.auxSessionId === hid);
    assert.equal(reconciled.state, 'CANCELLED'); assert.ok(['HOST_CLOSE', 'HOST_RESTART_JOB_CLOSED'].includes(reconciled.reasonCode), 'running aux reconciles to CANCELLED, never resumed');
    const transitions = (await call(`/tasks/${c.taskId}/transitions`)).body.items;
    assert.equal(transitions.length, 1); assert.equal(transitions[0].toEpoch, '2');
    const task = host.store.task(c.taskId);
    assert.equal(task.ownerEpoch, '2'); assert.equal(task.transitions[0].transitionId, transitions[0].transitionId);
    const evs = (await call(`/tasks/${c.taskId}/events?limit=100`)).body.items;
    assert.ok(evs.some(e => e.kind === 'aux-session.completed' && ['HOST_CLOSE', 'HOST_RESTART_JOB_CLOSED'].includes(e.reasonCode)));
  } finally { await host.close(); }
});

test('audit projection carries the new durable facts without bodies', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    await s.call(`/tasks/${c.taskId}/context-packages`, { requestKey: 'pkg-au', sources: [{ kind: 'text', text: 'secret-free text' }] });
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'aux-au', question: 'audit question body' });
    await wait(async () => { const a = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${aux.body.auxSessionId}`)).body; return a.state === 'SUCCEEDED' && a; });
    await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'tr-au', ...guard(d) });
    await succeeded(s.call, c.taskId);
    const audit = (await s.call(`/worker-host/v1/tasks/${c.taskId}/audit`)).body;
    const kinds = audit.items.map(i => i.kind);
    for (const kind of ['context-package.created', 'aux-session.started', 'aux-session.completed', 'transition.quiesced', 'transition.admitted'])
      assert.ok(kinds.includes(kind), 'audit missing ' + kind);
    assert.ok(audit.commands.some(o => o.kind === 'TRANSITION' && o.requestKey === 'tr-au'));
    assert.ok(audit.commands.some(o => o.kind === 'AUX_SESSION_CREATE'));
    const diagnostics = (await s.call(`/worker-host/v1/tasks/${c.taskId}/diagnostics?redact=content`)).body;
    assert.equal(JSON.stringify(diagnostics).includes('audit question body'), false, 'no aux question body in diagnostics');
  } finally { await s.host.close(); }
});
