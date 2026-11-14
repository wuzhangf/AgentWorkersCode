import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

const token = 'deterministic-w5c-test-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w5c-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w5c-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port), put: putter(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
function putter(port) { return async (path, bytes, sha) => { const response = await fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: 'PUT', headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/octet-stream', 'x-chunk-sha256': sha }, body: bytes }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi', ...(extra.policyPresetId ? { policyPresetId: extra.policyPresetId } : {}) })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, receipt: result.body, status: result.status, code: result.body.code }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const events = async (call, taskId) => (await call(`/tasks/${taskId}/events?limit=100`)).body.items;
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });
const running = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'RUNNING' && d; });
const collabDone = async (call, taskId, collabId) => wait(async () => { const c = (await call(`/tasks/${taskId}/collaborations/${collabId}`)).body; return ['COMPLETED', 'CANCELLED'].includes(c.state) && c; });
const collabGet = async (call, taskId, collabId) => (await call(`/tasks/${taskId}/collaborations/${collabId}`)).body;
const auxGet = async (call, taskId, auxId) => (await call(`/tasks/${taskId}/auxiliary-sessions/${auxId}`)).body;

test('source capabilities + execution policy projection and policy presets', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const detail = (await s.call('/tasks/' + c.taskId)).body;
    const sc = detail.sourceCapabilities;
    assert.equal(sc.schemaVersion, 'source-capabilities/1');
    assert.ok(typeof sc.observedAt === 'string' && typeof sc.revision === 'string' && sc.revision.length > 8);
    const expected = ['sideQuestion.create', 'collaboration.create', 'collaboration.preflight', 'childMessages.read', 'adoption.preview', 'adoption.apply', 'handoff.export', 'handoff.seal', 'materials.upload', 'message.materials', 'voice.transcribe', 'policy.read', 'policy.presets', 'stop.affectedChildren'];
    for (const key of expected) {
      const a = sc.actions[key]; assert.ok(a, 'action ' + key);
      assert.ok(['AVAILABLE', 'UNSUPPORTED', 'NOT_INTEGRATED', 'BLOCKED_BY_STATE'].includes(a.state), key + ' state ' + a.state);
      assert.ok('reasonCode' in a || a.state === 'AVAILABLE', key + ' reasonCode field');
      assert.ok(typeof a.semantics === 'string' || a.limits, key + ' limits/semantics');
    }
    assert.equal(sc.actions['voice.transcribe'].state, 'NOT_INTEGRATED');
    const policy = detail.executionPolicy;
    assert.equal(policy.frozen, true); assert.equal(typeof policy.sandbox, 'string'); assert.equal(policy.approvalPolicy, 'HUMAN_VIA_ASK_USER_TOOL');
    const engines = (await s.call('/engines')).body.items[0];
    assert.ok(engines.executionPolicy, 'engines executionPolicy');
    assert.deepEqual(engines.executionPolicy.policyPresets, ['standard', 'read-only']);
    assert.equal(engines.executionPolicy.frozen, false);
    // preset selection freezes into the task; unknown presets refuse
    const pf = (await s.call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi', policyPresetId: 'read-only' })).body;
    assert.equal(pf.policyPreset.presetId, 'read-only'); assert.equal(pf.policyPreset.effectiveRange, 'NEW_TASKS');
    const c2 = await create(s.call, 'hello', { policyPresetId: 'read-only' });
    const d2 = await succeeded(s.call, c2.taskId);
    assert.equal(d2.executionPolicy.policyPreset.presetId, 'read-only');
    assert.ok(Array.isArray(d2.executionPolicy.toolAllowlist) && d2.executionPolicy.toolAllowlist.every(t => !['write', 'edit', 'bash'].includes(t)), 'read-only preset narrows tools');
    assert.equal((await s.call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi', policyPresetId: 'bogus' })).body.code, 'POLICY_PRESET_UNKNOWN');
    for (const pid of ['P24', 'P25', 'P26', 'P27', 'P29', 'P30']) {
      const row = (await s.call('/worker-host/v1/capabilities')).body.items.find(i => i.id === pid);
      assert.ok(row, pid + ' row'); assert.equal(row.implementation, 'IMPLEMENTED');
    }
  } finally { await s.host.close(); }
});

test('child messages: durable capture, pagination, coverage levels, ownership', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'writefile seed.txt'); await succeeded(s.call, c.taskId);
    // aux stream: ASSIGNMENT + STATUS + ASSISTANT_MESSAGE + RETURN
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'ax-msg-1', question: 'aux question one' });
    const auxId = aux.body.auxSessionId;
    await wait(async () => (await auxGet(s.call, c.taskId, auxId)).state === 'SUCCEEDED');
    const page1 = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages`)).body;
    assert.equal(page1.schemaVersion, 'child-messages/1');
    assert.equal(page1.coverage, 'FULL_SINCE_CAPTURE'); assert.ok(page1.captureStartedAt);
    const kinds = page1.items.map(i => i.kind);
    for (const k of ['ASSIGNMENT', 'STATUS', 'ASSISTANT_MESSAGE', 'RETURN']) assert.ok(kinds.includes(k), 'aux kind ' + k + ' in ' + kinds.join(','));
    assert.equal(page1.items[0].kind, 'ASSIGNMENT'); assert.match(page1.items[0].text, /aux question one/);
    assert.equal(page1.items.at(-1).kind, 'RETURN'); assert.equal(page1.items.at(-1).status, 'SUCCEEDED');
    assert.match(page1.items[0].seq, /^[0-9]+$/);
    const seqs = page1.items.map(i => Number(i.seq));
    assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b), 'monotonic sequence');
    // pagination: latest page by default, cursor forward, before backward, mutual exclusion
    const latest = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages?limit=2`)).body;
    assert.equal(latest.items.length, 2); assert.equal(latest.hasMoreBefore, true); assert.equal(latest.hasMoreAfter, false);
    const forward = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages?cursor=${page1.items[0].seq}&limit=2`)).body;
    assert.equal(forward.items[0].seq, '2');
    const back = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages?before=${latest.items[0].seq}&limit=2`)).body;
    assert.ok(back.items.every(i => Number(i.seq) < Number(latest.items[0].seq)) && back.items.length >= 1);
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages?before=1&cursor=1`)).body.code, 'INVALID_QUERY');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages?limit=500`)).body.code, 'INVALID_QUERY');
    // collab stream: tool call/result kinds captured only for public facts
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-msg', children: [
      { childId: 'worker', goal: 'child-write cm.txt' },
      { childId: 'blocked', goal: 'child-write cm2.txt', dependsOn: ['worker'] },
    ] });
    const cid = made.body.collaborationId;
    await collabDone(s.call, c.taskId, cid);
    const wpage = (await s.call(`/tasks/${c.taskId}/collaborations/${cid}/children/worker/messages`)).body;
    assert.equal(wpage.coverage, 'FULL_SINCE_CAPTURE'); assert.equal(wpage.childState, 'SUCCEEDED');
    const wk = wpage.items.map(i => i.kind);
    for (const k of ['ASSIGNMENT', 'STATUS', 'TOOL_CALL', 'TOOL_RESULT', 'ASSISTANT_MESSAGE', 'RETURN']) assert.ok(wk.includes(k), 'collab kind ' + k);
    const tool = wpage.items.find(i => i.kind === 'TOOL_CALL'); assert.equal(tool.eventRef, 'tool-cw-0');
    // blocked sibling: dependency satisfied here, so run a real blocked case
    const made2 = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-msg-2', children: [
      { childId: 'bad', goal: 'child-fail' },
      { childId: 'never', goal: 'child-write n.txt', dependsOn: ['bad'] },
    ] });
    const cid2 = made2.body.collaborationId; await collabDone(s.call, c.taskId, cid2);
    const never = (await s.call(`/tasks/${c.taskId}/collaborations/${cid2}/children/never/messages`)).body;
    assert.equal(never.coverage, 'UNAVAILABLE'); assert.deepEqual(never.items, []);
    // SUMMARY_ONLY: a pre-capture row that only has aggregate output
    const t = s.host.store.task(c.taskId);
    t.collaborations[cid].children.blocked.output = 'legacy aggregate output';
    delete t.collaborations[cid].children.blocked.messageStreamId;
    s.host.store.save(t);
    const legacy = (await s.call(`/tasks/${c.taskId}/collaborations/${cid}/children/blocked/messages`)).body;
    assert.equal(legacy.coverage, 'SUMMARY_ONLY'); assert.equal(legacy.summaryText, 'legacy aggregate output'); assert.deepEqual(legacy.items, []);
    // ownership: wrong task/collab/child all refuse
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/children/nope/messages`)).body.code, 'COLLABORATION_CHILD_NOT_FOUND');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/collab-nope/children/worker/messages`)).body.code, 'COLLABORATION_NOT_FOUND');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions/aux-nope/messages`)).body.code, 'AUX_SESSION_NOT_FOUND');
    const c3 = await create(s.call, 'hello'); await succeeded(s.call, c3.taskId);
    const cross = await s.call(`/tasks/${c3.taskId}/collaborations/${cid}/children/worker/messages`);
    assert.equal(cross.body.code, 'COLLABORATION_NOT_FOUND', 'cross-task stream access refuses');
  } finally { await s.host.close(); }
});

test('collaboration preflight: validates without spawn, digest guard on create', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); await succeeded(s.call, c.taskId);
    const plan = { name: 'reviewed', children: [{ childId: 'a', goal: 'child-write pa.txt' }, { childId: 'b', goal: 'child-write pb.txt', dependsOn: ['a'] }] };
    const pf = await s.call(`/tasks/${c.taskId}/collaborations/preflight`, { requestKey: 'pf-1', ...plan });
    assert.equal(pf.status, 202);
    assert.match(pf.body.preflightId, /^pf-/); assert.equal(typeof pf.body.planDigest, 'string');
    assert.ok(pf.body.expiresAt && pf.body.sourceBindings && pf.body.allowedScopes && pf.body.budgetTerms);
    assert.equal(pf.body.budgetTerms.enforcement, 'SOFT');
    // validate-only: no collaboration row, no child process, no write effect
    assert.deepEqual((await s.call(`/tasks/${c.taskId}/collaborations`)).body.items, []);
    assert.equal(existsSync(join(s.state, 'collab')), false, 'no checkout directory created');
    // replay of the same requestKey returns the durable record
    const replay = await s.call(`/tasks/${c.taskId}/collaborations/preflight`, { requestKey: 'pf-1', ...plan });
    assert.equal(replay.body.preflightId, pf.body.preflightId); assert.equal(replay.body.replayed, true);
    // same validation surface as create: cycle + unknown dep refuse identically
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/preflight`, { requestKey: 'pf-bad1', children: [{ childId: 'x', goal: 'g', dependsOn: ['x'] }] })).body.code, 'COLLABORATION_DEPENDENCY_CYCLE');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/preflight`, { requestKey: 'pf-bad2', children: [{ childId: 'x', goal: 'g', dependsOn: ['ghost'] }] })).body.code, 'COLLABORATION_DEPENDENCY_UNKNOWN');
    // final create bound to the review digest lands
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-pf', ...plan, expectedPreflightId: pf.body.preflightId, expectedPlanDigest: pf.body.planDigest });
    assert.equal(made.status, 202); assert.equal(made.body.planDigest, pf.body.planDigest);
    await collabDone(s.call, c.taskId, made.body.collaborationId);
    // mismatched digest and unknown preflight both conflict
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-pf-x', ...plan, expectedPlanDigest: 'deadbeef' })).body.code, 'PREFLIGHT_STALE');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-pf-y', ...plan, expectedPreflightId: 'pf-nope' })).body.code, 'PREFLIGHT_STALE');
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'collaboration.preflighted' && e.preflightId === pf.body.preflightId));
    const created = evs.find(e => e.kind === 'collaboration.created' && e.collaborationId === made.body.collaborationId);
    assert.equal(created.preflightId, pf.body.preflightId); assert.equal(created.planDigest, pf.body.planDigest);
  } finally { await s.host.close(); }
});

test('adoption preview: read-only plan, digest guard, live re-check on apply', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'writefile base.txt'); const d = await succeeded(s.call, c.taskId);
    const workspace = d.workspace;
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-pv', children: [{ childId: 'w1', goal: 'child-write new.txt' }, { childId: 'w2', goal: 'child-write README.md' }] });
    const cid = made.body.collaborationId; await collabDone(s.call, c.taskId, cid);
    const pv = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/previews`, { requestKey: 'pv-1', childId: 'w1' });
    assert.equal(pv.status, 202);
    assert.match(pv.body.previewId, /^pv-/); assert.equal(typeof pv.body.planDigest, 'string');
    assert.equal(pv.body.writable, true); assert.equal(pv.body.childId, 'w1');
    assert.deepEqual(pv.body.files, [{ path: 'new.txt', action: 'write', code: 'OK', candidateSha256: hash('collab write new.txt'), baselineSha256: null, parentSha256: null }]);
    assert.equal(existsSync(join(workspace, 'new.txt')), false, 'preview writes nothing');
    const got = (await s.call(`/tasks/${c.taskId}/collaborations/${cid}/previews/${pv.body.previewId}`)).body;
    assert.equal(got.planDigest, pv.body.planDigest); assert.equal(got.expired, false);
    const replay = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/previews`, { requestKey: 'pv-1', childId: 'w1' });
    assert.equal(replay.body.previewId, pv.body.previewId); assert.equal(replay.body.replayed, true);
    // guarded adoption binds the reviewed frozen plan
    const a1 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'adp-1', childId: 'w1', mode: 'ALL', expectedPreviewId: pv.body.previewId, expectedPreviewDigest: pv.body.planDigest });
    assert.equal(a1.status, 202); assert.equal(a1.body.outcome, 'ADOPTED'); assert.equal(a1.body.fileEffect, 'SUCCEEDED');
    assert.equal(a1.body.verification, 'NOT_SUPPORTED'); assert.equal(a1.body.contextContinuation, 'NOT_SUPPORTED');
    assert.equal(readFileSync(join(workspace, 'new.txt'), 'utf8'), 'collab write new.txt');
    // stale guard: unknown preview id, wrong digest, wrong child all conflict
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'adp-x', childId: 'w1', mode: 'ALL', expectedPreviewId: 'pv-nope' })).body.code, 'PREVIEW_STALE');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'adp-y', childId: 'w1', mode: 'ALL', expectedPreviewId: pv.body.previewId, expectedPreviewDigest: 'deadbeef' })).body.code, 'PREVIEW_STALE');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'adp-z', childId: 'w2', mode: 'ALL', expectedPreviewId: pv.body.previewId })).body.code, 'PREVIEW_STALE');
    // a preview taken before parent drift does not bypass the live re-check
    const pv2 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/previews`, { requestKey: 'pv-2', childId: 'w2' });
    assert.equal(pv2.body.files[0].code, 'OK'); assert.equal(pv2.body.writable, true);
    writeFileSync(join(workspace, 'README.md'), 'parent drifted after preview');
    const a2 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'adp-2', childId: 'w2', mode: 'ALL', expectedPreviewId: pv2.body.previewId });
    assert.equal(a2.status, 409); assert.equal(a2.body.outcome, 'CONFLICT'); assert.equal(a2.body.fileEffect, 'NOT_STARTED');
    assert.equal(a2.body.conflicts[0].kind, 'PARENT_DRIFT');
    assert.equal(readFileSync(join(workspace, 'README.md'), 'utf8'), 'parent drifted after preview');
    // the preview itself now shows the conflict on re-read computation (new preview)
    const pv3 = await s.call(`/tasks/${c.taskId}/collaborations/${cid}/previews`, { requestKey: 'pv-3', childId: 'w2' });
    assert.equal(pv3.body.files[0].code, 'PARENT_DRIFT'); assert.equal(pv3.body.writable, false);
  } finally { await s.host.close(); }
});

test('source seal: drains children, fences mutations, handoff export stays readable', async () => {
  const s = await setup(); try {
    // the main task must be quiescent before the fence lands (F33): a terminal
    // SUCCEEDED verdict qualifies — the idle owner cannot write once the fence
    // blocks every new dispatch; a RUNNING task refuses TASK_NOT_QUIESCENT.
    const c = await create(s.call, 'seal me');
    const d = await succeeded(s.call, c.taskId);
    // a running collaboration child and a running aux session must be drained by the seal
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-seal', children: [{ childId: 'hang', goal: 'child-hang' }] });
    const cid = made.body.collaborationId;
    await wait(async () => { const g = await collabGet(s.call, c.taskId, cid); return g.children[0].state === 'RUNNING' && g.children[0].nativeSessionId && g; });
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'ax-seal', question: 'aux-hang now' });
    await wait(async () => { const a = await auxGet(s.call, c.taskId, aux.body.auxSessionId); return a.state === 'RUNNING' && a; });
    const bytes = Buffer.from('seal-bound handoff material');
    const up = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-seal', target: { taskId: c.taskId, handoff: true }, displayName: 'handoff.bin', mediaType: 'application/x-binary-patch', sizeBytes: bytes.length, sha256: hash(bytes) });
    assert.equal(up.status, 202, JSON.stringify(up.body));
    await s.put(`/material-uploads/${up.body.uploadKey}/chunks/0`, bytes, hash(bytes));
    const sealedUp = await s.call(`/material-uploads/${up.body.uploadKey}/seal`, { requestKey: 'mu-seal-2', sha256: hash(bytes) });
    assert.equal(sealedUp.body.state, 'SEALED');
    const seal = await s.call(`/tasks/${c.taskId}/source-seals`, { requestKey: 'seal-1', target: 'HANDOFF', materials: [{ materialRef: sealedUp.body.materialRef, sha256: hash(bytes) }] });
    assert.equal(seal.status, 202, JSON.stringify(seal.body));
    assert.match(seal.body.sealId, /^seal-/); assert.equal(seal.body.schemaVersion, 'source-seal/1');
    assert.equal(seal.body.target, 'HANDOFF'); assert.equal(seal.body.materials[0].sha256, hash(bytes));
    // the drain: collab child + aux cancelled with the seal reason
    const collab = await collabGet(s.call, c.taskId, cid);
    assert.equal(collab.state, 'CANCELLED'); assert.equal(collab.reasonCode, 'SOURCE_SEALED');
    assert.equal(collab.children[0].state, 'CANCELLED'); assert.equal(collab.children[0].reasonCode, 'SOURCE_SEALED');
    assert.equal((await auxGet(s.call, c.taskId, aux.body.auxSessionId)).state, 'CANCELLED');
    // mutation fence: every write path refuses; reads and STOP stay open
    const g = { generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId };
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-sealed', kind: 'NATIVE_STEER', content: 'x', ...g })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-sealed', children: [{ goal: 'g' }] })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/preflight`, { requestKey: 'pf-sealed', children: [{ goal: 'g' }] })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'ax-sealed', question: 'q' })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/adoptions`, { requestKey: 'ad-sealed', childId: 'hang', mode: 'ALL' })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/collaborations/${cid}/previews`, { requestKey: 'pv-sealed', childId: 'hang' })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals`, { requestKey: 't-sealed', argv: ['echo', 'hi'] })).body.code, 'SOURCE_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'tr-sealed', targetProfile: 'x', ...g })).body.code, 'SOURCE_SEALED');
    // seal listing and lookup stay readable
    const list = (await s.call(`/tasks/${c.taskId}/source-seals`)).body;
    assert.equal(list.items.length, 1); assert.equal(list.sealedFor.digest, seal.body.digest);
    const byId = (await s.call(`/tasks/${c.taskId}/source-seals/${seal.body.sealId}`)).body;
    assert.equal(byId.digest, seal.body.digest);
    // a second seal with different parameters refuses; replay returns the record
    assert.equal((await s.call(`/tasks/${c.taskId}/source-seals`, { requestKey: 'seal-2', target: 'OTHER' })).body.code, 'SOURCE_ALREADY_SEALED');
    const same = await s.call(`/tasks/${c.taskId}/source-seals`, { requestKey: 'seal-1', target: 'HANDOFF', materials: [{ materialRef: sealedUp.body.materialRef, sha256: hash(bytes) }] });
    assert.equal(same.body.replayed, true);
    // STOP is deliberately not fenced
    const stop = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-sealed', intent: 'STOP', ...g });
    assert.equal(stop.body.state, 'APPLIED');
    // handoff export: read-only, bounded, digest-stamped, still open after seal
    const ex = (await s.call(`/tasks/${c.taskId}/handoff-export?limit=5`)).body;
    assert.equal(ex.schemaVersion, 'handoff-export/1'); assert.equal(ex.sealed, true);
    assert.equal(ex.targetStart, 'NOT_STARTED');
    assert.equal(typeof ex.exportDigest, 'string'); assert.ok(ex.items.length <= 5);
    const all = await s.call(`/tasks/${c.taskId}/handoff-export?limit=100`);
    const kinds = new Set(all.body.items.map(i => i.kind));
    assert.ok(kinds.has('MESSAGE') && kinds.has('EVENT') && kinds.has('AUX_ANSWER') && kinds.has('COLLABORATION_REPORT'), 'projection kinds ' + [...kinds]);
    const report = all.body.items.find(i => i.kind === 'COLLABORATION_REPORT');
    assert.equal(report.children[0].state, 'CANCELLED');
    // cursor pagination walks the whole projection exactly once
    const seen = new Set(); let cursor = '0';
    for (let i = 0; i < 20; i++) {
      const page = (await s.call(`/tasks/${c.taskId}/handoff-export?cursor=${cursor}&limit=4`)).body;
      for (const item of page.items) seen.add(item.id);
      cursor = page.cursor; if (!page.hasMore) break;
    }
    assert.equal(seen.size, all.body.items.length);
    // chunk route serves digest-bound blob slices; wrong digest refuses
    const out = (await s.call(`/tasks/${c.taskId}/output`)).body;
    const sha = hash(out.text);
    const chunk = (await s.call(`/tasks/${c.taskId}/handoff-export/output/chunks/${sha}/0`)).body;
    assert.equal(Buffer.from(chunk.bytes, 'base64').toString('utf8'), out.text.slice(0, 65536));
    assert.equal((await s.call(`/tasks/${c.taskId}/handoff-export/output/chunks/${'0'.repeat(64)}/0`)).body.code, 'HANDOFF_CHUNK_DIGEST_MISMATCH');
    assert.equal((await s.call(`/tasks/${c.taskId}/handoff-export/nope/chunks/${sha}/0`)).body.code, 'HANDOFF_PART_NOT_FOUND');
    const evs = await events(s.call, c.taskId);
    assert.ok(evs.some(e => e.kind === 'task.source.sealed' && e.sealId === seal.body.sealId));
  } finally { await s.host.close(); }
});

test('material uploads: chunk idempotency, seal, delete rules, message attach', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const g = { generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId };
    const bytes = Buffer.from('reference material body 0123456789');
    const one = bytes.subarray(0, 16); const two = bytes.subarray(16);
    // unknown project / media / bad hashes refuse up front
    assert.equal((await s.call('/projects/nope/material-uploads', { requestKey: 'mu-x', displayName: 'a.txt', mediaType: 'text/plain', sizeBytes: 4, sha256: hash('aaaa') })).body.code, 'PROJECT_NOT_REGISTERED');
    assert.equal((await s.call('/projects/demo/material-uploads', { requestKey: 'mu-y', displayName: 'a.exe', mediaType: 'application/x-msdownload', sizeBytes: 4, sha256: hash('aaaa') })).body.code, 'MATERIAL_MEDIA_UNSUPPORTED');
    const up = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-1', target: { taskId: c.taskId }, displayName: 'note.txt', mediaType: 'text/plain', sizeBytes: bytes.length, sha256: hash(bytes) });
    assert.equal(up.status, 202, JSON.stringify(up.body));
    assert.match(up.body.uploadKey, /^mat-/); assert.equal(up.body.state, 'UPLOADING'); assert.equal(up.body.nextOffset, 0);
    // requestKey replay returns the same upload; a digest collision refuses
    const upReplay = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-1', target: { taskId: c.taskId }, displayName: 'note.txt', mediaType: 'text/plain', sizeBytes: bytes.length, sha256: hash(bytes) });
    assert.equal(upReplay.body.uploadKey, up.body.uploadKey);
    // chunk writes: offset + digest idempotent, out-of-order offsets refuse
    const c1 = await s.put(`/material-uploads/${up.body.uploadKey}/chunks/0`, one, hash(one));
    assert.equal(c1.status, 200); assert.equal(c1.body.nextOffset, 16);
    const c1r = await s.put(`/material-uploads/${up.body.uploadKey}/chunks/0`, one, hash(one));
    assert.equal(c1r.body.nextOffset, 16, 'identical chunk replay confirms');
    const conflict = await s.put(`/material-uploads/${up.body.uploadKey}/chunks/0`, Buffer.from('DIFFERENT CHUNK!2'), hash(Buffer.from('DIFFERENT CHUNK!2')));
    assert.equal(conflict.body.code, 'MATERIAL_CHUNK_CONFLICT');
    const gap = await s.put(`/material-uploads/${up.body.uploadKey}/chunks/32`, Buffer.from('late'), hash(Buffer.from('late')));
    assert.equal(gap.body.code, 'MATERIAL_OFFSET_INVALID');
    const lie = await s.put(`/material-uploads/${up.body.uploadKey}/chunks/16`, two, hash(one));
    assert.equal(lie.body.code, 'MATERIAL_CHUNK_DIGEST_MISMATCH');
    // early seal refuses; completing the stream seals and issues materialRef
    assert.equal((await s.call(`/material-uploads/${up.body.uploadKey}/seal`, { requestKey: 'mu-s1', sha256: hash(bytes) })).body.code, 'MATERIAL_INCOMPLETE');
    const c2 = await s.put(`/material-uploads/${up.body.uploadKey}/chunks/16`, two, hash(two));
    assert.equal(c2.body.nextOffset, bytes.length);
    assert.equal((await s.call(`/material-uploads/${up.body.uploadKey}/seal`, { requestKey: 'mu-s2', sha256: hash('wrong') })).body.code, 'MATERIAL_SHA256_MISMATCH');
    const sealed = await s.call(`/material-uploads/${up.body.uploadKey}/seal`, { requestKey: 'mu-s3', sha256: hash(bytes) });
    assert.equal(sealed.body.state, 'SEALED'); assert.match(sealed.body.materialRef, /^matref-/);
    // attaching an unsealed or mismatched ref refuses honestly
    const up2 = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-2', target: { taskId: c.taskId }, displayName: 'open.txt', mediaType: 'text/plain', sizeBytes: 3, sha256: hash('abc') });
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-1', kind: 'NATIVE_FOLLOW_UP', content: 'x', materials: [{ materialRef: up2.body.uploadKey, sha256: hash('abc'), role: 'REFERENCE_TEXT' }], ...g })).body.code, 'MATERIAL_NOT_SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-2', kind: 'NATIVE_FOLLOW_UP', content: 'x', materials: [{ materialRef: sealed.body.materialRef, sha256: hash('nope'), role: 'REFERENCE_TEXT' }], ...g })).body.code, 'MATERIAL_SHA256_MISMATCH');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-3', kind: 'NATIVE_FOLLOW_UP', content: 'x', materials: [{ materialRef: 'mat-ghost', sha256: hash(bytes), role: 'REFERENCE_TEXT' }], ...g })).body.code, 'MATERIAL_NOT_FOUND');
    // attachments on a non-prompt intent refuse
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-4', kind: 'INPUT_RESPONSE', content: 'x', materials: [{ materialRef: sealed.body.materialRef, sha256: hash(bytes), role: 'REFERENCE_TEXT' }], ...g })).body.code, 'MATERIALS_UNSUPPORTED_INTENT');
    // REFERENCE_TEXT injection reaches the model input; the generic fixture path
    // persists the assembled prompt into result.txt
    const m = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-5', kind: 'NATIVE_FOLLOW_UP', content: 'attach-probe', materials: [{ materialRef: sealed.body.materialRef, sha256: hash(bytes), role: 'REFERENCE_TEXT' }], ...g });
    assert.equal(m.status, 202, JSON.stringify(m.body));
    assert.deepEqual(m.body.materials.map(r => r.assembledAs), ['REFERENCE_TEXT_INJECTED']);
    await wait(async () => { const file = join(d.workspace, 'result.txt'); return existsSync(file) && readFileSync(file, 'utf8').includes('reference material body 0123456789') && readFileSync(file, 'utf8').includes('<reference-material'); });
    // the follow-up rotated targetCommandId; wait for the task to settle and re-guard
    const g2 = guard(await succeeded(s.call, c.taskId));
    // a bound/consumed upload refuses deletion; an unbound one revokes cleanly
    assert.equal((await s.call(`/material-uploads/${up.body.uploadKey}`, null, {}, 'DELETE')).body.code, 'MATERIAL_BOUND');
    const del = await s.call(`/material-uploads/${up2.body.uploadKey}`, null, {}, 'DELETE');
    assert.equal(del.body.state, 'CANCELLED');
    assert.equal((await s.call(`/material-uploads/${up2.body.uploadKey}`)).body.state, 'CANCELLED');
    // IMAGE refs are rejected honestly — Pi image input is unverified
    const img = Buffer.from([0x89, 0x50, 0x4e, 0x47, 1, 2, 3, 4]);
    const iup = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-3', target: { taskId: c.taskId }, displayName: 'shot.png', mediaType: 'image/png', sizeBytes: img.length, sha256: hash(img) });
    await s.put(`/material-uploads/${iup.body.uploadKey}/chunks/0`, img, hash(img));
    const isel = await s.call(`/material-uploads/${iup.body.uploadKey}/seal`, { requestKey: 'mu-3s', sha256: hash(img) });
    assert.equal(isel.body.state, 'SEALED');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-6', kind: 'NATIVE_FOLLOW_UP', content: 'x', materials: [{ materialRef: isel.body.materialRef, sha256: hash(img), role: 'IMAGE' }], ...g2 })).body.code, 'MODEL_IMAGE_INPUT_UNSUPPORTED');
    // non-attachable kinds (GIT_BUNDLE) refuse message attachment
    const bundle = Buffer.from('bundle-bytes');
    const bup = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-4', target: { taskId: c.taskId, handoff: true }, displayName: 'pack.bundle', mediaType: 'application/x-git-bundle', sizeBytes: bundle.length, sha256: hash(bundle) });
    await s.put(`/material-uploads/${bup.body.uploadKey}/chunks/0`, bundle, hash(bundle));
    const bsel = await s.call(`/material-uploads/${bup.body.uploadKey}/seal`, { requestKey: 'mu-4s', sha256: hash(bundle) });
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-att-7', kind: 'NATIVE_FOLLOW_UP', content: 'x', materials: [{ materialRef: bsel.body.materialRef, sha256: hash(bundle), role: 'REFERENCE_TEXT' }], ...g2 })).body.code, 'MATERIALS_UNSUPPORTED_INTENT');
    // list endpoint projects the ledger
    const list = (await s.call('/projects/demo/material-uploads')).body;
    assert.ok(list.items.length >= 4 && list.items.every(i => i.uploadKey && i.state));
  } finally { await s.host.close(); }
});

test('STOP affectedChildren: collab children drained, aux left running honestly', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting'); const d = await running(s.call, c.taskId);
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-aff', children: [{ childId: 'hang', goal: 'child-hang' }] });
    const cid = made.body.collaborationId;
    await wait(async () => { const g = await collabGet(s.call, c.taskId, cid); return g.children[0].state === 'RUNNING' && g.children[0].nativeSessionId && g; });
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'ax-aff', question: 'aux-hang here' });
    await wait(async () => { const a = await auxGet(s.call, c.taskId, aux.body.auxSessionId); return a.state === 'RUNNING' && a; });
    const stop = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-aff', intent: 'STOP', ...guard(d) });
    assert.equal(stop.body.state, 'APPLIED');
    const aff = stop.body.affectedChildren;
    assert.ok(aff, 'receipt carries affectedChildren');
    const hang = aff.collabChildren.find(x => x.childId === 'hang');
    assert.equal(hang.state, 'CANCELLED'); assert.equal(hang.reason, 'PARENT_STOP');
    const auxEntry = aff.aux.find(x => x.id === aux.body.auxSessionId);
    assert.equal(auxEntry.state, 'LEFT_RUNNING'); assert.equal(auxEntry.reason, 'TASK_STOP_AUX_NOT_PROPAGATED');
    // aux really is still running — parent STOP never propagates to it
    assert.equal((await auxGet(s.call, c.taskId, aux.body.auxSessionId)).state, 'RUNNING');
    const evs = await events(s.call, c.taskId);
    const stopEvent = evs.find(e => e.kind === 'STOP_APPLIED');
    assert.equal(stopEvent.affectedChildren.aux[0].state, 'LEFT_RUNNING');
    // the stopped child stream captured its terminal STATUS + RETURN
    const mp = (await s.call(`/tasks/${c.taskId}/collaborations/${cid}/children/hang/messages`)).body;
    assert.equal(mp.coverage, 'FULL_SINCE_CAPTURE');
    assert.equal(mp.items.at(-1).kind, 'RETURN'); assert.equal(mp.items.at(-1).status, 'CANCELLED');
  } finally { await s.host.close(); }
});

// L0-PI-04 / GAP-08 — IMAGE-role message refusal (materials.ts:151). The Pi
// image-input path is unverified, so role:IMAGE on a message refuses
// MODEL_IMAGE_INPUT_UNSUPPORTED — and the refusal must be honest, never a
// silent downgrade to a text-only send: the message leaves no operation row,
// no INPUT_ACCEPTED/message.materials event, no consumed material, and no
// dispatched content.
test('L0-PI-04 image-role material refusal: rejected upfront, never dispatched or silently downgraded', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const g = { generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId };
    // a sealed image upload, legally bound to this task
    const img = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3, 4]);
    const iup = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-img', target: { taskId: c.taskId }, displayName: 'shot.png', mediaType: 'image/png', sizeBytes: img.length, sha256: hash(img) });
    assert.equal(iup.status, 202, JSON.stringify(iup.body));
    await s.put(`/material-uploads/${iup.body.uploadKey}/chunks/0`, img, hash(img));
    const isel = await s.call(`/material-uploads/${iup.body.uploadKey}/seal`, { requestKey: 'mu-img-s', sha256: hash(img) });
    assert.equal(isel.body.state, 'SEALED'); assert.equal(isel.body.mediaRole, 'IMAGE');
    const before = s.host.store.task(c.taskId);
    // image-only attach → refused with the honest code, synchronously
    const denied = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-img-1', kind: 'NATIVE_FOLLOW_UP', content: 'look-at-this-image', materials: [{ materialRef: isel.body.materialRef, sha256: hash(img), role: 'IMAGE' }], ...g });
    assert.equal(denied.status, 409); assert.equal(denied.body.code, 'MODEL_IMAGE_INPUT_UNSUPPORTED');
    // the refusal is pre-accept (attach runs before store.accept): no op row
    // exists, so the requestKey is not poisoned for a corrected retry.
    assert.equal(s.host.store.operation('m-img-1'), null, 'refused message leaves no operation row');
    // nothing was dispatched: the task row is untouched, no turn opened, the
    // refused content never reaches the workspace/session, and no acceptance
    // or materials event names the refused key.
    const after = s.host.store.task(c.taskId);
    assert.equal(after.currentCommandId, before.currentCommandId);
    assert.equal(after.state, 'SUCCEEDED');
    assert.deepEqual(Object.keys(after.turns), Object.keys(before.turns));
    const evs = await events(s.call, c.taskId);
    assert.ok(!evs.some(e => e.requestKey === 'm-img-1'), 'no INPUT_ACCEPTED/message.materials event for the refused key');
    assert.equal(existsSync(join(d.workspace, 'result.txt')) && readFileSync(join(d.workspace, 'result.txt'), 'utf8').includes('look-at-this-image'), false, 'refused content never dispatched');
    // the referenced upload is not half-bound by the refused attach
    const upload = (await s.call(`/material-uploads/${iup.body.uploadKey}`)).body;
    assert.equal(upload.state, 'SEALED'); assert.equal(upload.boundAt, null); assert.equal(upload.consumedAt, null);
    // a mixed payload (valid text ref + image ref) refuses the WHOLE message —
    // never silently downgraded to a text-only send
    const txt = Buffer.from('text ref body');
    const tup = await s.call('/projects/demo/material-uploads', { requestKey: 'mu-txt', target: { taskId: c.taskId }, displayName: 'note.txt', mediaType: 'text/plain', sizeBytes: txt.length, sha256: hash(txt) });
    await s.put(`/material-uploads/${tup.body.uploadKey}/chunks/0`, txt, hash(txt));
    const tsel = await s.call(`/material-uploads/${tup.body.uploadKey}/seal`, { requestKey: 'mu-txt-s', sha256: hash(txt) });
    const mixed = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-img-2', kind: 'NATIVE_FOLLOW_UP', content: 'mixed-payload-probe', materials: [
      { materialRef: tsel.body.materialRef, sha256: hash(txt), role: 'REFERENCE_TEXT' },
      { materialRef: isel.body.materialRef, sha256: hash(img), role: 'IMAGE' },
    ], ...g });
    assert.equal(mixed.status, 409); assert.equal(mixed.body.code, 'MODEL_IMAGE_INPUT_UNSUPPORTED');
    assert.equal(s.host.store.operation('m-img-2'), null, 'mixed refused message leaves no operation row');
    assert.equal((await s.call(`/material-uploads/${tup.body.uploadKey}`)).body.state, 'SEALED', 'text ref not half-bound by the refused mixed message');
    // the refused requestKey remains usable with a corrected payload — proof
    // the refusal recorded nothing (and the corrected send carries only the
    // text ref, with no image bytes injected).
    const ok = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'm-img-2', kind: 'NATIVE_FOLLOW_UP', content: 'mixed-payload-probe', materials: [{ materialRef: tsel.body.materialRef, sha256: hash(txt), role: 'REFERENCE_TEXT' }], ...g });
    assert.equal(ok.status, 202, JSON.stringify(ok.body));
    assert.deepEqual(ok.body.materials.map(r => r.assembledAs), ['REFERENCE_TEXT_INJECTED']);
    await succeeded(s.call, c.taskId);
    const file = join(d.workspace, 'result.txt');
    await wait(async () => existsSync(file) && readFileSync(file, 'utf8').includes('text ref body'));
    assert.equal(readFileSync(file, 'utf8').includes('shot.png'), false, 'image ref never reaches model input');
    assert.equal(readFileSync(file, 'utf8').includes('look-at-this-image'), false, 'refused message content never dispatched');
  } finally { await s.host.close(); }
});
