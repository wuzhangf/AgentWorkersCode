import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';

// L0-PI-03 / F19 — managed policy-preset boundary.
// Source under test: workers/pi/src/host/server.ts policyPresets()/policyPreset()
// (managed table, ~:190-205), create() preset freeze (:289-292) and the P30
// RUNNING_POLICY_IMMUTABLE row (:173). Asserts three boundary facts:
//   1. an unknown/malformed policyPresetId refuses POLICY_PRESET_UNKNOWN at
//      BOTH task-preflights and task create — the preset table is closed;
//   2. a preset narrows the declared toolAllowlist exactly — the frozen row
//      carries the declared set verbatim, never a widened union with the
//      profile toolset;
//   3. once frozen at launch there is no in-run rebind path: the transitions
//      targetProfile whitelist refuses a preset rebind and an allowlist that
//      names a tool the session does not actually expose refuses
//      INCOMPATIBLE_TRANSITION with zero state change.
const token = 'deterministic-f19-boundary-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-f19-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-f19-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi', ...(extra.policyPresetId ? { policyPresetId: extra.policyPresetId } : {}) })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, receipt: result.body, status: result.status, code: result.body.code }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });

test('L0-PI-03 policy preset boundary: unknown refuses, narrowing exact, frozen in-run', async () => {
  const s = await setup(); try {
    // 1) The managed preset table is a closed set: unknown or malformed ids
    //    refuse identically at preflight and at create — and a refused create
    //    leaves no task row behind.
    for (const bad of ['bogus', 'read_only', 'READ-ONLY', '', 42, { id: 'read-only' }, ['read-only']]) {
      const r = await s.call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi', policyPresetId: bad });
      assert.equal(r.body.code, 'POLICY_PRESET_UNKNOWN', 'preflight preset ' + JSON.stringify(bad));
      assert.equal(r.status, 409);
    }
    const pf0 = (await s.call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body;
    assert.equal(pf0.policyPreset, null, 'omitted preset is legal and binds nothing');
    const denied = await s.call('/tasks', { requestKey: 'f19-bad-preset', projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf0.optionsRevision, instruction: 'hello', policyPresetId: 'bogus' });
    assert.equal(denied.status, 409); assert.equal(denied.body.code, 'POLICY_PRESET_UNKNOWN');
    assert.equal(s.host.store.tasks().length, 0, 'a refused preset creates no task row');

    // 2) 'read-only' narrows to a strict subset of the engine toolset and the
    //    frozen task row carries the declared set verbatim — the effective
    //    launch set is the preset declaration, never a widened union.
    const pf = (await s.call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi', policyPresetId: 'read-only' })).body;
    assert.equal(pf.policyPreset.presetId, 'read-only');
    assert.equal(pf.policyPreset.effectiveRange, 'NEW_TASKS');
    const declared = pf.policyPreset.frozenParameters.toolAllowlist;
    assert.ok(Array.isArray(declared) && declared.length > 0, 'read-only declares a toolAllowlist');
    const engineTools = (await s.call('/engines')).body.items[0].executionPolicy.toolAllowlist;
    for (const t of declared) assert.ok(engineTools.includes(t), `preset tool ${t} inside engine toolset`);
    assert.ok(engineTools.some(t => !declared.includes(t)), 'read-only is a strict narrowing of the engine toolset');
    const c = await create(s.call, 'hello', { policyPresetId: 'read-only' });
    assert.equal(c.status, 202, JSON.stringify(c.receipt));
    const d = await succeeded(s.call, c.taskId);
    const frozen = s.host.store.task(c.taskId);
    assert.deepEqual(frozen.toolAllowlist, declared, 'frozen allowlist is exactly the preset declaration');
    assert.deepEqual(d.executionPolicy.toolAllowlist, declared, 'projected policy reports the declared set');
    for (const t of d.executionPolicy.toolAllowlist) assert.ok(!['write', 'edit', 'bash'].includes(t), t + ' mutating tool excluded');
    assert.equal(d.executionPolicy.frozen, true);
    assert.equal(d.executionPolicy.policyPreset.presetId, 'read-only');
    assert.equal(d.executionPolicy.policyPreset.effectiveRange, 'NEW_TASKS');
    assert.equal(typeof frozen.policyPreset.frozenAt, 'string');
    // 'standard' declares toolAllowlist:null — it binds the preset record
    // without narrowing the profile toolset.
    const cStd = await create(s.call, 'hello', { policyPresetId: 'standard' });
    assert.equal(cStd.status, 202);
    const dStd = await succeeded(s.call, cStd.taskId);
    assert.equal(dStd.executionPolicy.toolAllowlist, null, 'standard preset narrows nothing');
    assert.equal(dStd.executionPolicy.policyPreset.presetId, 'standard');

    // 3) RUNNING_POLICY_IMMUTABLE: the frozen row has no in-run rebind path.
    //    The transitions targetProfile exact-field whitelist refuses a preset
    //    rebind outright, and a toolAllowlist naming a tool the session does
    //    not actually expose refuses INCOMPATIBLE_TRANSITION — with zero
    //    mutation of generation/epoch/allowlist/transitions.
    const guardOf = async () => guard((await s.call('/tasks/' + c.taskId)).body);
    const before = s.host.store.task(c.taskId);
    const rebind = await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'f19-rebind', targetProfile: { policyPresetId: 'standard' }, ...(await guardOf()) });
    assert.equal(rebind.body.code, 'UNEXPECTED_FIELD', 'policyPresetId is not a transition parameter');
    const smuggle = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'f19-smuggle', kind: 'NATIVE_FOLLOW_UP', content: 'x', policyPresetId: 'standard', ...(await guardOf()) });
    assert.equal(smuggle.body.code, 'UNEXPECTED_FIELD', 'no message intent can smuggle a preset rebind');
    for (const list of [['no_such_tool'], ['bash']]) {
      const r = await s.call(`/tasks/${c.taskId}/transitions`, { requestKey: 'f19-refuse-' + list[0], targetProfile: { toolAllowlist: list }, ...(await guardOf()) });
      assert.equal(r.body.code, 'INCOMPATIBLE_TRANSITION', list.join(','));
    }
    const after = s.host.store.task(c.taskId);
    assert.deepEqual(after.toolAllowlist, declared, 'refused transitions leave the frozen allowlist untouched');
    assert.deepEqual(after.policyPreset, before.policyPreset, 'preset record unchanged');
    assert.equal(after.generation, before.generation); assert.equal(after.ownerEpoch, before.ownerEpoch);
    assert.equal((after.transitions ?? []).length, 0, 'no transition was admitted');

    // WITNESS (fixture fidelity, not a production claim): a transition
    // redeclaring a tool the session reports active DOES land even when it
    // exceeds the preset's declared set — the gate (server.ts:1468-1471) is
    // bounded by child-reported activeTools, not by the preset table. The
    // fixture child reports tools:['write','ask_user'] unconditionally
    // (fixture-child.mjs:22) and ignores init.toolAllowlist, so 'write' passes
    // here; a real SDK child filters its session tools by the launch
    // allowlist (engine/child.ts:186-188) and would not report 'write' — the
    // same request refuses INCOMPATIBLE_TRANSITION in production. If F19 is
    // meant as a task-lifetime hard ceiling the host would ALSO have to
    // intersect the request with the launch-frozen preset declaration; the
    // durable preset record itself is never rewritten either way.
    const cW = await create(s.call, 'hello', { policyPresetId: 'read-only' });
    const dW = await succeeded(s.call, cW.taskId);
    assert.equal((await s.call(`/tasks/${cW.taskId}/commands`, { requestKey: 'f19-stop-w', intent: 'STOP', ...guard(dW) })).status, 202);
    const trW = await s.call(`/tasks/${cW.taskId}/transitions`, { requestKey: 'f19-widen', targetProfile: { toolAllowlist: ['write'] }, ...guard((await s.call('/tasks/' + cW.taskId)).body) });
    assert.equal(trW.status, 202, 'fixture child over-reports activeTools so the widening lands — see witness note: ' + JSON.stringify(trW.body));
    const doneW = await succeeded(s.call, cW.taskId);
    const widened = s.host.store.task(cW.taskId);
    assert.deepEqual(widened.toolAllowlist, ['write'], 'transition wrote a set outside the preset declaration');
    assert.equal(widened.policyPreset.presetId, 'read-only', 'the durable preset record is untouched');
    assert.equal(doneW.executionPolicy.policyPreset.presetId, 'read-only');
    assert.ok(!['write'].every(t => declared.includes(t)), 'witnessed set is outside the preset declaration');
  } finally { await s.host.close(); }
});
