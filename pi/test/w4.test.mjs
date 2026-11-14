import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync, spawn } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

const token = 'deterministic-w4-test-token-not-a-secret';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w4-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
const w4host = fileURLToPath(new URL('./w4-host.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w4-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, receipt: result.body, status: result.status, code: result.body.code }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const events = async (call, taskId) => (await call(`/tasks/${taskId}/events?limit=100`)).body.items;

test('budget admission validates positive integer caps and SOFT enforcement only', async () => {
  const s = await setup(); try {
    for (const [budget, code] of [
      [{ maxTotalTokens: 100 }, 'BUDGET_ENFORCEMENT_UNSUPPORTED'],
      [{ maxTotalTokens: 100, enforcement: 'HARD' }, 'BUDGET_ENFORCEMENT_UNSUPPORTED'],
      [{ maxTotalTokens: 0, enforcement: 'SOFT' }, 'INVALID_BUDGET'],
      [{ maxTotalTokens: -5, enforcement: 'SOFT' }, 'INVALID_BUDGET'],
      [{ maxInputTokens: 1.5, enforcement: 'SOFT' }, 'INVALID_BUDGET'],
      [{ maxTotalTokens: 100, enforcement: 'SOFT', bogus: 1 }, 'UNEXPECTED_FIELD'],
      [{ enforcement: 'SOFT' }, 'INVALID_BUDGET'],
    ]) assert.equal((await create(s.call, 'hello', { budget })).code, code, JSON.stringify(budget));
    assert.equal(s.host.store.tasks().length, 0, 'rejected admissions create no task');
    const ok = await create(s.call, 'hello', { budget: { maxTotalTokens: 10, enforcement: 'SOFT' } });
    assert.equal(ok.status, 202);
    const task = s.host.store.task(ok.taskId);
    assert.equal(task.budget.enforcement, 'SOFT'); assert.equal(task.budget.basis, 'ENGINE_REPORTED_POST_HOC'); assert.equal(task.budget.caps.maxTotalTokens, 10);
  } finally { await s.host.close(); }
});

test('budget cap exceeded at provider boundary -> BUDGET_EXCEEDED, no further dispatch, EXTEND_BUDGET resumes new segment', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'budget-multi', { budget: { maxTotalTokens: 4, enforcement: 'SOFT' } });
    assert.equal(c.status, 202);
    let d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'BUDGET_EXCEEDED' && d.quiescent && d; });
    const items = await events(s.call, c.taskId);
    assert.equal(items.filter(e => e.kind === 'ASSISTANT_MESSAGE').length, 1, 'second provider request never dispatched');
    const hit = items.find(e => e.kind === 'budget.exceeded');
    assert.equal(hit.field, 'maxTotalTokens'); assert.equal(hit.cap, 4); assert.equal(hit.observed, 5); assert.equal(hit.enforcement, 'SOFT');
    assert.equal(d.budgetExceeded.observed, 5); assert.equal(d.usage.input, 3); assert.equal(d.usage.output, 2);
    assert.equal(d.stopProof?.method, process.platform === 'win32' ? 'WINDOWS_JOB_ACTIVE_ZERO' : process.platform === 'darwin' ? 'DARWIN_KQUEUE_NO_FORK_GROUP_ZERO' : 'OWNED_PROCESS_GROUP');
    const ledger = d.usageLedger; assert.equal(ledger.length, 1); assert.equal(ledger[0].source, 'ENGINE_REPORTED'); assert.equal(ledger[0].costMicroUsd, null); assert.equal(ledger[0].costCoverage, 'UNKNOWN');
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'fu', kind: 'NATIVE_FOLLOW_UP', content: 'x', ...guard(d) })).body.code, 'BUDGET_EXCEEDED');
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'extend-small', intent: 'EXTEND_BUDGET', budget: { maxTotalTokens: 3 }, ...guard(d) })).body.code, 'BUDGET_CAP_NOT_INCREASED');
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'extend-hard', intent: 'EXTEND_BUDGET', budget: { maxTotalTokens: 100, enforcement: 'HARD' }, ...guard(d) })).body.code, 'BUDGET_ENFORCEMENT_UNSUPPORTED');
    const segment = d.segmentId, generation = d.generation;
    const extBody = { requestKey: 'extend-1', intent: 'EXTEND_BUDGET', budget: { maxTotalTokens: 100 }, ...guard(d) };
    const extended = await s.call(`/tasks/${c.taskId}/commands`, extBody);
    assert.equal(extended.status, 202); assert.equal(extended.body.kind, 'EXTEND_BUDGET'); assert.equal(extended.body.resumeVia, 'SESSION_REOPEN');
    await wait(async () => (await s.call(`/tasks/${c.taskId}/commands/extend-1`)).body.state === 'APPLIED');
    d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal(d.nativeSessionId, 'fixture-pi-session'); assert.notEqual(d.segmentId, segment); assert.notEqual(d.generation, generation);
    assert.equal(d.budget.caps.maxTotalTokens, 100); assert.equal(d.budget.history.length, 1); assert.equal(d.usage.input, 6);
    assert.equal(d.reasonCode, undefined, 'extension clears the prior BUDGET_EXCEEDED reason so later failures report their own cause');
    const replayed = await s.call(`/tasks/${c.taskId}/commands`, extBody);
    assert.equal(replayed.body.requestKey, 'extend-1'); assert.equal(replayed.body.state, 'APPLIED');
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'extend-2', intent: 'EXTEND_BUDGET', budget: { maxTotalTokens: 1000 }, ...guard(d) })).body.code, 'BUDGET_NOT_EXCEEDED');
    // a second task proves the tool-admission boundary and STOP on the terminal state
    const c2 = await create(s.call, 'budget-tool', { budget: { maxTotalTokens: 4, enforcement: 'SOFT' } });
    const d2 = await wait(async () => { const d = (await s.call('/tasks/' + c2.taskId)).body; return d.state === 'BUDGET_EXCEEDED' && d.quiescent && d; });
    assert.equal((await events(s.call, c2.taskId)).find(e => e.kind === 'budget.exceeded').field, 'maxTotalTokens');
    assert.equal(d2.effects['tool-b1'].status, 'UNKNOWN', 'blocked tool settlement unconfirmed after release');
    const stopped = await s.call(`/tasks/${c2.taskId}/commands`, { requestKey: 'stop-b', intent: 'STOP', ...guard(d2) });
    assert.equal(stopped.body.state, 'APPLIED');
    assert.equal((await s.call('/tasks/' + c2.taskId)).body.state, 'BUDGET_EXCEEDED', 'STOP does not erase the budget verdict');
    assert.equal((await s.call(`/tasks/${c2.taskId}/artifacts`)).body.items.length >= 0, true, 'receipts and artifacts stay readable');
  } finally { await s.host.close(); }
});

test('native failure at settle reports its own reasonCode instead of a stale terminal reason', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'fail-model');
    assert.equal(c.status, 202);
    const d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'FAILED' && d; });
    assert.equal(d.reasonCode, 'MODEL_REQUEST_FAILED');
  } finally { await s.host.close(); }
});

test('writeScope gates file-mutating tool paths: allow workspace/extra, deny .., case variants and denyPaths', async () => {
  const s = await setup(); try {
    const extra = mkdtempSync(join(tmpdir(), 'nwf-pi-w4-extra-')); const deniedDir = join(extra, 'denied'); mkdirSync(deniedDir);
    const scope = { writeScope: { extraWriteDirs: [extra], denyPaths: [deniedDir] } };
    const c = await create(s.call, 'writefile ok.txt', scope);
    assert.equal(c.status, 202);
    let d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    const workspace = d.workspace;
    assert.equal(readFileSync(join(workspace, 'ok.txt'), 'utf8'), 'fixture write');
    const scopeId = s.host.store.task(c.taskId).writeScope.scopeId; assert.match(scopeId, /^wscope-/);
    const deny = async (target) => { const r = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'w-' + Math.random().toString(36).slice(2), kind: 'NATIVE_FOLLOW_UP', content: 'writefile ' + target, ...guard(await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; })) }); assert.equal(r.status, 202); await wait(async () => (await s.call('/tasks/' + c.taskId)).body.state === 'SUCCEEDED'); return (await events(s.call, c.taskId)).at(-1); };
    const lastViolation = () => events(s.call, c.taskId).then(items => items.filter(e => e.kind === 'write.scope.violation').at(-1));
    // outside the workspace via ..
    await deny('../escape.txt');
    let v = await lastViolation(); assert.equal(v.toolName, 'write'); assert.equal(v.scopeId, scopeId); assert.match(v.deniedPath, /escape\.txt$/);
    assert.equal(existsSync(resolve(workspace, '../escape.txt')), false, 'denied write never happened');
    // declared denyPaths win over extraWriteDirs
    await deny(join(deniedDir, 'x.txt')); v = await lastViolation(); assert.match(v.deniedPath, /denied.x\.txt$/);
    // absolute path inside workspace with .. normalization stays allowed
    await deny('sub/../ok2.txt'); assert.equal(readFileSync(join(workspace, 'ok2.txt'), 'utf8'), 'fixture write');
    assert.equal((await events(s.call, c.taskId)).filter(e => e.kind === 'write.scope.violation').length, 2, 'no violation recorded for in-scope write');
    // extraWriteDirs admit
    const follow = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'w-extra', kind: 'NATIVE_FOLLOW_UP', content: 'writefile ' + join(extra, 'extra-ok.txt'), ...guard((await s.call('/tasks/' + c.taskId)).body) });
    assert.equal(follow.status, 202); await wait(async () => (await s.call('/tasks/' + c.taskId)).body.state === 'SUCCEEDED');
    assert.equal(readFileSync(join(extra, 'extra-ok.txt'), 'utf8'), 'fixture write');
    if (process.platform === 'win32') {
      const swapped = workspace.replace(/^[A-Za-z]/, ch => ch === ch.toUpperCase() ? ch.toLowerCase() : ch.toUpperCase());
      await deny(join(swapped, 'inside-case.txt')); assert.equal(readFileSync(join(workspace, 'inside-case.txt'), 'utf8'), 'fixture write', 'case-insensitive inside path allowed');
      await deny(join(deniedDir.toUpperCase(), 'case.txt')); v = await lastViolation(); assert.ok(v.deniedPath, 'case-variant deny path still denied');
    }
    // scope binding is recorded into the sealed checkpoint manifest
    const p = await create(s.call, 'waiting', scope);
    const dp = await wait(async () => { const d = (await s.call('/tasks/' + p.taskId)).body; return d.state === 'RUNNING' && d; });
    assert.equal((await s.call(`/tasks/${p.taskId}/commands`, { requestKey: 'pause-s', intent: 'PAUSE', ...guard(dp) })).body.state, 'APPLIED');
    const manifest = (await s.call('/tasks/' + p.taskId)).body.checkpointManifest;
    assert.equal(manifest.writeScope.scopeId, s.host.store.task(p.taskId).writeScope.scopeId);
  } finally { await s.host.close(); }
});

test('environment binding resolves, fails terminally BINDING_UNAVAILABLE, and rejects secret allowlist names', async () => {
  const s = await setup(); try {
    process.env.NWF_W4_VISIBLE = 'w4-probe-value';
    const ok = await create(s.call, 'hello', { environment: { requiresCommands: ['node'], requiresDirs: [s.state], envAllowlist: ['NWF_W4_VISIBLE', 'NWF_W4_MISSING_OK'] } });
    assert.equal(ok.status, 202);
    const d = await wait(async () => { const d = (await s.call('/tasks/' + ok.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal(d.envProbe.NWF_W4_VISIBLE, true, 'allowlisted var reached the child env');
    assert.equal(d.envProbe.NWF_W4_MISSING_OK, false, 'absent allowlisted name is simply not copied');
    const binding = s.host.store.task(ok.taskId).environmentBinding;
    assert.ok(binding.resolved['command:node']); assert.equal(binding.envCopied.join(','), 'NWF_W4_VISIBLE');
    const bogus = await create(s.call, 'hello', { environment: { requiresCommands: ['nwf-w4-bogus-command-xyz'] } });
    const db = await wait(async () => { const d = (await s.call('/tasks/' + bogus.taskId)).body; return d.state === 'BINDING_UNAVAILABLE' && d; });
    assert.equal(db.quiescent, true); assert.equal(db.pid, undefined); assert.equal(db.launchState, 'REJECTED');
    assert.equal(db.bindingUnavailable.missing[0].kind, 'command'); assert.equal(db.bindingUnavailable.missing[0].name, 'nwf-w4-bogus-command-xyz');
    assert.ok((await events(s.call, bogus.taskId)).some(e => e.kind === 'BINDING_UNAVAILABLE'));
    assert.equal(db.usage.input, 0, 'no model call happened');
    const missingDir = await create(s.call, 'hello', { environment: { requiresDirs: [join(s.state, 'no-such-dir-xyz')] } });
    const dd = await wait(async () => { const d = (await s.call('/tasks/' + missingDir.taskId)).body; return d.state === 'BINDING_UNAVAILABLE' && d; });
    assert.equal(dd.bindingUnavailable.missing[0].kind, 'dir');
    for (const [environment, code] of [
      [{ envAllowlist: ['MODEL_API_KEY'] }, 'ENV_ALLOWLIST_DENIED'],
      [{ envAllowlist: ['NWF_BUILD_TOKEN'] }, 'ENV_ALLOWLIST_DENIED'],
      [{ envAllowlist: ['MY_SECRET'] }, 'ENV_ALLOWLIST_DENIED'],
      [{ envAllowlist: ['PAC_ENDPOINT'] }, 'ENV_ALLOWLIST_DENIED'],
      [{ envAllowlist: ['lowercase'] }, 'INVALID_ENVIRONMENT'],
      [{ envAllowlist: ['1ABC'] }, 'INVALID_ENVIRONMENT'],
      [{ requiresCommands: ['../escape'] }, 'INVALID_ENVIRONMENT'],
      [{ requiresDirs: ['relative/dir'] }, 'INVALID_ENVIRONMENT'],
    ]) { const r = await create(s.call, 'hello', { environment }); assert.equal(r.code, code, JSON.stringify(environment)); }
  } finally { await s.host.close(); delete process.env.NWF_W4_VISIBLE; }
});

test('audit chain and diagnostics view: complete durable facts, no bodies, redacted refs', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'writefile audit.txt');
    let d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    const secret = 'benign-w4s3cr3t-value';
    await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'secret-fu', kind: 'NATIVE_FOLLOW_UP', content: 'note api_key = "' + secret + '" and D:\\outside\\path', ...guard(d) });
    d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d.currentCommandId === 'secret-fu' && d; });
    assert.equal((await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-a', intent: 'STOP', ...guard(d) })).body.state, 'APPLIED');
    const manifest = (await s.call(`/tasks/${c.taskId}/artifacts`)).body;
    const audit = (await s.call(`/worker-host/v1/tasks/${c.taskId}/audit`)).body;
    assert.equal(audit.schemaVersion, 'worker-audit/1'); assert.equal(audit.subject.requestKey, c.request.requestKey);
    const kinds = audit.items.map(i => i.kind);
    for (const kind of ['ACCEPTED', 'DISPATCHING', 'INPUT_CONSUMED', 'TOOL_START', 'TOOL_END', 'ASSISTANT_MESSAGE', 'NATIVE_SETTLED', 'STOP_APPLIED']) assert.ok(kinds.includes(kind), 'audit missing ' + kind);
    assert.ok(Number(audit.items.at(-1).seq) > 0); assert.equal(audit.items.some(i => 'text' in i), false, 'no message bodies in audit');
    assert.equal(JSON.stringify(audit).includes('writefile audit.txt'), false, 'no prompt bodies anywhere in audit');
    assert.equal(audit.commands.find(o => o.requestKey === 'stop-a').kind, 'STOP');
    assert.equal(audit.commands.find(o => o.kind === 'TASK_CREATE').state, 'CONSUMED');
    assert.equal(audit.effects.find(e => e.toolCallId === 'tool-w1').status, 'SETTLED');
    const art = audit.artifacts.find(a => a.path === 'audit.txt'); assert.match(art.sha256, /^[0-9a-f]{64}$/); assert.ok(art.segmentId);
    assert.ok(audit.stateTransitions.length >= 1); assert.equal(audit.usage.source, 'ENGINE_REPORTED');
    const diagnostics = (await s.call(`/worker-host/v1/tasks/${c.taskId}/diagnostics?redact=content`)).body;
    assert.equal(diagnostics.schemaVersion, 'worker-diagnostics/1'); assert.equal(diagnostics.redaction, 'content');
    const blob = JSON.stringify(diagnostics);
    assert.equal(blob.includes('writefile audit.txt'), false); assert.equal(blob.includes(secret), false);
    assert.equal(blob.includes(d.workspace), false, 'workspace absolute path redacted');
    assert.equal(diagnostics.messages[2].text.sha256, hash('note api_key = "' + secret + '" and D:\\outside\\path'));
    assert.ok(diagnostics.messages[0].text.size > 0); assert.match(diagnostics.goal.sha256, /^[0-9a-f]{64}$/);
    assert.equal((await s.call(`/worker-host/v1/tasks/${c.taskId}/diagnostics?redact=none`)).body.code, 'INVALID_QUERY');
  } finally { await s.host.close(); }
});

test('completion releases the execution slot and queued creates run once in FIFO order', async () => {
  const s = await setup({ maxActiveTasks: 1 }); try {
    const a = await create(s.call, 'waiting-never'); const da = await wait(async () => { const d = (await s.call('/tasks/' + a.taskId)).body; return d.state === 'RUNNING' && d; });
    const b = await create(s.call, 'question'); assert.equal(b.status, 202);
    const c = await create(s.call, 'hello-again');
    let db = (await s.call('/tasks/' + b.taskId)).body; let dc = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(db.state, 'QUEUED'); assert.equal(db.launchState, 'QUEUED'); assert.equal(db.queue.position, 1); assert.ok(db.queueOrdinal);
    assert.equal(dc.queue.position, 2); assert.ok(dc.queueOrdinal > db.queueOrdinal);
    assert.equal(db.capabilities.stop, true, 'queued task accepts STOP');
    const stopped = await s.call(`/tasks/${a.taskId}/commands`, { requestKey: 'stop-a', intent: 'STOP', ...guard(da) }); assert.equal(stopped.body.state, 'APPLIED');
    db = await wait(async () => {
      const requests = (await s.call('/tasks/' + b.taskId + '/input-requests')).body.items;
      if (requests.length) return (await s.call('/tasks/' + b.taskId)).body;
    });
    assert.equal((await s.call('/tasks/' + c.taskId)).body.state, 'QUEUED', 'B holds the slot while it is executing');
    assert.equal(readFileSync(join(db.workspace, 'dispatch-count.txt'), 'utf8').trim().split('\n').length, 1, 'admitted intent dispatched exactly once');
    assert.ok((await events(s.call, b.taskId)).some(e => e.kind === 'ADMITTED'));
    const answered = await s.call(`/tasks/${b.taskId}/input-requests/q1/responses`, { requestKey: 'answer-b', response: { value: 'yes' }, ...guard(db) });
    assert.equal(answered.status, 202, JSON.stringify(answered.body));
    dc = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.ok((await events(s.call, c.taskId)).find(e => e.kind === 'ADMITTED').queueOrdinal > (await events(s.call, b.taskId)).find(e => e.kind === 'ADMITTED').queueOrdinal);
    assert.equal((await s.call('/tasks/' + b.taskId)).body.quiescent, false, 'settlement frees capacity while retaining the idle session');
    const running = await create(s.call, 'waiting-never');
    await wait(async () => (await s.call('/tasks/' + running.taskId)).body.state === 'RUNNING');
    const e = await create(s.call, 'hello-after-restart'); assert.equal((await s.call('/tasks/' + e.taskId)).body.state, 'QUEUED');
    await s.host.close();
    const host = new PiHost({ state: s.state, token, childModule: fixture, maxActiveTasks: 1 }); const call = client(await host.listen());
    try {
      const de = await wait(async () => { const d = (await call('/tasks/' + e.taskId)).body; return d.state === 'SUCCEEDED' && d; });
      assert.equal(readFileSync(join(de.workspace, 'dispatch-count.txt'), 'utf8').trim().split('\n').length, 1, 'restart re-admission dispatched exactly once');
      assert.equal((await call('/tasks/' + e.taskId)).body.nativeSessionId, 'fixture-pi-session');
    } finally { await host.close(); }
  } finally { await s.host.close(); }
});

test('SIGKILL re-admits queued work without replaying the interrupted dispatch', { skip: !['win32', 'darwin'].includes(process.platform) }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w4-qkill-')); const p = project();
  const child = spawn(process.execPath, [w4host, state, p, token, '', '1'], { stdio: ['ignore', 'pipe', 'pipe'] });
  const port = await new Promise(resolve => child.stdout.once('data', data => resolve(Number(String(data).trim()))));
  const call = client(port);
  const a = await create(call, 'waiting-never'); await wait(async () => (await call('/tasks/' + a.taskId)).body.state === 'RUNNING');
  const b = await create(call, 'hello'); assert.equal((await call('/tasks/' + b.taskId)).body.state, 'QUEUED');
  child.kill('SIGKILL'); await new Promise(resolve => child.once('exit', resolve));
  const host = new PiHost({ state, token, childModule: fixture, maxActiveTasks: 1 }); const call2 = client(await host.listen());
  try {
    const da = (await call2('/tasks/' + a.taskId)).body; assert.equal(da.state, 'UNKNOWN');
    assert.equal(da.intakeClosed, true);
    if (process.platform === 'win32') {
      // Host death closed the never-inherited Job: writer quiescence is proven
      // (outcome stays UNKNOWN) and the queued intent wins the freed slot.
      const row = host.store.task(a.taskId);
      assert.equal(row.quiescent, true);
      assert.equal(row.stopProof?.method, 'WINDOWS_JOB_CLOSED_ON_HOST_DEATH');
    } else {
      assert.equal(da.quiescent, false);
      assert.equal(da.stopProof, undefined);
    }
    const db = await wait(async () => { const d = (await call2('/tasks/' + b.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal(readFileSync(join(db.workspace, 'dispatch-count.txt'), 'utf8').trim().split('\n').length, 1);
    assert.equal(readFileSync(join(da.workspace, 'dispatch-count.txt'), 'utf8').trim().split('\n').length, 1);
  } finally { await host.close(); }
});

test('SIGKILL after budget-exceed decision before state write reconciles UNKNOWN', { skip: !['win32', 'darwin'].includes(process.platform) }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w4-bkill-')); const p = project();
  const child = spawn(process.execPath, [w4host, state, p, token, 'budget_decision'], { stdio: ['ignore', 'pipe', 'pipe'] });
  const port = await new Promise(resolve => child.stdout.once('data', data => resolve(Number(String(data).trim()))));
  const call = client(port);
  const c = await create(call, 'budget-multi', { budget: { maxTotalTokens: 4, enforcement: 'SOFT' } });
  await new Promise(resolve => child.once('exit', resolve));
  const host = new PiHost({ state, token, childModule: fixture });
  try {
    const task = host.store.task(c.taskId);
    assert.equal(task.state, 'UNKNOWN'); assert.equal(task.reasonCode, 'RESTART_RECONCILIATION_REQUIRED');
    assert.equal(task.budgetExceeded, undefined, 'decision was never durably written');
    const items = await (client(await host.listen()))(`/tasks/${c.taskId}/events?limit=100`);
    assert.equal(items.body.items.some(e => e.kind === 'budget.exceeded'), false);
  } finally { await host.close(); }
});
