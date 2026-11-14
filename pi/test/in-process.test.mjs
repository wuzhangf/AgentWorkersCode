import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { artifacts, artifact } from '../dist/host/artifacts.js';
import { hash } from '../dist/host/store.js';
import { IN_PROCESS_PROFILE, SANDBOX_EXEC, NO_FORK_POLICY } from '../dist/control/profiles.js';

const token = 'isolated-in-process-profile-test-token';
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'nwf-pi-inprocess-')), project = join(root, 'source'); mkdirSync(project);
  execFileSync('git', ['init', project], { stdio: 'ignore' }); writeFileSync(join(project, 'base.txt'), 'baseline');
  for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'base']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
  return { root, project, state: join(root, 'state') };
}
async function wait(fn) { for (let n = 0; n < 400; n++) { const result = await fn(); if (result) return result; await new Promise(done => setTimeout(done, 25)); } throw new Error('WAIT_TIMEOUT'); }
function create(host, key, instruction) { const p = host.preflight({ engineId: 'sage-pi', projectId: 'demo' }); return host.create({ requestKey: key, projectId: 'demo', engineId: 'sage-pi', optionsRevision: p.optionsRevision, instruction }); }

test('unknown execution profile fails explicitly before opening state', () => {
  assert.throws(() => new PiHost({ state: '/unused', token, executionProfile: 'implicit-fallback' }), /EXECUTION_PROFILE_UNSUPPORTED/);
});

test('macOS explicit in-process profile denies an actual subprocess while guardian proves no fork', { skip: process.platform !== 'darwin' }, async () => {
  const f = fixture(); const host = new PiHost({ ...f, token, executionProfile: IN_PROCESS_PROFILE, childModule: fileURLToPath(new URL('./fixture-child.mjs', import.meta.url)) });
  try {
    host.register('demo', f.project); const c = create(host, 'fork-denied', 'fork-denied');
    const task = await wait(() => { const t = host.store.task(c.taskId); return ['SUCCEEDED', 'FAILED', 'UNKNOWN'].includes(t.state) && t.nativeIdle && t; });
    assert.equal(task.state, 'SUCCEEDED'); assert.match(task.output, /fork denied EPERM/);
    assert.equal(existsSync(join(task.workspace, 'fork-must-not-exist.txt')), false);
    assert.equal((await host.stop(task.taskId, { requestKey: 'probe-stop', intent: 'STOP', ...guard(task) })).state, 'APPLIED');
    assert.equal(host.store.task(task.taskId).processOwnership.forkObserved, false);
    assert.equal(host.manifest().executionProfile, IN_PROCESS_PROFILE);
    assert.equal(host.engineBinding.provenance.files.find(f => f.name === 'sandbox/executable').sha256, hash(readFileSync(SANDBOX_EXEC)));
    assert.equal(host.engineBinding.provenance.processCreationPolicy.policySha256, hash(NO_FORK_POLICY));
  } finally { await host.close(); }
  assert.throws(() => new PiHost({ state: f.state, token }), /EXECUTION_PROFILE_CHANGED/);
  const restarted = new PiHost({ state: f.state, token, executionProfile: IN_PROCESS_PROFILE }); await restarted.close();
  const native = fixture(); const old = new PiHost({ state: native.state, token }); old.store.db.prepare('DELETE FROM meta WHERE key=?').run('execution-profile'); await old.close();
  assert.throws(() => new PiHost({ state: native.state, token, executionProfile: IN_PROCESS_PROFILE }), /EXECUTION_PROFILE_CHANGED/, 'legacy stores bind native and cannot silently migrate');
});

test('macOS actual SDK in-process tools, sealed bytes and persisted context reopen under the same profile', { skip: process.platform !== 'darwin' }, async () => {
  const names = ['MODEL_API_KEY', 'MODEL_BASE_URL', 'MODEL_DEFAULT_MODEL'], saved = Object.fromEntries(names.map(k => [k, process.env[k]]));
  let calls = 0, priorContext = false; const toolSets = [];
  const api = createServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk; const body = JSON.parse(raw);
    toolSets.push(body.tools.map(t => t.function.name)); priorContext ||= body.messages.some(m => m.role === 'assistant' && m.content?.includes('BUILTIN_PROFILE_DONE'));
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const delta = calls++ === 0 ? { role: 'assistant', tool_calls: [{ index: 0, id: 'builtin-write', type: 'function', function: { name: 'write', arguments: JSON.stringify({ path: 'result.txt', content: 'bounded bytes' }) } }] } : { role: 'assistant', content: 'BUILTIN_PROFILE_DONE' };
    res.write('data: ' + JSON.stringify({ id: 'bounded-model', object: 'chat.completion.chunk', choices: [{ index: 0, delta, finish_reason: null }] }) + '\n\n');
    res.write('data: ' + JSON.stringify({ id: 'bounded-model', choices: [{ index: 0, delta: {}, finish_reason: calls === 1 ? 'tool_calls' : 'stop' }], usage: { prompt_tokens: 2, completion_tokens: 2, total_tokens: 4 } }) + '\n\n'); res.end('data: [DONE]\n\n');
  });
  await new Promise(done => api.listen(0, '127.0.0.1', done)); const f = fixture();
  process.env.MODEL_API_KEY = 'benign-inprocess-model-sentinel'; process.env.MODEL_DEFAULT_MODEL = 'fixture-model'; process.env.MODEL_BASE_URL = `http://127.0.0.1:${api.address().port}/v1`;
  let host = new PiHost({ state: f.state, token, executionProfile: IN_PROCESS_PROFILE });
  try {
    host.register('demo', f.project); const c = create(host, 'bounded-first', 'Create the builtin fixture.');
    let task = await wait(() => { const t = host.store.task(c.taskId); return t.nativeIdle && t; }); assert.equal(task.state, 'SUCCEEDED');
    assert.deepEqual(task.activeTools.sort(), ['ask_user', 'edit', 'read', 'write']);
    for (const tools of toolSets) assert.deepEqual(tools.sort(), ['ask_user', 'edit', 'read', 'write']);
    assert.equal((await host.stop(task.taskId, { requestKey: 'bounded-stop', intent: 'STOP', ...guard(task) })).state, 'APPLIED');
    const item = artifacts(host.store, host.store.task(task.taskId)).items.find(i => i.path === 'result.txt'); assert.equal(item.sha256, hash('bounded bytes'));
    assert.equal(Buffer.from(artifact(host.store, host.store.task(task.taskId), item.artifactId).bytes, 'base64').toString(), 'bounded bytes');
    const sessionId = task.nativeSessionId; await host.close();
    host = new PiHost({ state: f.state, token, executionProfile: IN_PROCESS_PROFILE }); task = host.store.task(c.taskId);
    assert.equal(host.detail(task).capabilities.completedSessionFollowUpAfterRestart, true);
    host.interact(task.taskId, { requestKey: 'bounded-follow', kind: 'NATIVE_FOLLOW_UP', content: 'Recall the previous message.', ...guard(task) });
    task = await wait(() => { const t = host.store.task(c.taskId); return t.nativeIdle && t; }); assert.equal(task.state, 'SUCCEEDED');
    assert.equal(task.nativeSessionId, sessionId); assert.equal(priorContext, true);
    assert.equal((await host.stop(task.taskId, { requestKey: 'bounded-follow-stop', intent: 'STOP', ...guard(task) })).state, 'APPLIED');
  } finally { await host.close(); await new Promise(done => api.close(done)); for (const key of names) if (saved[key] === undefined) delete process.env[key]; else process.env[key] = saved[key]; }
});
