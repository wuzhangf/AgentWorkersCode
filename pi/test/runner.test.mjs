import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { CommandRunner, qualifyReceipt, runnerEntry } from '../dist/host/runner.js';
import { RUNNER_PROFILE } from '../dist/control/profiles.js';

const token = 'pi-isolated-runner-fixture-owner-token';
const enabled = process.platform === 'darwin' && existsSync(runnerEntry);
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
async function wait(fn, ms = 30000) { const end = Date.now() + ms; while (Date.now() < end) { const value = await fn(); if (value) return value; await new Promise(done => setTimeout(done, 25)); } throw new Error('RUNNER_WAIT_TIMEOUT'); }
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'nwf-pi-runner-')), project = join(root, 'project'); mkdirSync(project);
  writeFileSync(join(project, 'calc.js'), 'exports.add=(a,b)=>a-b;\n');
  writeFileSync(join(project, 'calc.test.js'), "const test=require('node:test'),assert=require('node:assert/strict');test('addition',()=>assert.equal(require('./calc.js').add(2,3),5));\n");
  execFileSync('git', ['init', project], { stdio: 'ignore' });
  for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'base']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
  return { root, project, state: join(root, 'state') };
}
async function model(steps) {
  let calls = 0; const tools = [];
  const api = createServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk; const body = JSON.parse(raw); tools.push(body.tools.map(t => t.function.name));
    const step = steps[calls++]; const delta = step ? { role: 'assistant', tool_calls: [{ index: 0, id: 'runner-tool-' + calls, type: 'function', function: { name: step.name, arguments: JSON.stringify(step.input) } }] } : { role: 'assistant', content: 'RUNNER_FIXTURE_DONE' };
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.write('data: ' + JSON.stringify({ id: 'runner-fixture', object: 'chat.completion.chunk', choices: [{ index: 0, delta, finish_reason: null }] }) + '\n\n');
    res.write('data: ' + JSON.stringify({ id: 'runner-fixture', choices: [{ index: 0, delta: {}, finish_reason: step ? 'tool_calls' : 'stop' }], usage: { prompt_tokens: 2, completion_tokens: 2, total_tokens: 4 } }) + '\n\n'); res.end('data: [DONE]\n\n');
  });
  await new Promise(done => api.listen(0, '127.0.0.1', done));
  return { api, tools, calls: () => calls, env: { MODEL_API_KEY: 'benign-runner-fixture-sentinel', MODEL_DEFAULT_MODEL: 'fixture-model', MODEL_BASE_URL: `http://127.0.0.1:${api.address().port}/v1` }, close: () => new Promise(done => api.close(done)) };
}
function create(host, instruction = 'Run the fixture') { const pf = host.preflight({ engineId: 'sage-pi', projectId: 'demo' }); const body = { requestKey: 'runner-create', projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction }; return { body, receipt: host.create(body) }; }
async function withEnv(env, fn) { const saved = Object.fromEntries(Object.keys(env).map(k => [k, process.env[k]])); Object.assign(process.env, env); try { return await fn(); } finally { for (const k of Object.keys(env)) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; } }

test('test qualification requires nonzero observed tests independently of command success', () => {
  assert.equal(qualifyReceipt({ kind: 'test', status: 'SUCCEEDED', exitCode: 0, stdout: '' }).tests.evidence, 'UNKNOWN');
  assert.equal(qualifyReceipt({ kind: 'test', status: 'SUCCEEDED', exitCode: 0, stdout: '# tests 0\n# pass 0\n# fail 0\n' }).tests.passedWithTests, false);
  assert.equal(qualifyReceipt({ kind: 'test', status: 'FAILED', exitCode: 1, stdout: '# tests 1\n# pass 0\n# fail 1\n' }).tests.nonzeroExecuted, true);
});

test('macOS SDK Runner tools execute command/failure/build/test, preserve no-fork model and scoped seal/reopen', { skip: !enabled }, async () => {
  const f = fixture(), m = await model([
    { name: 'run_command', input: { command: 'printf RUNNER_STDOUT' } },
    { name: 'bash', input: { command: 'printf RUNNER_STDERR >&2; exit 7' } },
    { name: 'run_build', input: { command: 'node --check calc.js' } },
    { name: 'run_tests', input: { command: 'node --test --test-reporter=tap calc.test.js' } },
    { name: 'write', input: { path: 'calc.js', content: 'exports.add=(a,b)=>a+b;\n' } },
    { name: 'run_tests', input: { command: 'node --test --test-reporter=tap calc.test.js' } },
    null, { name: 'run_command', input: { command: 'printf FOLLOWUP_CONTEXT' } },
  ]);
  await withEnv(m.env, async () => {
    let host = new PiHost({ ...f, token, executionProfile: RUNNER_PROFILE });
    try {
      host.register('demo', f.project); const c = create(host);
      const t = await wait(() => { const row = host.store.task(c.receipt.taskId); return row.nativeIdle && row; });
      assert.equal(t.state, 'SUCCEEDED'); assert.equal(t.profileId, 'MANAGED');
      assert.deepEqual(t.activeTools.sort(), ['ask_user', 'bash', 'edit', 'read', 'run_build', 'run_command', 'run_tests', 'write']);
      for (const tools of m.tools) assert.deepEqual(tools.sort(), t.activeTools);
      const receipts = Object.values(t.runnerCommands).map(r => r.receipt); assert.equal(receipts.length, 5);
      assert.equal(host.audit(t).runnerCommands[0].receipt.stdout.size, Buffer.byteLength('RUNNER_STDOUT')); assert.equal(JSON.stringify(host.audit(t)).includes('RUNNER_STDOUT'), false);
      assert.equal(receipts[0].stdout, 'RUNNER_STDOUT'); assert.equal(receipts[1].exitCode, 7); assert.equal(receipts[1].stderr, 'RUNNER_STDERR');
      assert.equal(receipts[2].buildSucceeded, true); assert.equal(receipts[3].exitCode, 1); assert.equal(receipts[3].tests.executed, 1); assert.equal(receipts[3].tests.passedWithTests, false);
      assert.equal(receipts[4].exitCode, 0); assert.equal(receipts[4].tests.passedWithTests, true);
      host.interact(t.taskId, { requestKey: 'runner-follow-live', kind: 'NATIVE_FOLLOW_UP', content: 'Use the next command', ...guard(t) });
      const followed = await wait(() => { const row = host.store.task(t.taskId); return row.nativeIdle && row.currentCommandId === 'runner-follow-live' && row; });
      assert.equal(followed.generation, t.generation); assert.notEqual(followed.segmentId, t.segmentId);
      assert.equal(new Set(Object.values(followed.runnerCommands).map(r => r.contextId)).size, 2);
      assert.equal((await host.stop(t.taskId, { requestKey: 'runner-stop', intent: 'STOP', ...guard(followed) })).state, 'APPLIED');
      const stopped = host.store.task(t.taskId); assert.equal(stopped.processOwnership.forkObserved, false); assert.equal(stopped.stopProof.scope, 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS');
      assert(stopped.stopProof.runnerContexts.every(ctx => ctx.closed && ctx.activeCommands.length === 0 && ctx.unknownCommands.length === 0 && ctx.stopProof.scope === 'REGISTERED_PROCESS_GROUP'));
      await host.close(); host = new PiHost({ ...f, token, executionProfile: RUNNER_PROFILE });
      assert.equal(host.detail(host.store.task(t.taskId)).capabilities.completedSessionFollowUpAfterRestart, true); assert.equal(host.create(c.body).taskId, t.taskId);
      assert.equal(Object.values(host.store.task(t.taskId).runnerCommands).length, 6);
      const persisted = host.store.task(t.taskId); host.interact(t.taskId, { requestKey: 'runner-reopen', kind: 'NATIVE_FOLLOW_UP', content: 'Recall prior fixture', ...guard(persisted) });
      const reopened = await wait(() => { const row = host.store.task(t.taskId); return row.nativeIdle && row.currentCommandId === 'runner-reopen' && row; });
      assert.equal(reopened.nativeSessionId, t.nativeSessionId); assert.notEqual(reopened.generation, t.generation);
      assert.equal((await host.stop(t.taskId, { requestKey: 'runner-reopen-stop', intent: 'STOP', ...guard(reopened) })).state, 'APPLIED');
    } finally { await host.close(); }
  });
  await m.close();
});


test('macOS Runner rejects outside cwd before dispatch and exposes no host/model credentials to commands', { skip: !enabled }, async () => {
  const f = fixture(), m = await model([
    { name: 'run_command', input: { command: 'printf MUST_NOT_RUN', cwd: '..' } },
    { name: 'run_command', input: { command: `node -e 'if(Object.keys(process.env).some(k=>/^(MODEL|LLM|WORKER)_/.test(k)))process.exit(9);process.stdout.write("CREDENTIALS_ABSENT")'` } },
  ]);
  try { await withEnv(m.env, async () => {
    const host = new PiHost({ ...f, token, executionProfile: RUNNER_PROFILE });
    try {
      host.register('demo', f.project); const c = create(host); const t = await wait(() => { const t = host.store.task(c.receipt.taskId); return t.nativeIdle && t; });
      assert.equal(t.state, 'SUCCEEDED'); const records = Object.values(t.runnerCommands); assert.equal(records[0].status, 'REJECTED'); assert.equal(records[0].reasonCode, 'CWD_OUTSIDE_WORKSPACE'); assert.equal(records[0].commandId, undefined);
      assert.equal(records[1].receipt.stdout, 'CREDENTIALS_ABSENT'); assert.equal(records[1].receipt.exitCode, 0);
      assert.equal((await host.stop(t.taskId, { requestKey: 'rejected-stop', intent: 'STOP', ...guard(t) })).state, 'APPLIED');
    } finally { await host.close(); }
  }); } finally { await m.close(); }
});

test('macOS Runner STOP stops an actual long command heartbeat before model teardown', { skip: !enabled }, async () => {
  const f = fixture(), m = await model([{ name: 'run_command', input: { command: `node -e 'setInterval(()=>require("fs").appendFileSync("heartbeat.txt","x"),25)'`, timeoutMs: 600000 } }]);
  try { await withEnv(m.env, async () => {
    const host = new PiHost({ ...f, token, executionProfile: RUNNER_PROFILE });
    try {
      host.register('demo', f.project); const c = create(host);
      const t = await wait(() => { const t = host.store.task(c.receipt.taskId); return existsSync(join(t.workspace, 'heartbeat.txt')) && t; });
      assert.equal((await host.stop(t.taskId, { requestKey: 'runner-long-stop', intent: 'STOP', ...guard(t) })).state, 'APPLIED');
      const size = readFileSync(join(t.workspace, 'heartbeat.txt')).length; await new Promise(done => setTimeout(done, 200)); assert.equal(readFileSync(join(t.workspace, 'heartbeat.txt')).length, size);
      assert.equal(host.store.task(t.taskId).quiescent, true); assert.equal(Object.values(host.store.task(t.taskId).runnerCommands)[0].status, 'CANCELLED');
    } finally { await host.close(); }
  }); } finally { await m.close(); }
});

test('macOS Runner owner EOF after host SIGKILL stops heartbeat and original task key never replays', { skip: !enabled }, async () => {
  const f = fixture(), m = await model([{ name: 'run_command', input: { command: `node -e 'require("fs").appendFileSync("dispatch.txt","x");setInterval(()=>require("fs").appendFileSync("heartbeat.txt","x"),25)'`, timeoutMs: 600000 } }]);
  const child = spawn(process.execPath, [new URL('./runner-host.mjs', import.meta.url).pathname, f.state, f.project], { env: { ...process.env, ...m.env, WORKER_LOCAL_TOKEN: token }, stdio: ['ignore', 'pipe', 'pipe'] }); child.stderr.resume(); let host;
  try {
    const port = await new Promise((done, reject) => { child.stdout.once('data', data => done(Number(String(data).trim()))); child.once('error', reject); });
    const call = async (path, body) => { const res = await fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); assert(res.ok); return res.json(); };
    const pf = await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' }), body = { requestKey: 'crash-runner-original', projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction: 'Run heartbeat' }, c = await call('/tasks', body);
    const t = await wait(async () => { const t = await call('/tasks/' + c.taskId); return existsSync(join(t.workspace, 'heartbeat.txt')) && t; });
    const exited = new Promise(done => child.once('exit', done)); child.kill('SIGKILL'); await exited;
    await new Promise(done => setTimeout(done, 500)); const size = readFileSync(join(t.workspace, 'heartbeat.txt')).length; await new Promise(done => setTimeout(done, 200)); assert.equal(readFileSync(join(t.workspace, 'heartbeat.txt')).length, size);
    host = new PiHost({ ...f, token, executionProfile: RUNNER_PROFILE }); const restarted = host.store.task(t.taskId); assert.equal(restarted.state, 'UNKNOWN'); assert.equal(restarted.quiescent, false); assert.equal(host.create(body).taskId, t.taskId);
    const originalCommand = Object.values(restarted.runnerCommands)[0]; assert.equal(originalCommand.status, 'UNKNOWN');
    const queryRunner = await CommandRunner.start(join(f.state, 'command-runner', originalCommand.contextId), { contextId: originalCommand.contextId, workspace: t.workspace, binding: originalCommand.binding, envAllowlist: [] });
    try { const recovered = await queryRunner.lookup(originalCommand.requestKey); assert.equal(recovered.status, 'UNKNOWN'); assert.equal(recovered.commandId, originalCommand.commandId); } finally { await queryRunner.close(); }
    await new Promise(done => setTimeout(done, 100)); assert.equal(readFileSync(join(t.workspace, 'dispatch.txt'), 'utf8'), 'x'); assert.equal(m.calls(), 1);
  } finally { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await host?.close(); await m.close(); }
});
