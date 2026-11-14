import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { artifact, artifacts } from '../dist/host/artifacts.js';

test('real SDK bash settles and admits queued work while keeping credentials out of the shell', async () => {
  const names = ['MODEL_API_KEY', 'MODEL_BASE_URL', 'MODEL_DEFAULT_MODEL', 'LLM_HEADER_x_session_sentinel', 'WORKER_LOCAL_TOKEN'];
  const saved = Object.fromEntries(names.map(k => [k, process.env[k]])); let calls = 0; let networkAuth = false; let networkHeader = false; let sawPersistedContext = false;
  const api = createServer(async (req, res) => {
    let request = ''; for await (const part of req) request += part;
    if (calls >= 2) sawPersistedContext ||= JSON.parse(request).messages.some(m => m.role === 'assistant' && m.content?.includes('ENVIRONMENT_CHECK_DONE'));
    networkAuth ||= req.headers.authorization === 'Bearer benign-model-sentinel-not-credential';
    networkHeader ||= req.headers['x-session-sentinel'] === 'benign-header-sentinel-not-credential';
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const chunk = delta => res.write('data: ' + JSON.stringify({ id: 'local-fixture', object: 'chat.completion.chunk', created: 1, model: 'fixture-model', choices: [{ index: 0, delta, finish_reason: null }] }) + '\n\n');
    if (calls++ === 0) {
      const code = "require('fs').writeFileSync('env-check.json', JSON.stringify(Object.fromEntries(['MODEL_API_KEY','LLM_HEADER_x_session_sentinel','WORKER_LOCAL_TOKEN'].map(k=>[k,process.env[k]??null]))))";
      chunk({ role: 'assistant', tool_calls: [{ index: 0, id: 'call_environment', type: 'function', function: { name: 'bash', arguments: JSON.stringify({ command: 'node -e ' + JSON.stringify(code) }) } }] });
      res.write('data: ' + JSON.stringify({ id: 'local-fixture', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] }) + '\n\n');
    } else if (calls === 3) {
      chunk({ role: 'assistant', tool_calls: [{ index: 0, id: 'call_builtin_write', type: 'function', function: { name: 'write', arguments: JSON.stringify({ path: 'env-check.json', content: 'builtin file bytes' }) } }] });
      res.write('data: ' + JSON.stringify({ id: 'local-fixture', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] }) + '\n\n');
    } else {
      chunk({ role: 'assistant', content: 'ENVIRONMENT_CHECK_DONE' });
      res.write('data: ' + JSON.stringify({ id: 'local-fixture', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 2, completion_tokens: 2, total_tokens: 4 } }) + '\n\n');
    }
    res.end('data: [DONE]\n\n');
  });
  await new Promise(done => api.listen(0, '127.0.0.1', done)); const apiPort = api.address().port;
  process.env.MODEL_API_KEY = 'benign-model-sentinel-not-credential'; process.env.MODEL_BASE_URL = `http://127.0.0.1:${apiPort}/v1`; process.env.MODEL_DEFAULT_MODEL = 'fixture-model';
  process.env.LLM_HEADER_x_session_sentinel = 'benign-header-sentinel-not-credential'; process.env.WORKER_LOCAL_TOKEN = 'benign-owner-sentinel-not-credential';
  const root = mkdtempSync(join(tmpdir(), 'nwf-pi-env-')); const project = join(root, 'source'); execFileSync('git', ['init', project], { stdio: 'ignore' }); writeFileSync(join(project, 'README.md'), 'baseline');
  execFileSync('git', ['-C', project, 'add', '.']); execFileSync('git', ['-C', project, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'base'], { stdio: 'ignore' });
  const config = { state: join(root, 'state'), token: 'isolated-network-fixture-owner-token', maxActiveTasks: 1 };
  let host = new PiHost(config);
  try {
    host.register('fixture', project); const p = host.preflight({ engineId: 'sage-pi', projectId: 'fixture' });
    const shell = host.create({ requestKey: 'env-start', projectId: 'fixture', engineId: 'sage-pi', optionsRevision: p.optionsRevision, instruction: 'Exercise the scripted shell fixture.' });
    const queued = host.create({ requestKey: 'builtin-start', projectId: 'fixture', engineId: 'sage-pi', optionsRevision: p.optionsRevision, instruction: 'Exercise the builtin file fixture.' });
    assert.equal(queued.launchState, 'QUEUED');
    const unexpectedGateChecks = [];
    host.gateCheck = (_taskId, _live, event) => { unexpectedGateChecks.push(event.kind); };
    let receipt = shell;
    let task;
    for (let n = 0; n < 1200; n++) { task = host.store.task(receipt.taskId); if (task.state === 'SUCCEEDED' || task.state === 'FAILED') break; await new Promise(done => setTimeout(done, 50)); }
    assert.equal(task.state, 'SUCCEEDED'); assert.equal(networkAuth, true); assert.equal(networkHeader, true);
    assert.deepEqual(unexpectedGateChecks, [], 'ordinary requests and tools do not wait for host approval');
    assert.equal(task.quiescent, false);
    const shellDeadline = Number(task.deadlineEpochMillis);
    const environment = JSON.parse(readFileSync(join(task.workspace, 'env-check.json'), 'utf8')); assert.deepEqual(environment, { MODEL_API_KEY: null, LLM_HEADER_x_session_sentinel: null, WORKER_LOCAL_TOKEN: null });
    let original = { ...task };
    receipt = queued;
    for (let n = 0; n < 400; n++) { task = host.store.task(receipt.taskId); if (task.state === 'SUCCEEDED' || task.state === 'FAILED') break; await new Promise(done => setTimeout(done, 50)); }
    assert.equal(task.state, 'SUCCEEDED', 'queued task completed without a STOP on the preceding shell task');
    assert.equal(host.store.events(queued.taskId, '0', 100).items.filter(e => e.kind === 'ADMITTED').length, 1);
    await host.sweepTaskDeadlines(shellDeadline + 1);
    assert.equal(host.store.task(shell.taskId).state, 'SUCCEEDED', 'execution deadline ignores a settled turn');
    assert.deepEqual(unexpectedGateChecks, []);
    delete process.env.MODEL_API_KEY; assert.equal(host.manifest().currentAvailability, 'UNCONFIGURED');
    process.env.MODEL_API_KEY = 'benign-model-sentinel-not-credential';
    if (process.platform === 'darwin') {
      await assert.rejects(() => host.sourceSeal(shell.taskId, { requestKey: 'seal-shell', target: 'HANDOFF' }), /TASK_NOT_QUIESCENT/);
      const unproven = host.store.task(shell.taskId);
      assert.equal(unproven.state, 'SUCCEEDED'); assert.equal(unproven.quiescent, false);
      assert.equal(unproven.processOwnership.forkObserved, true); assert.equal(unproven.processOwnership.rootExited, true);
      assert.equal(unproven.sealRequested, undefined);
      assert.equal(artifacts(host.store, unproven).reasonCode, 'STOP_TO_SEAL_RESULTS');
    }
    original = { ...task };
    const builtinStop = await host.stop(task.taskId, { requestKey: 'builtin-stop', intent: 'STOP', generation: task.generation, nativeSessionId: task.nativeSessionId, targetCommandId: task.currentCommandId });
    assert.equal(builtinStop.state, 'APPLIED');
    const sealed = artifacts(host.store, host.store.task(task.taskId)).items.find(r => r.path === 'env-check.json');
    const originalBytes = artifact(host.store, host.store.task(task.taskId), sealed.artifactId).bytes;
    await host.close(); host = new PiHost(config);
    assert.equal(host.store.task(shell.taskId).state, 'SUCCEEDED');
    assert.equal(host.store.events(queued.taskId, '0', 100).items.filter(e => e.kind === 'DISPATCHING').length, 1);
    const completed = host.store.task(task.taskId); assert.equal(host.detail(completed).capabilities.completedSessionFollowUpAfterRestart, true);
    const follow = { requestKey: 'after-restart', kind: 'NATIVE_FOLLOW_UP', content: 'Continue the persisted conversation.', generation: completed.generation, nativeSessionId: completed.nativeSessionId, targetCommandId: completed.currentCommandId };
    const sessionBytes = readFileSync(completed.nativeSessionFile); writeFileSync(completed.nativeSessionFile, Buffer.concat([sessionBytes, Buffer.from(' ')]));
    assert.throws(() => host.interact(task.taskId, follow), /NATIVE_SESSION_SEAL_CHANGED/); writeFileSync(completed.nativeSessionFile, sessionBytes);
    assert.equal(host.interact(task.taskId, follow).reopenedCompletedSession, true);
    for (let n = 0; n < 1200; n++) { task = host.store.task(receipt.taskId); if (task.state === 'SUCCEEDED' || task.state === 'FAILED') break; await new Promise(done => setTimeout(done, 50)); }
    assert.equal(task.state, 'SUCCEEDED'); assert.equal(sawPersistedContext, true); assert.equal(task.nativeSessionId, original.nativeSessionId); assert.notEqual(task.generation, original.generation); assert.notEqual(task.segmentId, original.segmentId);
    assert.equal(host.interact(task.taskId, follow).requestKey, follow.requestKey, 'original old-generation request replays after reopening');
    await host.stop(task.taskId, { requestKey: 'stop-reopened', intent: 'STOP', generation: task.generation, nativeSessionId: task.nativeSessionId, targetCommandId: task.currentCommandId });
    const newRow = artifacts(host.store, host.store.task(task.taskId)).items.find(r => r.path === 'env-check.json'); assert.notEqual(newRow.artifactId, sealed.artifactId);
    assert.equal(artifact(host.store, host.store.task(task.taskId), sealed.artifactId).bytes, originalBytes, 'old artifact ref remains immutable');
  } finally {
    await host.close(); await new Promise(done => api.close(done)); for (const key of names) if (saved[key] === undefined) delete process.env[key]; else process.env[key] = saved[key];
  }
});
