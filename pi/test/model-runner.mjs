import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';
import { CommandRunner } from '../dist/host/runner.js';

if (process.platform !== 'darwin' || !process.env.MODEL_API_KEY) throw new Error('Approved macOS model environment required');
const token = randomBytes(32).toString('hex'), profile = 'macos-runner-v1';
const local = resolve('.local'); mkdirSync(local, { recursive: true }); const run = mkdtempSync(join(local, 'model-runner-')), project = join(run, 'project'), state = join(run, 'state'); mkdirSync(project);
writeFileSync(join(project, 'package.json'), JSON.stringify({ name: 'runner-real-fixture', private: true, scripts: { build: 'node --check calc.js', test: 'node --test --test-reporter=tap calc.test.js' } }));
writeFileSync(join(project, 'calc.js'), 'exports.add=(a,b)=>a-b;\n');
writeFileSync(join(project, 'calc.test.js'), "const test=require('node:test'),assert=require('node:assert/strict');test('addition',()=>assert.equal(require('./calc.js').add(2,3),5));\n");
execFileSync('git', ['init', project], { stdio: 'ignore' }); for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'fixture']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
let host = new PiHost({ state, token, executionProfile: profile }); host.register('demo', project); let port = await host.listen(); let crashHost;
const callAt = port => async (path, body) => { const res = await fetch(`http://127.0.0.1:${port}/local/v1${path}`, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await res.json(); if (!res.ok) throw new Error(value.code); return value; };
const call = (...args) => callAt(port)(...args);
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
async function wait(fn, timeout = 240000) { const end = Date.now() + timeout; while (Date.now() < end) { const v = await fn(); if (v) return v; await new Promise(done => setTimeout(done, 100)); } throw new Error('REAL_RUNNER_TIMEOUT'); }
async function settled(taskId, key) { return wait(async () => { const t = await call('/tasks/' + taskId); if (['FAILED', 'UNKNOWN'].includes(t.state) && t.nativeIdle) throw new Error('REAL_MODEL_' + t.state + ':' + t.reasonCode); return t.state === 'SUCCEEDED' && t.nativeIdle && t.currentCommandId === key && t; }); }
async function create(key, instruction) { const pf = await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' }); const body = { requestKey: key, projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction }; return { body, receipt: await call('/tasks', body) }; }
async function follow(t, key, content) { await call(`/tasks/${t.taskId}/messages`, { requestKey: key, kind: 'NATIVE_FOLLOW_UP', content, ...guard(t) }); return settled(t.taskId, key); }
const records = task => Object.values(task.runnerCommands ?? {});
try {
  console.log('REAL_RUNNER_START ' + JSON.stringify({ run, profile, model: process.env.MODEL_DEFAULT_MODEL }));
  const c = await create('real-runner-main', 'Remember the selected word VIOLET for later. For this step only: use run_command with command printf REAL_RUNNER_STDOUT; use bash with command printf REAL_RUNNER_STDERR >&2; exit 7 (one shell script). Do not modify files, build or test. Report the actual exit codes.');
  let t = await settled(c.receipt.taskId, c.body.requestKey);
  let rs = records(t); assert(rs.some(r => r.receipt?.stdout.includes('REAL_RUNNER_STDOUT'))); assert(rs.some(r => r.receipt?.exitCode === 7 && r.receipt.stderr.includes('REAL_RUNNER_STDERR')));
  console.log('REAL_RUNNER_COMMAND_FAILURE_PASS');
  t = await follow(t, 'real-runner-failure-test', 'Use run_build with command npm run build. Then use run_tests with command npm test. This is the baseline verification step: do not modify any file and do not fix the failing addition test yet. Report the actual build and test results then stop this turn.');
  rs = records(t); assert(rs.some(r => r.receipt?.kind === 'build' && r.receipt.buildSucceeded)); const firstFailed = rs.find(r => r.receipt?.kind === 'test' && r.receipt.exitCode !== 0 && r.receipt.tests?.nonzeroExecuted); assert(firstFailed); assert.equal(firstFailed.receipt.tests.passedWithTests, false);
  console.log('REAL_RUNNER_BUILD_AND_FAILING_TEST_PASS');
  t = await follow(t, 'real-runner-repair', 'Fix calc.js using the read/edit/write file tools so add(2,3) equals 5. Then use run_build with command npm run build and run_tests with command npm test. Do not alter calc.test.js or remove assertions. Report the actual results.');
  rs = records(t); const pass = rs.find(r => r.binding.currentCommandId === 'real-runner-repair' && r.receipt?.kind === 'test' && r.receipt.tests?.passedWithTests); assert(pass); assert.equal(pass.receipt.tests.executed, 1); assert.equal(readFileSync(join(t.workspace, 'calc.test.js'), 'utf8'), readFileSync(join(project, 'calc.test.js'), 'utf8'));
  assert.equal((await call(`/tasks/${t.taskId}/commands`, { requestKey: 'real-runner-main-stop', intent: 'STOP', ...guard(t) })).state, 'APPLIED');
  t = await call('/tasks/' + t.taskId); assert.equal(t.processOwnership.forkObserved, false); assert.equal(t.stopProof.scope, 'MODEL_NO_FORK_AND_REGISTERED_PROCESS_GROUPS');
  const originalSession = t.nativeSessionId, originalReceipts = rs.map(r => ({ key: r.requestKey, status: r.status, digest: hash(JSON.stringify(r.receipt)) })); const repairProof = t.stopProof;
  const artifacts = await call(`/tasks/${t.taskId}/artifacts`); assert(artifacts.items.some(i => i.path === 'calc.js' && i.status === 'AVAILABLE'));
  await host.close(); host = new PiHost({ state, token, executionProfile: profile }); port = await host.listen(); t = await call('/tasks/' + t.taskId); assert(t.capabilities.completedSessionFollowUpAfterRestart);
  t = await follow(t, 'real-runner-reopen', 'From prior conversation use write to create recalled.txt containing exactly the remembered selected word. Then run_command printf REOPEN_RUNNER_OK. Do not read files to recover the word.'); assert.equal(t.nativeSessionId, originalSession); assert.equal(readFileSync(join(t.workspace, 'recalled.txt'), 'utf8').trim(), 'VIOLET');
  for (const r of originalReceipts) { const same = records(t).find(x => x.requestKey === r.key); assert.equal(hash(JSON.stringify(same.receipt)), r.digest); }
  assert.equal((await call(`/tasks/${t.taskId}/commands`, { requestKey: 'real-runner-reopen-stop', intent: 'STOP', ...guard(t) })).state, 'APPLIED');
  t = await call('/tasks/' + t.taskId); assert.equal(t.processOwnership.forkObserved, false);
  console.log('REAL_RUNNER_REPAIR_REOPEN_PASS');
  const heartbeat = `node -e 'require("fs").appendFileSync("dispatch.txt","x");setInterval(()=>require("fs").appendFileSync("heartbeat.txt","x"),25)'`;
  const long = await create('real-runner-long', 'Use run_command once with timeoutMs 600000 and this exact shell command, then wait for it to complete: ' + heartbeat);
  let longTask = await wait(async () => { const row = await call('/tasks/' + long.receipt.taskId); if (row.state === 'FAILED') throw new Error('LONG_MODEL_FAILED'); return existsSync(join(row.workspace, 'heartbeat.txt')) && row; });
  assert.equal((await call(`/tasks/${longTask.taskId}/commands`, { requestKey: 'real-runner-long-stop', intent: 'STOP', ...guard(longTask) })).state, 'APPLIED');
  const heartbeatSize = readFileSync(join(longTask.workspace, 'heartbeat.txt')).length; await new Promise(done => setTimeout(done, 250)); assert.equal(readFileSync(join(longTask.workspace, 'heartbeat.txt')).length, heartbeatSize); longTask = await call('/tasks/' + longTask.taskId);
  assert(records(longTask).some(r => r.status === 'CANCELLED')); console.log('REAL_RUNNER_LONG_STOP_PASS');
  const crashState = join(run, 'crash-state'); crashHost = spawn(process.execPath, [new URL('./runner-host.mjs', import.meta.url).pathname, crashState, project], { env: { ...process.env, WORKER_LOCAL_TOKEN: token }, stdio: ['ignore', 'pipe', 'pipe'] }); crashHost.stderr.resume();
  const crashPort = await new Promise((done, reject) => { crashHost.stdout.once('data', data => done(Number(String(data).trim()))); crashHost.once('error', reject); }), cc = callAt(crashPort);
  const pf = await cc('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' }), crashBody = { requestKey: 'real-runner-kill-original', projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction: 'Use run_command once with timeoutMs 600000 and this exact shell command, then wait for it to complete: ' + heartbeat }, killed = await cc('/tasks', crashBody);
  const killedTask = await wait(async () => { const row = await cc('/tasks/' + killed.taskId); if (row.state === 'FAILED') throw new Error('KILL_MODEL_FAILED'); return existsSync(join(row.workspace, 'heartbeat.txt')) && row; });
  const exited = new Promise(done => crashHost.once('exit', done)); crashHost.kill('SIGKILL'); await exited; await new Promise(done => setTimeout(done, 700)); const killSize = readFileSync(join(killedTask.workspace, 'heartbeat.txt')).length; await new Promise(done => setTimeout(done, 250)); assert.equal(readFileSync(join(killedTask.workspace, 'heartbeat.txt')).length, killSize);
  const recovered = new PiHost({ state: crashState, token, executionProfile: profile }); let recoveredTask;
  try { recoveredTask = recovered.store.task(killed.taskId); assert.equal(recoveredTask.state, 'UNKNOWN'); assert.equal(recoveredTask.quiescent, false); assert.equal(recovered.create(crashBody).taskId, killed.taskId); await new Promise(done => setTimeout(done, 200)); assert.equal(readFileSync(join(killedTask.workspace, 'dispatch.txt'), 'utf8'), 'x'); assert(records(recoveredTask).some(r => r.status === 'UNKNOWN'));
    const original = records(recoveredTask)[0], query = await CommandRunner.start(join(crashState, 'command-runner', original.contextId), { contextId: original.contextId, workspace: killedTask.workspace, binding: original.binding, envAllowlist: [] });
    try { const found = await query.lookup(original.requestKey); assert.equal(found.status, 'UNKNOWN'); assert.equal(found.commandId, original.commandId); } finally { await query.close(); }
  } finally { await recovered.close(); }
  console.log('REAL_RUNNER_SIGKILL_NO_REPLAY_PASS');
  const evidence = { schemaVersion: 'nwf-pi-runner-model/1', result: 'PASS', executionProfile: profile, platform: process.platform, arch: process.arch, node: process.version, model: t.configuredModel, actualModel: t.actualModel, engineBindingDigest: t.engineBindingDigest, provider: 'OpenCode Go', command: { stdoutVerified: true, stderrVerified: true, failureExitCode: 7 }, build: { succeeded: true }, tests: { baseline: firstFailed.receipt.tests, repaired: pass.receipt.tests, repairedExitCode: pass.receipt.exitCode, testSourceUnchanged: true }, modelNoFork: t.processOwnership.forkObserved === false, stopProof: repairProof, reopen: { sameNativeSession: true, rememberedWord: true, originalCommandReceiptsImmutable: true, realRunnerCommand: true }, longStop: { receipt: 'APPLIED', heartbeatStopped: true, commandStatus: records(longTask).find(r => r.status === 'CANCELLED').status, proof: longTask.stopProof }, crash: { hostSignal: 'SIGKILL', ownerLeaseCleanup: true, heartbeatStopped: true, taskState: recoveredTask.state, originalTaskKeyReceiptRecovered: true, commandReplayCount: 0, runnerCommandStatus: records(recoveredTask)[0].status, originalRunnerCommandKeyLookup: true }, usage: t.usage };
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2)); console.log('REAL_RUNNER_COMPLETE ' + JSON.stringify({ result: 'PASS', evidence: join(run, 'evidence.json') }));
} finally { if (crashHost && crashHost.exitCode === null && crashHost.signalCode === null) crashHost.kill('SIGKILL'); await host.close(); }
