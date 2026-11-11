import test from 'node:test';
import assert from 'node:assert/strict';
import { cpSync, existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { EnterpriseWireAdapter } from '../dist/wire/adapter.js';
import { startDaemon } from '../dist/wire/main.js';
import { createControlPlane } from './fixture-control-plane.mjs';

/**
 * W6/P25: enterprise wire — fixture control plane + real PiHost +
 * adapter. Deterministic: the plane is in-process, the child is the
 * fixture, every protocol call is recorded for assertion.
 */
const token = 'deterministic-test-token-not-a-real-secret';
const workerToken = 'deterministic-worker-token-w6-not-real';
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
const localStub = fileURLToPath(new URL('./fixture-local-stub.mjs', import.meta.url));
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w6-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
async function wait(fn, tries = 400) { for (let n = 0; n < tries; n++) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }

function adapterConfig(root, cp, localPort, extra = {}) {
  return {
    stateDir: join(root, 'wire'), controlPlaneUrl: cp.url, workerToken,
    localUrl: `http://127.0.0.1:${localPort}`, localToken: token,
    workerId: 'worker-w6', deploymentId: 'dep-w6',
    projects: { 'proj-wire-demo': 'demo', '*': 'demo' },
    heartbeatMillis: 200, pumpMillis: 50, taskPollMillis: 20, pollWaitSeconds: 1,
    quiesceWaitMillis: 10000, finishPollMaxAttempts: 6,
    ...extra,
  };
}
async function setup(opts = {}) {
  const root = mkdtempSync(join(tmpdir(), 'nwf-pi-w6-'));
  const p = project();
  const host = new PiHost({ state: join(root, 'host'), token, childModule: fixture, maxActiveTasks: 1 });
  host.register('demo', p);
  const localPort = await host.listen();
  const cp = createControlPlane(opts.cp ?? {});
  await cp.start();
  const adapter = new EnterpriseWireAdapter(adapterConfig(root, cp, localPort, opts.adapter));
  await adapter.start();
  const local = (path, body) => fetch(`http://127.0.0.1:${localPort}/local/v1${path}`, {
    method: body ? 'POST' : 'GET',
    headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  }).then(async r => ({ status: r.status, body: await r.json() }));
  return { root, host, cp, adapter, local, localPort, project: p };
}
const finishOf = (cp, attemptId) => [...cp.state.finishes.values()].find(f => f.attemptId === attemptId);
const receiptsFor = (cp, commandId) => cp.state.receipts.filter(r => r.commandId === commandId);
const eventsOfType = (cp, type) => cp.state.events.filter(e => e.type === type);
const startPayload = (over = {}) => ({
  taskId: 'task-' + Math.random().toString(36).slice(2, 8), attemptId: 'att-' + Math.random().toString(36).slice(2, 8),
  attemptEpoch: '1', runCommandId: 'run-' + Math.random().toString(36).slice(2, 8),
  workspaceId: 'ws-demo', workspaceGeneration: '7', goal: 'writefile out.txt',
  projectId: 'proj-wire-demo', ...over,
});

test('W6 registration binds instance/stream/epoch durably; heartbeats carry lease + backlog counts', async () => {
  const s = await setup();
  try {
    const reg = s.cp.state.registrations[0];
    assert.equal(reg.protocol, 'agent-worker/1.4');
    assert.equal(reg.workerId, 'worker-w6');
    assert.equal(reg.deploymentId, 'dep-w6');
    assert.ok(reg.bootId);
    assert.deepEqual(reg.capabilities.providers, ['pi']);
    assert.equal(reg.capabilities.changeCapture, 'FINAL_TREE');
    assert.equal(reg.capabilities.cancel, true);
    const status = s.adapter.status();
    assert.equal(status.instanceId, reg.granted.instanceId);
    assert.equal(status.streamId, reg.granted.streamId);
    assert.equal(status.workerLeaseEpoch, reg.granted.workerLeaseEpoch);
    const hb = await wait(() => s.cp.state.heartbeats.length >= 2 && s.cp.state.heartbeats[1]);
    assert.equal(hb.workerLeaseEpoch, reg.granted.workerLeaseEpoch);
    assert.ok(hb.heartbeatId.startsWith('hb-'));
    assert.ok(['READY', 'BUSY', 'DRAINING'].includes(hb.state));
    assert.equal(typeof hb.pendingEventCount, 'number');
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 startTask end-to-end: receipts, local bridge, events, artifacts, finish', async () => {
  const s = await setup();
  try {
    const p = startPayload({ attemptId: 'att-w6-happy', taskId: 'task-w6-happy', runCommandId: 'run-w6-happy' });
    const env = s.cp.enqueue(s.cp.command('startTask', p));
    const finish = await wait(() => finishOf(s.cp, 'att-w6-happy'));
    await wait(() => finish.state === 'DELIVERY_ACCEPTED');

    // Command receipts: DURABLY_ACCEPTED then STARTED with attempt+engineRunId.
    const receipts = receiptsFor(s.cp, env.commandId);
    assert.equal(receipts[0].state, 'DURABLY_ACCEPTED');
    assert.equal(receipts[0].receiptRevision, '0');
    const started = receipts.find(r => r.state === 'STARTED');
    assert.equal(started.attemptId, 'att-w6-happy');
    assert.ok(started.engineRunId);

    // Local bridge: exactly one task, deterministic requestKey, succeeded, stop-proven.
    const tasks = s.host.store.tasks();
    assert.equal(tasks.length, 1);
    assert.equal(tasks[0].currentCommandId, 'wire-run-w6-happy');
    assert.equal(tasks[0].state, 'SUCCEEDED');
    assert.equal(tasks[0].quiescent, true);
    assert.ok(tasks[0].stopProof, 'stop proof must be recorded');

    // Event stream: monotone sequence on the bound stream.
    const seqs = s.cp.state.events.map(e => Number(e.sequence));
    assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b));
    for (const type of ['task.prepared', 'execution.started', 'execution.finished', 'lifecycle.transition', 'change.collected', 'artifact.uploaded', 'attempt.delivery.closed', 'task.finished']) {
      await wait(() => eventsOfType(s.cp, type).length >= 1, 200).catch(() => assert.fail('missing event ' + type));
    }
    const closed = eventsOfType(s.cp, 'attempt.delivery.closed')[0];
    const quiesced = s.cp.state.events.find(e => e.type === 'lifecycle.transition' && e.data.to === 'QUIESCED');
    assert.ok(quiesced.data.receipt.startsWith('stop:') || quiesced.data.receipt.startsWith('terminal:'), 'quiescence receipt must carry proof');

    // Artifact flow: intents granted, PUTs digest-verified, completes receipted.
    assert.ok(s.cp.state.uploadIntents.length >= 2, 'blob + manifest intents');
    const manifestIntent = s.cp.state.uploadIntents.find(i => i.artifactId === 'manifest-att-w6-happy');
    assert.ok(manifestIntent.digest.startsWith('sha256:'));
    for (const up of s.cp.state.uploads) {
      assert.equal(up.authorization, 'Bearer ' + workerToken);
      const a = s.cp.state.artifacts.get(up.path.split('/').pop());
      assert.equal(a.status, 'RECEIPTED');
      assert.equal(a.digest, up.sha256);
    }

    // Finish: exact body, finalDurableSequence == closed event sequence.
    assert.equal(finish.request.attemptId, 'att-w6-happy');
    assert.equal(finish.request.attemptEpoch, '1');
    assert.equal(finish.request.workspaceGeneration, '7');
    assert.equal(finish.request.engineOutcome, 'SUCCEEDED');
    assert.equal(finish.request.engineRunId, tasks[0].taskId);
    assert.equal(finish.request.finalDurableSequence, closed.sequence);
    assert.equal(finish.request.manifestDigest, s.cp.state.artifacts.get('manifest-att-w6-happy').digest);
    assert.ok(finish.request.quiescenceReceiptRef);
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 command dedupe: replay resends stored receipt; envelope conflict is REJECTED', async () => {
  const s = await setup();
  try {
    const p = startPayload({ goal: 'waiting-never' });
    const env = s.cp.enqueue(s.cp.command('startTask', p));
    await wait(() => receiptsFor(s.cp, env.commandId).some(r => r.state === 'STARTED'));

    // Same commandId + same envelope → duplicate: stored receipt replayed, no second task.
    s.cp.knobs.replayNextPoll = env;
    await wait(() => receiptsFor(s.cp, env.commandId).filter(r => r.state === 'STARTED').length >= 2);
    assert.equal(s.host.store.tasks().length, 1);

    // Same commandId + different envelope → conflict receipted REJECTED.
    const conflict = s.cp.command('cancelTask', { cancelCommandId: 'cx-1', attemptId: p.attemptId, attemptEpoch: '1', workerLeaseEpoch: '1' }, { commandId: env.commandId, commandSequence: '999' });
    s.cp.knobs.extraPollCommands = [conflict];
    await wait(() => receiptsFor(s.cp, env.commandId).some(r => r.state === 'REJECTED' && r.detailCode === 'IDEMPOTENCY_PAYLOAD_MISMATCH'));
    assert.equal(s.host.store.tasks().length, 1);
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 receipt retransmission: dropped receipt reposts identical state+revision', async () => {
  const s = await setup();
  try {
    s.cp.knobs.dropReceipts = 1;
    const p = startPayload({ goal: 'waiting-never' });
    const env = s.cp.enqueue(s.cp.command('startTask', p));
    const dup = await wait(() => {
      const posts = s.cp.recorded.filter(r => r.method === 'POST' && r.path === `/worker/v1/instances/${s.adapter.status().instanceId}/commands/${env.commandId}/receipt`);
      return posts.length >= 2 && posts;
    });
    // Same durable decision, same revision — only the delivery-honest
    // detail marker may differ on retransmission.
    for (const field of ['commandId', 'receiptRevision', 'state', 'payloadDigest']) {
      assert.deepEqual(dup[0].body[field], dup[1].body[field], field + ' must be identical on retransmission');
    }
    assert.equal(dup[0].body.state, 'DURABLY_ACCEPTED');
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 cancelTask: STOP propagates to the local task; finish declares CANCELLED', async () => {
  const s = await setup();
  try {
    const p = startPayload({ attemptId: 'att-w6-cancel', goal: 'waiting-never' });
    const start = s.cp.enqueue(s.cp.command('startTask', p));
    await wait(() => receiptsFor(s.cp, start.commandId).some(r => r.state === 'STARTED'));

    const cancel = s.cp.enqueue(s.cp.command('cancelTask', {
      cancelCommandId: 'cc-w6-1', attemptId: p.attemptId, attemptEpoch: p.attemptEpoch,
      workerLeaseEpoch: '1', reason: 'operator test', requestedAt: new Date().toISOString(),
    }));
    const finish = await wait(() => finishOf(s.cp, p.attemptId));
    await wait(() => finish.state === 'DELIVERY_ACCEPTED');

    assert.equal(finish.request.engineOutcome, 'CANCELLED');
    assert.ok(receiptsFor(s.cp, cancel.commandId).some(r => r.state === 'CANCELLED'));
    const task = s.host.store.tasks()[0];
    assert.equal(task.state, 'CANCELLED');
    assert.equal(task.quiescent, true);
    assert.ok(task.stopProof, 'cancel must prove the owned tree stopped');
    // Finish acceptance and event delivery use independent durable pumps.
    await wait(() => eventsOfType(s.cp, 'task.cancelled').length >= 1);
    const stopping = s.cp.state.events.find(e => e.type === 'lifecycle.transition' && e.data.to === 'STOPPING');
    assert.ok(stopping, 'STOPPING transition event expected');
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 finish PENDING: identical declaration reposted until receipt terminal', async () => {
  const s = await setup({ cp: { finishMode: 'manual' } });
  try {
    const p = startPayload({ attemptId: 'att-w6-pend' });
    s.cp.enqueue(s.cp.command('startTask', p));
    const finish = await wait(() => finishOf(s.cp, p.attemptId));
    await wait(() => finish.submissions >= 2, 300);

    // Reposts are byte-identical (the fixture would 409 on divergence).
    const posts = s.cp.recorded.filter(r => r.method === 'POST' && r.path === `/worker/v1/attempts/${p.attemptId}/finish`);
    assert.ok(posts.length >= 2);
    assert.deepEqual(posts[0].body, posts[1].body);

    finish.state = 'DELIVERY_ACCEPTED';
    await wait(() => s.cp.state.finishReceipts.some(r => r.finishId === finish.finishId && r.found));
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 lease loss forces re-registration under the same bootId', async () => {
  const s = await setup();
  try {
    const first = s.cp.state.registrations[0].granted;
    s.cp.knobs.leaseExpireHeartbeats = 1;
    await wait(() => s.cp.state.registrations.length >= 2);
    const second = s.cp.state.registrations[1];
    assert.equal(second.bootId, s.cp.state.registrations[0].bootId, 'boot identity replays');
    assert.notEqual(second.granted.instanceId, first.instanceId);
    const hb = await wait(() => s.cp.state.heartbeats.some(h => h.workerLeaseEpoch === second.granted.workerLeaseEpoch));
    assert.ok(hb);
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 DRAINING desired state rejects new startTask honestly; drain command receipts STARTED', async () => {
  const s = await setup({ cp: { desiredState: 'DRAINING' } });
  try {
    const env = s.cp.enqueue(s.cp.command('startTask', startPayload()));
    await wait(() => receiptsFor(s.cp, env.commandId).some(r => r.state === 'REJECTED' && r.detailCode === 'WORKER_DRAINING'));
    assert.equal(s.host.store.tasks().length, 0);

    const drain = s.cp.enqueue(s.cp.command('drain', { reason: 'test drain', deadlineMillis: 60000 }));
    await wait(() => receiptsFor(s.cp, drain.commandId).some(r => r.state === 'STARTED'));
  } finally { await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

test('W6 restart: scoped recovery reconciles a stranded attempt and delivers its finish under recovery creds', async () => {
  const s = await setup();
  let a2 = null;
  try {
    const p = startPayload({ attemptId: 'att-w6-rec', taskId: 'task-w6-rec', runCommandId: 'run-w6-rec', goal: 'writefile recovered.txt' });
    s.cp.enqueue(s.cp.command('startTask', p));
    // Let the attempt bind a local task + reach STARTED, then "kill" adapter A.
    await wait(() => receiptsFor(s.cp, s.cp.state.receipts[0]?.commandId ?? '').some(r => r.state === 'STARTED'));
    const instA = s.adapter.status().instanceId;
    const streamA = s.adapter.status().streamId;
    await s.adapter.stop(0);

    a2 = new EnterpriseWireAdapter(adapterConfig(s.root, s.cp, s.localPort));
    await a2.start();
    try {
      // Recovery session granted for the dead instance only.
      await wait(() => s.cp.state.recoveries.some(r => r.oldInstanceId === instA));
      const finish = await wait(() => finishOf(s.cp, p.attemptId));
      await wait(() => finish.state === 'DELIVERY_ACCEPTED', 400);
      assert.equal(finish.request.engineOutcome, 'SUCCEEDED');
      assert.equal(finish.request.workerLeaseEpoch, '1', 'finish replays the ATTEMPT-bound epoch, not the new one');
      assert.ok(finish.recoveryScope, 'scoped recovery creds must ride the finish body');
      // Events rebound on the OLD stream are delivered under the granted session.
      const oldStreamEvents = s.cp.state.events.filter(e => e.streamId === streamA);
      assert.ok(oldStreamEvents.length >= 1, 'old-stream events must be recovered');
      // A scoped artifact upload carried recovery query params on the SAME origin.
      assert.ok(s.cp.state.uploads.some(u => u.query.includes('recoverySessionId=')), 'recovery-scoped upload expected');
    } finally { await a2.stop(300); }
  } finally { await s.host.close(); await s.cp.close(); }
});

test('W6 R11: cross-origin upload receives NO worker credentials or recovery params', async () => {
  const s = await setup();
  const foreign = await s.cp.startForeignOrigin();
  try {
    s.cp.knobs.uploadOrigin = 'cross';
    s.cp.knobs.crossOriginUrl = foreign.url;
    const p = startPayload({ attemptId: 'att-w6-r11' });
    s.cp.enqueue(s.cp.command('startTask', p));
    const up = await wait(() => foreign.recorded.length >= 1 && foreign.recorded[0]);
    assert.equal(up.authorization, null, 'worker bearer must never cross origins');
    assert.ok(!up.path.includes('recoverySessionId'), 'recovery params must never cross origins');
    assert.ok(up.bytes > 0);
    // Every upload in this scenario is cross-origin — nothing may carry
    // worker credentials to the foreign sink.
    await wait(() => foreign.recorded.length >= 2);
    for (const u of foreign.recorded) {
      assert.equal(u.authorization, null);
      assert.ok(!u.path.includes('recoverySessionId') && !u.path.includes('recoveryTicket'));
    }
    assert.equal(s.cp.state.uploads.length, 0, 'no same-origin PUT should occur while cross-origin intents are issued');
    // Let completes succeed so the finish resolves — the plane owns receipt state.
    for (const [, a] of s.cp.state.artifacts) if (a.status === 'INTENT_GRANTED') a.status = 'UPLOADED';
    const finish = await wait(() => finishOf(s.cp, p.attemptId));
    await wait(() => finish.state === 'DELIVERY_ACCEPTED');
  } finally { await foreign.close(); await s.adapter.stop(300); await s.host.close(); await s.cp.close(); }
});

// ----------------------------------------------------------------------
// W6c: external host mode — spawn a /local/v1 process, generated bearer,
// ready-line scrape; the wire adapter runs unchanged on top.
// ----------------------------------------------------------------------

function daemonConfig(root, cp, host, extra = {}) {
  return {
    stateDir: join(root, 'wire'), controlPlaneUrl: cp.url, workerToken,
    workerId: 'worker-w6', deploymentId: 'dep-w6',
    projects: { 'proj-wire-demo': 'demo', '*': 'demo' },
    heartbeatMillis: 200, pumpMillis: 50, taskPollMillis: 20, pollWaitSeconds: 1,
    quiesceWaitMillis: 10000, finishPollMaxAttempts: 6,
    host,
    ...extra,
  };
}

test('W6 external host mode: spawned stub /local/v1 host under generated token — register, task lifecycle, clean stop', async () => {
  const root = mkdtempSync(join(tmpdir(), 'nwf-w6-ext-'));
  const cp = createControlPlane();
  await cp.start();
  const daemon = await startDaemon(daemonConfig(root, cp, {
    external: {
      command: process.execPath, args: [localStub],
      tokenEnv: 'W6_STUB_TOKEN', readyTimeoutMillis: 15000,
      env: { STUB_TOKEN_ENV: 'W6_STUB_TOKEN' },
    },
  }));
  try {
    assert.equal(daemon.host, null, 'external mode must not embed PiHost');
    assert.ok(daemon.hostExit, 'spawned host exit promise exposed');
    assert.ok(daemon.localToken.length >= 24, 'generated host token >= 24 chars');
    assert.match(daemon.localUrl, /^http:\/\/127\.0\.0\.1:\d+$/, 'ready line scraped to a loopback URL');
    // Registration + heartbeat against the fixture plane — identical wire.
    const reg = await wait(() => cp.state.registrations[0]);
    assert.equal(reg.workerId, 'worker-w6');
    assert.equal(daemon.adapter.status().instanceId, reg.granted.instanceId);
    await wait(() => cp.state.heartbeats.length >= 1);

    // A startTask drives the full external bridge: preflight → create →
    // observed terminal → quiescence → artifacts → finish.
    const p = startPayload({ attemptId: 'att-w6-ext', taskId: 'task-w6-ext', runCommandId: 'run-w6-ext' });
    const env = cp.enqueue(cp.command('startTask', p));
    const finish = await wait(() => finishOf(cp, 'att-w6-ext'));
    await wait(() => finish.state === 'DELIVERY_ACCEPTED');
    assert.equal(finish.request.engineOutcome, 'SUCCEEDED');
    assert.ok(receiptsFor(cp, env.commandId).some(r => r.state === 'STARTED'));

    // Token plumbing proven: the stub recorded every request; each
    // /local/v1 call carried the generated bearer — never a missing one.
    const recorded = await fetch(daemon.localUrl + '/__recorded').then(r => r.json());
    const localCalls = recorded.filter(r => r.path.startsWith('/local/v1'));
    assert.ok(localCalls.some(c => c.method === 'POST' && c.path === '/local/v1/task-preflights'), 'preflight call expected');
    assert.ok(localCalls.some(c => c.method === 'POST' && c.path === '/local/v1/tasks'), 'create call expected');
    assert.ok(localCalls.some(c => c.method === 'GET' && /\/local\/v1\/tasks\/[^/]+$/.test(c.path)), 'task detail poll expected');
    for (const c of localCalls) assert.equal(c.authed, true, c.method + ' ' + c.path + ' missing generated bearer');

    // Command pump on the same daemon: drain receipts STARTED.
    const drain = cp.enqueue(cp.command('drain', { reason: 'ext drain', deadlineMillis: 60000 }));
    await wait(() => receiptsFor(cp, drain.commandId).some(r => r.state === 'STARTED'));
  } finally { await daemon.stop(); await cp.close(); }
  // Clean stop killed the spawned host and awaited its exit.
  const ex = await daemon.hostExit;
  assert.ok(ex.code !== null || ex.signal !== null, 'spawned host must exit on stop');
  const gone = await fetch(daemon.localUrl + '/__recorded').then(() => false).catch(() => true);
  assert.equal(gone, true, 'external host must be unreachable after stop');
});

test('W6 external host crash propagates honestly — hostExit resolves, no zombie', async () => {
  const root = mkdtempSync(join(tmpdir(), 'nwf-w6-extcrash-'));
  const cp = createControlPlane();
  await cp.start();
  const daemon = await startDaemon(daemonConfig(root, cp, {
    external: { command: process.execPath, args: [localStub], tokenEnv: 'W6_STUB_TOKEN', env: { STUB_TOKEN_ENV: 'W6_STUB_TOKEN' } },
  }));
  try {
    await wait(() => cp.state.registrations.length >= 1);
    // Crash the spawned host out from under the daemon.
    await fetch(daemon.localUrl + '/__die', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ code: 3 }) }).catch(() => undefined);
    const ex = await daemon.hostExit;
    assert.equal(ex.code, 3, 'non-zero child exit surfaces on hostExit');
    assert.equal(daemon.stopping, false, 'unexpected exit is a crash, not teardown');
  } finally { await daemon.stop(); await cp.close(); }
  assert.equal(daemon.stopping, true);
});

// ----------------------------------------------------------------------
// W6c: real codex host attach — the built codex-native-worker.exe serving
// an initialized state dir fronts the same wire daemon.
// ----------------------------------------------------------------------
const repoRoot = fileURLToPath(new URL('../../', import.meta.url));
const CODEX_HOST_EXE = process.env.NWF_CODEX_HOST_EXE ?? join(repoRoot, 'codex', 'target', 'debug', 'codex-native-worker.exe');
const CODEX_TEMPLATE = process.env.NWF_CODEX_TEMPLATE ?? join(repoRoot, '.local', 'codex-w4b-template');
const CODEX_ENGINE = process.env.NWF_CODEX_EXECUTABLE ?? '';
const CODEX_BUILD = process.env.NWF_CODEX_BUILD ?? join(repoRoot, 'codex', 'scripts', 'build.ps1');
const CODEX_LLVM = process.env.NWF_LLVM_MINGW ?? '';

function codexPrereqs() {
  if (!existsSync(CODEX_HOST_EXE) && existsSync(CODEX_BUILD) && CODEX_LLVM) {
    try { execFileSync('pwsh', ['-File', CODEX_BUILD, '-LlvmMingwHome', CODEX_LLVM], { stdio: 'inherit', timeout: 600_000 }); } catch { /* fall through to NOT_RUN */ }
  }
  const missing = [];
  if (!existsSync(CODEX_HOST_EXE)) missing.push('host exe ' + CODEX_HOST_EXE);
  if (!existsSync(CODEX_ENGINE)) missing.push('pinned engine (NWF_CODEX_EXECUTABLE) ' + CODEX_ENGINE);
  if (!existsSync(join(CODEX_TEMPLATE, 'registry.json'))) missing.push('template ' + CODEX_TEMPLATE);
  return missing;
}

/**
 * Operator lane for the codex e2e: the wire adapter honestly declares
 * approval:false, so engine input-requests are never bridged through the
 * wire protocol. The test plays the local operator role — it answers
 * PENDING command/fileChange approvals with 'accept' directly on the
 * host's /local/v1 surface (fixture workspace, sandboxed) so the real
 * engine turn can complete; other input kinds are declined, never faked.
 */
async function pumpCodexApprovals(daemon, localTaskId, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  while (Date.now() < deadline) {
    const d = await fetch(daemon.localUrl + '/local/v1/tasks/' + localTaskId, { headers: { authorization: 'Bearer ' + daemon.localToken } }).then(r => r.json()).catch(() => null);
    if (d && ['SUCCEEDED', 'FAILED', 'CANCELLED', 'BUDGET_EXCEEDED'].includes(d.state)) return d;
    for (const [qid, q] of Object.entries(d?.inputRequests ?? {})) {
      if (q.state !== 'PENDING') continue;
      const approvable = q.method === 'item/commandExecution/requestApproval' || q.method === 'item/fileChange/requestApproval';
      const rk = 'test-approve-' + qid;
      await fetch(`${daemon.localUrl}/local/v1/tasks/${localTaskId}/input-requests/${qid}/responses`, {
        method: 'POST',
        headers: { authorization: 'Bearer ' + daemon.localToken, 'content-type': 'application/json', 'idempotency-key': rk },
        body: JSON.stringify({
          requestKey: rk, response: { decision: approvable ? 'accept' : 'decline' },
          generation: q.generation, nativeSessionId: q.nativeSessionId,
          nativeTurnId: q.nativeTurnId, targetCommandId: q.targetCommandId,
        }),
      }).catch(() => undefined);
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  return null;
}

test('W6 codex external host: real codex-native-worker boots under the wire daemon', async () => {
  const missing = codexPrereqs();
  if (missing.length) { console.log('NOT_RUN: codex real-boot test skipped — ' + missing.join('; ')); return; }
  const root = mkdtempSync(join(tmpdir(), 'nwf-w6-codex-'));
  const state = join(root, 'codex-state');
  cpSync(CODEX_TEMPLATE, state, { recursive: true }); // never init into the template itself
  const cp = createControlPlane();
  await cp.start();
  const registry = JSON.parse(readFileSync(join(state, 'registry.json'), 'utf8'));
  const daemon = await startDaemon(daemonConfig(root, cp, {
    codex: { hostExecutable: CODEX_HOST_EXE, state, readyTimeoutMillis: 60_000 },
  }, { projects: { 'proj-wire-demo': registry.project_id, '*': registry.project_id } }));
  try {
    assert.equal(daemon.host, null);
    assert.ok(daemon.localToken.length >= 24);
    // Registered with honest codex capabilities (sugar defaults).
    const reg = await wait(() => cp.state.registrations[0]);
    assert.deepEqual(reg.capabilities.providers, ['codex']);
    assert.deepEqual(reg.capabilities.executionProfiles, ['codex.standard']);
    await wait(() => cp.state.heartbeats.length >= 1);
    // The real codex host answers /local/v1 under the generated token.
    const id = await fetch(daemon.localUrl + '/local/v1/identity', { headers: { authorization: 'Bearer ' + daemon.localToken } }).then(r => r.json());
    assert.ok(id.storeId, 'codex identity served');
    const denied = await fetch(daemon.localUrl + '/local/v1/identity');
    assert.equal(denied.status, 401, 'generated token is enforced by the real host');

    // Optional: real startTask end-to-end against the pinned engine —
    // needs MODEL_API_KEY + LLM_HEADER_* for the managed profile. The
    // approval pump answers engine input-requests locally (wire declares
    // approval:false honestly); the file write + sealed artifacts +
    // finish all travel the wire for real.
    if (process.env.MODEL_API_KEY) {
      const p = startPayload({ attemptId: 'att-w6-codex', taskId: 'task-w6-codex', runCommandId: 'run-w6-codex', goal: 'create file out.txt containing exactly: hello-from-codex-wire' });
      cp.enqueue(cp.command('startTask', p));
      // The adapter's deterministic requestKey → operations lookup gives
      // the local taskId the wire created.
      const op = await wait(() => fetch(daemon.localUrl + '/local/v1/task-requests/wire-run-w6-codex', { headers: { authorization: 'Bearer ' + daemon.localToken } }).then(r => r.ok ? r.json() : null).catch(() => null), 1200);
      assert.ok(op?.taskId, 'wire-created codex task must be queryable by requestKey');
      const pump = pumpCodexApprovals(daemon, op.taskId, 300_000);
      try {
        const finish = await wait(() => finishOf(cp, 'att-w6-codex'), 12000);
        await wait(() => finish.state === 'DELIVERY_ACCEPTED', 400);
        assert.equal(finish.request.engineOutcome, 'SUCCEEDED');
        const quiesced = cp.state.events.find(e => e.type === 'lifecycle.transition' && e.data.to === 'QUIESCED');
        assert.ok(quiesced, 'codex task must reach proven quiescence');
        // Real evidence flowed: manifest + the sealed out.txt blob.
        assert.ok(cp.state.uploads.length >= 2, 'blob + manifest uploads expected');
        const ws = join(state, 'workspaces', op.taskId, 'out.txt');
        assert.equal(readFileSync(ws, 'utf8'), 'hello-from-codex-wire', 'engine actually wrote the goal file');
      } finally { await pump; }
    } else {
      console.log('NOT_RUN: codex startTask end-to-end skipped — MODEL_API_KEY unavailable');
    }

    // Command pump: drain receipts STARTED through the codex-backed wire
    // (kept last — a drain flips the daemon to draining and would reject
    // a startTask honestly).
    const drain = cp.enqueue(cp.command('drain', { reason: 'codex drain', deadlineMillis: 60000 }));
    await wait(() => receiptsFor(cp, drain.commandId).some(r => r.state === 'STARTED'));
  } finally { await daemon.stop(); await cp.close(); }
  const ex = await daemon.hostExit;
  assert.ok(ex.code !== null || ex.signal !== null, 'codex host must exit on stop');
  const gone = await fetch(daemon.localUrl + '/local/v1/identity', { headers: { authorization: 'Bearer ' + daemon.localToken } }).then(() => false).catch(() => true);
  assert.equal(gone, true, 'codex host must be unreachable after stop');
});
