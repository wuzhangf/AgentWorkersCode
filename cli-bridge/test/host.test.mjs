// cli-bridge execution-reliability tests (review items 2–6, 2026-11-17).
// Spawns host.mjs serve against fixture-cli.js — deterministic: no model, no
// network beyond loopback, no real agent, no user project writes.
//
// Run: node --test cli-bridge/test/host.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, mkdirSync, existsSync, rmSync, readFileSync, chmodSync, copyFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';

const ROOT = dirname(fileURLToPath(import.meta.url));
const HOST = join(ROOT, '..', 'host.mjs');
const FIXTURE = join(ROOT, 'fixture-cli.js');
const FIXTURE_CMD = join(ROOT, 'fixture.cmd');
const TOKEN = 'cli-bridge-test-token-0123456789abcdef';
const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED']);

const freePort = async () => {
  const s = createServer();
  await new Promise((r) => s.listen(0, '127.0.0.1', r));
  const port = s.address().port;
  await new Promise((r) => s.close(r));
  return port;
};
const wait = async (fn, n = 400, gap = 25) => {
  let last;
  for (let i = 0; i < n; i++) {
    try { const v = await fn(); if (v) return v; } catch (e) { last = e; }
    await new Promise((r) => setTimeout(r, gap));
  }
  throw last ?? new Error('WAIT_TIMEOUT');
};

async function serve(env = {}, executableOverride = null, engine = 'claude') {
  const state = mkdtempSync(join(tmpdir(), 'cb-state-'));
  const project = mkdtempSync(join(tmpdir(), 'cb-project-'));
  const home = mkdtempSync(join(tmpdir(), 'cb-home-'));
  const conf = join(state, 'fixture-conf.json');
  const argvLog = join(state, 'argv.log');
  writeFileSync(conf, JSON.stringify({ argvLog }));
  const port = await freePort();
  const executable = executableOverride ?? (process.platform === 'win32' ? FIXTURE_CMD : FIXTURE);
  if (process.platform !== 'win32') chmodSync(FIXTURE, 0o755);
  const child = spawn(process.execPath, [HOST, 'serve',
    '--engine', engine, '--state', state, '--port', String(port),
    '--project', project, '--executable', executable], {
    env: { ...process.env, WORKER_LOCAL_TOKEN: TOKEN, HOME: home, USERPROFILE: home,
      FIXTURE_CONF: conf, CLI_BRIDGE_WATCH_TICK_MS: '60', ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (d) => { stderr += d; });
  const call = async (path, body) => {
    const r = await fetch(`http://127.0.0.1:${port}${path}`, {
      method: body ? 'POST' : 'GET',
      headers: { authorization: 'Bearer ' + TOKEN,
        ...(body ? { 'content-type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await r.text();
    let j = null; try { j = JSON.parse(text); } catch {}
    return { status: r.status, body: j, text };
  };
  await wait(async () => (await call('/local/v1/identity')).status === 200)
    .catch((e) => { child.kill('SIGKILL'); throw new Error(`bridge did not start: ${stderr}\n${e}`); });
  const h = {
    port, state, project, home, argvLog, call, child,
    stderr: () => stderr,
    setConf: (o) => writeFileSync(conf, JSON.stringify({ argvLog, ...o })),
    facts: async (id) => (await call(`/local/v1/tasks/${id}`)).body,
    launch: (instruction, extra = {}) => call('/local/v1/tasks',
      { requestKey: 'rk-' + randomUUID(), projectId: 'workspace', engineId: engine, instruction, ...extra }),
    terminal: (id) => wait(async () => TERMINAL.has((await h.facts(id)).state)),
    quiescent: (id) => wait(async () => (await h.facts(id)).quiescent === true),
    waitEvent: (id, kind) => wait(async () =>
      (await call(`/local/v1/tasks/${id}/events`)).body.items.some((e) => e.kind === kind)),
    argvLines: () => existsSync(argvLog)
      ? readFileSync(argvLog, 'utf8').trim().split('\n').map((l) => JSON.parse(l)) : [],
    close: () => { try { child.kill('SIGKILL'); } catch {} },
  };
  return h;
}

const pins = (facts) => ({
  generation: facts.generation, nativeSessionId: facts.nativeSessionId,
  nativeTurnId: facts.nativeTurnId, targetCommandId: facts.currentCommandId,
});

const writeClaudeSession = (home, uuid, cwd) => {
  const dir = join(home, '.claude', 'projects', 'proj-a');
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `session-${uuid}.jsonl`),
    JSON.stringify({ type: 'user', cwd, message: { role: 'user', content: [{ type: 'text', text: 'original user turn' }] } }) + '\n'
    + JSON.stringify({ type: 'assistant', cwd, message: { role: 'assistant', content: [{ type: 'text', text: 'original assistant turn' }] } }) + '\n');
};
const writeCodexSession = (home, uuid) => {
  const dir = join(home, '.codex', 'sessions');
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `rollout-${uuid}.jsonl`),
    JSON.stringify({ payload: { type: 'message', role: 'user', content: [{ type: 'text', text: 'codex original question' }] } }) + '\n');
};

test('shared Codex history stays in the registered workspace and supports incremental messages', async () => {
  const h = await serve({}, null, 'codex');
  try {
    const inside = randomUUID();
    const outside = randomUUID();
    const sessions = join(h.home, '.codex', 'sessions');
    mkdirSync(sessions, { recursive: true });
    for (const [id, cwd] of [[inside, h.project], [outside, h.home]]) {
      writeFileSync(join(sessions, `rollout-${id}.jsonl`), [
        { type: 'session_meta', payload: { cwd } },
        { payload: { type: 'message', role: 'user', content: [{ text: 'first' }] } },
        { payload: { type: 'message', role: 'assistant', content: [{ text: 'second' }] } },
      ].map(row => JSON.stringify(row)).join('\n'));
    }
    const query = 'engine=codex&projectId=workspace&limit=20';
    const listed = await h.call('/local/v1/history?' + query);
    assert.equal(listed.body.projectId, 'workspace');
    assert.deepEqual(listed.body.items.map(item => item.id), [inside]);
    const delta = await h.call(`/local/v1/history/sessions/${inside}?${query}&after=1`);
    assert.deepEqual(delta.body.items.map(item => [item.id, item.text]), [['2', 'second']]);
    assert.equal((await h.call(`/local/v1/history/sessions/${outside}?${query}`)).status, 404);
    assert.equal((await h.launch('reject out of scope', { sharedSession: true, adoptSessionId: outside })).status, 404);
    assert.equal((await h.call('/local/v1/history?engine=codex&projectId=other&limit=20')).status, 400);
  } finally { h.close(); }
});

// §5 — plain CLI never had in-turn input: declare it, refuse the op, and
// never relaunch a steer as a new round.
test('§5 steer is declared false and rejected at engine, task and route level', async () => {
  const h = await serve();
  try {
    const engines = await h.call('/local/v1/engines');
    assert.equal(engines.body.items[0].capabilities.steer, false);
    const hold = join(h.state, 'hold');
    writeFileSync(hold, 'x');
    h.setConf({ hold });
    const r = await h.launch('first task');
    assert.equal(r.status, 200, r.text);
    const id = r.body.taskId;
    await wait(async () => (await h.facts(id)).state === 'RUNNING');
    assert.equal((await h.facts(id)).capabilities.steer, false);
    const steer1 = await h.call(`/local/v1/tasks/${id}/messages`,
      { requestKey: 'rk-s1', kind: 'NATIVE_STEER', content: 'steer now' });
    assert.equal(steer1.status, 409);
    assert.equal(steer1.body.code, 'NATIVE_ACTION_UNSUPPORTED');
    rmSync(hold);
    await h.terminal(id);
    assert.equal((await h.facts(id)).capabilities.steer, false);
    const steer2 = await h.call(`/local/v1/tasks/${id}/messages`,
      { requestKey: 'rk-s2', kind: 'NATIVE_STEER', content: 'steer again' });
    assert.equal(steer2.status, 409);
    assert.equal(steer2.body.code, 'NATIVE_ACTION_UNSUPPORTED');
    // Neither steer attempt spawned a turn — only the launch ran.
    assert.equal(h.argvLines().length, 1);
  } finally { h.close(); }
});

// §6a — default config: silence produces a STALL_WATCH signal, not a kill.
test('§6 silence is observed, never auto-killed by default', async () => {
  const h = await serve({ CLI_BRIDGE_QUIET_MS: '200' });
  const hold = join(h.state, 'hold');
  writeFileSync(hold, 'x');
  try {
    h.setConf({ hold });
    const r = await h.launch('quiet long task');
    assert.equal(r.status, 200, r.text);
    const id = r.body.taskId;
    await h.waitEvent(id, 'STALL_WATCH');
    assert.equal((await h.facts(id)).state, 'RUNNING');
    rmSync(hold);
    await h.terminal(id);
    assert.equal((await h.facts(id)).state, 'SUCCEEDED');
  } finally { rmSync(hold, { force: true }); h.close(); }
});

// §6b — an explicitly configured deadline still kills; the next round starts
// clean (old code: t.stalled persisted and failed round two too).
test('§6 configured deadline kills, and the next round is not polluted', async () => {
  const h = await serve({ CLI_BRIDGE_STALL_MS: '200' });
  const hold = join(h.state, 'hold');
  writeFileSync(hold, 'x');
  try {
    h.setConf({ hold });
    const r = await h.launch('doomed quiet task');
    const id = r.body.taskId;
    await h.terminal(id);
    let facts = await h.facts(id);
    assert.equal(facts.state, 'FAILED');
    assert.equal(facts.reasonCode, 'ENGINE_STALLED');
    // Round two, quick fixture: must succeed on its own merits.
    h.setConf({});
    const f = await h.call(`/local/v1/tasks/${id}/messages`,
      { requestKey: 'rk-f2', kind: 'NATIVE_FOLLOW_UP', content: 'round two', ...pins(facts) });
    assert.equal(f.status, 200, f.text);
    // The old failure must not ride into the new round's facts either —
    // reasonCode resets at dispatch, not only at the next close.
    assert.equal((await h.facts(id)).reasonCode, '');
    await h.terminal(id);
    facts = await h.facts(id);
    assert.equal(facts.state, 'SUCCEEDED');
    assert.equal(facts.reasonCode, '');
  } finally { rmSync(hold, { force: true }); h.close(); }
});

// §2 — resume must wait until the dying turn is confirmed gone; a late old
// close must not clobber the new round.
test('§2 resume during wind-down is refused; after close it runs cleanly', async () => {
  const h = await serve();
  const hold = join(h.state, 'hold');
  writeFileSync(hold, 'x');
  try {
    h.setConf({ hold, pipeHoldMs: 1500 });
    const r = await h.launch('pausable task');
    const id = r.body.taskId;
    // RUNNING is set on spawn. The init event proves the CLI fixture has
    // already started its pipe holder before the test requests PAUSE.
    await wait(async () => Boolean((await h.facts(id)).nativeSessionId));
    const p = await h.call(`/local/v1/tasks/${id}/commands`, { requestKey: 'rk-p1', intent: 'PAUSE' });
    assert.equal(p.status, 200, p.text);
    const update = { operationId: 'prepare-update' };
    assert.equal((await h.call('/local/v1/update-reservation', update)).body.state, 'busy');
    // SIGTERM killed the fixture, but a survivor holds its pipes ~1.5s —
    // the old turn has not closed, so RESUME cannot start the next round.
    const early = await h.call(`/local/v1/tasks/${id}/commands`, { requestKey: 'rk-r1', intent: 'RESUME' });
    assert.equal(early.status, 409);
    assert.equal(early.body.code, 'NATIVE_ACTION_UNSUPPORTED');
    assert.equal((await h.facts(id)).state, 'PAUSED');
    rmSync(hold);
    await h.quiescent(id);
    h.setConf({});
    const reserved = (await h.call('/local/v1/update-reservation', update)).body;
    assert.equal(reserved.state, 'held');
    assert.equal(reserved.bootId, (await h.call('/local/v1/identity')).body.bootId);
    assert.equal((await h.launch('blocked while reserved')).body.code, 'UPDATE_RESERVED');
    assert.equal((await h.call('/local/v1/update-reservation/release', { operationId: 'other' })).status, 409);
    assert.equal((await h.call('/local/v1/update-reservation/release', update)).body.state, 'released');
    const ok = await h.call(`/local/v1/tasks/${id}/commands`, { requestKey: 'rk-r2', intent: 'RESUME' });
    assert.equal(ok.status, 200, ok.text);
    await h.terminal(id);
    assert.equal((await h.facts(id)).state, 'SUCCEEDED');
    rmSync(hold, { force: true });
  } finally { rmSync(hold, { force: true }); h.close(); }
});

// §3 — adopting a same-engine session executes the submitted instruction
// exactly once (native resume argv), and the user's typed text is what the
// transcript shows.
test('§3 same-engine adopt runs the instruction once through native resume', async () => {
  const h = await serve();
  try {
    const uuid = randomUUID();
    writeClaudeSession(h.home, uuid, h.project);
    h.setConf({ sessionId: 'sess-continued' });
    const r = await h.launch('carry on now', { adoptSessionId: uuid });
    assert.equal(r.status, 200, r.text);
    const id = r.body.taskId;
    await h.terminal(id);
    const facts = await h.facts(id);
    assert.equal(facts.state, 'SUCCEEDED');
    const argv = h.argvLines();
    assert.equal(argv.length, 1, 'instruction must dispatch exactly once');
    assert.ok(argv[0].includes('--resume') && argv[0].includes(uuid) && argv[0].includes('carry on now'));
    const msgs = await h.call(`/local/v1/tasks/${id}/messages`);
    const users = msgs.body.items.filter((m) => m.role === 'user');
    assert.equal(users.at(-1).text, 'carry on now');
  } finally { h.close(); }
});

// §4 — a cross-engine adopt cannot fake the source id: the first real turn
// creates a fresh native session whose id then satisfies the pin contract.
test('§4 cross-engine adopt launches first round and gains a real native id', async () => {
  const h = await serve();
  try {
    const uuid = randomUUID();
    writeCodexSession(h.home, uuid);
    h.setConf({ sessionId: 'target-session-1' });
    const r = await h.launch('take over please', { adoptSessionId: uuid, adoptEngine: 'codex' });
    assert.equal(r.status, 200, r.text);
    const id = r.body.taskId;
    await h.terminal(id);
    const facts = await h.facts(id);
    assert.equal(facts.state, 'SUCCEEDED');
    assert.equal(facts.nativeSessionId, 'target-session-1');
    const argv = h.argvLines();
    assert.equal(argv.length, 1);
    assert.ok(!argv[0].includes('--resume'));
    const instruction = argv[0][argv[0].indexOf('-p') + 1];
    assert.match(instruction, /PRIOR-TRANSCRIPT/);
    assert.match(instruction, /codex original question/);
    assert.match(instruction, /take over please/);
    // Real id landed → the pin contract admits a follow-up turn.
    h.setConf({});
    const f = await h.call(`/local/v1/tasks/${id}/messages`,
      { requestKey: 'rk-fx', kind: 'NATIVE_FOLLOW_UP', content: 'next step', ...pins(facts) });
    assert.equal(f.status, 200, f.text);
    await h.terminal(id);
    assert.equal((await h.facts(id)).state, 'SUCCEEDED');
  } finally { h.close(); }
});

// An engine binary that vanishes after startup (mid-update uninstall) makes
// spawn fire 'error' — without a listener that event crashes the whole
// bridge. The round must land FAILED and the bridge must stay up.
test('a vanished engine binary fails the round, never the bridge', async () => {
  const exe = join(mkdtempSync(join(tmpdir(), 'cb-vanish-')),
    process.platform === 'win32' ? 'fixture-node.exe' : 'fixture-node');
  copyFileSync(process.execPath, exe);
  const h = await serve({}, exe);
  try {
    rmSync(exe, { force: true });
    const r = await h.launch('doomed task');
    assert.equal(r.status, 200, r.text);
    await h.terminal(r.body.taskId);
    assert.equal((await h.facts(r.body.taskId)).state, 'FAILED');
    // The bridge itself survived the spawn error.
    assert.equal((await h.call('/local/v1/identity')).status, 200);
  } finally { h.close(); }
});

// §3 — RESUME on an adopted task that never earned a native session id must
// re-carry the takeover transcript, or the nudge runs context-free.
test('§3 resume re-injects the adoption pack when no native session exists', async () => {
  const h = await serve();
  const hold = join(h.state, 'hold');
  writeFileSync(hold, 'x');
  try {
    const uuid = randomUUID();
    writeCodexSession(h.home, uuid);
    // noSession — the engine reports no session_id, so the task never gains
    // a resumable native id and every spawn needs the transcript pack.
    h.setConf({ noSession: true, hold });
    const r = await h.launch('take over please', { adoptSessionId: uuid, adoptEngine: 'codex' });
    const id = r.body.taskId;
    await wait(async () => (await h.facts(id)).state === 'RUNNING');
    // RUNNING flips at dispatch — the fixture still needs ~100ms to boot and
    // write its argv log. PAUSE before that write races the kill and loses
    // the line, so wait for it deterministically.
    await wait(() => h.argvLines().length >= 1);
    const pause = await h.call(`/local/v1/tasks/${id}/commands`, { requestKey: 'rk-pp', intent: 'PAUSE' });
    assert.equal(pause.status, 200, pause.text);
    await h.quiescent(id);
    h.setConf({ noSession: true });
    const resume = await h.call(`/local/v1/tasks/${id}/commands`, { requestKey: 'rk-rr', intent: 'RESUME' });
    assert.equal(resume.status, 200, resume.text);
    await h.terminal(id);
    const argv = h.argvLines();
    assert.equal(argv.length, 2);
    const resumed = argv[1][argv[1].indexOf('-p') + 1];
    assert.match(resumed, /PRIOR-TRANSCRIPT/);
    assert.match(resumed, /Continue from where you were interrupted/);
  } finally { rmSync(hold, { force: true }); h.close(); }
});
