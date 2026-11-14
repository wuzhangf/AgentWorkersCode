import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync, spawn, fork } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';

const token = 'deterministic-w4b-test-token-not-a-secret';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w4b-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
const w4host = fileURLToPath(new URL('./w4-host.mjs', import.meta.url));
const realChild = fileURLToPath(new URL('../dist/engine/child.js', import.meta.url));
const mcpFixture = fileURLToPath(new URL('./fixtures/mcp-fixture-server.mjs', import.meta.url));
async function wait(fn, timeout = 15000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w4b-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, receipt: result.body, status: result.status, code: result.body.code }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const events = async (call, taskId) => (await call(`/tasks/${taskId}/events?limit=100`)).body.items;
const nodeArgv = code => [process.execPath, '-e', code];
const b64 = s => Buffer.from(s, 'utf8').toString('base64');
const fromB64 = s => Buffer.from(s, 'base64').toString('utf8');
function skillSource(name = 'nwf-skill') { const dir = mkdtempSync(join(tmpdir(), 'nwf-w4b-skill-src-')); mkdirSync(join(dir, name)); writeFileSync(join(dir, name, 'SKILL.md'), `---\nname: ${name}\ndescription: deterministic W4b skill\n---\nMagic word NWF_W4B_SKILL.\n`); return join(dir, name); }
function extensionSource(name = 'nwf-ext', body) { const dir = mkdtempSync(join(tmpdir(), 'nwf-w4b-ext-src-')); mkdirSync(join(dir, name)); writeFileSync(join(dir, name, 'index.js'), body ?? `export default (pi) => { pi.on('session_start', () => {}); };\n`); return join(dir, name); }

test('capability surface advertises P18/P19/P20 honestly and engines report pty:false', async () => {
  const s = await setup(); try {
    const caps = (await s.call('/worker-host/v1/capabilities')).body;
    for (const pid of ['P18', 'P19', 'P20']) {
      const row = caps.items.find(i => i.id === pid); assert.ok(row, pid + ' row exists');
      assert.equal(row.implementation, 'IMPLEMENTED'); assert.equal(row.qualification, 'PARTIAL');
    }
    assert.equal(caps.items.find(i => i.id === 'P18').reasonCode, 'PTY_NOT_CLAIMED');
    const engines = (await s.call('/engines')).body;
    const c = engines.items[0].capabilities;
    assert.equal(c.terminals.pty, false); assert.equal(c.terminals.resize, false);
    assert.deepEqual(c.terminals.signals, ['TERM', 'KILL', 'INT']);
    assert.equal(c.mcpServers.transport, 'stdio'); assert.equal(c.mcpServers.envIndirection, false);
    assert.equal(c.skills.managedProfileOnly, true); assert.equal(c.extensions.contentBinding, 'sha256');
  } finally { await s.host.close(); }
});

test('mcpServers validation: shape, namespace clash, secret env names, env indirection rejected', async () => {
  const s = await setup(); try {
    for (const [mcpServers, code] of [
      [[{ id: 'Fix', command: 'node' }], 'INVALID_MCP_SERVERS'],
      [[{ id: '-lead', command: 'node' }], 'INVALID_MCP_SERVERS'],
      [[{ id: 'a', command: 'node' }, { id: 'a', command: 'node' }], 'MCP_SERVER_NAME_CLASH'],
    ]) assert.equal((await create(s.call, 'hello', { mcpServers })).code, code, JSON.stringify(mcpServers));
    for (const env of [{ MY_API_KEY: 'x' }, { FIXTURE_TOKEN: 'x' }, { MY_SECRET: 'x' }, { MODEL_AUTH: 'x' }])
      assert.equal((await create(s.call, 'hello', { mcpServers: [{ id: 'srv', command: 'node', env }] })).code, 'MCP_ENV_DENIED', JSON.stringify(env));
    for (const env of [{ V: '$MODEL_API_KEY' }, { V: '${HOME}' }, { V: '$$' }, { V: '!cat /etc/passwd' }])
      assert.equal((await create(s.call, 'hello', { mcpServers: [{ id: 'srv', command: 'node', env }] })).code, 'MCP_ENV_DENIED', 'env indirection ' + JSON.stringify(env));
    assert.equal((await create(s.call, 'hello', { mcpServers: [{ id: 'srv', command: 'node', args: ['-e', null] }] })).code, 'INVALID_MCP_SERVERS');
    assert.equal((await create(s.call, 'hello', { mcpServers: [{ id: 'srv', command: 'node', transport: 'http' }] })).code, 'UNEXPECTED_FIELD');
    assert.equal((await create(s.call, 'hello', { mcpServers: Array.from({ length: 9 }, (_, i) => ({ id: 's' + i, command: 'node' })) })).code, 'INVALID_MCP_SERVERS');
    assert.equal(s.host.store.tasks().length, 0, 'rejected admissions create no task');
    const ok = await create(s.call, 'hello', { mcpServers: [{ id: 'fixsrv', command: process.execPath, args: [mcpFixture], env: { FIXTURE_FLAG: 'literal' } }] });
    assert.equal(ok.status, 202);
    assert.deepEqual(s.host.store.task(ok.taskId).mcpServers, [{ id: 'fixsrv', command: process.execPath, args: [mcpFixture], env: { FIXTURE_FLAG: 'literal' } }]);
    await wait(async () => (await s.call('/tasks/' + ok.taskId)).body.state === 'SUCCEEDED');
    const declared = (await events(s.call, ok.taskId)).find(e => e.kind === 'mcp.declared');
    assert.equal(declared.server, 'fixsrv'); assert.deepEqual(declared.envKeys, ['FIXTURE_FLAG']);
    assert.equal(JSON.stringify(declared).includes('literal'), false, 'env values never enter durable facts');
  } finally { await s.host.close(); }
});

test('skills/extensions validation, managed-profile copy and sha256 binding facts', async () => {
  const s = await setup(); try {
    const skillSrc = skillSource(); const extSrc = extensionSource();
    for (const [field, value, code] of [
      ['skills', [{ name: 'Bad_Name', sourcePath: skillSrc }], 'INVALID_SKILLS'],
      ['skills', [{ name: 'x', sourcePath: 'relative/path' }], 'INVALID_SKILLS'],
      ['skills', [{ name: 'x', sourcePath: join(s.state, 'missing-dir-xyz') }], 'SKILL_SOURCE_UNAVAILABLE'],
      ['skills', [{ name: 'x', sourcePath: skillSrc }, { name: 'x', sourcePath: skillSrc }], 'INVALID_SKILLS'],
      ['extensions', [{ name: 'x', sourcePath: join(s.state, 'missing-ext-xyz') }], 'EXTENSION_SOURCE_UNAVAILABLE'],
      ['extensions', [{ name: 'x', sourcePath: skillSrc, extra: 1 }], 'UNEXPECTED_FIELD'],
    ]) assert.equal((await create(s.call, 'hello', { [field]: value })).code, code, JSON.stringify(value));
    const c = await create(s.call, 'hello', { skills: [{ name: 'nwf-skill', sourcePath: skillSrc }], extensions: [{ name: 'nwf-ext', sourcePath: extSrc }] });
    assert.equal(c.status, 202);
    await wait(async () => (await s.call('/tasks/' + c.taskId)).body.state === 'SUCCEEDED');
    const task = s.host.store.task(c.taskId);
    const profile = join(task.sessionDir, 'profile');
    assert.equal(readFileSync(join(profile, 'skills', 'nwf-skill', 'SKILL.md'), 'utf8'), readFileSync(join(skillSrc, 'SKILL.md'), 'utf8'), 'skill copied into managed profile');
    assert.equal(readFileSync(join(profile, 'extensions', 'nwf-ext', 'index.js'), 'utf8'), readFileSync(join(extSrc, 'index.js'), 'utf8'), 'extension copied into managed profile');
    for (const [kind, name] of [['skill', 'nwf-skill'], ['extension', 'nwf-ext']]) {
      const bound = task.resourceBindings[kind + ':' + name];
      assert.match(bound.sha256, /^[0-9a-f]{64}$/); assert.ok(bound.fileCount >= 1);
      const ev = (await events(s.call, c.taskId)).find(e => e.kind === kind + '.bound');
      assert.equal(ev.name, name); assert.equal(ev.sha256, bound.sha256, 'binding fact matches task row');
    }
    // a symlinked source member is refused at bind time and fails the launch honestly
    const linkSrc = extensionSource('linked-ext'); symlinkSync(extSrc, join(linkSrc, 'alias'), process.platform === 'win32' ? 'junction' : 'dir');
    const bad = await create(s.call, 'hello', { extensions: [{ name: 'linked-ext', sourcePath: linkSrc }] });
    const d = await wait(async () => { const d = (await s.call('/tasks/' + bad.taskId)).body; return d.state === 'FAILED' && d; });
    assert.equal(d.reasonCode, 'RESOURCE_SYMLINK_DENIED');
  } finally { await s.host.close(); }
});

test('terminal lifecycle: create, pty:false, base64 input/output cursor, signal, delete, audit', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello');
    const d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'SUCCEEDED' && d; });
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals`, { argv: [] })).body.code, 'INVALID_ARGV');
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals`, { argv: ['x'], bogus: 1 })).body.code, 'UNEXPECTED_FIELD');
    const created = await s.call(`/tasks/${c.taskId}/terminals`, { argv: nodeArgv(`process.stdin.on('data',d=>process.stdout.write('got:'+d))`) });
    assert.equal(created.status, 202); const tid = created.body.terminalId;
    assert.equal(created.body.pty, false); assert.equal(created.body.cursor, '0');
    const input = await s.call(`/tasks/${c.taskId}/terminals/${tid}/input`, { data: b64('hello-term') });
    assert.equal(input.body.accepted, 'hello-term'.length);
    const out = await s.call(`/tasks/${c.taskId}/terminals/${tid}/output?cursor=0&waitMs=5000`);
    assert.equal(out.body.exited, false); assert.equal(fromB64(out.body.data), 'got:hello-term');
    assert.equal(Number(out.body.cursor) > 0, true);
    const again = await s.call(`/tasks/${c.taskId}/terminals/${tid}/output?cursor=${out.body.cursor}`);
    assert.equal(again.body.data, ''); assert.equal(again.body.cursor, out.body.cursor);
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals/${tid}/resize`, { cols: 80, rows: 24 })).body.code, 'NOT_SUPPORTED');
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals/${tid}/input`, { data: '!!!not-b64' })).body.code, 'INVALID_BASE64');
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals/${tid}/signal`, { signal: 'BREAK' })).body.code, 'INVALID_SIGNAL');
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals/${tid}/signal`, { signal: 'KILL' })).body.delivered, true);
    const exited = await wait(async () => { const o = (await s.call(`/tasks/${c.taskId}/terminals/${tid}/output?cursor=${out.body.cursor}&waitMs=2000`)).body; return o.exited && o; });
    assert.equal(exited.exited, true);
    const del = await s.call(`/tasks/${c.taskId}/terminals/${tid}`, undefined, {}, 'DELETE');
    assert.equal(del.body.closed, true);
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals/${tid}/input`, { data: b64('x') })).body.code, 'TERMINAL_NOT_RUNNING');
    const audit = (await s.call(`/worker-host/v1/tasks/${c.taskId}/audit`)).body;
    for (const kind of ['TERMINAL_CREATED', 'TERMINAL_INPUT', 'TERMINAL_SIGNAL', 'TERMINAL_EXITED', 'TERMINAL_CLOSED'])
      assert.ok(audit.items.some(i => i.kind === kind), kind + ' in audit projection');
    assert.equal(s.host.store.task(c.taskId).terminals[tid].state, 'CLOSED');
    // idempotent create replays the same terminalId
    const once = await s.call(`/tasks/${c.taskId}/terminals`, { requestKey: 'term-once', argv: nodeArgv('process.exit(0)') });
    const twice = await s.call(`/tasks/${c.taskId}/terminals`, { requestKey: 'term-once', argv: nodeArgv('process.exit(0)') });
    assert.equal(twice.body.terminalId, once.body.terminalId); assert.equal(twice.body.replayed, true);
  } finally { await s.host.close(); }
});

test('terminal output ring is bounded and overruns report truncated=true', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello');
    await wait(async () => (await s.call('/tasks/' + c.taskId)).body.state === 'SUCCEEDED');
    const created = await s.call(`/tasks/${c.taskId}/terminals`, { argv: nodeArgv(`process.stdout.write('x'.repeat(400000))`) });
    const tid = created.body.terminalId;
    const out = await wait(async () => { const o = (await s.call(`/tasks/${c.taskId}/terminals/${tid}/output?cursor=0&waitMs=2000&maxBytes=1000000`)).body; return o.exited && o; });
    assert.equal(out.truncated, true, 'cursor 0 expired past the retained ring');
    assert.ok(out.bytes <= 262144, 'ring retained at most the bounded buffer');
    assert.equal(Number(out.end), 400000, 'end offset tracks every byte emitted');
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals/${tid}/output?cursor=99999999`)).body.code, 'TERMINAL_CURSOR_INVALID');
  } finally { await s.host.close(); }
});

test('task STOP kills owned terminals with Job-drained proof and durable facts', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'waiting');
    const d = await wait(async () => { const d = (await s.call('/tasks/' + c.taskId)).body; return d.state === 'RUNNING' && d; });
    const t1 = (await s.call(`/tasks/${c.taskId}/terminals`, { argv: nodeArgv('setInterval(()=>{},1000)') })).body.terminalId;
    const t2 = (await s.call(`/tasks/${c.taskId}/terminals`, { argv: nodeArgv('setInterval(()=>{},1000)') })).body.terminalId;
    const stop = await s.call(`/tasks/${c.taskId}/commands`, { requestKey: 'stop-t', intent: 'STOP', ...guard(d) });
    assert.equal(stop.body.state, 'APPLIED');
    const stopped = (await events(s.call, c.taskId)).find(e => e.kind === 'TERMINALS_STOPPED');
    assert.equal(stopped.terminals.length, 2);
    for (const p of stopped.terminals) { assert.equal(p.method, process.platform === 'win32' ? 'WINDOWS_JOB_ACTIVE_ZERO' : 'OWNED_PROCESS_GROUP'); assert.equal(p.activeProcesses, 0); }
    const task = s.host.store.task(c.taskId);
    for (const tid of [t1, t2]) { assert.equal(task.terminals[tid].state, 'CLOSED'); assert.equal(task.terminals[tid].reasonCode, 'TASK_STOP'); }
    assert.equal((await s.call(`/tasks/${c.taskId}/terminals`, { argv: nodeArgv('1') })).body.code, 'TERMINAL_TASK_INACTIVE', 'terminal create denied on a stopped task');
  } finally { await s.host.close(); }
});

test('host SIGKILL marks persisted RUNNING terminals LOST on restart', { skip: process.platform !== 'win32' }, async () => {
  const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w4b-tkill-')); const p = project();
  const child = spawn(process.execPath, [w4host, state, p, token], { stdio: ['ignore', 'pipe', 'pipe'] });
  const port = await new Promise(resolve => child.stdout.once('data', data => resolve(Number(String(data).trim()))));
  const call = client(port);
  const c = await create(call, 'hello');
  await wait(async () => (await call('/tasks/' + c.taskId)).body.state === 'SUCCEEDED');
  const tid = (await call(`/tasks/${c.taskId}/terminals`, { argv: nodeArgv('setInterval(()=>{},1000)') })).body.terminalId;
  child.kill('SIGKILL'); await new Promise(resolve => child.once('exit', resolve));
  const host = new PiHost({ state, token, childModule: fixture });
  try {
    const task = host.store.task(c.taskId);
    assert.equal(task.terminals[tid].state, 'LOST'); assert.equal(task.terminals[tid].reasonCode, 'HOST_RESTART_JOB_CLOSED');
    const call2 = client(await host.listen());
    const items = (await call2(`/tasks/${c.taskId}/events?limit=100`)).body.items;
    assert.ok(items.some(e => e.kind === 'TERMINALS_LOST'));
    assert.equal((await call2(`/tasks/${c.taskId}/terminals/${tid}/output`)).body.code, 'TERMINAL_LOST');
  } finally { await host.close(); }
});

test('real engine child: managed skill+extension load, MCP fixture binds, load failure is a fact', async () => {
  const root = mkdtempSync(join(tmpdir(), 'nwf-pi-w4b-engine-'));
  const workspace = join(root, 'ws'); const sessionDir = join(root, 'sess'); const profile = join(sessionDir, 'profile');
  mkdirSync(workspace, { recursive: true }); mkdirSync(join(profile, 'skills', 'probe-skill'), { recursive: true }); mkdirSync(join(profile, 'extensions', 'probe-ext'), { recursive: true }); mkdirSync(join(profile, 'extensions', 'probe-bad'), { recursive: true });
  writeFileSync(join(profile, 'skills', 'probe-skill', 'SKILL.md'), '---\nname: probe-skill\ndescription: W4b probe skill\n---\nMagic word NWF_W4B_SKILL.\n');
  writeFileSync(join(profile, 'extensions', 'probe-ext', 'index.js'), `import { Type } from 'typebox';\nexport default (pi) => { pi.on('session_start', () => {}); pi.registerTool({ name: 'probe_tool', label: 'Probe', description: 'probe', parameters: Type.Object({}), async execute() { return { content: [{ type: 'text', text: 'ok' }] }; } }); };\n`);
  writeFileSync(join(profile, 'extensions', 'probe-bad', 'index.js'), `export default () => { throw new Error('deliberate load failure'); };\n`);
  const child = fork(realChild, [], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  const messages = []; child.on('message', m => messages.push(m)); child.stderr.on('data', () => {});
  try {
    child.send({ type: 'init', workspace, sessionDir, model: { name: 'fixture-model', baseUrl: 'http://127.0.0.1:1', headerEnvKeys: [] }, probeEnv: [],
      mcpServers: [{ id: 'fixsrv', config: { command: process.execPath, args: [mcpFixture], env: {}, exposure: 'direct', timeout: 30 } }],
      extensions: [{ name: 'probe-ext' }, { name: 'probe-bad' }] });
    const ready = await wait(() => messages.find(m => m.type === 'ready'), 30000);
    assert.equal(ready.resources.mcp.length, 1);
    assert.equal(ready.resources.mcp[0].state, 'BOUND', JSON.stringify(ready.resources.mcp));
    for (const name of ['mcp__fixsrv__echo', 'mcp__fixsrv__workspace_marker']) {
      assert.ok(ready.resources.mcp[0].tools.includes(name), name + ' registered');
      assert.ok(ready.tools.includes(name), name + ' in active tool list');
    }
    assert.ok(ready.tools.includes('probe_tool'), 'extension tool activated through the allowlist');
    assert.deepEqual(ready.resources.skills.map(s => s.name), ['probe-skill']);
    assert.ok(ready.resources.extensions.some(e => e.path.includes('probe-ext')));
    assert.ok(ready.resources.extensionErrors.some(e => e.path.includes('probe-bad') && /deliberate load failure/.test(e.error)));
  } finally { child.kill(); }
});
