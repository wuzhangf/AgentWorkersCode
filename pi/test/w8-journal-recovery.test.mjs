import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';

const token = 'deterministic-w8-journal-test-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w8-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup() { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w8-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body?.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello') { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'pi' })).body; const result = await call('/tasks', { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction }); return { taskId: result.body.taskId }; }
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });

const journalLines = (...messages) => [
  JSON.stringify({ type: 'session', id: 's1', timestamp: '2026-01-01T00:00:00.000Z' }),
  ...messages,
  'not-json',
].join('\n');
const userMsg = text => JSON.stringify({ type: 'message', timestamp: '2026-01-01T00:00:01.000Z', message: { role: 'user', content: [{ type: 'text', text }] } });
const assistantMsg = text => JSON.stringify({ type: 'message', timestamp: '2026-01-01T00:00:02.000Z', message: { role: 'assistant', content: [{ type: 'thinking', thinking: 'ok' }, { type: 'toolCall', toolName: 'bash' }, { type: 'text', text }] } });

test('UNKNOWN task with empty IPC feed recovers conversation from session journal', async () => {
  const s = await setup();
  try {
    const c = await create(s.call, 'recover me');
    await succeeded(s.call, c.taskId);
    const row = s.host.store.task(c.taskId);
    const journal = join(row.sessionDir, 'session-recovered.jsonl');
    writeFileSync(journal, journalLines(
      userMsg('写一个 hello world'),
      assistantMsg('已创建 hello.py 并输出 Hello, World!'),
      JSON.stringify({ type: 'message', message: { role: 'toolResult', content: [{ type: 'text', text: 'tool output is not a turn' }] } }),
    ));
    // Simulate the observed failure shape: UNKNOWN verdict, empty public feed.
    row.publicMessages = []; row.output = ''; row.state = 'UNKNOWN';
    row.reasonCode = 'PROCESS_ESCAPE_NOT_EXCLUDED'; row.nativeSessionFile = journal;
    s.host.store.save(row);
    const msgs = (await s.call(`/tasks/${c.taskId}/messages`)).body;
    assert.deepEqual(msgs.items.map(m => m.role), ['user', 'assistant']);
    assert.equal(msgs.items[0].text, '写一个 hello world');
    assert.equal(msgs.items[1].text, '已创建 hello.py 并输出 Hello, World!');
    assert.ok(msgs.text.includes('Hello, World!'));
    const out = (await s.call(`/tasks/${c.taskId}/output`)).body;
    assert.ok(out.text.includes('Hello, World!'));
    const after = (await s.call('/tasks/' + c.taskId)).body;
    assert.equal(after.state, 'UNKNOWN', 'read projection must not relabel the verdict');
    assert.equal(after.reasonCode, 'PROCESS_ESCAPE_NOT_EXCLUDED', 'verification reason stays explicit');
    assert.notEqual(after.quiescent, true, 'no false clean-quiescence claim');
  } finally { await s.host.close(); }
});

test('live public messages win over journal; UNKNOWN marking persists projection', async () => {
  const s = await setup();
  try {
    const c = await create(s.call, 'keep live feed');
    await succeeded(s.call, c.taskId);
    const row = s.host.store.task(c.taskId);
    const journal = join(row.sessionDir, 'session-shadowed.jsonl');
    writeFileSync(journal, journalLines(assistantMsg('journal-only text')));
    row.nativeSessionFile = journal; s.host.store.save(row);
    const live = (await s.call(`/tasks/${c.taskId}/messages`)).body;
    assert.ok(live.items.length > 0 && live.items.every(m => m.text !== 'journal-only text'), 'existing feed not overwritten');
    // The UNKNOWN marking path persists the recovered conversation.
    row.publicMessages = []; row.output = '';
    s.host.markEffectsUnknown(row);
    assert.equal(row.publicMessages.length, 1);
    assert.equal(row.publicMessages[0].role, 'assistant');
    assert.equal(row.publicMessages[0].text, 'journal-only text');
    assert.ok(row.output.includes('journal-only text'));
  } finally { await s.host.close(); }
});

test('journal outside sessionDir or absent yields empty projection, never a crash', async () => {
  const s = await setup();
  try {
    const c = await create(s.call, 'safety');
    await succeeded(s.call, c.taskId);
    const row = s.host.store.task(c.taskId);
    // Escaped nativeSessionFile is refused by the sessionDir prefix check.
    const outside = join(s.state, 'outside.jsonl');
    writeFileSync(outside, journalLines(assistantMsg('must not leak')));
    row.publicMessages = []; row.output = ''; row.nativeSessionFile = outside;
    s.host.store.save(row);
    const msgs = (await s.call(`/tasks/${c.taskId}/messages`)).body;
    assert.equal(msgs.items.length, 0);
    // No journal at all also yields empty items rather than an error.
    row.nativeSessionFile = null; s.host.store.save(row);
    assert.equal((await s.call(`/tasks/${c.taskId}/messages`)).status, 200);
  } finally { await s.host.close(); }
});
