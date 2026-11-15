import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';

test('vendor history supports independent Agent pages, old Devin lookup and read-only transcripts', async t => {
  const fixture = await mkdtemp(join(tmpdir(), 'cli-history-'));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const history = join(fixture, '.claude', 'projects', 'fixture');
  const database = join(fixture, '.local', 'share', 'devin', 'cli');
  await Promise.all([mkdir(history, { recursive: true }), mkdir(database, { recursive: true })]);

  const db = new DatabaseSync(join(database, 'sessions.db'));
  db.exec(`CREATE TABLE sessions (id TEXT PRIMARY KEY, title TEXT, working_directory TEXT, last_activity_at INTEGER, hidden INTEGER);
    CREATE TABLE message_nodes (session_id TEXT, chat_message TEXT, created_at INTEGER);`);
  const insert = db.prepare('INSERT INTO sessions VALUES (?, ?, ?, ?, ?)');
  for (let n = 0; n < 220; n++) {
    const title = n === 12 ? '独立恢复记录' : 'Devin fixture ' + n;
    insert.run('devin-' + n, title, fixture, 1794000000 + n, 0);
  }
  insert.run('devin-hidden', 'hidden', fixture, 1794009000, 1);
  const message = db.prepare('INSERT INTO message_nodes VALUES (?, ?, ?)');
  for (let n = 0; n < 16; n++) {
    message.run('devin-12', JSON.stringify({ role: n % 2 ? 'assistant' : 'user', content: 'message ' + n }), n);
  }
  db.close();
  await Promise.all(Array.from({ length: 215 }, (_, n) => writeFile(join(history, 'claude-' + n + '.jsonl'),
    JSON.stringify({ type: 'user', cwd: fixture, message: { content: 'Claude fixture ' + n } }) + '\n')));

  const preload = join(fixture, 'fixture-home.mjs');
  await writeFile(preload, `import os from 'node:os'; import { syncBuiltinESMExports } from 'node:module';
    os.homedir = () => ${JSON.stringify(fixture)}; syncBuiltinESMExports();`);
  const listener = createServer().listen(0, '127.0.0.1');
  await once(listener, 'listening');
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  const token = randomBytes(24).toString('hex');
  const child = spawn(process.execPath, [
    '--import', preload, new URL('../cli-bridge/host.mjs', import.meta.url).pathname,
    'serve', '--engine', 'claude', '--state', join(fixture, 'workers', 'claude'),
    '--project', fixture, '--port', String(port), '--executable', process.execPath,
  ], { env: { ...process.env, WORKER_LOCAL_TOKEN: token }, stdio: ['ignore', 'pipe', 'pipe'] });
  const stopped = once(child, 'exit');
  t.after(async () => { child.kill(); await stopped; });
  const ready = await Promise.race([once(child.stdout, 'data'), stopped]);
  assert.match(String(ready[0]), /127\.0\.0\.1/);
  const base = 'http://127.0.0.1:' + port + '/local/v1';
  const request = (path, body) => fetch(base + path, {
    method: body ? 'POST' : 'GET',
    headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const read = async path => {
    const response = await request(path);
    assert.equal(response.status, 200, path);
    return response.json();
  };
  const first = await read('/history?limit=20');
  assert.equal(first.schemaVersion, 'worker-local-history/2');
  assert.equal(first.items.length, 40);
  for (const [engine, count] of [['devin', 220], ['claude', 215]]) {
    let cursor = first.pages.find(page => page.engine === engine).cursor;
    const ids = first.items.filter(item => item.engine === engine).map(item => item.id);
    while (cursor) {
      const page = await read('/history?limit=20&engine=' + engine + '&cursor=' + cursor);
      ids.push(...page.items.map(item => item.id));
      cursor = page.pages[0].cursor;
    }
    assert.equal(ids.length, count);
    assert.equal(new Set(ids).size, count);
    if (engine === 'devin') assert.ok(ids.includes('devin-12'));
  }
  const search = await read('/history?limit=20&q=' + encodeURIComponent('独立恢复'));
  assert.deepEqual(search.items.map(item => item.id), ['devin-12']);
  assert.match(search.items[0].at, /^2026-/);

  const latest = await read('/history/sessions/devin-12?engine=devin&limit=10');
  assert.equal(latest.items[0].id, '7');
  assert.equal(latest.items.at(-1).text, 'message 15');
  const earlier = await read('/history/sessions/devin-12?engine=devin&before=' + latest.before);
  assert.equal(earlier.items.length, 6);
  assert.equal((await request('/history/sessions/devin-hidden?engine=devin')).status, 404);

  const preflight = await request('/task-preflights', { projectId: 'workspace', engineId: 'claude' });
  assert.equal(preflight.status, 200);
  const options = await preflight.json();
  const launch = await request('/tasks', {
    requestKey: 'test-adopt-1', projectId: 'workspace', engineId: 'claude', optionsRevision: options.optionsRevision,
    instruction: '独立恢复记录', adoptSessionId: 'devin-12', adoptEngine: 'devin', model: 'opus',
  });
  assert.equal(launch.status, 200);
  const task = await read('/tasks/' + (await launch.json()).taskId);
  assert.equal(task.adoption.from, 'devin');
  assert.equal(task.configuredModel, 'opus');
  assert.equal(task.quiescent, true);
});
