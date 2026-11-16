import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { once } from 'node:events';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';

test('vendor history supports independent Agent pages, old Devin lookup and read-only transcripts', async t => {
  const fixture = await mkdtemp(join(tmpdir(), 'cli-history-'));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const history = join(fixture, '.claude', 'projects', 'fixture');
  // DEVIN_DBS prefers %APPDATA%\Devin\cli on win32 — homedir alone does not
  // redirect it, so the db must live where the child's APPDATA points.
  const database = process.platform === 'win32'
    ? join(fixture, 'AppData', 'Roaming', 'Devin', 'cli')
    : join(fixture, '.local', 'share', 'devin', 'cli');
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
  const startBridge = async (engine, executable) => {
    const listener = createServer().listen(0, '127.0.0.1');
    await once(listener, 'listening');
    const port = listener.address().port;
    await new Promise(resolve => listener.close(resolve));
    const token = randomBytes(24).toString('hex');
    const child = spawn(process.execPath, [
      // Windows needs file:// URLs for --import and fileURLToPath for script
      // args — a bare C:\ path reads as scheme 'c:' and pathname keeps a
      // leading slash POSIX doesn't have.
      '--import', pathToFileURL(preload).href,
      fileURLToPath(new URL('../cli-bridge/host.mjs', import.meta.url)),
      'serve', '--engine', engine, '--state', join(fixture, 'workers', engine),
      '--project', fixture, '--port', String(port),
      ...(executable ? ['--executable', executable] : []),
    ], { env: { ...process.env, WORKER_LOCAL_TOKEN: token,
      // Redirect the win32-preferred store location into the fixture too —
      // homedir() alone leaves %APPDATA%\Devin\cli pointing at the real db.
      ...(process.platform === 'win32'
        ? { APPDATA: join(fixture, 'AppData', 'Roaming') } : {}) },
      stdio: ['ignore', 'pipe', 'pipe'] });
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
    return { read, request };
  };
  const claude = await startBridge('claude', process.execPath);
  const devin = await startBridge('devin');
  for (const [engine, count, bridge] of [['devin', 220, devin], ['claude', 215, claude]]) {
    const first = await bridge.read('/history?limit=20');
    assert.equal(first.schemaVersion, 'worker-local-history/2');
    assert.deepEqual(first.pages.map(page => page.engine), [engine]);
    assert.equal(first.items.length, 20);
    assert.ok(first.items.every(item => item.engine === engine));
    let cursor = first.pages[0].cursor;
    const ids = first.items.map(item => item.id);
    while (cursor) {
      const page = await bridge.read('/history?limit=20&engine=' + engine + '&cursor=' + cursor);
      ids.push(...page.items.map(item => item.id));
      cursor = page.pages[0].cursor;
    }
    assert.equal(ids.length, count);
    assert.equal(new Set(ids).size, count);
    if (engine === 'devin') assert.ok(ids.includes('devin-12'));
  }
  // A store bridge still needs one scope row: launchScopes are generated
  // from catalog engines and /history is gated per-backend — an empty table
  // would orphan its sessions. available:false + ENGINE_READ_ONLY keeps it
  // off launch pickers while 'AVAILABLE' leaves the row grantable.
  assert.deepEqual((await devin.read('/engines')).items, [{
    engineId: 'devin', engineVersion: 'read-only', version: 'read-only',
    availability: 'AVAILABLE', available: false, reasonCode: 'ENGINE_READ_ONLY',
    profileId: 'STORE', sourceKind: 'store',
    capabilities: { executionModes: [], steer: false, followUp: false,
                    stop: false, pause: false, resume: false },
  }]);
  const search = await devin.read('/history?limit=20&q=' + encodeURIComponent('独立恢复'));
  assert.deepEqual(search.items.map(item => item.id), ['devin-12']);
  assert.match(search.items[0].at, /^2026-/);

  const latestPath = '/history/sessions/devin-12?engine=devin&limit=10';
  const latest = await devin.read(latestPath);
  assert.equal(latest.items[0].id, '7');
  assert.equal(latest.items.at(-1).text, 'message 15');
  assert.deepEqual((await claude.read(latestPath)).items, latest.items);
  const earlier = await devin.read('/history/sessions/devin-12?engine=devin&before=' + latest.before);
  assert.equal(earlier.items.length, 6);
  assert.equal((await devin.request('/history/sessions/devin-hidden?engine=devin')).status, 404);

  const preflight = await claude.request('/task-preflights', { projectId: 'workspace', engineId: 'claude' });
  assert.equal(preflight.status, 200);
  const options = await preflight.json();
  const launch = await claude.request('/tasks', {
    requestKey: 'test-adopt-1', projectId: 'workspace', engineId: 'claude', optionsRevision: options.optionsRevision,
    instruction: '独立恢复记录', adoptSessionId: 'devin-12', adoptEngine: 'devin', model: 'opus',
  });
  assert.equal(launch.status, 200);
  const task = await claude.read('/tasks/' + (await launch.json()).taskId);
  assert.equal(task.adoption.from, 'devin');
  assert.equal(task.configuredModel, 'opus');
  assert.equal(task.quiescent, true);
});

test('a follow-up updates the sealed file digest under the same artifact identity', async t => {
  const fixture = await mkdtemp(join(tmpdir(), 'cli-file-versions-'));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const project = join(fixture, 'project');
  await mkdir(project);
  // POSIX runs the fake CLI as a sh script; Windows needs a .cmd shim that
  // resolvePath unwraps to `node <entry.js>` — same body in CJS.
  const executable = join(fixture,
    process.platform === 'win32' ? 'test-claude.cmd' : 'test-claude');
  if (process.platform === 'win32') {
    await writeFile(executable, '@node "%~dp0\\test-claude.js" %*\r\n');
    await writeFile(join(fixture, 'test-claude.js'), `const fs = require('node:fs');
if (process.argv[2] === '--version') { console.log('fixture'); process.exit(0); }
if (fs.existsSync('report.md')) fs.writeFileSync('report.md', 'report version two\\n');
else { fs.writeFileSync('report.md', 'report version one\\n'); fs.writeFileSync('log.txt', 'log file\\n'); }
console.log('{"type":"system","subtype":"init","session_id":"session-fixture"}');
`);
  } else {
    await writeFile(executable, `#!/bin/sh
if [ "$1" = "--version" ]; then echo fixture; exit 0; fi
if [ -f report.md ]; then printf 'report version two\\n' > report.md
else printf 'report version one\\n' > report.md; printf 'log file\\n' > log.txt; fi
printf '%s\\n' '{"type":"system","subtype":"init","session_id":"session-fixture"}'
`, { mode: 0o700 });
  }
  const listener = createServer().listen(0, '127.0.0.1');
  await once(listener, 'listening');
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  const token = randomBytes(24).toString('hex');
  const child = spawn(process.execPath, [
    fileURLToPath(new URL('../cli-bridge/host.mjs', import.meta.url)),
    'serve', '--engine', 'claude', '--state', join(fixture, 'state'),
    '--project', project, '--port', String(port), '--executable', executable,
  ], { env: { ...process.env, WORKER_LOCAL_TOKEN: token }, stdio: ['ignore', 'pipe', 'pipe'] });
  const stopped = once(child, 'exit');
  t.after(async () => { child.kill(); await stopped; });
  const ready = await Promise.race([once(child.stdout, 'data'), stopped]);
  assert.match(String(ready[0]), /127\.0\.0\.1/);
  const request = async (path, body) => {
    const response = await fetch('http://127.0.0.1:' + port + '/local/v1' + path, {
      method: body ? 'POST' : 'GET',
      headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    assert.equal(response.status, 200, path);
    return response.json();
  };
  const launched = await request('/tasks', {
    requestKey: 'create-files', projectId: 'workspace', engineId: 'claude', instruction: 'create files',
  });
  const path = '/tasks/' + launched.taskId;
  const completed = async () => {
    for (let i = 0; i < 60; i += 1) {
      const facts = await request(path);
      if (facts.state === 'SUCCEEDED') return facts;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.fail('execution did not finish');
  };
  const first = await completed();
  assert.equal(first.sessionFiles.complete, true);
  const initial = (await request(path + '/artifacts')).items;
  const report = initial.find(row => row.path === 'report.md');
  const log = initial.find(row => row.path === 'log.txt');
  await request(path + '/messages', {
    requestKey: 'update-report', kind: 'NATIVE_FOLLOW_UP', content: 'update report',
    generation: first.generation, nativeSessionId: first.nativeSessionId,
    nativeTurnId: first.nativeTurnId, targetCommandId: first.currentCommandId,
  });
  const second = await completed();
  const refreshed = second.sessionFiles.artifacts;
  assert.equal(refreshed.length, 2);
  assert.notEqual(refreshed.find(row => row.artifactId === report.artifactId).sha256, report.sha256);
  assert.equal(refreshed.find(row => row.artifactId === log.artifactId).sha256, log.sha256);
});
