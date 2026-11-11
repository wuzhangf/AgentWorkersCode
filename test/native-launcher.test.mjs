import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, writeFile, mkdir, chmod, readFile, rm} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';

const script = fileURLToPath(new URL('../Run-NativeWorker.sh', import.meta.url));
const run = (args, env = {}) => spawnSync('/bin/bash', [script, ...args], {env: {...process.env, WORKER_LOCAL_TOKEN: '', ...env}, encoding: 'utf8'});
const mac = {skip: process.platform !== 'darwin'};

test('Mac launcher rejects incomplete and invalid configuration before execution', mac, () => {
  const token = randomBytes(32).toString('hex');
  for (const [args, pattern] of [
    [[], /ENGINE_REQUIRED/],
    [['--engine', 'pi'], /STATE_REQUIRED/],
    [['--engine', 'pi', '--state', '/tmp/unused', '--port', '-1'], /INVALID_PORT/],
    [['--engine', 'pi', '--state', '/tmp/unused', '--port', '65536'], /INVALID_PORT/],
    [['--engine', 'codex', '--state', '/tmp/unused', '--codex-tool-profile', 'other'], /INVALID_CODEX_TOOL_PROFILE/],
    [['--engine', 'pi', '--state', '/tmp/unused', '--pi-execution-profile', 'other'], /INVALID_PI_EXECUTION_PROFILE/],
    [['--engine', 'pi', '--state', '/tmp/unused', '--unknown'], /UNKNOWN_OPTION/],
    [['--engine', 'pi', '--state'], /OPTION_VALUE_REQUIRED/],
  ]) {
    const r = run(args, {WORKER_LOCAL_TOKEN: token});
    assert.notEqual(r.status, 0); assert.match(r.stderr, pattern);
    assert(!r.stdout.includes(token) && !r.stderr.includes(token));
  }
  const missing = run(['--engine', 'pi', '--state', '/tmp/unused']);
  assert.notEqual(missing.status, 0); assert.match(missing.stderr, /WORKER_LOCAL_TOKEN_REQUIRED/);
});

test('Mac launcher preserves argument boundaries, refuses reinit, and execs the selected host', mac, async () => {
  const root = await mkdtemp(join(tmpdir(), 'nwf-launcher-'));
  try {
    const tool = join(root, 'fake host'); const calls = join(root, 'calls.jsonl');
    await writeFile(tool, `#!/usr/bin/env node\nimport('node:fs').then(fs => { fs.appendFileSync(process.env.NWF_TEST_CALLS, JSON.stringify({args:process.argv.slice(2), tokenPresent:!!process.env.WORKER_LOCAL_TOKEN, model:process.env.MODEL_DEFAULT_MODEL})+'\\n'); if(process.argv[2]==='init') fs.writeFileSync(process.argv[process.argv.indexOf('--state')+1]+'/registry.json','{}'); });\n`);
    await chmod(tool, 0o700);
    const state = join(root, '状态 state'); const project = join(root, 'project $(touch INJECTED)');
    await mkdir(state); await mkdir(project);
    const env = {WORKER_LOCAL_TOKEN: randomBytes(32).toString('hex'), NWF_TEST_CALLS: calls};
    const args = ['--engine', 'codex', '--state', state, '--initialize', '--project', project,
      '--project-id', 'fixture', '--model', 'deepseek-v4.1-flash', '--model-base-url', 'http://127.0.0.1:1/v1',
      '--codex-executable', tool, '--codex-host-executable', tool, '--port', '0'];
    const initialized = run(args, env); assert.equal(initialized.status, 0, initialized.stderr);
    const rows = (await readFile(calls, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 2); assert.equal(rows[0].args[0], 'init');
    assert.equal(rows[0].args[rows[0].args.indexOf('--project') + 1], project);
    assert.equal(rows[0].args[rows[0].args.indexOf('--tool-profile') + 1], 'macos-runner-v1');
    assert(rows[0].args.includes('--command-runner'));
    assert.equal(rows[0].tokenPresent, true); assert.equal(rows[1].args[0], 'serve');
    const rejected = run(args, env); assert.notEqual(rejected.status, 0); assert.match(rejected.stderr, /LOCAL_STATE_ALREADY_INITIALIZED/);
    assert.equal((await readFile(calls, 'utf8')).trim().split('\n').length, 2);
    const started = run(['--engine', 'codex', '--state', state, '--codex-host-executable', tool], env);
    assert.equal(started.status, 0, started.stderr);
    assert.equal((await readFile(calls, 'utf8')).trim().split('\n').map(JSON.parse).at(-1).args[0], 'serve');
  } finally { await rm(root, {recursive: true, force: true}); }
});
