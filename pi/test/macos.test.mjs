import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { MacOwnership } from '../dist/control/macos-ownership.js';

const alive = pid => { try { process.kill(pid, 0); return true; } catch { return false; } };
for (const entry of ['bootstrap', 'git-child']) test(`macOS host death before guardian arm leaves no gated ${entry}`, { skip: process.platform !== 'darwin' }, async () => {
  const module = fileURLToPath(new URL(`../dist/engine/${entry}.js`, import.meta.url)); let pid;
  const code = `const {fork}=require('node:child_process');const c=fork(${JSON.stringify(module)},[],{detached:true,stdio:['ignore','ignore','ignore','ipc']});console.log(c.pid);setInterval(()=>{},60000);`;
  const parent = spawn(process.execPath, ['-e', code], { stdio: ['ignore', 'pipe', 'ignore'] });
  try {
    pid = await new Promise((done, reject) => { parent.stdout.once('data', data => done(Number(String(data).trim()))); parent.once('error', reject); });
    assert(pid > 0); const exited = new Promise(done => parent.once('exit', done)); parent.kill('SIGKILL'); await exited;
    for (let n = 0; n < 200 && alive(pid); n++) await new Promise(r => setTimeout(r, 10));
    assert.equal(alive(pid), false, 'no config was delivered, so disconnect must self-terminate before any guardian exists');
  } finally { if (parent.exitCode === null && parent.signalCode === null) parent.kill('SIGKILL'); if (pid && alive(pid)) process.kill(pid, 'SIGKILL'); }
});

test('macOS failed kqueue registration rejects arm and closes its guardian', { skip: process.platform !== 'darwin' }, async () => {
  await assert.rejects(() => MacOwnership.arm(2147483647), /MAC_OBSERVATION_UNAVAILABLE|MAC_GUARDIAN_UNAVAILABLE/);
});
