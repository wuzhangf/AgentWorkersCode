import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

if (!process.env.MODEL_API_KEY) throw new Error('Set MODEL_API_KEY in the environment before running this test');
const ownerToken = hash('NWF-PI-MODEL-W4B:' + process.env.MODEL_API_KEY);
const root = resolve('.local'); mkdirSync(root, { recursive: true }); const run = mkdtempSync(join(root, 'model-w4b-')); const project = join(run, 'project'); mkdirSync(project);
execFileSync('git', ['init', project], { stdio: 'ignore' }); writeFileSync(join(project, 'README.md'), 'Native Pi W4b smoke');
for (const args of [['add', '.'], ['-c', 'user.name=NWF', '-c', 'user.email=nwf@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', project, ...args], { stdio: 'ignore' });
const mcpFixture = fileURLToPath(new URL('./fixtures/mcp-fixture-server.mjs', import.meta.url));
// Declared resource sources the host copies into the task-managed profile.
const skillSrc = join(run, 'src-skills', 'w4b-skill'); mkdirSync(skillSrc, { recursive: true });
writeFileSync(join(skillSrc, 'SKILL.md'), '---\nname: w4b-skill\ndescription: W4b smoke skill that reveals a marker\n---\nWhen the user asks for the skill marker, the marker is NWF_W4B_SKILL_OK.\n');
const extSrc = join(run, 'src-ext', 'w4b-ext'); mkdirSync(extSrc, { recursive: true });
writeFileSync(join(extSrc, 'index.js'), `export default (pi) => { pi.on('session_start', () => {}); };\n`);
const host = new PiHost({ state: join(run, 'state'), token: ownerToken }); host.register('smoke', project); const port = await host.listen();
async function call(path, body, method) { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + ownerToken, ...(body ? { 'Content-Type': 'application/json', 'Idempotency-Key': body.requestKey ?? '' } : {}) }, body: body ? JSON.stringify(body) : undefined }); const value = await response.json(); if (!response.ok) throw new Error(JSON.stringify(value)); return value; }
async function wait(fn, timeout = 240000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 500)); } throw new Error('MODEL_W4B_TIMEOUT'); }
const guard = d => ({ generation: d.generation, nativeSessionId: d.nativeSessionId, targetCommandId: d.currentCommandId });
const events = async taskId => (await call(`/tasks/${taskId}/events?limit=100`)).items;
const evidence = { schemaVersion: 'nwf-pi-w4b/1', result: 'FAIL', engine: '@earendil-works/pi-coding-agent@1.4.0', node: process.version, platform: process.platform, model: process.env.MODEL_DEFAULT_MODEL, scenarios: {} };
try {
  // Scenario A: declared MCP server + skill + extension bind into the managed session;
  // the model calls the fixture MCP tool and the tool execution is durable evidence.
  const pf = await call('/task-preflights', { engineId: 'sage-pi', projectId: 'smoke' });
  const a = await call('/tasks', { requestKey: 'w4b-mcp-a', engineId: 'sage-pi', projectId: 'smoke', optionsRevision: pf.optionsRevision,
    instruction: 'Call the tool named mcp__fixsrv__workspace_marker once, then reply with exactly the marker text it returns followed by NWF_W4B_DONE. Do not ask questions.',
    mcpServers: [{ id: 'fixsrv', command: process.execPath, args: [mcpFixture], env: { FIXTURE_FLAG: 'literal' } }],
    skills: [{ name: 'w4b-skill', sourcePath: skillSrc }],
    extensions: [{ name: 'w4b-ext', sourcePath: extSrc }] });
  console.log('W4B_A_START ' + JSON.stringify({ taskId: a.taskId, run }));
  let d = await wait(async () => { const d = await call('/tasks/' + a.taskId); if (d.state === 'FAILED') throw new Error('MODEL_FAILED ' + d.reasonCode); return ['SUCCEEDED', 'BUDGET_EXCEEDED'].includes(d.state) && d; });
  const items = await events(a.taskId);
  const bound = items.find(e => e.kind === 'mcp.bound');
  assert.ok(bound, 'mcp.bound fact recorded'); assert.equal(bound.server, 'fixsrv'); assert.ok(bound.toolCount >= 2);
  assert.ok(bound.tools.includes('mcp__fixsrv__workspace_marker'), 'fixture tools observed bound');
  assert.ok(items.some(e => e.kind === 'skill.loaded' && e.name === 'w4b-skill'), 'skill.loaded fact');
  assert.ok(items.some(e => e.kind === 'extension.loaded'), 'extension.loaded fact');
  const task = host.store.task(a.taskId);
  assert.match(task.resourceBindings['skill:w4b-skill'].sha256, /^[0-9a-f]{64}$/);
  assert.match(task.resourceBindings['extension:w4b-ext'].sha256, /^[0-9a-f]{64}$/);
  const toolStart = items.find(e => e.kind === 'TOOL_START' && e.toolName === 'mcp__fixsrv__workspace_marker');
  assert.ok(toolStart, 'model actually invoked the MCP tool');
  const toolEnd = items.find(e => e.kind === 'TOOL_END' && e.toolCallId === toolStart.toolCallId);
  assert.equal(toolEnd.isError, false, 'MCP tool call succeeded');
  assert.ok((await call(`/tasks/${a.taskId}/output`)).text.includes('NWF_W4B_MCP_OK'), 'model reported the real marker text');
  evidence.scenarios.mcpBound = { taskId: a.taskId, server: bound.server, tools: bound.tools, toolCallId: toolStart.toolCallId, markerInOutput: true };
  evidence.scenarios.resourceBindings = { skill: task.resourceBindings['skill:w4b-skill'].sha256, extension: task.resourceBindings['extension:w4b-ext'].sha256 };

  // Scenario B: task-scoped pipe terminal runs real argv in the task workspace, then
  // STOP kills it with Job-drained proof — all host-owned, no model involved.
  const term = await call(`/tasks/${a.taskId}/terminals`, { requestKey: 'w4b-term-1', argv: [process.execPath, '-e', `console.log('NWF_W4B_TERM_OK')`] });
  assert.equal(term.pty, false);
  const tout = await wait(async () => { const o = await call(`/tasks/${a.taskId}/terminals/${term.terminalId}/output?cursor=0&waitMs=3000`); return o.exited && o; }, 20000);
  assert.ok(Buffer.from(tout.data, 'base64').toString('utf8').includes('NWF_W4B_TERM_OK'), 'real terminal output captured');
  const proc = await call(`/tasks/${a.taskId}/terminals`, { requestKey: 'w4b-term-2', argv: [process.execPath, '-e', 'setInterval(()=>{},1000)'] });
  const stop = await call(`/tasks/${a.taskId}/commands`, { requestKey: 'w4b-stop-a', intent: 'STOP', ...guard(d) });
  assert.equal(stop.state, 'APPLIED');
  const stopped = (await events(a.taskId)).find(e => e.kind === 'TERMINALS_STOPPED');
  assert.ok(stopped, 'STOP drained owned terminals'); assert.ok(stopped.terminals.some(t => t.terminalId === proc.terminalId));
  for (const p of stopped.terminals) assert.equal(p.activeProcesses, 0);
  assert.equal(host.store.task(a.taskId).terminals[proc.terminalId].state, 'CLOSED');
  evidence.scenarios.terminals = { created: 2, pty: false, markerCaptured: true, stopProof: stopped.terminals.map(t => ({ method: t.method, activeProcesses: t.activeProcesses })) };
  evidence.taskIds = [a.taskId]; evidence.storeId = host.store.identity.storeId; evidence.result = 'PASS';
  writeFileSync(join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  mkdirSync(resolve('test/evidence'), { recursive: true }); writeFileSync(resolve('test/evidence/windows-deepseek-w4b.json'), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(evidence));
} finally { await host.close(); }
