// Real host process + real pinned app-server + real model (where noted).
// W4b slice coverage: task-scoped native command/exec terminals (streamed
// output, streamed stdin, native ConPTY, terminate, STOP job-kill proof with
// an externally observed heartbeat, HOST_RESTART -> LOST), task-bound stdio
// MCP servers with a real model tool call, staged CODEX_HOME skills with
// sha256 binding facts, pre-trusted command hooks that provably ran with the
// model credential scrubbed, plus intake refusals and capability records.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, randomUUID, createHash} from 'node:crypto';
import {mkdtemp, mkdir, readFile, writeFile, rm, readdir, stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';

const [binaryArg, templateArg] = process.argv.slice(2);
assert(binaryArg && templateArg, 'Usage: node tests/w4b.mjs <host executable> <initialized state template>');
const binary = resolve(binaryArg), template = resolve(templateArg);
assert(process.env.MODEL_API_KEY, 'REAL_MODEL_BINDING_REQUIRED');
const here = fileURLToPath(new URL('.', import.meta.url));
const root = await mkdtemp(join(tmpdir(), 'nwf-codex-w4b-'));
assert(root.startsWith(join(tmpdir(), 'nwf-codex-w4b-')), 'TEST_CLEANUP_SCOPE');
const token = randomBytes(32).toString('hex');
let live;
const exited = child => new Promise(r => child.once('exit', (code, signal) => r({code, signal})));
async function start(maxActive = '1') {
  const child = spawn(binary, ['serve', '--state', root, '--port', '0'], {env: {...process.env, WORKER_LOCAL_TOKEN: token, NWF_CODEX_MAX_ACTIVE_TASKS: maxActive}, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']});
  let output = '', diagnostic = '';
  child.stdout.on('data', b => output += b);
  child.stderr.on('data', b => diagnostic += b);
  const until = Date.now() + 30000;
  while (Date.now() < until) {
    const m = /LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output);
    if (m) { live = child; return m[1]; }
    if (child.exitCode !== null) throw new Error('HOST_START_FAILED ' + diagnostic.trim());
    await delay(25);
  }
  child.kill();
  throw new Error('HOST_START_TIMEOUT ' + diagnostic.trim());
}
async function request(base, method, tail, body, extra = {}) {
  const r = await fetch(base + tail, {method, headers: {authorization: 'Bearer ' + token, 'content-type': 'application/json', ...extra}, body: body === undefined ? undefined : JSON.stringify(body)});
  return {status: r.status, body: await r.json()};
}
const binding = d => ({generation: d.generation, nativeSessionId: d.nativeSessionId, nativeTurnId: d.nativeTurnId, targetCommandId: d.currentCommandId});
async function detail(base, taskId) { return (await request(base, 'GET', '/local/v1/tasks/' + taskId)).body; }
async function allEvents(base, taskId) {
  let cursor = 0, items = [];
  for (;;) {
    const p = await request(base, 'GET', '/local/v1/tasks/' + taskId + '/events?limit=100&cursor=' + cursor);
    const batch = p.body.items ?? [];
    items.push(...batch);
    if (batch.length < 100) return items;
    cursor = p.body.cursor ?? batch[batch.length - 1].cursor;
  }
}
const answered = new Set();
async function settle(base, taskId, d) {
  const questions = await request(base, 'GET', '/local/v1/tasks/' + taskId + '/input-requests');
  for (const q of questions.body.items ?? []) {
    if (q.state !== 'PENDING' || answered.has(q.inputRequestId)) continue;
    const response = (q.options ?? []).includes('accept') ? {decision: 'accept'} : {value: 'Proceed with the original instruction'};
    const answer = {requestKey: 'nwf-answer-' + q.inputRequestId, response, generation: q.generation, nativeSessionId: q.nativeSessionId, nativeTurnId: q.nativeTurnId, targetCommandId: q.targetCommandId};
    const sent = await request(base, 'POST', '/local/v1/tasks/' + taskId + '/input-requests/' + q.inputRequestId + '/responses', answer, {'Idempotency-Key': answer.requestKey});
    assert.equal(sent.status, 202);
    answered.add(q.inputRequestId);
  }
}
async function waitTask(base, taskId, states, ms) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    const d = await detail(base, taskId);
    if (states.includes(d.state) && (!['CANCELLED', 'BUDGET_EXCEEDED'].includes(d.state) || d.quiescent === true)) return d;
    assert(!['FAILED', 'REJECTED'].includes(d.state), 'NATIVE_TASK_FAILED ' + d.state + ' ' + (d.reasonCode ?? ''));
    await settle(base, taskId, d);
    await delay(500);
  }
  const last = await detail(base, taskId);
  const ev = (await request(base, 'GET', '/local/v1/tasks/' + taskId + '/events?limit=50')).body.items.map(e => e.type);
  throw new Error('NATIVE_DEADLINE_EXCEEDED ' + states.join('/') + ' last=' + JSON.stringify({state: last.state, reasonCode: last.reasonCode, launchState: last.launchState}) + ' events=' + ev.join(','));
}
async function termOutput(base, taskId, tid, cursor) {
  const r = await request(base, 'GET', `/local/v1/tasks/${taskId}/terminals/${tid}/output?cursor=${cursor}`);
  assert.equal(r.status, 200, JSON.stringify(r.body));
  return r.body;
}
async function waitOutput(base, taskId, tid, needle, ms) {
  const until = Date.now() + ms;
  let cursor = '0', text = '';
  while (Date.now() < until) {
    const out = await termOutput(base, taskId, tid, cursor);
    cursor = out.nextCursor;
    text += Buffer.from(out.dataBase64, 'base64').toString('utf8');
    if (text.includes(needle)) return {text, cursor, state: out.state};
    await delay(300);
  }
  throw new Error('TERMINAL_OUTPUT_DEADLINE ' + tid + ' wanted=' + needle + ' got=' + JSON.stringify(text.slice(-400)));
}
let checks = 0;
try {
  await mkdir(join(root, 'profile'));
  await writeFile(join(root, 'registry.json'), await readFile(join(template, 'registry.json')));
  await writeFile(join(root, 'profile', 'config.toml'), await readFile(join(template, 'profile', 'config.toml')));
  const registry = JSON.parse(await readFile(join(root, 'registry.json'), 'utf8'));
  const base = await start('1');
  const checked = (await request(base, 'POST', '/local/v1/task-preflights', {projectId: registry.project_id, engineId: 'codex'})).body;
  const mk = (requestKey, extra = {}) => ({requestKey, projectId: registry.project_id, engineId: 'codex', optionsRevision: checked.optionsRevision, instruction: 'placeholder', ...extra});
  // Capability surface: W4b records are IMPLEMENTED; strict pause stays GAP.
  const caps = (await request(base, 'GET', '/worker-host/v1/capabilities')).body.items;
  const cap = id => caps.find(c => c.id === id);
  assert.equal(cap('execution.terminal').implementation, 'IMPLEMENTED');
  assert.equal(cap('execution.mcpServers').implementation, 'IMPLEMENTED');
  assert.equal(cap('execution.extensions').implementation, 'IMPLEMENTED');
  assert.equal(cap('execution.pause').implementation, 'GAP');
  assert.equal(cap('execution.pause').reasonCode, 'NATIVE_BOUNDARY_GATE_ABSENT');
  checks++;
  // Intake refusals: malformed W4b fields fail before any task binds.
  for (const [key, patch, code] of [
    ['w4b-mcp-secret-name', {mcpServers: [{id: 'x', command: 'node', args: [], env: {API_TOKEN: 'v'}}]}, 'ENV_ALLOWLIST_DENIED'],
    ['w4b-mcp-secret-value', {mcpServers: [{id: 'x', command: 'node', args: [], env: {NWF_MARKER: 'pre ' + process.env.MODEL_API_KEY}}]}, 'MCP_ENV_SECRET_VALUE'],
    ['w4b-mcp-bad-id', {mcpServers: [{id: '9x', command: 'node'}]}, 'INVALID_ID'],
    ['w4b-hook-bad-event', {hooks: [{event: 'subagentStart', command: 'echo x'}]}, 'INVALID_HOOK_EVENT'],
    ['w4b-hook-extra', {hooks: [{event: 'sessionStart', command: 'x', payload: 'y'}]}, 'UNEXPECTED_FIELD'],
    ['w4b-skill-missing', {skills: [{name: 'x', sourcePath: join(root, 'no-skill-dir')}]}, 'SKILL_MANIFEST_MISSING'],
    ['w4b-skill-relative', {skills: [{name: 'x', sourcePath: 'relative/dir'}]}, 'INVALID_REQUEST'],
  ]) {
    const r = await request(base, 'POST', '/local/v1/tasks', mk(key, {instruction: 'x', ...patch}), {'Idempotency-Key': key});
    assert.equal(r.status, 409, key + ' ' + JSON.stringify(r.body));
    assert.equal(r.body.code, code, key);
    checks++;
  }
  // T1: task with MCP server, staged skill and two command hooks. The model is
  // asked to make a real MCP tool call so mcpToolCall settles through the
  // durable effect inventory.
  const mcpMarker = 'mcp-bound-' + randomUUID().slice(0, 8);
  const callText = 'mcp-real-call-' + randomUUID().slice(0, 8);
  const hookCmd = '(if defined MODEL_API_KEY (echo KEY-LEAKED) else (echo NWF-SESSION-HOOK-RAN)) > nwf-session-hook.log';
  const promptHookCmd = '(if defined MODEL_API_KEY (echo KEY-LEAKED) else (echo NWF-PROMPT-HOOK-RAN)) > nwf-prompt-hook.log';
  const t1key = 'w4b-ext-' + randomUUID().slice(0, 8);
  const t1 = await request(base, 'POST', '/local/v1/tasks', mk(t1key, {
    instruction: `Call the MCP tool "nwf_env_echo" on the MCP server "nwffake" with argument text="${callText}". Then reply with the exact text the tool returned.`,
    mcpServers: [{id: 'nwffake', command: process.execPath, args: [join(here, 'fixtures', 'fake-mcp.mjs')], env: {NWF_MCP_MARKER: mcpMarker}}],
    skills: [{name: 'nwf-w4b-skill', sourcePath: join(here, 'fixtures', 'skill-src')}],
    hooks: [
      {event: 'sessionStart', command: hookCmd, timeoutSec: '15'},
      {event: 'userPromptSubmit', command: promptHookCmd, timeoutSec: '15'},
    ],
  }), {'Idempotency-Key': t1key});
  assert.equal(t1.status, 202, JSON.stringify(t1.body));
  const t1Id = t1.body.taskId;
  const t1Done = await waitTask(base, t1Id, ['SUCCEEDED'], 300000);
  checks++;
  // MCP binding verified against the engine's own status surface.
  const mcpRow = (t1Done.mcpStatus ?? []).find(r => r.id === 'nwffake');
  assert(mcpRow, 'MCP_STATUS_MISSING');
  assert.equal(mcpRow.status, 'BOUND');
  assert.equal(mcpRow.runtimeStatus, 'connected');
  assert(mcpRow.tools.some(n => String(n).includes('nwf_env_echo')), 'MCP_TOOL_CATALOG_MISSING ' + JSON.stringify(mcpRow));
  // Skill staging verified via skills/list at user scope with sha256 facts.
  const skillRow = (t1Done.skillStatus ?? []).find(r => r.name === 'nwf-w4b-skill');
  assert(skillRow, 'SKILL_STATUS_MISSING');
  assert.equal(skillRow.status, 'BOUND');
  assert.equal(skillRow.scope, 'user');
  const mdSha = createHash('sha256').update(await readFile(join(here, 'fixtures', 'skill-src', 'SKILL.md'))).digest('hex');
  const mdRow = (skillRow.files ?? []).find(f => f.path === 'SKILL.md');
  assert(mdRow, 'SKILL_MANIFEST_FACT_MISSING');
  assert.equal(mdRow.sha256, mdSha);
  // Hook binding verified against hooks/list with offline trust.
  const hookRows = t1Done.hookStatus ?? [];
  const sessHook = hookRows.find(r => r.event === 'sessionStart');
  const promptHook = hookRows.find(r => r.event === 'userPromptSubmit');
  assert(sessHook && promptHook, 'HOOK_STATUS_MISSING ' + JSON.stringify(hookRows));
  assert.equal(sessHook.status, 'BOUND', JSON.stringify(hookRows));
  assert.equal(promptHook.status, 'BOUND', JSON.stringify(hookRows));
  assert.equal(sessHook.trustStatus, 'trusted');
  checks++;
  // Durable binding + lifecycle facts.
  const ev1 = await allEvents(base, t1Id);
  for (const kind of ['mcp.bound', 'skill.bound', 'hook.bound']) {
    assert(ev1.some(e => e.type === kind), 'BINDING_EVENT_MISSING ' + kind);
  }
  const mcpTool = ev1.find(e => e.type === 'tool.completed' && e.payload && e.payload.type === 'mcpToolCall');
  assert(mcpTool, 'MCP_TOOL_CALL_FACT_MISSING ' + JSON.stringify(ev1.filter(e => e.type === 'tool.completed')));
  assert.equal(mcpTool.payload.server, 'nwffake');
  assert.equal(mcpTool.payload.tool, 'nwf_env_echo');
  checks++;
  // The model's real tool call is visible in the task output.
  assert(t1Done.output.includes('ECHO:' + callText), 'MCP_CALL_OUTPUT_MISSING ' + t1Done.output.slice(-400));
  assert(t1Done.output.includes('MARKER=' + mcpMarker), 'MCP_ENV_MARKER_MISSING ' + t1Done.output.slice(-400));
  assert(t1Done.output.includes('KEY=ABSENT'), 'MCP_CHILD_INHERITED_MODEL_KEY ' + t1Done.output.slice(-400));
  checks++;
  // Command hooks actually ran (files in the task workspace) and saw no key.
  const ws = t1Done.workspace;
  const sessLog = await readFile(join(ws, 'nwf-session-hook.log'), 'utf8').catch(() => '');
  const promptLog = await readFile(join(ws, 'nwf-prompt-hook.log'), 'utf8').catch(() => '');
  assert(sessLog.includes('NWF-SESSION-HOOK-RAN'), 'SESSION_HOOK_DID_NOT_RUN ' + sessLog);
  assert(promptLog.includes('NWF-PROMPT-HOOK-RAN'), 'PROMPT_HOOK_DID_NOT_RUN ' + promptLog);
  assert(!sessLog.includes('KEY-LEAKED') && !promptLog.includes('KEY-LEAKED'), 'HOOK_SAW_MODEL_KEY');
  const hookEvents = ev1.filter(e => e.type === 'hook.started' || e.type === 'hook.completed');
  assert(hookEvents.length >= 1, 'HOOK_LIFECYCLE_EVENTS_MISSING');
  checks++;
  // Piped terminal: buffered + streamed output, stdin write, terminate.
  const tmk = async (taskId, body) => {
    const r = await request(base, 'POST', `/local/v1/tasks/${taskId}/terminals`, body);
    assert.equal(r.status, 200, JSON.stringify(r.body));
    return r.body;
  };
  const t1mk = body => tmk(t1Id, body);
  const term1 = await t1mk({argv: ['powershell.exe', '-NoProfile', '-Command', 'Write-Output W4B-TERM-READY; $x = [Console]::In.ReadLine(); Write-Output ("GOT-" + $x); Start-Sleep -Seconds 60'], tty: false});
  assert.equal(term1.pty, false);
  assert.equal(term1.state, 'OPEN');
  const tid1 = term1.terminalId;
  await waitOutput(base, t1Id, tid1, 'W4B-TERM-READY', 20000);
  // Input bytes are raw: the child sees exactly what is written, so the
  // line-oriented ReadLine needs an explicit CRLF terminator.
  const input = await request(base, 'POST', `/local/v1/tasks/${t1Id}/terminals/${tid1}/input`, {text: 'w4b-input-7\r\n'});
  assert.equal(input.status, 200, JSON.stringify(input.body));
  const echoed = await waitOutput(base, t1Id, tid1, 'GOT-w4b-input-7', 20000);
  checks++;
  // Cursor continuity: bytes already read are not replayed.
  const again = await termOutput(base, t1Id, tid1, echoed.cursor);
  assert.equal(Buffer.from(again.dataBase64, 'base64').toString('utf8').includes('W4B-TERM-READY'), false);
  // Bad signal refused; good signal settles the session through the deferred
  // native exit fact.
  const badSig = await request(base, 'POST', `/local/v1/tasks/${t1Id}/terminals/${tid1}/signal`, {signal: 'SIGKILL'});
  assert.equal(badSig.status, 409);
  assert.equal(badSig.body.code, 'TERMINAL_SIGNAL_UNSUPPORTED');
  const sig = await request(base, 'POST', `/local/v1/tasks/${t1Id}/terminals/${tid1}/signal`, {signal: 'terminate'});
  assert.equal(sig.status, 200, JSON.stringify(sig.body));
  const until1 = Date.now() + 20000;
  for (;;) {
    const row = (await detail(base, t1Id)).terminals.find(r => r.terminalId === tid1);
    if (row && row.state !== 'OPEN') { assert(['EXITED', 'TERMINATED'].includes(row.state), 'SIGNALLED_STATE ' + row.state); assert(row.exited); break; }
    assert(Date.now() < until1, 'SIGNALLED_SESSION_STILL_OPEN');
    await delay(300);
  }
  checks++;
  // Native ConPTY: tty:true yields real pseudo-terminal escape output.
  const term2 = await t1mk({argv: ['cmd.exe', '/d', '/c', 'echo PTY-W4B & exit'], tty: true});
  assert.equal(term2.pty, true);
  const ptyOut = await waitOutput(base, t1Id, term2.terminalId, 'PTY-W4B', 20000);
  assert(ptyOut.text.includes('\u001b'), 'PTY_ESCAPE_OUTPUT_MISSING ' + JSON.stringify(ptyOut.text.slice(0, 200)));
  checks++;
  // DELETE settles an open session.
  const term3 = await t1mk({argv: ['cmd.exe', '/d', '/c', 'ping -n 60 127.0.0.1 > nul']});
  const del = await request(base, 'DELETE', `/local/v1/tasks/${t1Id}/terminals/${term3.terminalId}`);
  assert.equal(del.status, 200);
  assert(['TERMINATED', 'EXITED'].includes(del.body.state), 'DELETE_STATE ' + del.body.state);
  checks++;
  // STOP job-kill proof: a heartbeat-writing terminal on T1 stops producing
  // output bytes once the managed tree is terminated — externally verifiable.
  const heart = await t1mk({argv: ['powershell.exe', '-NoProfile', '-Command', 'while ($true) { Write-Output "HB" ; Start-Sleep -Milliseconds 300 }']});
  const heartId = heart.terminalId;
  await waitOutput(base, t1Id, heartId, 'HB', 20000);
  const stopReq = {requestKey: 'nwf-w4b-stop-' + randomUUID(), intent: 'STOP', ...binding(t1Done)};
  const stopRes = await request(base, 'POST', '/local/v1/tasks/' + t1Id + '/commands', stopReq, {'Idempotency-Key': stopReq.requestKey});
  assert.equal(stopRes.status, 202, JSON.stringify(stopRes.body));
  const stopped = await waitTask(base, t1Id, ['CANCELLED'], 90000);
  assert(stopped.stopProof.terminals.includes(heartId), 'STOP_PROOF_TERMINAL_MISSING ' + JSON.stringify(stopped.stopProof));
  const heartRow = stopped.terminals.find(r => r.terminalId === heartId);
  assert.equal(heartRow.state, 'TERMINATED');
  assert.equal(heartRow.reason, 'STOP_JOB_TERMINATED');
  // No new output bytes can arrive from a killed process tree.
  const a = await termOutput(base, t1Id, heartId, '0');
  await delay(1200);
  const b = await termOutput(base, t1Id, heartId, '0');
  await delay(1200);
  const c = await termOutput(base, t1Id, heartId, '0');
  assert.equal(b.nextCursor, c.nextCursor, 'HEARTBEAT_STILL_PRODUCING ' + b.nextCursor + '->' + c.nextCursor);
  checks++;
  // Audit carries the terminal records; diagnostics redacts argv.
  const audit = (await request(base, 'GET', '/worker-host/v1/tasks/' + t1Id + '/audit')).body;
  assert(audit.items.some(i => i.kind === 'terminal'), 'AUDIT_TERMINAL_KIND_MISSING');
  const diag = (await request(base, 'GET', '/worker-host/v1/tasks/' + t1Id + '/diagnostics?redact=content')).body;
  const diagText = JSON.stringify(diag);
  assert(!diagText.includes('Start-Sleep -Seconds 60'), 'DIAGNOSTICS_LEAKED_ARGV');
  checks++;
  // T2: a second task with an OPEN terminal; host restart marks it LOST.
  const t2key = 'w4b-lost-' + randomUUID().slice(0, 8);
  const t2 = await request(base, 'POST', '/local/v1/tasks', mk(t2key, {instruction: 'Reply with exactly: W4B-T2-OK'}), {'Idempotency-Key': t2key});
  assert.equal(t2.status, 202);
  const t2Id = t2.body.taskId;
  const t2Done = await waitTask(base, t2Id, ['SUCCEEDED'], 300000);
  checks++;
  const t2term = await tmk(t2Id, {argv: ['cmd.exe', '/d', '/c', 'ping -n 60 127.0.0.1 > nul']});
  assert.equal(t2term.state, 'OPEN');
  {const dead = exited(live); live.kill(); await dead; live = null;}
  const base2 = await start('1');
  const t2After = await detail(base2, t2Id);
  assert.equal(t2After.state, 'UNKNOWN');
  const lostRow = t2After.terminals.find(r => r.terminalId === t2term.terminalId);
  assert(lostRow, 'TERMINAL_ROW_MISSING_AFTER_RESTART');
  assert.equal(lostRow.state, 'LOST');
  assert.equal(lostRow.reason, 'HOST_RESTART');
  // A restarted host cannot write to a session whose transport is gone.
  const lostInput = await request(base2, 'POST', `/local/v1/tasks/${t2Id}/terminals/${t2term.terminalId}/input`, {text: 'x'});
  assert.equal(lostInput.status, 409);
  const lostOutput = await termOutput(base2, t2Id, t2term.terminalId, '0');
  assert.equal(lostOutput.state, 'LOST');
  checks++;
  // Terminal persistence is durable (schema v5 carries the W5a tables too)
  // with the row visible to SQL.
  const db = new DatabaseSync(join(root, 'worker.sqlite'), {readOnly: true});
  const trow = db.prepare('SELECT state,reason FROM terminals WHERE task=? AND id=?').get(t2Id, t2term.terminalId);
  const version = db.prepare('PRAGMA user_version').get().user_version;
  db.close();
  assert.equal(version, 5);
  assert.equal(trow.state, 'LOST');
  assert.equal(trow.reason, 'HOST_RESTART');
  checks++;
  console.log(JSON.stringify({result: 'PASS', checks, scope: 'W4b intake refusals, mcp.bound+real model MCP call with env marker and credential isolation, skill.bound with sha256 facts, trusted command hooks ran without model key, piped+ConPTY terminals with stdin/out/signal/delete, STOP job-kill heartbeat proof, HOST_RESTART LOST recovery, audit/diagnostics'}));
} finally {
  if (live) { let done = exited(live); live.kill(); await done; }
  for (let attempt = 0; attempt < 40; attempt++) {
    try { await rm(root, {recursive: true, force: true}); break; }
    catch (e) { if (attempt === 39) console.error('CLEANUP_INCOMPLETE ' + e.message); else await delay(500); }
  }
}
