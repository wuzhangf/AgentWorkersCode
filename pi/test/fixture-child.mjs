import { writeFileSync, appendFileSync, existsSync, rmSync, mkdirSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { spawn } from 'node:child_process';
let config;
let pendingQuestion = null;
let inFlight = null; // toolCallId of a tool that has started but not ended
let held = false;
let running = null; // content of the prompt still in flight
const pauseWaiters = [];
const checks = new Map(); let checkSeq = 0;
const send = m => process.connected && process.send(m);
const gateCheck = (kind, fields = {}) => new Promise(done => { const checkId = 'fchk-' + (++checkSeq); checks.set(checkId, done); send({ type: 'gate_check', checkId, kind, ...fields }); });
const quiet = () => inFlight ? null : pendingQuestion ? 'INPUT_REQUEST' : running ? 'TURN_BOUNDARY' : 'IDLE';
function tryQuiet() {
  const boundary = quiet(); if (!boundary) return;
  for (const requestKey of pauseWaiters.splice(0)) send({ type: 'paused', requestKey, boundary, idle: !running, pendingQuestions: pendingQuestion ? [pendingQuestion] : [], pendingToolCallIds: inFlight ? [inFlight] : [] });
}
const finish = requestKey => { running = null; send({ type: 'settled', requestKey }); };
let turnSeq = 0;
// Mirrors the real child: SDK text_delta updates stream ahead of the
// message_end assistant text. Two deterministic chunks per message keep the
// durable OUTPUT_DELTA spine verifiable without timing-sensitive token splits.
const assistant = (text, extra = {}) => {
  if (typeof text === 'string' && text) {
    const nativeTurnId = 'fixture-turn-' + (++turnSeq); const mid = Math.ceil(text.length / 2);
    for (const delta of [text.slice(0, mid), text.slice(mid)]) if (delta) send({ type: 'assistant_delta', delta, contentIndex: 0, nativeTurnId });
  }
  send({ type: 'assistant', text, ...extra });
};
process.on('disconnect', () => { if (process.platform !== 'darwin' || !config) process.exit(0); });
process.on('message', command => { void handle(command).catch(() => send({ type: 'failure', requestKey: command.requestKey, code: 'PI_EXECUTION_FAILED' })); });
async function handle(command) {
  if (command.type === 'init') { config = command; if (process.platform === 'darwin') setInterval(() => {}, 60000); setTimeout(() => { const sessionFile = command.sessionFile ?? join(command.sessionDir, 'fixture-session.json'); if (!existsSync(sessionFile)) writeFileSync(sessionFile, 'fixture session\n'); send({ type: 'ready', nativeSessionId: 'fixture-pi-session', sessionFile, tools: ['write', 'ask_user'], envProbe: Object.fromEntries((command.probeEnv ?? []).map(k => [k, process.env[k] !== undefined])) }); }, 40); return; }
  if (command.type === 'gate_verdict') { const done = checks.get(command.checkId); checks.delete(command.checkId); done?.(command); return; }
  if (command.type === 'pause') { held = true; pauseWaiters.push(command.requestKey); tryQuiet(); return; }
  if (command.type === 'resume') {
    held = false;
    for (const requestKey of pauseWaiters.splice(0)) send({ type: 'pause_failed', requestKey, code: 'RESUME_SUPERSEDED' });
    send({ type: 'resumed', requestKey: command.requestKey, idle: !running });
    if (running && !pendingQuestion) { if (inFlight) { const id = inFlight; inFlight = null; send({ type: 'tool_end', toolName: 'bash', toolCallId: id, isError: false }); } running = null; assistant('resumed ' + command.requestKey, { usage: { input: 1, output: 1 } }); send({ type: 'settled', requestKey: command.requestKey }); }
    return;
  }
  if (command.type === 'prompt') return prompt(command);
  if (command.type === 'steer') { send({ type: 'queued', requestKey: command.requestKey, disposition: 'queued' }); send({ type: 'user_consumed', text: command.content }); return; }
  if (command.type === 'answer') { pendingQuestion = null; running = null; send({ type: 'question_consumed', inputRequestId: command.inputRequestId }); send({ type: 'answered', requestKey: command.requestKey }); if (held) { appendFileSync(config.sessionFile ?? join(config.sessionDir, 'fixture-session.json'), 'answered while held\n'); send({ type: 'gate_held', kind: 'PROVIDER_REQUEST' }); } assistant('answered ' + JSON.stringify(command.response)); send({ type: 'settled' }); return; }
}
async function prompt(command) {
  running = command.content;
  // The dispatch marker lands in the session dir for collab children so the
  // writable checkout's candidate diff only carries real child output.
  appendFileSync(join(command.collab ? config.sessionDir : config.workspace, 'dispatch-count.txt'), command.requestKey + '\n');
  const sessionFile = config.sessionFile ?? join(config.sessionDir, 'fixture-session.json'); appendFileSync(sessionFile, command.requestKey + '\n');
  const verdict = await gateCheck('PROVIDER_REQUEST');
  if (verdict.action !== 'allow') { finish(command.requestKey); return; }
  send({ type: 'started', requestKey: command.requestKey }); send({ type: 'user_consumed', requestKey: command.requestKey, text: command.content });
  if (command.aux) {
    const q = command.content.match(/<<QUESTION>>\n([\s\S]*?)\n<<\/QUESTION>>/)?.[1] ?? '';
    const m = command.content.match(/<<FIXED_MATERIAL>>\n([\s\S]*?)\n<<\/FIXED_MATERIAL>>/)?.[1] ?? '';
    if (/aux-hang/.test(q)) { inFlight = 'tool-aux-1'; send({ type: 'tool_start', toolName: 'bash', toolCallId: 'tool-aux-1' }); return; }
    writeFileSync(join(config.workspace, 'aux-result.txt'), 'aux wrote in its own snapshot');
    assistant('aux-answer: ' + q.trim() + ' | material: ' + m.trim().slice(0, 300), { usage: { input: 2, output: 3 } });
    finish(command.requestKey); return;
  }
  if (command.collab) {
    // Writable collaboration child: writes land in its own isolated checkout.
    const g = command.content.match(/<<GOAL>>\n([\s\S]*?)\n<<\/GOAL>>/)?.[1] ?? '';
    if (/child-hang/.test(g)) { inFlight = 'tool-collab-hang'; send({ type: 'tool_start', toolName: 'bash', toolCallId: 'tool-collab-hang' }); return; }
    if (/child-fail/.test(g)) { assistant('collab child failing on request', { usage: { input: 1, output: 1 }, failed: true, errorCode: 'MODEL_REQUEST_FAILED' }); finish(command.requestKey); return; }
    let denied = false;
    for (const [i, target] of [...g.matchAll(/child-write\s+(\S+)/g)].map(m => m[1]).entries()) {
      const id = 'tool-cw-' + i; inFlight = id; send({ type: 'tool_start', toolName: 'write', toolCallId: id });
      const admission = await gateCheck('TOOL_ADMISSION', { toolName: 'write', toolCallId: id, input: { path: target } });
      inFlight = null;
      if (admission.action === 'allow') { const abs = resolve(config.workspace, target); mkdirSync(dirname(abs), { recursive: true }); writeFileSync(abs, 'collab write ' + target); send({ type: 'tool_end', toolName: 'write', toolCallId: id, isError: false }); }
      else { denied = true; send({ type: 'tool_end', toolName: 'write', toolCallId: id, isError: true }); }
    }
    for (const [i, target] of [...g.matchAll(/child-delete\s+(\S+)/g)].map(m => m[1]).entries()) {
      const id = 'tool-cd-' + i; inFlight = id; send({ type: 'tool_start', toolName: 'write', toolCallId: id });
      const admission = await gateCheck('TOOL_ADMISSION', { toolName: 'write', toolCallId: id, input: { path: target } });
      inFlight = null;
      if (admission.action === 'allow') { rmSync(resolve(config.workspace, target), { force: true }); send({ type: 'tool_end', toolName: 'write', toolCallId: id, isError: false }); }
      else { denied = true; send({ type: 'tool_end', toolName: 'write', toolCallId: id, isError: true }); }
    }
    assistant('collab child done' + (denied ? ' (a write was blocked)' : ''), { usage: { input: 2, output: 2 } });
    finish(command.requestKey); return;
  }
  if (command.content === 'question') { pendingQuestion = 'q1'; send({ type: 'question', inputRequestId: 'q1', method: 'select', title: 'Choose', message: '', options: ['yes', 'no'] }); return; }
  if (command.content === 'budget-multi') {
    assistant('first segment', { usage: { input: 3, output: 2 } });
    const second = await gateCheck('PROVIDER_REQUEST');
    if (second.action !== 'allow') { finish(command.requestKey); return; }
    assistant('second segment', { usage: { input: 3, output: 2 } });
    finish(command.requestKey); return;
  }
  if (command.content === 'budget-tool') {
    assistant('first segment', { usage: { input: 3, output: 2 } });
    inFlight = 'tool-b1'; send({ type: 'tool_start', toolName: 'write', toolCallId: 'tool-b1' });
    const admission = await gateCheck('TOOL_ADMISSION', { toolName: 'write', toolCallId: 'tool-b1', input: { path: 'budget-tool.txt' } });
    inFlight = null;
    send({ type: 'tool_end', toolName: 'write', toolCallId: 'tool-b1', isError: admission.action !== 'allow' });
    finish(command.requestKey); return;
  }
  if (command.content.startsWith('writefile ')) {
    const target = command.content.slice('writefile '.length);
    inFlight = 'tool-w1'; send({ type: 'tool_start', toolName: 'write', toolCallId: 'tool-w1' });
    const admission = await gateCheck('TOOL_ADMISSION', { toolName: 'write', toolCallId: 'tool-w1', input: { path: target } });
    inFlight = null;
    if (admission.action === 'allow') { writeFileSync(resolve(config.workspace, target), 'fixture write'); send({ type: 'tool_end', toolName: 'write', toolCallId: 'tool-w1', isError: false }); }
    else send({ type: 'tool_end', toolName: 'write', toolCallId: 'tool-w1', isError: true });
    assistant(admission.action === 'allow' ? 'wrote ' + target : 'blocked ' + target, { usage: { input: 1, output: 1 } });
    finish(command.requestKey); return;
  }
  if (command.content === 'waiting') { inFlight = 'tool-wait-1'; send({ type: 'tool_start', toolName: 'write', toolCallId: 'tool-wait-1' }); setTimeout(() => { if (inFlight === 'tool-wait-1') { inFlight = null; send({ type: 'tool_end', toolName: 'write', toolCallId: 'tool-wait-1', isError: false }); tryQuiet(); } }, 250); return; }
  if (command.content === 'waiting-never') { inFlight = 'tool-stuck-1'; send({ type: 'tool_start', toolName: 'bash', toolCallId: 'tool-stuck-1' }); return; }
  if (command.content === 'fork-denied') {
    let probe;
    try { probe = spawn(process.execPath, ['-e', 'require("fs").writeFileSync("fork-must-not-exist.txt","spawned")'], { stdio: 'ignore' }); } catch (error) { assistant('fork denied ' + error.code); finish(command.requestKey); return; }
    probe.once('error', error => { assistant('fork denied ' + error.code); finish(command.requestKey); });
    probe.once('exit', () => { assistant('fork unexpectedly succeeded'); finish(command.requestKey); }); return;
  }
  if (['descendant', 'escaped-descendant'].includes(command.content)) {
    const descendant = spawn(process.execPath, ['-e', 'const fs=require("fs");setInterval(()=>fs.appendFileSync("heartbeat.txt","x"),20)'], { stdio: 'ignore', detached: command.content === 'escaped-descendant' });
    writeFileSync(join(config.workspace, 'descendant-pid.txt'), String(descendant.pid)); descendant.unref();
  }
  if (command.content === 'fail-model') { send({ type: 'assistant', failed: true, errorCode: 'MODEL_REQUEST_FAILED' }); finish(command.requestKey); return; }
  writeFileSync(join(config.workspace, 'result.txt'), command.content);
  running = null;
  assistant('completed ' + command.content, { usage: { input: 3, output: 2 } });
  send({ type: 'settled', requestKey: command.requestKey });
}
