// office-bridge: standalone translator daemon. Zero changes to our workers or
// to pixel-agents. Discovers tasks on worker-local backends, tails their
// worker-events/1 SSE spines, and projects each task (and each collab/aux
// child stream) into a Claude-Code-shaped .jsonl session file that the stock
// pixel office renders as a character.
//
//   WORKER_LOCAL_TOKEN=... node office-bridge.mjs
//
// Config via env: BRIDGE_WORKERS='url1,url2'  BRIDGE_LABEL='office'  BRIDGE_POLL_MS=3000
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { randomUUID } from 'node:crypto';

const TOKEN = process.env.WORKER_LOCAL_TOKEN;
const WORKERS = (process.env.BRIDGE_WORKERS || 'http://127.0.0.1:18731').split(',');
const POLL_MS = Number(process.env.BRIDGE_POLL_MS || 3000);
// slug dir under ~/.claude/projects. NOTE: pixel-agents only watches project
// dirs that existed at its startup — use a cwd whose slug is already watched,
// or restart the office server after first use of a new slug.
const OFFICE_CWD = process.env.BRIDGE_CWD || process.cwd();
const slug = OFFICE_CWD.replace(/[^a-zA-Z0-9-]/g, '-');
const dir = join(homedir(), '.claude', 'projects', slug);
mkdirSync(dir, { recursive: true });

const MAP = { read: 'Read', ls: 'Read', grep: 'Grep', glob: 'Glob', edit: 'Edit', write_file: 'Write', bash: 'Bash', run_command: 'Bash', run_build: 'Bash', run_tests: 'Bash', ask_user: 'AskUser' };
const mapName = (n) => MAP[String(n || '').toLowerCase()] || 'Bash';
const toolIdOf = (ref) => 'toolu_' + String(ref || randomUUID()).replace(/[^a-zA-Z0-9]/g, '').slice(0, 20);
const now = () => new Date().toISOString();

// session = one pixel-agents character. key = worker|taskId|streamTag
const sessions = new Map();
function session(key, title) {
  let s = sessions.get(key);
  if (!s) {
    const sid = randomUUID();
    const file = join(dir, `${sid}.jsonl`);
    writeFileSync(file,
      JSON.stringify({ type: 'user', uuid: randomUUID(), timestamp: now(), sessionId: sid, cwd: OFFICE_CWD, message: { role: 'user', content: [{ type: 'text', text: title }] } }) + '\n');
    s = { sid, file };
    sessions.set(key, s);
    console.log('[bridge] character spawned:', title, '->', sid.slice(0, 8));
  }
  return s;
}
const emit = (s, r) => appendFileSync(s.file, JSON.stringify({ ...r, sessionId: s.sid, cwd: OFFICE_CWD, timestamp: now() }) + '\n');

const seenTasks = new Set();
const attached = new Set();
let firstPass = true; // on the first discovery pass, skip tasks already in a
// terminal state (history from earlier runs); afterwards attach every new
// taskId regardless of state so fast-finishing tasks still get a character.

async function tail(worker, taskId, streamTag, title) {
  // streamTag: '' = main stream; collab/aux children get their own character
  const s = session(`${worker}|${taskId}|${streamTag}`, title);
  try {
    const res = await fetch(`${worker}/local/v1/tasks/${taskId}/events?follow=1`, {
      headers: { authorization: `Bearer ${TOKEN}`, accept: 'text/event-stream' },
    });
    if (!res.ok || !res.body) return;
    for await (const chunk of res.body) {
      for (const raw of Buffer.from(chunk).toString('utf8').split('\n')) {
        const t = raw.trim();
        if (!t.startsWith('data:')) continue;
        let ev; try { ev = JSON.parse(t.slice(5)); } catch { continue; }
        // child streams -> separate character files
        const childTag = ev.child ? `child:${ev.child}` : ev.collab ? `collab:${ev.collab}` : streamTag;
        const target = childTag !== streamTag
          ? session(`${worker}|${taskId}|${childTag}`, `${title} / ${childTag}`)
          : s;
        const kind = ev.kind || ev.type || '';
        if (kind === 'TOOL_START' || kind === 'TOOL_CALL') {
          const name = mapName(ev.toolName || ev.text);
          emit(target, { type: 'assistant', uuid: randomUUID(), message: { role: 'assistant', model: 'worker',
            content: [{ type: 'tool_use', id: toolIdOf(ev.toolCallId || ev.ref), name,
              input: { command: String(ev.detail || ev.toolName || '').slice(0, 120), description: String(ev.toolName || '') } }] } });
        } else if (kind === 'TOOL_END' || kind === 'TOOL_RESULT') {
          emit(target, { type: 'user', uuid: randomUUID(), message: { role: 'user',
            content: [{ type: 'tool_result', tool_use_id: toolIdOf(ev.toolCallId || ev.ref), content: String(ev.detail || (ev.isError ? 'error' : 'ok')).slice(0, 160), is_error: !!ev.isError }] } });
        } else if (kind === 'INPUT_REQUEST' || kind === 'INPUT_REQUIRED') {
          emit(target, { type: 'assistant', uuid: randomUUID(), message: { role: 'assistant', model: 'worker',
            content: [{ type: 'tool_use', id: toolIdOf(ev.ref || `ask${Date.now()}`), name: 'AskUser', input: { question: String(ev.text || ev.prompt || 'waiting for approval').slice(0, 120) } }] } });
        } else if (/SUCCEEDED|FAILED|CANCELLED|TERMINAL|STOP/i.test(kind) || /SUCCEEDED|FAILED|CANCELLED/i.test(String(ev.state))) {
          emit(target, { type: 'assistant', uuid: randomUUID(), message: { role: 'assistant', model: 'worker', content: [{ type: 'text', text: `task ${ev.state || kind}` }] } });
          return;
        }
      }
    }
  } catch (e) { console.log('[bridge] tail ended', taskId, e.message); }
}

async function discover(worker) {
  try {
    const res = await fetch(`${worker}/local/v1/tasks?limit=50`, { headers: { authorization: `Bearer ${TOKEN}` } });
    if (!res.ok) return;
    const page = await res.json();
    for (const t of page.items || []) {
      if (attached.has(t.taskId)) continue;
      attached.add(t.taskId);
      const terminal = /SUCCEEDED|FAILED|CANCELLED|STOPPED|REJECTED|UNKNOWN/i.test(t.state);
      if (firstPass && terminal) { seenTasks.add(t.taskId); continue; }
      console.log('[bridge] attach', t.taskId, t.engineId, t.state);
      tail(worker, t.taskId, '', `${t.engineId} · ${t.projectId}`);
      seenTasks.add(t.taskId);
    }
  } catch (e) { console.log('[bridge] discover fail', e.message); }
}

console.log('[bridge] watching', WORKERS.join(', '), '->', dir);
for (;;) {
  for (const w of WORKERS) await discover(w);
  firstPass = false;
  await new Promise(r => setTimeout(r, POLL_MS));
}
