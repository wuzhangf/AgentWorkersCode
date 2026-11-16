// worker-adapter/1 — opencode adapter: headless `run` turns plus the shared
// SQLite session store the desktop app and this CLI write side by side.
import { homedir } from 'node:os';
import { existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

/** OpenCode keeps sessions in one XDG-data SQLite store on every platform —
 * ~/.local/share/opencode/opencode.db (the desktop app shares it). Read-only
 * open; absent store reports empty. */
const DBS = [join(homedir(), '.local', 'share', 'opencode', 'opencode.db')];
const db = () => {
  for (const file of DBS) {
    if (!existsSync(file)) continue;
    try { return new DatabaseSync(file, { readOnly: true }); } catch { continue; }
  }
  return null;
};

const at = (value) => {
  const n = Number(value);
  const date = value == null || value === '' ? new Date(NaN)
    : new Date(Number.isFinite(n) ? (Math.abs(n) < 1e11 ? n * 1000 : n) : value);
  return Number.isFinite(+date) ? date.toISOString() : '';
};

/** The store has two generations side by side: pre-v2 rows in
 * session/message/part and the current schema session_v2/session_message
 * (session_part exists but stays empty; content lives inline in
 * session_message.data). Reads union both, deduped by id — the exact "old
 * and new data structures coexist" case adapters are expected to own. */
const tableExists = (handle, name) =>
  handle.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name) !== undefined;

const sessions = ({ id, limit, before } = {}) => {
  const handle = db();
  if (!handle) return [];
  try {
    // parent_id rows are sub-agent side sessions; time_archived rows are our
    // own --archived headless runs — neither is a "native session" to offer.
    const rows = new Map();
    for (const table of ['session_v2', 'session']) {
      if (!tableExists(handle, table)) continue;
      const args = [];
      let sql = `SELECT id, title, directory AS cwd, time_updated FROM ${table}
                 WHERE parent_id IS NULL AND time_archived IS NULL`;
      if (id !== undefined) { sql += ' AND id = ?'; args.push(id); }
      for (const r of handle.prepare(sql).all(...args)) {
        if (!rows.has(r.id)) rows.set(r.id, r);
      }
    }
    let out = [...rows.values()];
    if (before) out = out.filter(r =>
      (r.time_updated ?? 0) < before.at || ((r.time_updated ?? 0) === before.at && r.id > before.id));
    out.sort((a, b) => (b.time_updated ?? 0) - (a.time_updated ?? 0)
      || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    if (limit !== undefined) out = out.slice(0, limit);
    return out.map(r => ({
      id: String(r.id), title: String(r.title ?? ''), cwd: String(r.cwd ?? ''),
      at: at(r.time_updated), rawAt: r.time_updated ?? 0,
    }));
  } catch { return []; }
  finally { try { handle.close(); } catch {} }
};

const transcript = (id) => {
  const handle = db();
  if (!handle) return null;
  try {
    const items = [];
    // v1: message.data carries role; part.data carries typed blocks — text
    // parts only. part.session_id denormalizes the join key.
    if (tableExists(handle, 'message') && tableExists(handle, 'part')) {
      for (const r of handle.prepare(
        `SELECT p.time_created AS t,
                json_extract(m.data, '$.role') AS role,
                json_extract(p.data, '$.text') AS text
         FROM part p JOIN message m ON m.id = p.message_id
         WHERE p.session_id = ? AND json_extract(p.data, '$.type') = 'text'
         ORDER BY p.time_created, p.id`).all(id)) {
        if ((r.role === 'user' || r.role === 'assistant')
            && typeof r.text === 'string' && r.text.trim())
          items.push({ t: r.t ?? 0, role: r.role, text: r.text });
      }
    }
    // v2: session_message.type carries the role; user data holds .text,
    // assistant data holds a content[] array whose 'text' items carry prose
    // ('reasoning' and 'tool' items are skipped, matching the v1 filter).
    if (tableExists(handle, 'session_message')) {
      for (const r of handle.prepare(
        `SELECT time_created AS t, type, data
         FROM session_message WHERE session_id = ?
         ORDER BY COALESCE(seq, time_created), id`).all(id)) {
        let d;
        try { d = JSON.parse(r.data); } catch { continue; }
        if (r.type === 'user' && typeof d.text === 'string' && d.text.trim()) {
          items.push({ t: r.t ?? 0, role: 'user', text: d.text });
        } else if (r.type === 'assistant') {
          for (const c of d.content ?? []) {
            if (c.type === 'text' && typeof c.text === 'string' && c.text.trim())
              items.push({ t: r.t ?? 0, role: 'assistant', text: c.text });
          }
        }
      }
    }
    items.sort((a, b) => a.t - b.t);
    return items.map(({ role, text }) => ({ role, text }));
  } catch { return null; }
  finally { try { handle.close(); } catch {} }
};

export default {
  manifest: {
    id: 'opencode',
    kind: 'both',
    displayName: 'OpenCode',
    detect: {
      // Live badge: desktop app or interactive CLI the user started. The
      // headless subcommands we spawn ourselves stay out.
      process: {
        exe: 'opencode|opencode-cli|OpenCode',
        excludeArgv: '\\s(run|serve|mcp|auth|upgrade|export|import|stats|models|agent)\\b',
      },
      // No standalone CLI install on Windows — the desktop app ships
      // resources\opencode-cli.exe under %LOCALAPPDATA%\Programs\@opencode-*,
      // including side-by-side .update-<hash> folders from the updater.
      binCandidates: () => {
        if (process.platform !== 'win32' || !process.env.LOCALAPPDATA) return [];
        const root = join(process.env.LOCALAPPDATA, 'Programs');
        const out = [];
        try {
          for (const e of readdirSync(root, { withFileTypes: true }).sort((a, b) => b.name.localeCompare(a.name))) {
            if (!e.isDirectory() || !e.name.startsWith('@opencode')) continue;
            const p = join(root, e.name, 'resources', 'opencode-cli.exe');
            if (existsSync(p)) out.push(p);
          }
        } catch {}
        return out;
      },
      configDirs: DBS,
    },
  },
  /** opencode-db/2 (session_v2) or /1 (session+message+part) — either
   * generation counts as understood since the store unions both. An absent
   * db just means no sessions yet; a present one with neither generation
   * is a schema break → UNSUPPORTED instead of silently listing nothing. */
  supported: (ctx) => {
    for (const file of DBS) {
      if (!ctx.exists(file)) continue;
      try {
        const handle = ctx.openSqlite(file);
        const has = (name) => handle.prepare(
          "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name) !== undefined;
        const ok = has('session_v2') || (has('session') && has('message') && has('part'));
        handle.close();
        return ok || 'opencode.db 表结构不识（可能新版改了存储格式）';
      } catch { return 'opencode.db 打开失败'; }
    }
    return true;
  },
  spawn: {
    bin: 'opencode',
    argv: (instruction, sessionId, task, ctx) => [
      'run', instruction, '--format', 'json',
      // --standalone: a private per-run server — never hand the desktop app's
      // background service our headless sessions.
      '--standalone',
      // Headless runs share the same opencode.db the desktop app and our
      // store reader list — archive keeps them out of "native session" rows
      // (adoption/standalone title still makes them identifiable).
      '--archived',
      ...(task?.taskId ? ['--title', `agent-link ${task.taskId}`] : []),
      // Always --auto: headless has no channel to answer permission prompts
      // (gemini's yolo reasoning). Routing opencode's permission asks to the
      // phone is adapter-level future work; SKIP=off cannot sandbox here.
      '--auto',
      ...(task?.model ?? ctx?.model ? ['--model', task?.model ?? ctx?.model] : []),
      ...(sessionId ? ['--session', sessionId] : []),
    ],
    /** opencode run --format json: every event carries sessionID; `text` parts
     * arrive complete per block; step_finish ends the turn; `error` fails it. */
    onEvent: (row, task, addMessage) => {
      if (typeof row.sessionID === 'string') task.nativeSessionId = row.sessionID;
      if (row.type === 'text' && typeof row.part?.text === 'string' && row.part.text.trim()) {
        addMessage('assistant', row.part.text);
      }
      if (row.type === 'error') {
        task.output = String(row.error?.data?.message ?? row.error?.message ?? 'opencode 运行失败');
        return 'FAILED';
      }
      if (row.type === 'step_finish') return 'SUCCEEDED';
      return null;
    },
  },
  store: {
    sessions,
    transcript,
    row: (id) => sessions({ id, limit: 1 })[0],
  },
};
