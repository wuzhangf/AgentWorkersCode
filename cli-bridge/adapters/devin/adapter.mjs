// worker-adapter/1 — devin store adapter (reference implementation).
// Devin's sessions live in one SQLite store the CLI owns; this adapter
// reads it with no CLI dependency — kind 'store' means the bridge never
// spawns a binary for this engine.
import { homedir } from 'node:os';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

/** Devin CLI's sessions.db lives under the platform user-data dir: Windows
 * `%APPDATA%\Devin\cli`, macOS under Application Support, Linux/other XDG
 * under ~/.local/share — first existing file wins. */
const DBS = process.platform === 'win32'
  ? [join(process.env.APPDATA ?? join(homedir(), 'AppData', 'Roaming'), 'Devin', 'cli', 'sessions.db'),
     join(homedir(), '.local', 'share', 'devin', 'cli', 'sessions.db')]
  : [join(homedir(), '.local', 'share', 'devin', 'cli', 'sessions.db'),
     join(homedir(), 'Library', 'Application Support', 'Devin', 'cli', 'sessions.db')];
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

const sessions = ({ id, limit, before } = {}) => {
  const handle = db();
  if (!handle) return [];
  try {
    const args = [];
    let sql = 'SELECT id, title, working_directory, last_activity_at FROM sessions WHERE hidden = 0';
    if (id !== undefined) { sql += ' AND id = ?'; args.push(id); }
    if (before) {
      sql += ' AND (COALESCE(last_activity_at, 0) < ? OR (COALESCE(last_activity_at, 0) = ? AND id > ?))';
      args.push(before.at, before.at, before.id);
    }
    sql += ' ORDER BY COALESCE(last_activity_at, 0) DESC, id';
    if (limit !== undefined) { sql += ' LIMIT ?'; args.push(limit); }
    return handle.prepare(sql).all(...args).map(r => ({
      id: String(r.id), title: String(r.title ?? ''), cwd: String(r.working_directory ?? ''),
      at: at(r.last_activity_at), rawAt: r.last_activity_at ?? 0,
    }));
  } catch { return []; }
  finally { try { handle.close(); } catch {} }
};

const transcript = (id) => {
  const handle = db();
  if (!handle) return null;
  try {
    return handle.prepare(
      `SELECT chat_message FROM message_nodes WHERE session_id = ? ORDER BY created_at`).all(id)
      .map((r) => { try { const m = JSON.parse(r.chat_message);
        return { role: m.role, text: String(m.content ?? '') }; }
      catch { return null; } })
      .filter((m) => m && (m.role === 'user' || m.role === 'assistant') && m.text.trim());
  } catch { return null; }
  finally { try { handle.close(); } catch {} }
};

export default {
  manifest: {
    id: 'devin',
    kind: 'store',
    displayName: 'Devin',
    detect: { configDirs: DBS },
  },
  /** devin-db/1: sessions + message_nodes are the two tables we read. An
   * absent store means Devin is not installed (store reports empty); a
   * present-but-unrecognizable one is a schema upgrade → UNSUPPORTED. */
  supported: (ctx) => {
    for (const file of DBS) {
      if (!ctx.exists(file)) continue;
      try {
        const handle = ctx.openSqlite(file);
        const has = (name) => handle.prepare(
          "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name) !== undefined;
        const ok = has('sessions') && has('message_nodes');
        handle.close();
        return ok || 'sessions.db 表结构不识（可能 Devin 新版改了格式）';
      } catch { return 'sessions.db 打开失败'; }
    }
    return true;
  },
  store: {
    sessions,
    transcript,
    row: (id) => sessions({ id, limit: 1 })[0],
  },
};
