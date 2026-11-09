import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export type Row = Record<string, any>;
const compare = (a: string, b: string): number => {
  const aa = Array.from(a, c => c.codePointAt(0)!); const bb = Array.from(b, c => c.codePointAt(0)!);
  for (let i = 0; i < Math.min(aa.length, bb.length); i++) if (aa[i] !== bb[i]) return aa[i] - bb[i];
  return aa.length - bb.length;
};
export function canonical(value: any): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'number') { if (!Number.isSafeInteger(value)) throw new Error('INTEGER_JSON_REQUIRED'); return String(value); }
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort(compare).map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  throw new Error('INVALID_JSON');
}
export const hash = (v: string | Buffer) => createHash('sha256').update(v).digest('hex');
export const digest = (v: any) => hash(canonical(v));
export const id = (s: any): string => { if (typeof s !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(s)) throw new Error('INVALID_ID'); return s; };
export const now = () => new Date().toISOString();
export function exact(body: Row, fields: string[]) { if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some(k => !fields.includes(k))) throw new Error('UNEXPECTED_FIELD'); }
export function publicText(text: string) { return text.replace(/(bearer\s+|(?:api[_-]?key|password|access[_-]?token)\s*[:=]\s*)[^\s"']+/gi, '$1[redacted]').replace(/sk-[A-Za-z0-9_-]{20,}/g, '[redacted]'); }

/** An independent SQLite writer lock fences hosts; the ledger itself commits each acceptance. */
export class Store {
  readonly db: DatabaseSync; private readonly owner: DatabaseSync;
  readonly identity: Row;
  constructor(readonly directory: string) {
    mkdirSync(directory, { recursive: true });
    this.owner = new DatabaseSync(join(directory, 'owner.sqlite'));
    try { this.owner.exec('PRAGMA busy_timeout=0; CREATE TABLE IF NOT EXISTS ownership (id INTEGER PRIMARY KEY); BEGIN EXCLUSIVE'); } catch { this.owner.close(); throw new Error('LOCAL_HOST_ALREADY_OWNS_STATE'); }
    this.db = new DatabaseSync(join(directory, 'ledger.sqlite'));
    this.db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS operations (key TEXT PRIMARY KEY,task TEXT NOT NULL,digest TEXT NOT NULL,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS events (task TEXT NOT NULL,seq INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(task,seq)); CREATE TABLE IF NOT EXISTS artifacts (task TEXT NOT NULL,id TEXT NOT NULL,bytes BLOB NOT NULL,PRIMARY KEY(task,id)); CREATE TABLE IF NOT EXISTS child_messages (task TEXT NOT NULL,stream TEXT NOT NULL,seq INTEGER NOT NULL,collab TEXT,child TEXT,kind TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(stream,seq)); CREATE TABLE IF NOT EXISTS material_uploads (upload_key TEXT PRIMARY KEY,value TEXT NOT NULL);');
    // Derived, stable keyset index: task updates never move a directory entry.
    this.db.exec("CREATE TABLE IF NOT EXISTS task_directory (ordinal INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL UNIQUE); INSERT OR IGNORE INTO task_directory(task_id) SELECT id FROM tasks ORDER BY json_extract(value,'$.createdAt'),id;");
    const old = this.getMeta('identity');
    if (old?.schemaVersion && old.schemaVersion !== 'pi-ledger/1') throw new Error('SCHEMA_INCOMPATIBLE');
    this.identity = old ?? { schemaVersion: 'pi-ledger/1', authorityId: randomUUID(), storeId: randomUUID(), workerId: randomUUID() };
    this.setMeta('identity', this.identity);
  }
  getMeta(key: string): any { const row = this.db.prepare('SELECT value FROM meta WHERE key=?').get(key); return row ? JSON.parse(row.value as string) : null; }
  setMeta(key: string, value: any) { this.db.prepare('INSERT OR REPLACE INTO meta VALUES (?,?)').run(key, JSON.stringify(value)); }
  task(key: string): Row { const row = this.db.prepare('SELECT value FROM tasks WHERE id=?').get(id(key)); if (!row) throw new Error('TASK_NOT_FOUND'); return JSON.parse(row.value as string); }
  tasks(): Row[] { return this.db.prepare('SELECT value FROM tasks ORDER BY rowid').all().map(r => JSON.parse(r.value as string)); }
  save(task: Row) { this.db.prepare('INSERT OR REPLACE INTO tasks VALUES (?,?)').run(task.taskId, JSON.stringify(task)); this.db.prepare('INSERT OR IGNORE INTO task_directory(task_id) VALUES (?)').run(task.taskId); }
  taskDirectory(cursor: string | null, limit: number) {
    if (cursor !== null && (!/^[1-9][0-9]{0,15}$/.test(cursor) || !Number.isSafeInteger(Number(cursor)))) throw new Error('INVALID_CURSOR');
    const rows = this.db.prepare('SELECT d.ordinal,t.value FROM task_directory d JOIN tasks t ON t.id=d.task_id WHERE d.ordinal < ? ORDER BY d.ordinal DESC LIMIT ?').all(cursor === null ? Number.MAX_SAFE_INTEGER : Number(cursor), limit + 1);
    return { items: rows.slice(0, limit).map(r => JSON.parse(r.value as string)), nextCursor: rows.length > limit ? String(rows[limit - 1].ordinal) : null };
  }
  operation(key: string): Row | null { const row = this.db.prepare('SELECT value FROM operations WHERE key=?').get(id(key)); return row ? JSON.parse(row.value as string) : null; }
  accept(key: string, task: string, request: Row, row: Row): Row {
    const previous = this.operation(key); if (previous) { if (previous.bodyDigest !== digest(request)) throw new Error('IDEMPOTENCY_CONFLICT'); return previous; }
    const full = { ...row, taskId: task, requestKey: key, bodyDigest: digest(request), request, acceptedAt: now() };
    this.db.prepare('INSERT INTO operations VALUES (?,?,?,?)').run(key, task, full.bodyDigest, JSON.stringify(full)); return full;
  }
  saveOperation(row: Row) { this.db.prepare('UPDATE operations SET value=? WHERE key=?').run(JSON.stringify(row), row.requestKey); }
  transaction<T>(action: () => T): T { this.db.exec('BEGIN IMMEDIATE'); try { const result = action(); this.db.exec('COMMIT'); return result; } catch (e) { this.db.exec('ROLLBACK'); throw e; } }
  event(task: Row, kind: string, fields: Row = {}) {
    const row = this.db.prepare('SELECT COALESCE(MAX(seq),0) AS seq FROM events WHERE task=?').get(task.taskId)!;
    const seq = Number(row.seq) + 1;
    const value = { cursor: String(seq), sequence: String(seq), kind, occurredAt: now(), taskId: task.taskId, generation: task.generation, segmentId: task.segmentId, nativeSessionId: task.nativeSessionId, nativeTurnId: task.nativeTurnId, targetCommandId: task.currentCommandId, ...fields };
    this.db.prepare('INSERT INTO events VALUES (?,?,?)').run(task.taskId, seq, JSON.stringify(value));
    return value;
  }
  lastEventSeq(taskId: string) { return Number(this.db.prepare('SELECT COALESCE(MAX(seq),0) AS seq FROM events WHERE task=?').get(id(taskId))!.seq); }
  lastEvent(taskId: string): Row | null { const row = this.db.prepare('SELECT value FROM events WHERE task=? ORDER BY seq DESC LIMIT 1').get(id(taskId)); return row ? JSON.parse(row.value as string) : null; }
  events(task: string, cursor: string, limit: number) {
    if (!/^\d+$/.test(cursor) || !Number.isSafeInteger(Number(cursor)) || limit < 1 || limit > 100) throw new Error('INVALID_EVENT_LIMIT');
    const items = this.db.prepare('SELECT value FROM events WHERE task=? AND seq>? ORDER BY seq LIMIT ?').all(task, Number(cursor), limit).map(r => JSON.parse(r.value as string));
    return { items, cursor: items.at(-1)?.cursor ?? cursor, cursorExpired: false };
  }
  // ---- child-messages/1: per-stream durable child conversation capture.
  //      seq is a per-stream monotonic integer surfaced as a decimal string; it
  //      is assigned from MAX(seq)+1 inside the caller's transaction, so host
  //      restarts never renumber captured items.
  childStream(streamId: string): Row | null { return this.getMeta('child-stream:' + streamId); }
  ensureChildStream(streamId: string, meta: Row) {
    if (!this.childStream(streamId)) this.setMeta('child-stream:' + streamId, { schemaVersion: 'child-stream/1', streamId, captureStartedAt: now(), ...meta });
  }
  appendChildMessage(streamId: string, meta: { task: string; collab?: string | null; child?: string | null }, row: Row): Row {
    const seq = Number(this.db.prepare('SELECT COALESCE(MAX(seq),0) AS s FROM child_messages WHERE stream=?').get(streamId)!.s) + 1;
    const item: Row = { id: 'm-' + seq, seq: String(seq), kind: row.kind, text: row.text ?? '', createdAt: row.createdAt ?? now() };
    if (row.status !== undefined) item.status = row.status;
    if (row.ref !== undefined) item.ref = row.ref;
    if (row.truncated === true) item.truncated = true;
    // worker-events/1 OUTPUT_DELTA rows carry their own payload fields next to
    // the common envelope; absent optional fields simply never serialize.
    if (row.delta !== undefined) item.delta = row.delta;
    if (row.contentIndex !== undefined) item.contentIndex = row.contentIndex;
    if (row.nativeTurnId !== undefined) item.nativeTurnId = row.nativeTurnId;
    if (row.nativeItemId !== undefined) item.nativeItemId = row.nativeItemId;
    this.db.prepare('INSERT INTO child_messages VALUES (?,?,?,?,?,?,?)').run(meta.task, streamId, seq, meta.collab ?? null, meta.child ?? null, item.kind, JSON.stringify(item));
    return item;
  }
  private childMessageMax(streamId: string) { return Number(this.db.prepare('SELECT COALESCE(MAX(seq),0) AS s FROM child_messages WHERE stream=?').get(streamId)!.s); }
  private childMessageBelow(streamId: string, seq: number) { return !!this.db.prepare('SELECT 1 FROM child_messages WHERE stream=? AND seq<? LIMIT 1').get(streamId, seq); }
  /** cursor pages forward (seq > cursor); before pages backward (seq < before,
   *  items still returned oldest→newest). before and cursor are mutually
   *  exclusive; with neither, the latest page is returned. */
  childMessages(streamId: string, before: string | null, cursor: string | null, limit: number) {
    if (before !== null && cursor !== null) throw new Error('INVALID_QUERY');
    for (const v of [before, cursor]) if (v !== null && (!/^(0|[1-9][0-9]*)$/.test(v) || !Number.isSafeInteger(Number(v)))) throw new Error('INVALID_QUERY');
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error('INVALID_QUERY');
    const rows = (sql: string, args: any[]) => this.db.prepare(sql).all(...args).map((r: any) => JSON.parse(r.value as string) as Row);
    let items: Row[];
    if (before !== null) items = rows('SELECT value FROM child_messages WHERE stream=? AND seq<? ORDER BY seq DESC LIMIT ?', [streamId, Number(before), limit]).reverse();
    else if (cursor !== null) items = rows('SELECT value FROM child_messages WHERE stream=? AND seq>? ORDER BY seq LIMIT ?', [streamId, Number(cursor), limit]);
    else items = rows('SELECT value FROM child_messages WHERE stream=? ORDER BY seq DESC LIMIT ?', [streamId, limit]).reverse();
    const first = items[0]; const last = items.at(-1); const max = this.childMessageMax(streamId);
    return { items, hasMoreBefore: !!first && this.childMessageBelow(streamId, Number(first.seq)), hasMoreAfter: !!last && Number(last.seq) < max, maxSeq: String(max) };
  }
  materialUpload(key: string): Row | null { const row = this.db.prepare('SELECT value FROM material_uploads WHERE upload_key=?').get(id(key)); return row ? JSON.parse(row.value as string) : null; }
  saveMaterialUpload(row: Row) { this.db.prepare('INSERT OR REPLACE INTO material_uploads VALUES (?,?)').run(row.uploadKey, JSON.stringify(row)); }
  materialUploads(): Row[] { return this.db.prepare('SELECT value FROM material_uploads').all().map(r => JSON.parse(r.value as string) as Row); }
  close() { this.db.close(); this.owner.exec('ROLLBACK'); this.owner.close(); }
}
