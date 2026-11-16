// worker-local-files/1 — staged deliverables for the pi host.
// Mirrors the cli-bridge contract exactly: same pool limits, same meta row,
// same chunk schema, same files_stage MCP bridge, same FILE_CARD feed. The
// staged pool lives under the host store directory so file delivery survives
// the task's agent session exiting.
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { closeSync, copyFileSync, existsSync, lstatSync, mkdirSync, openSync, readFileSync, readSync, realpathSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { basename, isAbsolute, join, relative, resolve, sep } from 'node:path';

type Row = Record<string, any>;

const FILES_TTL_MS = 24 * 60 * 60 * 1000;
const FILES_STAGE_MAX = 64 << 20;
const FILES_MAX_TOTAL = 512 << 20;
const FILES_MAX_COUNT = 50;
export const FILES_CHUNK = 262144;

const hashBytes = (b: Buffer | string) => createHash('sha256').update(b).digest('hex');
const fail = (code: string): never => { throw new Error(code); };

const SECRET_RE = /(?:sk-[A-Za-z0-9_-]{20,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:api[_-]?key|password|access[_-]?token)\s*[:=]\s*["']?[^\s"']{8,})/i;
const knownSecrets = Object.entries(process.env).filter(([k, v]) => /(?:KEY|PASSWORD|TOKEN|SECRET)$/.test(k) && v && v.length >= 8).map(([, v]) => v as string);

// ---- files_stage MCP wiring --------------------------------------------------
// The task's MCP child gets: a stdio server (written into state/mcp on demand),
// the loopback port, the task id, the store pin and the files.key path — every
// one of those is non-secret. The actual credential stays in files.key,
// readable only by this user account; MCP children never see the worker token.
const MCP_FILES_SOURCE = `#!/usr/bin/env node
// pac-files MCP server (stdio JSON-RPC, newline-delimited). Spawned by the
// vendor CLI — offers files_stage(path) so an agent can hand a workspace file
// to the user's phone and return immediately (delivery is the host's job).
import { readFileSync } from 'node:fs';

const PORT = Number(process.env.PAC_BRIDGE_PORT ?? 0);
const TASK = process.env.PAC_TASK_ID ?? '';
const STORE = process.env.PAC_STORE_ID ?? '';
const KEYFILE = process.env.PAC_FILES_KEY_FILE ?? '';

const send = (obj) => process.stdout.write(JSON.stringify(obj) + '\\n');
const reply = (id, result) => send({ jsonrpc: '2.0', id, result });
const failure = (id, code, message) => send({ jsonrpc: '2.0', id, error: { code, message } });
const toolText = (id, text, isError) => reply(id, {
  content: [{ type: 'text', text }], ...(isError ? { isError: true } : {}),
});

const stage = async (args) => {
  const key = readFileSync(KEYFILE, 'utf8').trim();
  const res = await fetch('http://127.0.0.1:' + PORT + '/local/v1/tasks/' + TASK + '/files', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: 'Bearer ' + key,
      'x-worker-local-store': STORE,
    },
    body: JSON.stringify({ path: args?.path, name: args?.name }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.code ?? ('HTTP_' + res.status));
  return data;
};

const handle = async (msg) => {
  const id = msg.id;
  const method = msg.method ?? '';
  if (id === undefined || id === null) return; // notifications get no reply
  if (method === 'initialize') {
    return reply(id, {
      protocolVersion: msg.params?.protocolVersion ?? '2024-11-05',
      capabilities: { tools: {} },
      serverInfo: { name: 'pac-files', version: '1.0.0' },
    });
  }
  if (method === 'ping') return reply(id, {});
  if (method === 'tools/list') {
    return reply(id, { tools: [{
      name: 'files_stage',
      description: 'Stage a file from this workspace for delivery to the user phone. ' +
        'Returns a fileId immediately and posts a download card into this task - ' +
        'the agent does not need to wait for the phone. Use after producing a ' +
        'file the user should receive (report, export, build output). path may ' +
        'be relative to the workspace root or absolute inside it.',
      inputSchema: {
        type: 'object',
        properties: {
          path: { type: 'string', description: 'Workspace file to deliver (relative or absolute, must stay inside the workspace).' },
          name: { type: 'string', description: 'Optional display name on the phone card.' },
        },
        required: ['path'],
        additionalProperties: false,
      },
    }] });
  }
  if (method === 'tools/call') {
    if (msg.params?.name !== 'files_stage') {
      return toolText(id, 'unknown tool: ' + String(msg.params?.name), true);
    }
    try {
      const out = await stage(msg.params?.arguments ?? {});
      return toolText(id, 'Staged for the user phone: ' + out.name + ' (' + out.size +
        ' bytes, fileId ' + out.id + ', expires ' + out.expiresAt + '). ' +
        'A download card was posted to this task - nothing else to do.');
    } catch (e) {
      return toolText(id, 'files_stage failed: ' + String(e?.message ?? e), true);
    }
  }
  return failure(id, -32601, 'method not found: ' + method);
};

let buf = '';
process.stdin.on('data', (d) => {
  buf += d;
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    handle(msg).catch(() => {});
  }
});
`;

export class FilesPool {
  readonly directory: string;
  readonly keyFile: string;
  readonly mcpDir: string;
  private readonly ledgerFile: string;
  private records: Row = {};
  private keyValue: string | null = null;
  /** Bound loopback port — set after listen() so task-create can wire env. */
  port = 0;

  constructor(storeDirectory: string) {
    this.directory = join(storeDirectory, 'files');
    this.keyFile = join(storeDirectory, 'files.key');
    this.mcpDir = join(storeDirectory, 'mcp');
    this.ledgerFile = join(storeDirectory, 'files.json');
    try { this.records = JSON.parse(readFileSync(this.ledgerFile, 'utf8')).files ?? {}; } catch { this.records = {}; }
  }

  /** Bearer credential shared only with files_stage MCP children — a
   *  loopback-scoped token for the single stage route, never the worker key. */
  key(): string {
    if (this.keyValue) return this.keyValue;
    try { this.keyValue = readFileSync(this.keyFile, 'utf8').trim(); } catch { this.keyValue = null; }
    if (!this.keyValue || this.keyValue.length < 32) {
      this.keyValue = randomBytes(32).toString('hex');
      const tmp = this.keyFile + '.tmp';
      writeFileSync(tmp, this.keyValue, { mode: 0o600 });
      renameSync(tmp, this.keyFile);
    }
    return this.keyValue;
  }

  private save() {
    const tmp = this.ledgerFile + '.tmp';
    writeFileSync(tmp, JSON.stringify({ schemaVersion: 'worker-local-files-store/1', files: this.records }));
    renameSync(tmp, this.ledgerFile);
  }

  private expired(f: Row) { return (f.expiresAtMs ?? 0) <= Date.now(); }

  /** Public row — list items, the meta answer and the <<FILE>> marker all
   *  carry this shape (worker-local-file/1 fields the gateway + phone verify). */
  meta(f: Row) {
    return { schemaVersion: 'worker-local-file/1', fileId: f.id, id: f.id, taskId: f.taskId,
      name: f.name, size: f.size, mime: f.mime, sha256: f.sha256,
      state: this.expired(f) ? 'expired' : 'staged', storage: 'pc',
      stagedAt: new Date(f.stagedAtMs).toISOString(),
      expiresAt: new Date(f.expiresAtMs).toISOString() };
  }

  sweep() {
    const nowMs = Date.now();
    let total = 0, kept = 0, dirty = false;
    for (const [id, f] of Object.entries(this.records).sort((a, b) => b[1].stagedAtMs - a[1].stagedAtMs)) {
      const gone = !existsSync(f.staged);
      if (gone || f.expiresAtMs <= nowMs || kept >= FILES_MAX_COUNT || total + f.size > FILES_MAX_TOTAL) {
        if (!gone) { try { rmSync(f.staged, { force: true }); } catch {} }
        delete this.records[id]; dirty = true; continue;
      }
      total += f.size; kept++;
    }
    if (dirty) this.save();
  }

  list(taskId: string) {
    this.sweep();
    return { schemaVersion: 'worker-local-files/1', taskId,
      items: Object.values(this.records).filter((f: Row) => f.taskId === taskId)
        .map((f: Row) => this.meta(f)).sort((a, b) => b.stagedAt.localeCompare(a.stagedAt)) };
  }

  get(taskId: string, fileId: string) {
    this.sweep();
    const f = this.records[fileId];
    if (!f || f.taskId !== taskId || !existsSync(f.staged)) fail('FILE_NOT_FOUND');
    if (this.expired(f)) fail('FILE_EXPIRED');
    return this.meta(f);
  }

  chunk(taskId: string, fileId: string, digest: string, rawOffset: string) {
    this.sweep();
    const f = this.records[fileId];
    const offset = Number(rawOffset);
    if (!f || f.taskId !== taskId || !existsSync(f.staged)) fail('FILE_NOT_FOUND');
    if (this.expired(f)) fail('FILE_EXPIRED');
    if (f.sha256 !== digest
        || !/^(0|[1-9][0-9]*)$/.test(rawOffset) || !Number.isSafeInteger(offset)
        || offset % FILES_CHUNK !== 0 || offset >= f.size) fail('FILE_NOT_AVAILABLE');
    const fd = openSync(f.staged, 'r');
    try {
      const len = Math.min(FILES_CHUNK, f.size - offset);
      const chunk = Buffer.alloc(len);
      if (readSync(fd, chunk, 0, len, offset) !== len) fail('FILE_CHANGED');
      return { schemaVersion: 'worker-local-file-chunk/1', taskId,
        fileId: f.id, sha256: f.sha256, offset,
        sizeBytes: String(f.size), encoding: 'base64',
        bytes: chunk.toString('base64'), chunkSHA256: hashBytes(chunk) };
    } finally { closeSync(fd); }
  }

  /** Stage {path, name?, mime?} from inside the task workspace into the pool.
   *  The id commits to task+path+content — re-staging identical bytes refreshes
   *  expiry and re-posts the card (a deliberate second send), never duplicates. */
  stage(task: Row, body: Row) {
    const src = typeof body?.path === 'string' ? body.path.trim() : '';
    if (!src || /[\x00-\x1f]/.test(src)) fail('INVALID_REQUEST');
    let root = '';
    try { root = realpathSync(task.workspace); } catch { fail('FILE_NOT_FOUND'); }
    const candidate = isAbsolute(src) ? resolve(src) : resolve(root, src);
    const relTry = relative(root, candidate);
    if (relTry === '..' || relTry.startsWith('..' + sep)) fail('FILE_OUTSIDE_WORKSPACE');
    let real = '';
    try { real = realpathSync(candidate); } catch { fail('FILE_NOT_FOUND'); }
    const rel = relative(root, real);
    if (rel === '' || rel === '..' || rel.startsWith('..' + sep)) fail('FILE_OUTSIDE_WORKSPACE');
    const s = lstatSync(real);
    if (!s.isFile() || s.size <= 0) fail('FILE_NOT_FOUND');
    if (s.size > FILES_STAGE_MAX) fail('FILE_TOO_LARGE');
    // Same bounded secret window as the bridge: headers/config live up front.
    const probe = Buffer.alloc(Math.min(s.size, 524288));
    const pf = openSync(real, 'r');
    try { if (readSync(pf, probe, 0, probe.length, 0) !== probe.length) fail('FILE_CHANGED'); }
    finally { closeSync(pf); }
    const scanned = probe.toString('utf8');
    if (knownSecrets.some((x) => scanned.includes(x)) || SECRET_RE.test(scanned)) fail('FILE_SECRET');
    mkdirSync(this.directory, { recursive: true });
    const staged = join(this.directory, 'stage-' + randomUUID());
    copyFileSync(real, staged);
    // The source must not have moved mid-copy — the staged bytes are what we hash.
    const after = lstatSync(real);
    if (after.mtimeMs !== s.mtimeMs || after.size !== s.size) {
      try { rmSync(staged, { force: true }); } catch {}
      fail('FILE_CHANGED');
    }
    const sha256 = hashBytes(readFileSync(staged));
    const id = 'file-' + hashBytes(task.taskId + '\0' + real! + '\0' + sha256);
    const existing = this.records[id];
    if (existing) {
      try { rmSync(staged, { force: true }); } catch {}
      existing.expiresAtMs = Date.now() + FILES_TTL_MS;
      this.save();
      return existing;
    }
    const name = typeof body?.name === 'string' && body.name.trim() && !/[/\\\x00-\x1f]/.test(body.name)
      ? body.name.trim().slice(0, 160) : basename(real!);
    const mime = typeof body?.mime === 'string' && body.mime ? body.mime.slice(0, 100) : 'application/octet-stream';
    const f = { id, taskId: task.taskId, name, size: s.size, mime, sha256,
      sourcePath: real, staged, stagedAtMs: Date.now(),
      expiresAtMs: Date.now() + FILES_TTL_MS, storage: 'pc' };
    this.records[id] = f;
    this.save();
    return f;
  }

  /** Materialise the embedded MCP server once; restarts rewrite it when the
   *  bundled source changed so old bridges never run stale helpers. */
  mcpEntry(): string {
    const entry = join(this.mcpDir, 'files.mjs');
    try {
      if (!existsSync(entry) || readFileSync(entry, 'utf8') !== MCP_FILES_SOURCE) {
        mkdirSync(this.mcpDir, { recursive: true });
        const tmp = entry + '.tmp';
        writeFileSync(tmp, MCP_FILES_SOURCE);
        renameSync(tmp, entry);
      }
      return entry;
    } catch { return entry; }
  }

  /** Host-injected MCP server row stored on the task — durable across
   *  pause/resume and visible in the task record for audit. env carries only
   *  non-secret pointers (port, ids, keyfile path); the credential itself
   *  stays in files.key. Caller-declared 'files' servers win — we never
   *  shadow an explicit binding. */
  mcpServer(taskId: string, storeId: string): { id: string; command: string; args: string[]; env: Row } | null {
    if (!this.port) return null;
    return { id: 'files', command: process.execPath, args: [this.mcpEntry()], env: {
      PAC_BRIDGE_PORT: String(this.port),
      PAC_TASK_ID: taskId,
      PAC_STORE_ID: storeId,
      PAC_FILES_KEY_FILE: this.keyFile,
    } };
  }
}
