#!/usr/bin/env node
/**
 * cli-bridge — thin worker-local/1 host driving a stock vendor CLI.
 *
 * One process = one engine = one backendStoreId. Tasks spawn the installed CLI
 * (`claude -p --output-format stream-json`, `codex exec --json`) in the bound
 * project directory; follow-ups resume the recorded CLI session; STOP kills
 * the child. Deliberately implements only the conversation slice — no budget,
 * collaboration, terminals, materials or pause/resume — matching what the
 * phone's native-task surface needs to create/observe/message/stop a task.
 *
 *   host.mjs serve --engine claude --state <dir> --port 4397 \
 *     --project <dir> --project-id <id>
 *
 * Token comes from WORKER_LOCAL_TOKEN (≥24 chars). Loopback only; every route
 * requires bearer auth; non-identity routes pin X-Worker-Local-Store.
 */
import http from 'node:http';
import { spawn } from 'node:child_process';
import { randomUUID, createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync, renameSync, existsSync, readdirSync, lstatSync, realpathSync } from 'node:fs';
import { join, resolve, relative, sep, dirname, basename } from 'node:path';
import { execFileSync } from 'node:child_process';
import { homedir } from 'node:os';
import { zstdDecompressSync } from 'node:zlib';

const args = process.argv.slice(2);
const cmd = args[0];
const opt = {};
for (let i = 1; i < args.length; i += 2) opt[args[i].replace(/^--/, '')] = args[i + 1];

const ENGINE = opt.engine ?? '';
const STATE_DIR = opt.state ?? '';
const PORT = Number(opt.port ?? 0);
const PROJECT_DIR = opt.project ? resolve(opt.project) : '';
const PROJECT_ID = opt['project-id'] ?? 'workspace';
const TOKEN = process.env.WORKER_LOCAL_TOKEN ?? '';
// Stock CLIs carry account-bound defaults that may not exist on the signed-in
// plan — an explicit --model overrides the vendor config for spawned turns.
const MODEL = opt.model ?? process.env.CLI_BRIDGE_MODEL ?? '';
// --executable pins the wrapped CLI for installs where PATH/shim probing is
// unreliable; when set it replaces resolveBin() detection entirely.
const EXECUTABLE = opt.executable ?? '';
// --skip-permissions on|off: whether spawned turns bypass vendor tool
// approval. Unset keeps per-engine defaults — claude skips (headless -p turns
// cannot answer prompts), codex stays workspace-scoped.
const SKIP = opt['skip-permissions'] ?? '';
// ChatGPT-account codex rejects desktop-internal model names (gpt-6.1-sol)
// from ~/.codex/config.toml — pin a catalog-valid default; --model overrides.
const CODEX_DEFAULT_MODEL = 'gpt-5.6-sol';
// Model menu this bridge advertises: a per-engine default list plus
// operator extras via CLI_BRIDGE_MODELS (JSON array of names). Entries are
// names the wrapped CLI accepts verbatim; codex also declares effort levels
// mapped to `-c model_reasoning_effort=`.
const MODEL_CATALOG = (() => {
  const defaults = { claude: ['sonnet', 'opus', 'haiku'], codex: [CODEX_DEFAULT_MODEL], pi: MODEL ? [MODEL] : [] };
  let extra = [];
  try { const parsed = JSON.parse(process.env.CLI_BRIDGE_MODELS ?? '[]'); if (Array.isArray(parsed)) extra = parsed; } catch { /* malformed catalog ignored */ }
  const names = [...(defaults[ENGINE] ?? []), ...extra.filter(n => typeof n === 'string' && !(defaults[ENGINE] ?? []).includes(n))];
  if (MODEL && !names.includes(MODEL)) names.unshift(MODEL);
  const efforts = ENGINE === 'codex' ? ['minimal', 'low', 'medium', 'high'] : undefined;
  return names.map(name => ({ modelRef: name, displayName: name, imageInput: false, ...(efforts ? { efforts } : {}) }));
})();
// ~/.agent-gateway/agent-modes.json — owner-picked permission posture per
// engine, written from the Agent Link popover. "phone" (default): spawn with
// no OS sandbox so our PreToolUse hook is the permission layer — every risky
// call asks the phone. "workspace": workspace-write sandbox, silent (inside
// auto, outside denied — no card can lift an OS sandbox). "open": full
// access and the hook abstains — nothing asks, anything runs.
const agentMode = (engine) => {
  try {
    const modes = JSON.parse(readFileSync(join(homedir(), '.agent-gateway', 'agent-modes.json'), 'utf8'));
    const m = modes?.[engine];
    return m === 'workspace' || m === 'open' ? m : 'phone';
  } catch { return 'phone'; }
};

// ~/.agent-gateway/agent-hooks.json records what WE registered. A codex
// reinstall wipes ~/.codex/hooks.json; without this heal every spawn until
// the app's next refresh would run with the hook silently gone — and in
// "phone" posture that means sandbox-free with nobody asking. Re-add the
// recorded command before spawning; other entries are never touched.
const hookIntent = (engine) => {
  try {
    return JSON.parse(readFileSync(
      join(homedir(), '.agent-gateway', 'agent-hooks.json'), 'utf8'))?.[engine] ?? null;
  } catch { return null; }
};

const ourEntryPresent = (settingsPath) => {
  try { return readFileSync(settingsPath, 'utf8').includes('agent_pre_tool.py'); }
  catch { return false; }
};

const healAgentHooks = (engine, settingsPath) => {
  try {
    const rec = hookIntent(engine);
    if (!rec?.enabled || typeof rec.command !== 'string') return;
    let settings = {};
    try { settings = JSON.parse(readFileSync(settingsPath, 'utf8')) ?? {}; } catch {}
    const hooks = (settings.hooks && typeof settings.hooks === 'object') ? settings.hooks : {};
    let dirty = false;
    for (const name of Array.isArray(rec.events) ? rec.events : ['PreToolUse']) {
      const event = Array.isArray(hooks[name]) ? hooks[name] : [];
      const present = event.some(g => Array.isArray(g?.hooks)
        && g.hooks.some(h => String(h?.command ?? '').includes('agent_pre_tool.py')));
      if (!present) {
        event.push({ matcher: '*',
          hooks: [{ type: 'command', command: rec.command, timeout: rec.timeout ?? 360 }] });
        hooks[name] = event;
        dirty = true;
      }
    }
    if (dirty) {
      settings.hooks = hooks;
      mkdirSync(dirname(settingsPath), { recursive: true });
      writeFileSync(settingsPath, JSON.stringify(settings, null, 2) + '\n');
    }
  } catch { /* heal is best-effort — never break the spawn */ }
};

const MAX_BODY = 1 << 20;
const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED']);
const STALL_MS = Number(process.env.CLI_BRIDGE_STALL_MS ?? 180_000);

const fail = (code, status) => { const e = new Error(code); e.code = code; e.status = status; throw e; };

/** Vendor installer locations that never land on PATH or npm's shim dir —
 * OpenAI Codex: %LOCALAPPDATA%\OpenAI\Codex\bin\<hash>\codex.exe;
 * Anthropic Claude native: %USERPROFILE%\.local\bin\claude.exe. */
const wellKnownCliPaths = (name) => {
  if (process.platform !== 'win32') return [];
  const out = [];
  if (name === 'codex' && process.env.LOCALAPPDATA) {
    const root = join(process.env.LOCALAPPDATA, 'OpenAI', 'Codex', 'bin');
    try {
      for (const e of readdirSync(root, { withFileTypes: true })) {
        const p = join(root, e.name, 'codex.exe');
        if (e.isDirectory() && existsSync(p)) out.push(p);
      }
    } catch {}
  }
  if (name === 'claude' && process.env.USERPROFILE) {
    const p = join(process.env.USERPROFILE, '.local', 'bin', 'claude.exe');
    if (existsSync(p)) out.push(p);
  }
  return out;
};

/** Resolve a logical CLI name to an executable command + fixed arg prefix.
 * POSIX shims and real executables run directly; npm ".cmd" shims on Windows
 * are unwrapped to `node <entry.js>` so spawn keeps a clean argv and never
 * needs a shell — instruction text cannot break out through cmd quoting. */
const resolveBin = (name) => {
  try { execFileSync(name, ['--version'], { stdio: 'ignore' }); return { cmd: name, pre: [] }; } catch {}
  for (const p of wellKnownCliPaths(name)) {
    try { execFileSync(p, ['--version'], { stdio: 'ignore' }); return { cmd: p, pre: [] }; } catch {}
  }
  if (process.platform === 'win32') {
    const pathDirs = (process.env.PATH ?? process.env.Path ?? '')
      .split(';').map((s) => s.trim()).filter(Boolean);
    const candidates = [
      ...(process.env.APPDATA ? [join(process.env.APPDATA, 'npm', name + '.cmd')] : []),
      ...pathDirs.map((d) => join(d, name + '.cmd')),
    ];
    for (const shim of candidates) {
      try {
        const src = readFileSync(shim, 'utf8');
        const m = src.match(/"%~?dp0%?\\([^"\r\n]+?\.js)"/i);
        if (m) return { cmd: process.execPath, pre: [join(dirname(shim), m[1])] };
        return { cmd: 'cmd.exe', pre: ['/d', '/s', '/c', shim] };
      } catch {}
    }
  }
  return { cmd: name, pre: [] };
};

// Turn-end error translation — the CLI's raw error text stays in task.output,
// but the phone also gets one actionable line (auth → relogin, long prompt →
// shorten, connection → resend). Limit/billing text is left as-is because it
// usually already says when the quota resets.
const friendlyCliError = (text) => {
  const low = String(text ?? '').toLowerCase();
  if (!low) return null;
  if (['login expired', 'not logged in', 'please run /login', 'invalid api key',
       'invalid bearer token', 'oauth token has expired', 'authentication_failed',
       'invalid credentials'].some(k => low.includes(k))) {
    return '登录态失效：请在这台电脑上重新登录该 CLI（如 /login），然后重发消息。';
  }
  if (low.includes('prompt is too long') || low.includes('request too large')) {
    return '本轮内容太长：缩短消息或开新会话再试。';
  }
  if (['econnreset', 'tls handshake eof', 'etimedout', 'econnrefused',
       'fetch failed', 'connection lost'].some(k => low.includes(k))) {
    return '连接中断（网络问题，不是登录问题）：重发即可，反复失败就重启任务。';
  }
  return null;
};

// ---- stock-CLI engine adapters -------------------------------------------------

const ENGINES = {
  claude: {
    argv: (instruction, sessionId, task) => {
      // Same reinstall self-heal as codex: a wiped ~/.claude/settings.json
      // gets our recorded entries back before the spawn.
      healAgentHooks('claude', join(homedir(), '.claude', 'settings.json'));
      return [
        '-p', instruction, '--output-format', 'stream-json', '--verbose',
        ...(SKIP === 'off' ? [] : ['--dangerously-skip-permissions']),
        ...(task?.model ?? MODEL ? ['--model', task?.model ?? MODEL] : []),
        ...(sessionId ? ['--resume', sessionId] : []),
      ];
    },
    bin: 'claude',
    /** stream-json: init carries session_id; assistant rows carry text blocks;
     * result carries the terminal verdict. */
    onEvent: (row, task, addMessage) => {
      if (row.type === 'system' && row.subtype === 'init' && typeof row.session_id === 'string') {
        task.nativeSessionId = row.session_id;
      }
      if (row.type === 'assistant' && row.message && Array.isArray(row.message.content)) {
        const text = row.message.content
          .filter((b) => b && b.type === 'text' && typeof b.text === 'string')
          .map((b) => b.text).join('\n');
        if (text.trim()) addMessage('assistant', text);
      }
      if (row.type === 'result') {
        if (typeof row.session_id === 'string') task.nativeSessionId = row.session_id;
        if (typeof row.result === 'string' && row.result.trim()) task.output = row.result;
        const ok = row.subtype === 'success' && row.is_error !== true;
        if (!ok) {
          const friendly = friendlyCliError(task.output);
          if (friendly) addMessage('assistant', friendly);
        }
        return ok ? 'SUCCEEDED' : 'FAILED';
      }
      return null;
    },
  },
  codex: {
    argv: (instruction, sessionId, task) => {
      // Owner-selected posture (see agentMode). "phone" → danger-full-access:
      // the phone IS the sandbox — every tool call reaches the PreToolUse
      // hook which asks through the gateway, and allow/deny is the real
      // decision (a workspace sandbox denies OS-level before any ask could
      // happen, and a hook answer cannot lift it). "workspace" →
      // workspace-write: inside auto, outside fails closed, no asks.
      // "open" → danger-full-access with the hook abstaining: nothing asks.
      // 接管关 → stock-ish confinement: the mode picker only matters while
      // OUR hook is the one deciding. A hook that is off + full-access argv
      // would be silent full access — the worst combination, so off always
      // means workspace-write here. "On" = intent says enabled (written by
      // install.py) OR our entry is actually present (pre-intent installs).
      // --skip-permissions is an explicit CLI override: 'on' pins the 'open'
      // posture, 'off' pins 'workspace', unset defers to agent-modes.json.
      const codexHooks = join(homedir(), '.codex', 'hooks.json');
      const takenOver = hookIntent('codex')?.enabled === true || ourEntryPresent(codexHooks);
      const mode = SKIP === 'on' ? 'open'
        : SKIP === 'off' ? 'workspace'
        : takenOver ? agentMode('codex') : 'workspace';
      healAgentHooks('codex', codexHooks);
      const sandbox = mode === 'workspace' ? 'workspace-write' : 'danger-full-access';
      return [
        ...(sessionId
          ? ['exec', 'resume', sessionId, instruction, '--json', '--skip-git-repo-check',
             '-c', `sandbox_mode="${sandbox}"`]
          : ['exec', instruction, '--json', '-C', PROJECT_DIR,
             '-s', sandbox, '--skip-git-repo-check']),
        // on-request keeps policy-flagged prompts routable to the
        // PermissionRequest hook (phone decides); bypass-hook-trust because
        // headless exec cannot answer trust prompts and we own the hooks.
        ...(mode === 'phone' ? ['-c', 'approval_policy="on-request"'] : []),
        '--dangerously-bypass-hook-trust',
        '--model', (task?.model ?? MODEL) || CODEX_DEFAULT_MODEL,
        ...(task?.effort ? ['-c', `model_reasoning_effort=${task.effort}`] : []),
      ];
    },
    bin: 'codex',
    /** codex exec --json: thread.started carries thread_id; item.completed rows
     * carry agent_message text; turn.completed finishes the turn. */
    onEvent: (row, task, addMessage) => {
      if (row.type === 'thread.started' && typeof row.thread_id === 'string') {
        task.nativeSessionId = row.thread_id;
      }
      const item = row.item;
      if ((row.type === 'item.completed' || row.type === 'item.done') && item) {
        if ((item.type === 'agent_message' || item.type === 'assistant_message' || item.type === 'message')
            && typeof item.text === 'string' && item.text.trim()) {
          addMessage('assistant', item.text);
        }
      }
      if (row.type === 'turn.completed') return 'SUCCEEDED';
      if (row.type === 'turn.failed' || row.type === 'error') {
        const raw = row.error?.message ?? row.message ?? task.output ?? '';
        const friendly = friendlyCliError(raw);
        if (friendly) addMessage('assistant', friendly);
        return 'FAILED';
      }
      return null;
    },
  },
  pi: {
    argv: (instruction, sessionId, task) => [
      '-p', instruction, '--mode', 'json',
      '--session-dir', join(STATE_DIR, 'pi-sessions'),
      ...(sessionId ? ['--session', sessionId] : []),
      ...(task?.model ?? MODEL ? ['--model', task?.model ?? MODEL] : [])],
    bin: 'pi',
    /** pi -p --mode json: `session` carries the id; assistant message_end rows
     * carry completed text blocks; agent_settled ends the turn. */
    onEvent: (row, task, addMessage) => {
      if (row.type === 'session' && typeof row.id === 'string') {
        task.nativeSessionId = row.id;
      }
      if ((row.type === 'message_end' || row.type === 'message') && row.message?.role === 'assistant') {
        // Provider/auth errors end the turn with an empty assistant message —
        // surface the reason and fail honestly instead of a blank SUCCEEDED.
        if (row.message.stopReason === 'error') {
          if (!task.engineError) {
            task.engineError = true;
            addMessage('assistant', row.message.errorMessage ?? 'provider error', 'FAILED');
          }
          return 'FAILED';
        }
        const text = (row.message.content ?? [])
          .filter((b) => b.type === 'text' && typeof b.text === 'string' && b.text.trim())
          .map((b) => b.text).join('\n');
        if (text.trim() && row.type === 'message_end') addMessage('assistant', text);
      }
      if (row.type === 'agent_settled') return row.aborted ? 'FAILED' : 'SUCCEEDED';
      return null;
    },
  },
};

// ---- local CLI history reader (read-only) -----------------------------------
// `host.mjs history --engine codex|claude --root <dir> [--file <session>]`
// Lists recent vendor session files or renders one file as normalized
// {role, text} messages. Codex rollouts are zstd-compressed JSONL; claude
// transcripts are plain JSONL. One JSON document on stdout; needs no token.
const decodeSession = (file) => file.endsWith('.zst')
  ? zstdDecompressSync(readFileSync(file)).toString('utf8')
  : readFileSync(file, 'utf8');

/** Codex `response_item.message` rows (user→input_text, assistant→output_text)
 * and claude `type:user|assistant` rows both map to {role, text}. Messages whose
 * whole body is a `<…>` snapshot (desktop env-context/app-event injections) are
 * noise for reading — dropped. */
const transcriptMessages = (file) => {
  const out = [];
  for (const line of decodeSession(file).split('\n')) {
    if (!line.trim()) continue;
    let row; try { row = JSON.parse(line); } catch { continue; }
    const p = row.payload;
    if (p?.type === 'message' && (p.role === 'user' || p.role === 'assistant')) {
      const texts = (p.content ?? [])
        .filter(c => typeof c?.text === 'string').map(c => c.text);
      const text = texts.join('\n').trim();
      if (text && !text.startsWith('<')) out.push({ role: p.role, text });
    } else if ((row.type === 'user' || row.type === 'assistant') && !row.isSidechain) {
      const c = row.message?.content;
      const text = (typeof c === 'string' ? [c] : (c ?? [])
        .filter(b => b?.type === 'text' && typeof b.text === 'string').map(b => b.text))
        .join('\n').trim();
      if (text && !text.startsWith('<')) out.push({ role: row.type, text });
    }
  }
  return out;
};

const historyFiles = (engine, root) => {
  const out = [];
  const walk = (dir, depth) => {
    if (depth > 6) return;
    let items; try { items = readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const it of items) {
      const p = join(dir, it.name);
      if (it.isDirectory()) walk(p, depth + 1);
      else if (engine === 'codex' ? /^rollout-.*\.jsonl\.zst$/.test(it.name)
          : depth === 1 && /\.jsonl$/.test(it.name)) out.push(p);
    }
  };
  if (existsSync(root)) walk(root, 0);
  return out;
};

if (cmd === 'history') {
  const root = opt.root ?? '';
  const engineName = opt.engine ?? '';
  if (!['codex', 'claude'].includes(engineName) || !root) {
    console.log(JSON.stringify({ ok: false, error: 'history requires --engine codex|claude --root <dir>' }));
    process.exit(2);
  }
  try {
    if (opt.file) {
      const file = resolve(opt.file);
      if (!file.startsWith(resolve(root) + sep)) fail('FILE_OUTSIDE_ROOT', 400);
      const all = transcriptMessages(file);
      const limit = Math.min(Math.max(Number(opt.limit) || 60, 1), 200);
      const messages = all.slice(-limit).map(m => ({ role: m.role, text: m.text.slice(0, 3000) }));
      console.log(JSON.stringify({ ok: true, file, total: all.length, messages,
        truncated: all.length > limit }));
    } else {
      const limit = Math.min(Math.max(Number(opt.limit) || 10, 1), 50);
      const files = historyFiles(engineName, root)
        .map(f => { try { return { f, at: lstatSync(f).mtimeMs }; } catch { return null; } })
        .filter(Boolean).sort((a, b) => b.at - a.at).slice(0, limit);
      const sessions = files.map(({ f, at }) => {
        let title = '';
        try {
          title = transcriptMessages(f)
            .find(m => m.role === 'user' && !m.text.startsWith('# AGENTS.md'))?.text.slice(0, 80) ?? '';
        }
        catch { }
        const id = (basename(f).match(/([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})/i) ?? [])[1]
          ?? basename(f);
        return { id, file: f, at: new Date(at).toISOString(), title };
      });
      console.log(JSON.stringify({ ok: true, sessions }));
    }
  } catch (error) {
    console.log(JSON.stringify({ ok: false, error: String(error?.code ?? error?.message ?? error) }));
    process.exit(2);
  }
  process.exit(0);
}

if (!ENGINES[ENGINE]) { console.error(`unknown --engine ${ENGINE}`); process.exit(2); }
if (!STATE_DIR || !PORT || !PROJECT_DIR) { console.error('serve requires --state --port --project'); process.exit(2); }
if (TOKEN.length < 24) { console.error('WORKER_LOCAL_TOKEN must be at least 24 chars'); process.exit(2); }
if (cmd !== 'serve') { console.error('usage: host.mjs serve ...'); process.exit(2); }

const engine = ENGINES[ENGINE];
// An explicit --executable still goes through the .cmd shim unwrap so npm
// shims picked in the UI spawn as `node <entry.js>`, never via a shell.
const resolvePath = (p) => {
  if (process.platform === 'win32' && /\.(cmd|bat)$/i.test(p)) {
    try {
      const src = readFileSync(p, 'utf8');
      const m = src.match(/"%~?dp0%?\\([^"\r\n]+?\.js)"/i);
      if (m) return { cmd: process.execPath, pre: [join(dirname(p), m[1])] };
      return { cmd: 'cmd.exe', pre: ['/d', '/s', '/c', p] };
    } catch {}
  }
  return { cmd: p, pre: [] };
};
const resolved = EXECUTABLE ? resolvePath(EXECUTABLE) : resolveBin(engine.bin);
let engineVersion = 'unknown';
try {
  engineVersion = execFileSync(resolved.cmd, [...resolved.pre, '--version'], { encoding: 'utf8' }).trim();
} catch { console.error(`${EXECUTABLE || engine.bin} not runnable`); process.exit(2); }

// ---- durable ledger ------------------------------------------------------------

mkdirSync(STATE_DIR, { recursive: true });
const LEDGER = join(STATE_DIR, 'ledger.json');
const ledger = existsSync(LEDGER)
  ? JSON.parse(readFileSync(LEDGER, 'utf8'))
  : { storeId: randomUUID(), tasks: {}, requests: {} };
if (!ledger.storeId) ledger.storeId = randomUUID();
if (!ledger.tasks) ledger.tasks = {};
if (!ledger.requests) ledger.requests = {};

const save = () => {
  const tmp = LEDGER + '.tmp';
  writeFileSync(tmp, JSON.stringify(ledger));
  renameSync(tmp, LEDGER);
};

// A task left RUNNING by a dead process is unrecoverable — mark it so the
// phone sees a terminal verdict instead of a frozen spinner.
for (const t of Object.values(ledger.tasks)) {
  if (!TERMINAL.has(t.state)) { t.state = 'FAILED'; t.reasonCode = 'HOST_RESTARTED'; t.quiescent = true; }
}
save();

const live = new Map(); // taskId -> ChildProcess
const now = () => new Date().toISOString();
const msgId = () => 'msg-' + randomUUID();

const task = (id) => ledger.tasks[id] ?? fail('REQUEST_NOT_FOUND', 404);

const addMessage = (t, role, text, requestKey, status) => {
  const at = now();
  t.publicMessages.push({
    messageId: msgId(), sequence: String(t.publicMessages.length + 1), revision: '0',
    role, text, textTruncated: false,
    status: status ?? (role === 'user' ? 'CONSUMED' : 'COMPLETED'),
    occurredAt: at, updatedAt: at,
    ...(requestKey ? { sourceRequestKey: requestKey } : {}),
    ...(t.nativeTurnId ? { nativeTurnId: t.nativeTurnId } : {}),
    textComplete: true,
  });
};

/** Spawn the stock CLI for one turn; stream its events into the task row. */
// The bridge's own bearer must never reach the spawned CLI — children get the
// user's normal environment minus worker credentials.
const CHILD_ENV = { ...process.env };
delete CHILD_ENV.WORKER_LOCAL_TOKEN;
const dispatch = (t, instruction, requestKey) => {
  const argv = engine.argv(instruction, t.nativeSessionId, t);
  const child = spawn(resolved.cmd, [...resolved.pre, ...argv], {
    cwd: PROJECT_DIR,
    env: CHILD_ENV,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  live.set(t.taskId, child);
  t.state = 'RUNNING';
  t.launchState = 'RUNNING';
  t.quiescent = false;
  t.nativeTurnId = randomUUID();
  t.currentCommandId = requestKey;
  t.updatedAt = now();
  addMessage(t, 'user', instruction, requestKey);
  let lastActivity = Date.now();
  const watchdog = STALL_MS > 0 ? setInterval(() => {
    if (Date.now() - lastActivity < STALL_MS) return;
    t.stalled = true;
    try { child.kill('SIGTERM'); } catch {}
    setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 3000).unref();
  }, 15_000) : null;
  watchdog?.unref();
  let stderr = '';
  child.stderr.on('data', (d) => { lastActivity = Date.now(); stderr += d; if (stderr.length > 8192) stderr = stderr.slice(-8192); });
  let buf = '';
  child.stdout.on('data', (d) => {
    lastActivity = Date.now();
    buf += d.toString();
    let i;
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i); buf = buf.slice(i + 1);
      if (!line.trim()) continue;
      let row;
      try { row = JSON.parse(line); } catch { continue; }
      const verdict = engine.onEvent(row, t, (role, text, status) => addMessage(t, role, text, requestKey, status));
      if (verdict) t.pendingVerdict = verdict;
      t.updatedAt = now();
    }
    if (buf.length > (1 << 20)) buf = buf.slice(-(1 << 20));
  });
  child.on('close', (code) => {
    if (watchdog) clearInterval(watchdog);
    live.delete(t.taskId);
    t.quiescent = true;
    t.updatedAt = now();
    if (t.state === 'CANCELLED') { save(); return; }
    if (!t.output) {
      t.output = t.publicMessages.filter((m) => m.role === 'assistant').map((m) => m.text).join('\n');
    }
    if (t.stalled) {
      const note = `引擎 ${Math.round(STALL_MS / 60000)} 分钟无输出，已终止（多为网络或代理问题）`;
      t.state = 'FAILED'; t.reasonCode = 'ENGINE_STALLED';
      if (!t.output) t.output = note;
      addMessage(t, 'assistant', note, undefined, 'FAILED');
    } else if (t.pendingVerdict === 'SUCCEEDED' || (code === 0 && t.pendingVerdict !== 'FAILED')) {
      t.state = 'SUCCEEDED'; t.reasonCode = '';
    } else {
      t.state = 'FAILED';
      t.reasonCode = t.pendingVerdict === 'FAILED' ? 'ENGINE_FAILED' : 'ENGINE_EXIT_' + code;
      const tail = stderr.trim().split('\n').filter(Boolean).slice(-20).join('\n');
      if (tail && !t.output) t.output = tail;
      if (tail && !t.publicMessages.some((m) => m.role === 'assistant')) addMessage(t, 'assistant', tail, undefined, 'FAILED');
    }
    t.pendingVerdict = undefined;
    save();
  });
  save();
};

const stopTask = (t) => {
  const child = live.get(t.taskId);
  if (child) { try { child.kill('SIGTERM'); } catch {} setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 3000).unref(); }
  if (!TERMINAL.has(t.state)) { t.state = 'CANCELLED'; t.reasonCode = 'STOPPED_BY_OWNER'; t.quiescent = true; }
  t.updatedAt = now();
};

const facts = (t) => ({
  schemaVersion: 'worker-local/1',
  taskId: t.taskId, projectId: t.projectId, engineId: t.engineId, engineVersion,
  profileId: 'MANAGED', storeId: ledger.storeId,
  state: t.state, launchState: t.launchState ?? t.state, reasonCode: t.reasonCode ?? '',
  quiescent: t.quiescent === true,
  generation: t.generation, segmentId: t.segmentId,
  nativeSessionId: t.nativeSessionId ?? null, nativeTurnId: t.nativeTurnId ?? null,
  currentCommandId: t.currentCommandId ?? null,
  createdAt: t.createdAt, updatedAt: t.updatedAt,
  goal: t.instruction, displayName: t.instruction.split(/\r?\n/, 1)[0].slice(0, 128),
  instruction: t.instruction,
  output: t.output ?? '',
  result: t.output ?? '',
  capabilities: { stop: live.has(t.taskId), pause: false, resume: false,
                  steer: live.has(t.taskId), followUp: t.quiescent === true || TERMINAL.has(t.state) },
  configuredModel: 'vendor-cli',
});

const receipt = (r) => ({
  schemaVersion: 'worker-local-op/1',
  requestKey: r.requestKey, taskId: r.taskId ?? null, operation: r.operation ?? 'LAUNCH',
  // Pi op replies carry the request kind; the phone's admission check reads
  // source.kind to confirm a message-shaped request was the one observed.
  kind: r.operation ?? 'LAUNCH',
  state: r.state, result: r.result ?? null, code: r.code ?? null,
});

const digest = (v) => createHash('sha256').update(JSON.stringify(v)).digest('hex');

// ---- artifacts --------------------------------------------------------------
// Mirrors the pi host's seal-on-quiescent contract without git: a bounded
// inventory snapshot at task creation is diffed against the current tree.
const ART_DIR = join(STATE_DIR, 'artifacts');
const SKIP_DIRS = new Set(['.git', 'node_modules', '.idea', '.vscode', 'target', 'dist', 'build', '__pycache__', '.next', '.cache', '.Trash']);
const SECRET_RE = /(?:sk-[A-Za-z0-9_-]{20,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:api[_-]?key|password|access[_-]?token)\s*[:=]\s*["']?[^\s"']{8,})/i;
const knownSecrets = Object.entries(process.env).filter(([k, v]) => /(?:KEY|PASSWORD|TOKEN|SECRET)$/.test(k) && v && v.length >= 8).map(([, v]) => v);

const inventory = () => {
  const root = realpathSync(PROJECT_DIR);
  const map = new Map();
  const walk = (dir, depth) => {
    if (depth > 6 || map.size > 2000) return;
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      const p = join(dir, e.name);
      const rel = relative(root, p);
      if (e.isDirectory()) { if (!SKIP_DIRS.has(e.name)) walk(p, depth + 1); continue; }
      if (!e.isFile()) continue;
      try { const s = lstatSync(p); map.set(rel, { s: s.size, m: s.mtimeMs }); } catch {}
    }
  };
  try { walk(root, 0); } catch {}
  return map;
};

const hashBytes = (b) => createHash('sha256').update(b).digest('hex');

const sealArtifacts = (t) => {
  if (t.artifactManifest) return t.artifactManifest;
  if (!t.quiescent) return { items: [], reasonCode: 'STOP_TO_SEAL_RESULTS' };
  const root = realpathSync(PROJECT_DIR);
  const current = inventory();
  const before = new Map((t.baseline ?? []).map((r) => [r.p, r]));
  const names = new Map();
  for (const [rel, cur] of current) {
    const old = before.get(rel);
    if (!old || old.s !== cur.s || Math.abs(old.m - cur.m) > 0.001) names.set(rel, old ? 'M' : 'A');
  }
  for (const rel of before.keys()) if (!current.has(rel)) names.set(rel, 'D');
  const items = []; let total = 0;
  for (const [rel, status] of [...names.entries()].sort().slice(0, 100)) {
    const row = { artifactId: 'file-' + hashBytes(rel + '\0' + t.segmentId), path: rel };
    items.push(row);
    if (/[\x00-\x1f\x7f]/.test(rel)) { row.status = 'WITHHELD_PATH'; continue; }
    if (status === 'D') { row.status = 'DELETED'; continue; }
    const candidate = resolve(root, rel);
    const relCheck = relative(root, candidate);
    if (relCheck === '..' || relCheck.startsWith('..' + sep) || relCheck === '') { row.status = 'WITHHELD_PATH'; continue; }
    try {
      const s = lstatSync(candidate);
      if (!s.isFile() || s.isSymbolicLink() || !realpathSync(candidate).startsWith(root + sep)) { row.status = 'WITHHELD_PATH'; continue; }
      if (s.size > 524288 || total + s.size > 4194304) { row.status = 'WITHHELD_SIZE'; continue; }
      const bytes = readFileSync(candidate);
      const after = lstatSync(candidate);
      if (after.mtimeMs !== s.mtimeMs || after.size !== s.size || after.ino !== s.ino) { row.status = 'WITHHELD_CHANGED'; continue; }
      const text = bytes.toString('utf8');
      if (knownSecrets.some((x) => text.includes(x)) || SECRET_RE.test(text)) { row.status = 'WITHHELD_SECRET'; continue; }
      total += bytes.length;
      row.status = 'AVAILABLE'; row.size = bytes.length; row.sha256 = hashBytes(bytes); row.mediaType = 'application/octet-stream';
      mkdirSync(join(ART_DIR, t.taskId), { recursive: true });
      writeFileSync(join(ART_DIR, t.taskId, row.artifactId), bytes);
    } catch { row.status = 'WITHHELD_UNREADABLE'; }
  }
  const manifest = { items, bounded: names.size > items.length, coverage: 'CHANGED_OWNED_FILES', segmentId: t.segmentId };
  t.artifactManifest = manifest; save(); return manifest;
};

// Display name of the provider behind the wrapped CLI — read-only for the
// phone footnote. cc-switch (or the user's own config files) stays the owner;
// this only reports which provider the next spawn would inherit.
const currentProvider = () => {
  const hostHint = () => {
    try {
      if (ENGINE === 'claude') {
        const url = JSON.parse(readFileSync(join(homedir(), '.claude', 'settings.json'), 'utf8'))?.env?.ANTHROPIC_BASE_URL;
        if (url) return new URL(url).host;
        return 'Anthropic 官方';
      }
      if (ENGINE === 'codex') {
        const toml = readFileSync(join(homedir(), '.codex', 'config.toml'), 'utf8');
        const active = toml.match(/model_provider\s*=\s*"([^"]+)"/)?.[1];
        const section = active && toml.match(new RegExp(`\\[model_providers\\.${active.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\][^\\[]*`))?.[0];
        const url = (section ?? toml).match(/base_url\s*=\s*"([^"]+)"/)?.[1];
        if (url) return new URL(url).host;
        const mode = JSON.parse(readFileSync(join(homedir(), '.codex', 'auth.json'), 'utf8'))?.auth_mode;
        if (mode === 'chatgpt') return 'ChatGPT 账户';
        if (mode === 'apikey') return 'OpenAI API';
      }
    } catch { /* no readable config — provider stays unknown */ }
    return '';
  };
  const host = hostHint();
  const app = ENGINE === 'claude' || ENGINE === 'codex' ? ENGINE : '';
  if (app) {
    try {
      const db = join(homedir(), '.cc-switch', 'cc-switch.db');
      if (existsSync(db)) {
        const name = execFileSync('sqlite3', [db, `SELECT name FROM providers WHERE app_type='${app}' AND is_current=1 LIMIT 1`],
          { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 3000 }).trim();
        if (name && name !== 'default') return name;
      }
    } catch { /* cc-switch absent or schema changed — fall through */ }
  }
  return host;
};

const preflight = () => {
  const provider = currentProvider();
  return {
    schemaVersion: 'worker-local-preflight/1',
    projectId: PROJECT_ID, engineId: ENGINE, engineVersion,
    optionsRevision: digest([PROJECT_ID, ENGINE, engineVersion]),
    profileId: 'MANAGED', executionProfile: 'vendor-cli-v1',
    loadsUserExtensions: false, headRevision: null, hasUncommittedChanges: false,
    workingCopyPolicy: 'COMMITTED_BASE_ONLY',
    executionModes: ['WORKSPACE_TASK'], policyPreset: null,
    models: MODEL_CATALOG,
    ...(provider ? { provider } : {}),
  };
};

const identity = () => ({
  schemaVersion: 'worker-host/1', storeId: ledger.storeId, storeGuard: 'HEADER_V1',
  bootId: BOOT_ID, currentAvailability: 'AVAILABLE',
  engine: { id: ENGINE, version: engineVersion },
  skipPermissions: ENGINE === 'codex' ? SKIP === 'on' : SKIP !== 'off',
  contracts: ['worker-host/1', 'worker-local/1'],
  supportedPaths: ['/local/v1', '/worker-host/v1/manifest'],
  capabilities: { items: [] },
});
const BOOT_ID = randomUUID();

// ---- HTTP ---------------------------------------------------------------------

const routes = (method, seg, taskId, body, query) => {
  // /local/v1/* and /worker-host/v1/* both arrive with the full path segments.
  if (seg[0] === 'worker-host' && seg[1] === 'v1') {
    if (method === 'GET' && seg[2] === 'manifest') return identity();
    if (method === 'GET' && seg[2] === 'capabilities') return { ...identity(), items: [] };
    fail('NOT_FOUND', 404);
  }
  if (seg[0] !== 'local' || seg[1] !== 'v1') fail('NOT_FOUND', 404);
  const rest = seg.slice(2);
  if (method === 'GET' && rest[0] === 'identity') return identity();
  if (method === 'GET' && rest[0] === 'projects' && rest.length === 1) {
    return { ...identity(), items: [{ projectId: PROJECT_ID, displayName: opt['project-name'] ?? PROJECT_ID,
      availability: 'AVAILABLE', headRevision: null, hasUncommittedChanges: false, kind: 'WORKSPACE' }] };
  }
  if (method === 'GET' && rest[0] === 'engines' && rest.length === 1) {
    return { ...identity(), items: [{ engineId: ENGINE, engineVersion, version: engineVersion,
      availability: 'AVAILABLE', profileId: 'MANAGED', sourceKind: 'cli',
      capabilities: { executionModes: ['WORKSPACE_TASK'], steer: true, followUp: true,
                      stop: true, pause: false, resume: false } }] };
  }
  if (method === 'GET' && rest[0] === 'tasks' && rest.length === 1) {
    return { ...identity(), items: Object.values(ledger.tasks).map((t) => ({
      taskId: t.taskId, projectId: t.projectId, engineId: t.engineId,
      engineVersion, state: t.state })) };
  }
  if (method === 'GET' && rest[0] === 'task-requests' && rest.length === 2) {
    const r = ledger.requests[rest[1]];
    if (!r || r.operation !== 'LAUNCH') fail('REQUEST_NOT_FOUND', 404);
    return receipt(r);
  }
  if (method === 'POST' && rest[0] === 'task-preflights' && rest.length === 1) {
    if (!body || body.projectId !== PROJECT_ID || body.engineId !== ENGINE) fail('INVALID_SCOPE', 400);
    return preflight();
  }
  if (method === 'POST' && rest[0] === 'tasks' && rest.length === 1) {
    if (!body || typeof body.requestKey !== 'string' || body.projectId !== PROJECT_ID
        || body.engineId !== ENGINE || typeof body.instruction !== 'string' || !body.instruction.trim()) {
      fail('INVALID_REQUEST', 400);
    }
    if (body.optionsRevision !== undefined && body.optionsRevision !== preflight().optionsRevision) {
      fail('PREFLIGHT_CHANGED', 409);
    }
    if (body.model !== undefined && !MODEL_CATALOG.some(m => m.modelRef === body.model)) fail('MODEL_REF_UNKNOWN', 400);
    if (body.effort !== undefined && !MODEL_CATALOG.some(m => m.modelRef === (body.model ?? MODEL) && (m.efforts ?? []).includes(body.effort))) fail('MODEL_EFFORT_UNSUPPORTED', 400);
    // Adopt an existing vendor-CLI session (codex rollout / claude transcript):
    // the task lands quiescent with the session's nativeSessionId preset and
    // its messages imported, so the first follow-up runs through the engine's
    // resume path (codex `exec resume`, claude `--resume`) instead of a fresh
    // session. The file must live under the engine's session root; for claude
    // it must also belong to this project (session slug = cwd slug).
    let adoptId = null, adoptedMessages = null;
    if (body.adoptSessionId !== undefined) {
      if (typeof body.adoptSessionId !== 'string' || !/^[0-9a-f-]{6,64}$/i.test(body.adoptSessionId))
        fail('INVALID_REQUEST', 400);
      const root = ENGINE === 'codex' ? join(homedir(), '.codex', 'sessions')
        : ENGINE === 'claude' ? join(homedir(), '.claude', 'projects') : null;
      if (!root) fail('ADOPT_UNSUPPORTED', 400);
      const file = historyFiles(ENGINE, root).find(f => basename(f).includes(body.adoptSessionId));
      if (!file) fail('ADOPT_SESSION_NOT_FOUND', 404);
      if (ENGINE === 'claude'
          && basename(dirname(file)) !== PROJECT_DIR.replace(/[:\\/]/g, '-'))
        fail('ADOPT_WRONG_PROJECT', 409);
      adoptId = body.adoptSessionId;
      const imported = transcriptMessages(file).slice(-300);
      const at = new Date(lstatSync(file).mtimeMs).toISOString();
      adoptedMessages = imported.map((m, i) => ({
        messageId: 'adopted-' + (i + 1), sequence: String(i + 1), revision: '0',
        role: m.role, text: m.text.slice(0, 3000),
        status: m.role === 'user' ? 'CONSUMED' : 'COMPLETED',
        occurredAt: at, updatedAt: at,
      }));
    }
    const prior = ledger.requests[body.requestKey];
    if (prior) {
      if (prior.hash !== digest({ i: body.instruction, p: body.projectId, e: body.engineId, m: body.model, f: body.effort })) {
        fail('IDEMPOTENCY_PAYLOAD_MISMATCH', 409);
      }
      return receipt(prior);
    }
    const t = {
      taskId: 'native-' + randomUUID(), requestKey: body.requestKey,
      projectId: PROJECT_ID, engineId: ENGINE,
      state: adoptId ? 'SUCCEEDED' : 'PREPARING',
      launchState: adoptId ? 'COMPLETED' : 'PREPARING',
      reasonCode: adoptId ? 'ADOPTED' : '', quiescent: adoptId !== null,
      generation: randomUUID(), segmentId: randomUUID(),
      nativeSessionId: adoptId, nativeTurnId: null, currentCommandId: body.requestKey,
      createdAt: now(), updatedAt: now(),
      instruction: body.instruction, output: '',
      publicMessages: adoptedMessages ?? [], inputRequests: {},
      ...(body.model !== undefined ? { model: body.model } : {}),
      ...(body.effort !== undefined ? { effort: body.effort } : {}),
      baseline: [...inventory()].map(([p, v]) => ({ p, s: v.s, m: v.m })),
    };
    ledger.tasks[t.taskId] = t;
    ledger.requests[body.requestKey] = {
      requestKey: body.requestKey, taskId: t.taskId, operation: 'LAUNCH',
      state: 'ACCEPTED', result: { taskId: t.taskId, engineId: ENGINE, engineVersion, state: 'ACCEPTED' },
      hash: digest({ i: body.instruction, p: body.projectId, e: body.engineId, m: body.model, f: body.effort }),
    };
    save();
    if (!adoptId) dispatch(t, body.instruction, body.requestKey);
    return { ...receipt(ledger.requests[body.requestKey]), taskId: t.taskId, engineId: ENGINE, engineVersion, state: 'ACCEPTED' };
  }
  if (rest[0] === 'tasks' && rest.length >= 2) {
    const t = task(rest[1]);
    const sub = rest[2];
    if (method === 'GET' && rest.length === 2) return facts(t);
    if (method === 'GET' && rest.length === 3) {
      if (sub === 'output') {
        const text = t.output || t.publicMessages.filter((m) => m.role === 'assistant').map((m) => m.text).join('\n');
        return { text, coverage: 'BOUNDED_PUBLIC_MESSAGES' };
      }
      if (sub === 'messages') {
        // Rows persisted by older builds lack the native-messages/1 fields —
        // normalise at the edge so every served row validates on the phone.
        const all = t.publicMessages.map((m, i) => ({
          messageId: m.messageId,
          sequence: String(m.sequence ?? i + 1),
          revision: String(m.revision ?? '0'),
          role: m.role,
          ...(m.text !== undefined ? { text: m.text } : {}),
          textTruncated: m.textTruncated === true,
          status: m.status ?? (m.role === 'user' ? 'CONSUMED' : 'COMPLETED'),
          ...(m.sourceRequestKey ? { sourceRequestKey: m.sourceRequestKey } : {}),
          ...(m.nativeTurnId ? { nativeTurnId: m.nativeTurnId } : {}),
          occurredAt: m.occurredAt ?? t.createdAt,
          updatedAt: m.updatedAt ?? m.occurredAt ?? t.createdAt,
        }));
        const num = (k) => (query.has(k) ? Number(query.get(k)) : null);
        const cursor = num('cursor'), before = num('before'), limit = num('limit') ?? 100;
        let slice = all;
        if (cursor !== null) slice = slice.filter((m) => Number(m.sequence) >= cursor);
        if (before !== null) slice = slice.filter((m) => Number(m.sequence) < before).slice(-limit);
        else slice = slice.slice(0, limit);
        const nextSeq = slice.length ? Number(slice[slice.length - 1].sequence) + 1 : null;
        const hasMore = nextSeq !== null && all.length >= nextSeq;
        const items = slice;
        return { messageSchemaVersion: 'native-messages/1', taskId: t.taskId, storeId: ledger.storeId,
          firstSequence: items.length ? items[0].sequence : null,
          latestSequence: all.length ? all[all.length - 1].sequence : null,
          transcriptRevision: '0', historyMode: 'STRUCTURED_FROM_START',
          legacy: null,
          text: t.output || all.filter((m) => m.role === 'assistant').map((m) => m.text).join('\n'),
          turns: {}, items, coverage: 'BOUNDED_PUBLIC_MESSAGES', hasMore,
          cursor: hasMore ? String(nextSeq) : null };
      }
      if (sub === 'input-requests') return { items: Object.values(t.inputRequests ?? {}) };
      if (sub === 'artifacts') return sealArtifacts(t);
      if (sub === 'events') return { items: [], nextCursor: null };
      if (sub === 'metrics') return { cost: null, costCoverage: 'UNKNOWN', usageCoverage: 'NATIVE_TASK_TOTAL', configuredModel: 'vendor-cli' };
      fail('NOT_FOUND', 404);
    }
    if (method === 'GET' && rest.length === 4 && sub === 'artifacts') {
      const manifest = sealArtifacts(t);
      const row = (manifest.items ?? []).find((r) => r.artifactId === rest[3] && r.status === 'AVAILABLE');
      if (!row) fail('ARTIFACT_NOT_AVAILABLE', 404);
      const staged = join(ART_DIR, t.taskId, row.artifactId);
      if (!existsSync(staged)) fail('ARTIFACT_NOT_AVAILABLE', 404);
      const bytes = readFileSync(staged);
      if (hashBytes(bytes) !== row.sha256) fail('ARTIFACT_CHANGED', 409);
      return { artifactId: row.artifactId, path: row.path, sha256: row.sha256, encoding: 'base64', bytes: bytes.toString('base64') };
    }
    if (method === 'GET' && rest.length === 3 && sub === 'commands') fail('NOT_FOUND', 404);
    if (method === 'GET' && rest.length === 3 && sub === 'interaction-requests') fail('NOT_FOUND', 404);
    if (method === 'GET' && rest.length === 4 && sub === 'interaction-requests') {
      const r = ledger.requests[rest[3]];
      if (!r || r.taskId !== t.taskId) fail('REQUEST_NOT_FOUND', 404);
      return receipt(r);
    }
    if (method === 'POST' && rest.length === 3 && sub === 'messages') {
      if (!body || typeof body.requestKey !== 'string') fail('INVALID_REQUEST', 400);
      if (!['NATIVE_FOLLOW_UP', 'NATIVE_STEER'].includes(body.kind)) fail('OPERATION_NOT_ALLOWED', 403);
      if (typeof body.content !== 'string' || !body.content.trim()) fail('INVALID_REQUEST', 400);
      // A message may carry a per-turn model switch — the next spawned CLI
      // invocation simply launches with the new --model/--effort argv.
      if (body.model !== undefined && !MODEL_CATALOG.some(m => m.modelRef === body.model)) fail('MODEL_REF_UNKNOWN', 400);
      if (body.effort !== undefined) {
        const name = body.model ?? t.model ?? MODEL;
        if (!MODEL_CATALOG.find(m => m.modelRef === name)?.efforts?.includes(body.effort)) fail('MODEL_EFFORT_UNSUPPORTED', 400);
      }
      const prior = ledger.requests[body.requestKey];
      if (prior) {
        if (prior.taskId !== t.taskId || prior.hash !== digest({ c: body.content, k: body.kind, m: body.model ?? null, e: body.effort ?? null })) {
          fail('IDEMPOTENCY_PAYLOAD_MISMATCH', 409);
        }
        return receipt(prior);
      }
      // Target pins: the gateway already verified these against facts; the host
      // rechecks so a stale phone target never silently steers a live CLI.
      for (const [bf, tf] of [['generation', 'generation'], ['nativeSessionId', 'nativeSessionId'],
                             ['nativeTurnId', 'nativeTurnId'], ['targetCommandId', 'currentCommandId']]) {
        const bv = body[bf] ?? null, tv = t[tf] ?? null;
        if (bv !== tv) fail('NATIVE_TARGET_CHANGED', 409);
      }
      if (live.has(t.taskId)) fail('TASK_BUSY', 409);
      ledger.requests[body.requestKey] = {
        requestKey: body.requestKey, taskId: t.taskId, operation: body.kind,
        state: 'ACCEPTED', hash: digest({ c: body.content, k: body.kind, m: body.model ?? null, e: body.effort ?? null }),
      };
      if (body.model !== undefined) t.model = body.model;
      if (body.effort !== undefined) t.effort = body.effort;
      save();
      dispatch(t, body.content, body.requestKey);
      return receipt(ledger.requests[body.requestKey]);
    }
    if (method === 'POST' && rest.length === 3 && sub === 'commands') {
      if (!body || typeof body.requestKey !== 'string' || body.intent !== 'STOP') {
        fail(body && body.intent ? 'NATIVE_ACTION_UNSUPPORTED' : 'INVALID_REQUEST',
             body && body.intent ? 409 : 400);
      }
      const prior = ledger.requests[body.requestKey];
      if (prior) {
        if (prior.taskId !== t.taskId || prior.hash !== 'stop') fail('IDEMPOTENCY_PAYLOAD_MISMATCH', 409);
        return receipt(prior);
      }
      stopTask(t);
      ledger.requests[body.requestKey] = {
        requestKey: body.requestKey, taskId: t.taskId, operation: 'STOP',
        state: 'SUCCEEDED', result: { taskId: t.taskId, state: t.state }, hash: 'stop',
      };
      save();
      return receipt(ledger.requests[body.requestKey]);
    }
    if (method === 'POST' && rest.length === 5 && sub === 'input-requests' && rest[4] === 'responses') {
      fail('REQUEST_NOT_FOUND', 404);
    }
  }
  fail('NOT_FOUND', 404);
};

http.createServer((req, res) => {
  const send = (status, obj) => {
    const data = JSON.stringify(obj);
    res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) });
    res.end(data);
  };
  (async () => {
    if (req.headers.authorization !== 'Bearer ' + TOKEN) return send(401, { code: 'LOCAL_AUTH_REQUIRED' });
    const url = new URL(req.url, 'http://127.0.0.1');
    const seg = url.pathname.split('/').filter(Boolean);
    const isIdentity = seg.join('/') === 'local/v1/identity';
    if (!isIdentity) {
      const store = req.headers['x-worker-local-store'];
      if (store !== undefined && store !== ledger.storeId) return send(409, { code: 'LOCAL_SOURCE_IDENTITY_CHANGED' });
    }
    let body;
    if (req.method === 'POST') {
      let raw = '';
      for await (const c of req) { raw += c; if (raw.length > MAX_BODY) return send(413, { code: 'REQUEST_TOO_LARGE' }); }
      try { body = raw ? JSON.parse(raw) : {}; } catch { return send(422, { code: 'INVALID_REQUEST' }); }
      const key = req.headers['idempotency-key'];
      if (key !== undefined && body.requestKey !== undefined && key !== body.requestKey) {
        return send(400, { code: 'IDEMPOTENCY_KEY_MISMATCH' });
      }
    }
    const result = routes(req.method, seg, null, body, url.searchParams);
    send(200, result);
  })().catch((e) => {
    send(e.status ?? 500, { code: e.code ?? 'LOCAL_SOURCE_UNAVAILABLE' });
  });
}).listen(PORT, '127.0.0.1', () => {
  console.log(`cli-bridge ${ENGINE} store=${ledger.storeId} on 127.0.0.1:${PORT} project=${PROJECT_DIR}`);
});
