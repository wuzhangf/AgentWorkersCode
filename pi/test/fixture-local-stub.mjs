import { createServer } from 'node:http';

/**
 * W6 external-host fixture: a trivial spawned /local/v1 responder — NOT
 * a real engine. It proves the daemon's spawn/ready-parse/token plumbing:
 * every /local/v1 request must carry `Authorization: Bearer <token>`
 * where the token arrives through the env var named by STUB_TOKEN_ENV
 * (default W6_STUB_TOKEN), exactly like the codex host's --token-env.
 *
 * Prints `LOCAL_TASK_READY http://127.0.0.1:<port>` once bound (the same
 * ready-line shape the codex host emits). `/__recorded` (loopback, no
 * auth) returns every request seen so the test can assert auth plumbing;
 * `POST /__die {"code":n}` exits the process — the crash-propagation
 * probe.
 */
const tokenEnv = process.env.STUB_TOKEN_ENV ?? 'W6_STUB_TOKEN';
const token = process.env[tokenEnv] ?? '';
const recorded = [];
let taskCount = 0;

const json = (res, status, body) => {
  const bytes = Buffer.from(JSON.stringify(body), 'utf8');
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': bytes.length });
  res.end(bytes);
};

const server = createServer(async (req, res) => {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks);
  let body = null;
  if (raw.length) { try { body = JSON.parse(raw.toString('utf8')); } catch { body = null; } }
  const url = new URL(req.url, 'http://stub');
  const path = url.pathname;
  if (path === '/__die') { json(res, 200, { ok: true }); process.exitCode = body?.code ?? 2; setTimeout(() => process.exit(process.exitCode), 10); return; }
  if (path === '/__recorded') return json(res, 200, recorded);
  const authed = token.length >= 24 && req.headers.authorization === 'Bearer ' + token;
  recorded.push({ method: req.method, path, authed, body });
  if (!authed) return json(res, 401, { code: 'UNAUTHORIZED', message: 'missing or wrong bearer' });
  if (req.method === 'GET' && path === '/local/v1/identity') {
    return json(res, 200, { schemaVersion: 'worker-local/1', storeId: 'stub-store', bootId: 'stub-boot' });
  }
  if (req.method === 'GET' && path === '/local/v1/engines') {
    return json(res, 200, { schemaVersion: 'worker-local/1', storeId: 'stub-store', items: [{ engineId: 'stub', engineVersion: '0.0.0' }] });
  }
  if (req.method === 'POST' && path === '/local/v1/task-preflights') {
    return json(res, 200, { projectId: body?.projectId ?? null, engineId: body?.engineId ?? 'stub', optionsRevision: 'stub-rev-1' });
  }
  if (req.method === 'POST' && path === '/local/v1/tasks') {
    taskCount++;
    return json(res, 202, { state: 'ACCEPTED', taskId: 'stub-task-' + taskCount, launchState: 'RUNNING', requestKey: body?.requestKey ?? null });
  }
  const task = path.match(/^\/local\/v1\/tasks\/([^/]+)(\/.*)?$/);
  if (task) {
    const rest = task[2] ?? '';
    if (req.method === 'GET' && rest === '') {
      // Immediate terminal verdict with a native session bound — the
      // wire's observedAtTerminal path: STARTED receipt → QUIESCED.
      return json(res, 200, {
        taskId: task[1], state: 'SUCCEEDED', nativeSessionId: 'sess-stub-1',
        generation: 'gen-1', currentCommandId: 'stub-command', quiescent: true,
        stopProof: { method: 'stub-terminal', detail: 'clean' }, reasonCode: null,
      });
    }
    if (req.method === 'GET' && rest === '/artifacts') {
      return json(res, 200, { items: [], baseline: 'stub-base', segmentId: 'seg-1', coverage: 'FULL', bounded: false });
    }
    if (req.method === 'POST' && rest === '/commands') {
      return json(res, 202, { state: 'APPLIED' });
    }
  }
  return json(res, 404, { code: 'STUB_ROUTE_UNKNOWN', message: req.method + ' ' + path });
});
server.listen(0, '127.0.0.1', () => {
  console.log('LOCAL_TASK_READY http://127.0.0.1:' + server.address().port);
});
