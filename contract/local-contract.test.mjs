// worker-local/1 + worker-host/1 跨引擎合同套。
//
// 一份 spec 对每个被选中的 engine host 跑同一组断言——证明 pi / codex 在
// 同一合同下可互换，而不是各测各的静默漂移。确定性：不发真实模型请求
// （pi 挂 fixture-child；codex 经外部提供的二进制+预初始化 state）。
//
// 选择引擎（默认只跑 pi——它可自启动）：
//   WORKER_CONTRACT_ENGINES=pi,codex node --test local-contract.test.mjs
//
// codex 目标激活方式（环境注入，不落盘密钥）：
//   CODEX_HOST_EXE=<已构建 host 二进制> CODEX_STATE_TEMPLATE=<含项目绑定的
//   初始化 state 目录> CODEX_PROJECT_ID=<模板内 projectId>
//   缺失时该引擎记 SKIPPED，不算失败——合同缺口显式可见。
//
// 断言分两组：
//   A. 线合同（两引擎都必须）：auth、identity/manifest/capabilities 形状、
//      supportedPaths 如实、engines/projects 列表、未配模型时确定性拒绝。
//   B. 执行合同（仅 det=true 的引擎，当前 pi）：preflight→create→events
//      游标→终态→artifacts→requestKey 幂等重放→STOP 命令。
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, cpSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const TOKEN = 'worker-contract-sentinel-token-not-a-secret';
const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED', 'STOPPED', 'TIMED_OUT']);
const wait = async (fn, n = 400) => {
  for (let i = 0; i < n; i++) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); }
  throw new Error('WAIT_TIMEOUT');
};
function gitProject() {
  const p = mkdtempSync(join(tmpdir(), 'wc-project-'));
  execFileSync('git', ['init', p], { stdio: 'ignore' });
  writeFileSync(join(p, 'README.md'), 'baseline');
  for (const a of [['add', '.'], ['-c', 'user.name=Contract', '-c', 'user.email=contract@example.invalid', 'commit', '-m', 'baseline']])
    execFileSync('git', ['-C', p, ...a], { stdio: 'ignore' });
  return p;
}

function client(port) {
  return async (path, body, headers = {}) => {
    const r = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, {
      method: body ? 'POST' : 'GET',
      headers: { Authorization: 'Bearer ' + TOKEN, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers },
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await r.text();
    let j = null; try { j = JSON.parse(text); } catch { }
    return { status: r.status, body: j, text };
  };
}

// ---- engine launchers ------------------------------------------------------

async function launchPi() {
  const { PiHost } = await import('../pi/dist/host/server.js');
  const fixture = fileURLToPath(new URL('../pi/test/fixture-child.mjs', import.meta.url));
  const state = mkdtempSync(join(tmpdir(), 'wc-pi-state-'));
  const host = new PiHost({ state, token: TOKEN, childModule: fixture });
  host.register('contract-demo', gitProject());
  const port = await host.listen();
  return {
    engine: 'pi', port, call: client(port), close: () => host.close(), projectId: 'contract-demo', det: true,
    // 无 child/无模型 env 的第二实例，专跑 UNCONFIGURED 拒绝合同。
    async unconfigured() {
      const modelKeys = ['MODEL_DEFAULT_MODEL', 'MODEL_BASE_URL', 'MODEL_API_KEY', 'LLM_HEADER_x_opencode_session'];
      const saved = Object.fromEntries(modelKeys.map(k => [k, process.env[k]]));
      for (const k of modelKeys) delete process.env[k];
      try {
        const h = new PiHost({ state: mkdtempSync(join(tmpdir(), 'wc-pi-uncfg-')), token: TOKEN });
        h.register('contract-demo', gitProject());
        const p = await h.listen();
        return { port: p, call: client(p), close: async () => { await h.close(); for (const k of modelKeys) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; } };
      } catch (e) { for (const k of modelKeys) if (saved[k] !== undefined) process.env[k] = saved[k]; throw e; }
    },
  };
}

async function launchCodex() {
  const exe = process.env.CODEX_HOST_EXE;
  if (!exe || !existsSync(exe)) return null;
  const state = mkdtempSync(join(tmpdir(), 'wc-codex-state-'));
  const tpl = process.env.CODEX_STATE_TEMPLATE;
  if (tpl) cpSync(tpl, state, { recursive: true });
  const child = spawn(exe, ['serve', '--state', state, '--port', '0'], {
    env: { ...process.env, WORKER_LOCAL_TOKEN: TOKEN, MODEL_API_KEY: process.env.MODEL_API_KEY ?? 'WC_NON_SECRET_SENTINEL' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '', diag = '';
  child.stdout.on('data', b => out += b); child.stderr.on('data', b => diag += b);
  const url = await wait(() => /LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(out)?.[1])
    .catch(e => { child.kill(); throw new Error('CODEX_HOST_START ' + diag.trim()); });
  return {
    engine: 'codex', port: Number(new URL(url).port), call: client(Number(new URL(url).port)), close: () => child.kill(),
    projectId: process.env.CODEX_PROJECT_ID ?? null, det: process.env.CODEX_CONTRACT_DET === '1',
  };
}

const LAUNCHERS = { pi: launchPi, codex: launchCodex };
const ENGINES = (process.env.WORKER_CONTRACT_ENGINES ?? 'pi').split(',').map(s => s.trim()).filter(Boolean);

// ---- shared contract --------------------------------------------------------

for (const name of ENGINES) {
  const launcher = LAUNCHERS[name];
  test(`worker-local/1 contract — ${name}`, async (t) => {
    assert(launcher, `unknown engine ${name}`);
    const ctx = await launcher();
    if (!ctx) { t.skip(`${name} host not provided (set CODEX_HOST_EXE + CODEX_STATE_TEMPLATE)`); return; }
    const { call } = ctx;
    try {
      const raw = (p, headers) => fetch(`http://127.0.0.1:${ctx.port}${p}`, { headers }).then(r => r.status);
      let manifest;
      await t.test('A1 auth: bearer required, wrong token refused', async () => {
        assert.equal(await raw('/local/v1/identity'), 401);
        assert.equal(await raw('/local/v1/identity', { Authorization: 'Bearer wrong-token' }), 401);
        assert.equal(await raw('/worker-host/v1/manifest'), 401);
        assert.equal((await call('/identity')).status, 200); // authed path alive
      });

      await t.test('A2 identity + manifest shape', async () => {
        const id = (await call('/identity')).body;
        assert.equal(id.schemaVersion, 'worker-local/1');
        assert.ok(id.storeId && id.bootId);
        manifest = (await call('/worker-host/v1/manifest')).body;
        assert.equal(manifest.schemaVersion, 'worker-host/1');
        assert.equal(manifest.engine?.id, name);
        assert.ok(manifest.contracts.includes('worker-local/1') && manifest.contracts.includes('worker-host/1'));
        assert.ok(Array.isArray(manifest.supportedPaths) && manifest.supportedPaths.length > 0);
        const caps = manifest.capabilities?.items ?? manifest.capabilities;
        assert.ok(Array.isArray(caps) && caps.length > 0);
        const capRes = await call('/worker-host/v1/capabilities');
        assert.equal(capRes.status, 200);
        assert.ok(Array.isArray(capRes.body?.items ?? capRes.body));
      });

      await t.test('A3 supportedPaths truthful: every listed path answers <500', async () => {
        for (const p of manifest.supportedPaths) {
          const probe = p.replace(/\{[^}]+\}/g, 'contract-missing-id');
          const r = await call(probe);
          assert.ok(r.status < 500, `${p} -> ${r.status}`);
        }
      });

      await t.test('A4 engines + projects list shape', async () => {
        const eng = (await call('/engines')).body;
        assert.ok(Array.isArray(eng.items) && eng.items.length > 0);
        for (const e of eng.items) assert.ok(typeof e.engineId === 'string');
        const pr = await call('/projects');
        assert.equal(pr.status, 200);
        assert.ok(Array.isArray(pr.body?.items) || pr.body?.items === undefined);
      });

      await t.test('A5 unconfigured model -> deterministic structured refusal', async () => {
        let c = ctx, own = null;
        if (manifest?.currentAvailability !== 'UNCONFIGURED') {
          if (!ctx.unconfigured) { t.skip(`availability=${manifest?.currentAvailability}, no unconfigured launcher`); return; }
          own = await ctx.unconfigured();
          c = { ...ctx, ...own };
          const m = (await c.call('/worker-host/v1/manifest')).body;
          assert.equal(m.currentAvailability, 'UNCONFIGURED');
        }
        try {
          const pf = await c.call('/task-preflights', { projectId: ctx.projectId ?? 'contract-demo', engineId: name });
          assert.ok(pf.status < 500 && pf.body?.optionsRevision);
          const body = { requestKey: 'contract-refuse-' + name, projectId: ctx.projectId ?? 'contract-demo', engineId: pf.body.engineId ?? name, optionsRevision: pf.body.optionsRevision, instruction: 'must not dispatch' };
          const denied = await c.call('/tasks', body);
          assert.ok(denied.status >= 400 && denied.status < 500, `refusal status ${denied.status}`);
          assert.ok(typeof denied.body?.code === 'string' && denied.body.code.length > 0);
          const replay = await c.call('/tasks', body);
          assert.equal(replay.status, denied.status);
          assert.equal(replay.body?.code, denied.body.code);
          const op = await c.call('/task-requests/' + encodeURIComponent(body.requestKey));
          assert.ok(op.status < 500);
        } finally { if (own) await own.close(); }
      });

      await t.test('A6 requestKey must equal Idempotency-Key header', async () => {
        const r = await call('/tasks', { requestKey: 'contract-mismatch-' + name, projectId: ctx.projectId ?? 'contract-demo', engineId: name },
          { 'Idempotency-Key': 'different-key' });
        assert.equal(r.status, 409);
        assert.equal(r.body?.code, 'KEY_MISMATCH');
      });

      await t.test('B execution contract (deterministic executor)', async (bt) => {
        if (!ctx.det) { bt.skip('no deterministic executor for this engine'); return; }
        const pf = (await call('/task-preflights', { projectId: ctx.projectId, engineId: name })).body;
        const create = async (instruction, key) => {
          const r = await call('/tasks', { requestKey: key, projectId: ctx.projectId, engineId: name, optionsRevision: pf.optionsRevision, instruction });
          assert.equal(r.status, 202, r.text);
          return r.body;
        };

        // B1 lifecycle: create -> terminal -> events cursor -> artifacts
        const key = 'contract-life-' + name + '-' + Math.random().toString(36).slice(2);
        const receipt = await create('contract-basic-turn', key);
        assert.ok(receipt.taskId);
        const detail = await wait(async () => {
          const d = (await call('/tasks/' + receipt.taskId)).body;
          return TERMINAL.has(d.state) ? d : null;
        });
        assert.equal(detail.state, 'SUCCEEDED');
        let cursor = '0'; const kinds = new Set(); let prev = -1;
        for (let page = 0; page < 50; page++) {
          const ev = (await call(`/tasks/${receipt.taskId}/events?cursor=${cursor}&limit=30`)).body;
          assert.equal(typeof ev.cursorExpired, 'boolean');
          for (const e of ev.items ?? []) {
            const s = Number(e.sequence ?? e.seq ?? e.cursor);
            assert.ok(Number.isSafeInteger(s) && s > prev, 'event seq monotonic');
            prev = s; kinds.add(e.kind);
          }
          if (!(ev.items ?? []).length) break;
          cursor = ev.cursor ?? cursor;
        }
        assert.ok(kinds.size > 0, 'task produced events');
        const arts = await call(`/tasks/${receipt.taskId}/artifacts`);
        assert.equal(arts.status, 200);
        assert.ok(Array.isArray(arts.body?.items ?? arts.body));

        // B2 requestKey idempotent replay returns original receipt, no second task
        const replay = await call('/tasks', { requestKey: key, projectId: ctx.projectId, engineId: name, optionsRevision: pf.optionsRevision, instruction: 'contract-basic-turn' });
        assert.equal(replay.body?.taskId, receipt.taskId);
        const op = (await call('/task-requests/' + encodeURIComponent(key))).body;
        assert.equal(op?.taskId ?? op?.receipt?.taskId, receipt.taskId);

        // B3 STOP command with durable guard cancels a held task
        const stopKey = 'contract-stop-' + Math.random().toString(36).slice(2);
        const held = await create('waiting-never', stopKey);
        const running = await wait(async () => {
          const d = (await call('/tasks/' + held.taskId)).body;
          return d.state === 'RUNNING' && d.generation ? d : null;
        });
        const stop = await call(`/tasks/${held.taskId}/commands`, {
          requestKey: 'contract-stop-cmd-' + Math.random().toString(36).slice(2), intent: 'STOP',
          generation: running.generation, nativeSessionId: running.nativeSessionId, targetCommandId: running.currentCommandId,
        });
        assert.equal(stop.status, 202, stop.text);
        assert.equal(stop.body?.state, 'APPLIED');
        await wait(async () => {
          const d = (await call('/tasks/' + held.taskId)).body;
          return TERMINAL.has(d.state) ? d : null;
        });
      });
    } finally {
      await ctx.close();
    }
  });
}


