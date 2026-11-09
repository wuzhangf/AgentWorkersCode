import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';
import { PiHost } from '../host/server.js';
import { EnterpriseWireAdapter } from './adapter.js';
import type { Obj } from './fields.js';

/**
 * W6/P25 daemon entry — one process owns the local host (the executor,
 * with its durable task store) and the enterprise wire adapter (the
 * protocol ledger). The adapter is a thin wire: it never executes, it
 * translates /worker/v1 command intents into the host's /local/v1
 * contract — engine-agnostic, so the same daemon fronts the embedded
 * PiHost or an external host binary such as codex-native-worker.
 *
 * Configuration comes from a JSON file or PI_WIRE_CONFIG env — no
 * secrets are embedded in this file; the worker token is read from the
 * environment (PI_WORKER_TOKEN) exactly like a daemon deployment.
 *
 *   node dist/wire/main.js --config wire.json
 *   PI_WIRE_CONFIG='{...}' node dist/wire/main.js
 *
 * Config shape:
 *   {
 *     stateDir, controlPlaneUrl, workerId, deploymentId,
 *     projects: { "<wire projectId>": "<local project id>", "*": "..." },
 *     imageDigest?, executionProfiles?, capabilities?, localEngineId?,
 *     host: { state?, childModule?, projectRoots: {id: dir}, token?, maxActiveTasks? }
 *       — embedded PiHost (default, unchanged)
 *     host: { external: { command, args?, readyPattern?, tokenEnv?, env?, state?, readyTimeoutMillis? } }
 *       — spawned /local/v1 host process; a generated >=24-char token is
 *         exported under tokenEnv (default WORKER_LOCAL_TOKEN); the
 *         daemon waits for a stdout line matching readyPattern (default
 *         /LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/) whose first
 *         capture group is the bound URL. `{state}` and `{tokenEnv}` in
 *         args are substituted; state defaults to <stateDir>/host.
 *     host: { codex: { hostExecutable, state, executable?, init?, port?, tokenEnv?, env?, readyTimeoutMillis? } }
 *       — sugar over external for the codex native host:
 *         `hostExecutable serve --state <state> --port <0> --token-env <name>`.
 *         `state` must be initialized (registry.json present); if `init`
 *         is declared and state is uninitialized, `executable` +
 *         init.{project,projectId,model,baseUrl} run `hostExecutable
 *         init` once first. Codex mode defaults localEngineId='codex',
 *         capabilities.providers=['codex'], executionProfiles=
 *         ['codex.standard'].
 *   }
 *
 * Teardown: stop() drains the adapter then kills the spawned host and
 * awaits its exit. An external host dying while the daemon is up is a
 * daemon failure (hostExit resolves; the standalone entry exits non-zero)
 * — never a silent zombie.
 */

export interface ExternalHostConfig {
  command: string;
  args?: string[];
  readyPattern?: string;
  tokenEnv?: string;
  env?: Record<string, string>;
  state?: string;
  readyTimeoutMillis?: number;
}

export interface CodexHostConfig {
  hostExecutable: string;
  state: string;
  /** pinned codex engine executable — required when `init` is declared. */
  executable?: string;
  /** one-time init inputs; runs only when `state` lacks registry.json. */
  init?: { project: string; projectId: string; model: string; baseUrl: string };
  port?: number;
  tokenEnv?: string;
  env?: Record<string, string>;
  readyTimeoutMillis?: number;
}

export interface WireDaemonConfig {
  stateDir: string;
  controlPlaneUrl: string;
  workerId: string;
  deploymentId: string;
  workerToken?: string;
  projects?: Record<string, string>;
  imageDigest?: string;
  executionProfiles?: string[];
  capabilities?: Obj;
  localEngineId?: string;
  heartbeatMillis?: number;
  pumpMillis?: number;
  host?: {
    state?: string;
    childModule?: string;
    projectRoots?: Record<string, string>;
    token?: string;
    maxActiveTasks?: number;
    external?: ExternalHostConfig;
    codex?: CodexHostConfig;
  };
}

export interface HostExit { code: number | null; signal: NodeJS.Signals | null }

export interface RunningDaemon {
  /** embedded PiHost, or null when an external host was spawned. */
  host: PiHost | null;
  adapter: EnterpriseWireAdapter;
  localUrl: string;
  localToken: string;
  /** resolves when a spawned external host exits; null for embedded. */
  hostExit: Promise<HostExit> | null;
  /** true once stop() was invoked — a later hostExit is expected teardown, not a crash. */
  readonly stopping: boolean;
  stop: () => Promise<void>;
}

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));
const DEFAULT_READY_PATTERN = 'LOCAL_TASK_READY (http:\\/\\/127\\.0\\.0\\.1:\\d+)';

interface SpawnedHost {
  child: ChildProcess;
  url: string;
  token: string;
  exit: Promise<HostExit>;
}

/**
 * Spawn an external /local/v1 host: generated >=24-char bearer exported
 * under tokenEnv, ready line scraped from stdout. Stderr tail is kept
 * for the failure message; child stdout is mirrored into the daemon log.
 */
async function spawnExternalHost(ext: ExternalHostConfig, stateDir: string, log?: (l: string) => void): Promise<SpawnedHost> {
  const tokenEnv = ext.tokenEnv ?? 'WORKER_LOCAL_TOKEN';
  const token = randomBytes(24).toString('hex'); // 48 chars — hosts refuse < 24
  const state = ext.state ?? join(stateDir, 'host');
  const args = (ext.args ?? []).map(a => a.replaceAll('{state}', state).replaceAll('{tokenEnv}', tokenEnv));
  const ready = new RegExp(ext.readyPattern ?? DEFAULT_READY_PATTERN);
  const timeout = ext.readyTimeoutMillis ?? 30_000;
  const child = spawn(ext.command, args, {
    env: { ...process.env, ...(ext.env ?? {}), [tokenEnv]: token },
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
  if (!child.stdout || !child.stderr) throw new Error('external host stdio not piped');
  const exit = new Promise<HostExit>(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  const url = await new Promise<string>((resolve, reject) => {
    let settled = false;
    let stderrTail = '';
    child.stderr!.on('data', d => { stderrTail = (stderrTail + String(d)).slice(-2048); });
    const timer = setTimeout(() => fail(new Error('external host ready line not seen within ' + timeout + 'ms' + tail())), timeout);
    const tail = () => stderrTail.trim() ? ' — host stderr: ' + stderrTail.trim() : '';
    const fail = (e: Error) => { if (!settled) { settled = true; clearTimeout(timer); reject(e); } };
    const rl = createInterface({ input: child.stdout! });
    rl.on('line', line => {
      try { log?.('[ext-host] ' + line); } catch { /* logging must never break the daemon */ }
      const m = ready.exec(line);
      if (!settled && m) {
        const found = m[1] ?? m[0];
        if (found.startsWith('http')) { settled = true; clearTimeout(timer); resolve(found); }
      }
    });
    rl.on('close', () => fail(new Error('external host stdout closed before ready' + tail())));
    child.once('error', fail);
    void exit.then(({ code, signal }) => fail(new Error('external host exited before ready: code=' + code + ' signal=' + signal + tail())));
  });
  return { child, url, token, exit };
}

/** SIGTERM then SIGKILL escalation; always resolves after the child is dead. */
async function killSpawned(s: SpawnedHost, graceMillis = 5_000): Promise<void> {
  if (s.child.exitCode !== null || s.child.signalCode !== null) { await s.exit; return; }
  try { s.child.kill('SIGTERM'); } catch { /* already gone */ }
  const exited = await Promise.race([s.exit.then(() => true), sleep(graceMillis).then(() => false as const)]);
  if (!exited) {
    try { s.child.kill('SIGKILL'); } catch { /* already gone */ }
    await s.exit;
  }
}

function loadConfig(argv: string[]): WireDaemonConfig {
  const fromEnv = process.env.PI_WIRE_CONFIG;
  const flagAt = argv.indexOf('--config');
  let raw: string | null = null;
  if (flagAt >= 0 && argv[flagAt + 1]) {
    raw = readFileSync(argv[flagAt + 1], 'utf8');
  } else if (fromEnv) {
    raw = fromEnv;
  }
  if (!raw) {
    throw new Error('missing wire configuration: pass --config <file> or set PI_WIRE_CONFIG');
  }
  const cfg = JSON.parse(raw) as WireDaemonConfig;
  for (const k of ['stateDir', 'controlPlaneUrl', 'workerId', 'deploymentId'] as const) {
    if (!cfg[k]) throw new Error('wire config missing ' + k);
  }
  return cfg;
}

/**
 * startDaemon — composable for tests AND the standalone process. Default
 * mode spins up the embedded PiHost on an ephemeral loopback port and
 * registers the declared project roots; `host.external`/`host.codex`
 * instead spawn an external /local/v1 host and wait for its ready line.
 * Either way the wire adapter runs identically on top.
 */
export async function startDaemon(cfg: WireDaemonConfig, opts: { workerToken?: string; log?: (l: string) => void } = {}): Promise<RunningDaemon> {
  const workerToken = opts.workerToken ?? cfg.workerToken ?? process.env.PI_WORKER_TOKEN;
  if (!workerToken) throw new Error('missing worker token: PI_WORKER_TOKEN or config.workerToken');
  if (cfg.host?.external && cfg.host?.codex) throw new Error('host.external and host.codex are mutually exclusive');
  const codex = cfg.host?.codex ?? null;
  if (codex?.init && !existsSync(join(codex.state, 'registry.json'))) {
    if (!codex.executable) throw new Error('host.codex.init requires host.codex.executable (pinned engine)');
    opts.log?.('codex host init: ' + codex.state + ' project=' + codex.init.project + ' model=' + codex.init.model);
    execFileSync(codex.hostExecutable, [
      'init', '--state', codex.state, '--executable', codex.executable,
      '--project', codex.init.project, '--project-id', codex.init.projectId,
      '--model', codex.init.model, '--base-url', codex.init.baseUrl,
    ], { stdio: ['ignore', 'inherit', 'inherit'] });
  }
  const external: ExternalHostConfig | undefined = cfg.host?.external ?? (codex ? {
    command: codex.hostExecutable,
    args: ['serve', '--state', codex.state, '--port', String(codex.port ?? 0), '--token-env', codex.tokenEnv ?? 'WORKER_LOCAL_TOKEN'],
    tokenEnv: codex.tokenEnv ?? 'WORKER_LOCAL_TOKEN',
    env: codex.env,
    state: codex.state,
    readyTimeoutMillis: codex.readyTimeoutMillis,
  } : undefined);

  const state = { stopping: false, stopped: false };
  let spawned: SpawnedHost | null = null;
  let host: PiHost | null = null;
  let localUrl: string;
  let localToken: string;
  if (external) {
    spawned = await spawnExternalHost(external, cfg.stateDir, opts.log);
    localUrl = spawned.url;
    localToken = spawned.token;
  } else {
    const hostToken = cfg.host?.token ?? 'wire-host-' + randomBytes(16).toString('hex');
    host = new PiHost({
      state: cfg.host?.state ?? join(cfg.stateDir, 'host'),
      token: hostToken,
      childModule: cfg.host?.childModule,
      maxActiveTasks: cfg.host?.maxActiveTasks ?? 1,
    });
    for (const [projectId, dir] of Object.entries(cfg.host?.projectRoots ?? {})) {
      host.register(projectId, dir);
    }
    const port = await host.listen(0);
    localUrl = 'http://127.0.0.1:' + port;
    localToken = hostToken;
  }
  const adapter = new EnterpriseWireAdapter({
    stateDir: cfg.stateDir,
    controlPlaneUrl: cfg.controlPlaneUrl,
    workerToken,
    localUrl,
    localToken,
    workerId: cfg.workerId,
    deploymentId: cfg.deploymentId,
    imageDigest: cfg.imageDigest,
    projects: cfg.projects,
    localEngineId: cfg.localEngineId ?? (codex ? 'codex' : 'pi'),
    executionProfiles: cfg.executionProfiles ?? (codex ? ['codex.standard'] : undefined),
    capabilities: (codex ? { providers: ['codex'], ...(cfg.capabilities ?? {}) } : cfg.capabilities) as any,
    heartbeatMillis: cfg.heartbeatMillis,
    pumpMillis: cfg.pumpMillis,
    log: opts.log,
  });
  try {
    await adapter.start();
  } catch (e) {
    if (spawned) await killSpawned(spawned).catch(() => undefined);
    if (host) await host.close().catch(() => undefined);
    throw e;
  }
  const stop = async () => {
    if (state.stopped) return;
    state.stopping = true;
    state.stopped = true;
    try { await adapter.stop(); } finally {
      if (spawned) await killSpawned(spawned).catch(() => undefined);
      if (host) await host.close();
    }
  };
  return {
    host, adapter, localUrl, localToken,
    hostExit: spawned?.exit ?? null,
    get stopping() { return state.stopping; },
    stop,
  };
}

const isMain = (() => {
  try { return !!process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href; }
  catch { return false; }
})();

if (isMain) {
  const log = (l: string) => console.log(new Date().toISOString() + ' ' + l);
  try {
    const cfg = loadConfig(process.argv.slice(2));
    const daemon = await startDaemon(cfg, { log });
    const status = daemon.adapter.status();
    log('pi-wire daemon up: instance=' + status.instanceId + ' local=' + daemon.localUrl + (daemon.host ? ' (embedded pi host)' : ' (external host)'));
    let shutting = false;
    const shutdown = async (code = 0) => {
      if (shutting) return;
      shutting = true;
      log('shutting down');
      await daemon.stop();
      process.exit(code);
    };
    process.on('SIGINT', () => void shutdown());
    process.on('SIGTERM', () => void shutdown());
    // External host dying while the daemon is up is a daemon failure —
    // propagate honestly, never linger as a zombie claiming READY.
    void daemon.hostExit?.then(({ code, signal }) => {
      if (shutting || daemon.stopping) return;
      log('external host exited unexpectedly code=' + code + ' signal=' + signal);
      void shutdown(typeof code === 'number' && code !== 0 ? code : 1);
    });
  } catch (e) {
    console.error('pi-wire daemon failed to start: ' + String(e));
    process.exit(1);
  }
}
