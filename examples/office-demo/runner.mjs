// office-demo runner — self-bootstrapping demo experience for AgentWorkersCode.
// Brings up: fixture repo -> pi worker -> office bridge -> pixel-agents office,
// narrates a compiled scenario (plan.json), then drops into interactive mode
// where the user types natural-language tasks and watches new characters work.
//
//   MODEL_API_KEY=... node runner.mjs
//
// Requires: git, node >=22, ../../pi built (npm --prefix ../../pi run build),
// and npx (pixel-agents is fetched on first run).
import { spawn, execSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync, readFileSync, readdirSync, unlinkSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir, tmpdir, platform } from 'node:os';
import { randomUUID, randomBytes } from 'node:crypto';
import { createInterface } from 'node:readline';

const HERE = dirname(fileURLToPath(import.meta.url));
const PI_DIR = resolve(HERE, '../../pi');
const WORK = join(HERE, '.local');
// fresh state dir per run: tasks left mid-flight by a killed run keep
// quiescent=false forever and would eat the worker's maxActive slots,
// queueing every future task. A run-scoped dir keeps repeat runs clean.
const RUN_ID_STATE = `pi-state-${Date.now().toString(36)}`;
let STATE = join(WORK, RUN_ID_STATE);
const FIXTURE = join(WORK, 'fixture-repo');
const PORT_START = Number(process.env.DEMO_WORKER_PORT || 18731);
let WORKER_PORT = PORT_START;
let WORKER = `http://127.0.0.1:${WORKER_PORT}`;
const children = [];

// repeatable one-click runs: persist the worker token and a pid list so a new
// run can reuse (or clean up) whatever a previous run left behind — even if
// the console window was just closed without Ctrl+C.
mkdirSync(WORK, { recursive: true });
const TOKEN_FILE = join(WORK, 'worker-token');
const PID_FILE = join(WORK, 'children.json');
const TOKEN = process.env.WORKER_LOCAL_TOKEN
  || (existsSync(TOKEN_FILE) ? readFileSync(TOKEN_FILE, 'utf8').trim() : null)
  || (() => { const t = randomBytes(16).toString('hex'); writeFileSync(TOKEN_FILE, t); return t; })();

function killPreviousRun() {
  if (!existsSync(PID_FILE)) return;
  try {
    for (const pid of JSON.parse(readFileSync(PID_FILE, 'utf8')))
      try { process.kill(pid, 'SIGTERM'); } catch {}
  } catch {}
}
function noteChild(p) { p.on('exit', () => { p._dead = true; }); children.push(p); writeFileSync(PID_FILE, JSON.stringify(children.map(c => c.pid).filter(Boolean))); }

const say = (s) => console.log(`[demo] ${s}`);
const narrate = (s) => console.log(`\n[demo] ${s}\n`);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const die = (s) => { console.error(`[demo] ${s}`); cleanup(); process.exit(1); };

function cleanup() {
  for (const c of children) { if (!c._dead) try { c.kill('SIGTERM'); } catch {} }
  try { unlinkSync(PID_FILE); } catch {}
}
process.on('SIGINT', () => { cleanup(); process.exit(0); });
process.on('exit', cleanup);

function sh(cmd, cwd) { return execSync(cmd, { cwd, stdio: 'pipe', encoding: 'utf8' }).trim(); }

// ---------- fixture repo ----------
const FILES = {
  'README.md': '# ACME intranet demo\n\nA tiny project for the office demo.\n\n## Modules\n- docs: internal guides\n- data: seed data files\n- src: a pretend service\n\nThe word "quasar" appears in exactly one data file.\n',
  'docs/deploy-guide.md': '# Deploy guide\n\n1. Build the bundle\n2. Ship to staging\n3. Ping #ops\n',
  'docs/onboarding.md': '# Onboarding\n\nNew hires read README.md first, then this file.\n',
  'data/customers.csv': 'id,name,region\n1,Acme East,east\n2,Acme West,west\n',
  'data/notes.txt': 'release checklist\n- verify quasar flag\n- tag the build\n',
  'src/app.js': 'export function hello(name) {\n  return `hello ${name}`;\n}\n',
  'src/config.json': '{ "env": "demo", "quasar": false }\n',
};
function ensureFixture() {
  mkdirSync(FIXTURE, { recursive: true });
  for (const [p, content] of Object.entries(FILES)) {
    const f = join(FIXTURE, p);
    mkdirSync(dirname(f), { recursive: true });
    if (!existsSync(f)) writeFileSync(f, content);
  }
  if (!existsSync(join(FIXTURE, '.git'))) {
    sh('git init -q', FIXTURE);
    sh('git add -A', FIXTURE);
    sh('git -c user.email=demo@local -c user.name=demo commit -qm seed', FIXTURE);
  } else {
    sh('git add -A', FIXTURE);
    try { sh('git -c user.email=demo@local -c user.name=demo commit -qm reseed', FIXTURE); } catch {}
  }
  // claude slug dir must exist BEFORE pixel-agents starts (it only watches
  // project dirs present at its own startup)
  const slug = FIXTURE.replace(/\\/g, '/').replace(/[^a-zA-Z0-9-]/g, '-').replace(/^-+/, '');
  const slugDir = join(homedir(), '.claude', 'projects', slug);
  mkdirSync(slugDir, { recursive: true });
  // fresh office each run: drop transcripts left by previous runs
  for (const f of readdirSync(slugDir).filter(f => f.endsWith('.jsonl')))
    try { unlinkSync(join(slugDir, f)); } catch {}
  return slug;
}

// ---------- pi worker ----------
async function workerStatus(url) {
  try {
    const r = await fetch(`${url}/local/v1/identity`, { headers: { authorization: `Bearer ${TOKEN}` } });
    return r.status === 200 ? 'ours' : r.status === 401 ? 'foreign' : 'busy';
  } catch { return 'free'; }
}
async function startPi() {
  // pick a port: reuse a still-running demo worker that already has the
  // fixture project; otherwise find the first free port (never kill or claim
  // a foreign service / a worker without our fixture project).
  let firstFree = null;
  for (let port = PORT_START; port < PORT_START + 20; port++) {
    const url = `http://127.0.0.1:${port}`;
    const st = await workerStatus(url);
    if (st === 'ours') {
      const pj = await fetch(`${url}/local/v1/projects`, { headers: { authorization: `Bearer ${TOKEN}` } }).then(r => r.json()).catch(() => ({ items: [] }));
      if ((pj.items || []).some(p => p.projectId === 'fixture')) { WORKER_PORT = port; WORKER = url; say(`reusing running pi worker ${WORKER}`); return; }
    } else if (st === 'free' && firstFree === null) firstFree = { port, url };
  }
  if (!firstFree) die(`no free port in ${PORT_START}..${PORT_START + 19} — set DEMO_WORKER_PORT`);
  WORKER_PORT = firstFree.port; WORKER = firstFree.url;
  if (!existsSync(join(PI_DIR, 'dist/host/main.js'))) {
    say('building pi worker (npm run build)...');
    try { execSync('npm run build', { cwd: PI_DIR, stdio: 'inherit' }); }
    catch { die('pi build failed — run `npm --prefix ../pi install && npm --prefix ../pi run build` first.'); }
  }
  mkdirSync(STATE, { recursive: true });
  const init = spawn('node', ['dist/host/main.js', 'init', '--state', STATE, '--project', FIXTURE, '--project-id', 'fixture'], { cwd: PI_DIR, env: process.env });
  await new Promise((res, rej) => { init.on('exit', c => c === 0 ? res() : rej(new Error('pi init failed'))); init.stderr.on('data', d => process.stderr.write(d)); });
  const svc = spawn('node', ['dist/host/main.js', 'serve', '--state', STATE, '--port', String(WORKER_PORT)], { cwd: PI_DIR, env: { ...process.env, WORKER_LOCAL_TOKEN: TOKEN } });
  noteChild(svc);
  await new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error('pi did not become ready')), 20000);
    svc.stdout.on('data', d => { if (String(d).includes('LOCAL_TASK_READY')) { clearTimeout(t); res(); } });
  });
  say(`pi worker ready ${WORKER}`);
}

// ---------- office bridge ----------
function startBridge() {
  const b = spawn('node', [join(HERE, 'bridge.mjs')], {
    env: { ...process.env, WORKER_LOCAL_TOKEN: TOKEN, BRIDGE_WORKERS: WORKER, BRIDGE_CWD: FIXTURE, BRIDGE_POLL_MS: '2000' },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  noteChild(b);
  b.stdout.on('data', d => process.stdout.write(String(d).replace(/\[bridge\]/g, '[demo bridge]')));
  say('office bridge watching worker-events');
}

// ---------- pixel-agents office ----------
async function startOffice() {
  const o = platform() === 'win32'
    ? spawn('cmd.exe', ['/c', 'npx', '-y', 'pixel-agents'], { env: process.env })
    : spawn('npx', ['-y', 'pixel-agents'], { env: process.env });
  noteChild(o);
  const url = await new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error('pixel-agents did not print its URL')), 90000);
    o.stdout.on('data', d => {
      process.stdout.write(String(d).replace(/Pixel Agents/g, '[office]'));
      const m = String(d).match(/http:\/\/[^\s]+\?token=[^\s]+/);
      if (m) { clearTimeout(t); res(m[0]); }
    });
    o.stderr?.on('data', d => process.stderr.write(d));
  });
  say(`office ready ${url}`);
  const opener = platform() === 'win32' ? 'start ""' : platform() === 'darwin' ? 'open' : 'xdg-open';
  try { execSync(`${opener} "${url}"`, { shell: true }); } catch { say(`open this URL in your browser: ${url}`); }
}

// ---------- task api ----------
async function api(path, opts = {}) {
  const r = await fetch(`${WORKER}/local/v1${path}`, {
    ...opts,
    headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json', ...(opts.headers || {}) },
  });
  if (!r.ok) throw new Error(`${path} -> ${r.status}`);
  return r.json();
}
const RUN_ID = Date.now().toString(36);
async function dispatch(project, key, instruction) {
  // requestKeys are idempotent — namespace per run so replays across runs
  // create fresh tasks instead of replaying stale ones.
  const k = `${key}-${RUN_ID}`;
  const pf = await api('/task-preflights', { method: 'POST', body: JSON.stringify({ projectId: project, engineId: 'pi' }) });
  const r = await api('/tasks', { method: 'POST', headers: { 'idempotency-key': k },
    body: JSON.stringify({ requestKey: k, projectId: project, engineId: 'pi', optionsRevision: pf.optionsRevision, instruction }) });
  return r.taskId;
}
async function taskState(id) { try { return (await api(`/tasks/${id}`)).state; } catch { return 'UNKNOWN'; } }
const TERMINAL = /SUCCEEDED|FAILED|CANCELLED|STOPPED|REJECTED/;

// ---------- plan execution ----------
async function runPlan(plan) {
  narrate(plan.title);
  for (const beat of plan.beats) {
    if (beat.type === 'narrate') narrate(beat.text);
    else if (beat.type === 'dispatch') {
      const id = await dispatch(beat.project || 'fixture', beat.key, beat.instruction);
      say(`→ task dispatched (${beat.label || beat.key}) — watch the office`);
      beat._taskId = id;
    }
    else if (beat.type === 'wait') await sleep((beat.seconds || 5) * 1000);
    else if (beat.type === 'wait-tasks') {
      const ids = plan.beats.filter(b => b._taskId);
      const deadline = Date.now() + (beat.timeoutSeconds || 240) * 1000;
      for (;;) {
        const states = await Promise.all(ids.map(b => taskState(b._taskId)));
        if (states.every(s => TERMINAL.test(s))) { say('all tasks finished'); break; }
        if (Date.now() > deadline) { say('(some tasks still running — moving on)'); break; }
        await sleep(3000);
      }
    }
    else if (beat.type === 'show-outputs') {
      narrate(beat.text || 'Here is what the agents actually produced:');
      const ownIds = new Set(plan.beats.filter(b => b._taskId).map(b => b._taskId));
      // task workspaces may live under any pi-state-* dir (e.g. when we
      // reused a still-running worker from an earlier run)
      const stateDirs = readdirSync(WORK).filter(d => d.startsWith('pi-state')).map(d => join(WORK, d, 'workspaces')).filter(existsSync);
      for (const t of ownIds) for (const wsDir of stateDirs.filter(w => existsSync(join(w, t)))) {
        const d = join(wsDir, t);
        let changed = [];
        try {
          changed = sh('git status --porcelain', d).split('\n')
            .map(l => l.slice(3).trim().replace(/^"|"$/g, ''))
            .filter(f => f && (beat.pattern ? f.includes(beat.pattern) : true));
        } catch { changed = readdirSync(d).filter(f => beat.pattern ? f.includes(beat.pattern) : false); }
        for (const f of changed) {
          const fp = join(d, f);
          if (existsSync(fp) && !f.endsWith('/')) {
            console.log(`\n--- ${t.slice(0, 30)} / ${f} ---`);
            try { console.log(readFileSync(fp, 'utf8').slice(0, 1200)); } catch {}
          }
        }
      }
    }
    else if (beat.type === 'interactive') await interactive();
  }
}

// ---------- interactive ----------
async function interactive() {
  narrate('Now it is your turn. Type any instruction in natural language — a new character will walk in and do it. Empty line exits.');
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  const ask = (q) => new Promise(r => rl.question(q, r));
  let n = 0;
  for (;;) {
    const line = (await ask('> ')).trim();
    if (!line) break;
    n++;
    try {
      const id = await dispatch('fixture', `user-${Date.now()}-${n}`, line);
      say(`dispatched (${id.slice(0, 24)}…) — watch the office, it will show up in a few seconds`);
    } catch (e) { say(`dispatch failed: ${e.message}`); }
  }
  rl.close();
}

// ---------- main ----------
const plan = JSON.parse(readFileSync(join(HERE, 'plan.json'), 'utf8'));
if (!process.env.MODEL_API_KEY) die('MODEL_API_KEY is not set — the pi engine needs a model key. Export it and retry (MODEL_BASE_URL / MODEL_NAME optional overrides).');
say('preparing fixture workspace...');
killPreviousRun();
ensureFixture();
await startPi();
startBridge();
await startOffice();
await sleep(2500); // let the office finish its first directory scan
await runPlan(plan);
say('demo finished. Ctrl+C to stop all services.');
