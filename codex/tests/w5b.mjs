// Real host process + real pinned app-server + real model.
// W5b slice coverage: writable collaboration DAGs (P14) — durable intake,
// isolated child checkouts, dependency-gated scheduling, no parent leakage —
// and guarded explicit adoption (P15): ADOPTED, idempotent NO_CHANGES, parent
// drift conflict with zero writes, plus stop/restart honesty. Mirrors the
// worker-testkit parity N19/N20 flow against the codex engine.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, randomUUID} from 'node:crypto';
import {existsSync} from 'node:fs';
import {mkdtemp, mkdir, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';

const [binaryArg, templateArg] = process.argv.slice(2);
assert(binaryArg && templateArg, 'Usage: node tests/w5b.mjs <host executable> <initialized state template>');
const binary = resolve(binaryArg), template = resolve(templateArg);
assert(process.env.MODEL_API_KEY, 'REAL_MODEL_BINDING_REQUIRED');
const root = await mkdtemp(join(tmpdir(), 'nwf-codex-w5b-'));
assert(root.startsWith(join(tmpdir(), 'nwf-codex-w5b-')), 'TEST_CLEANUP_SCOPE');
const token = randomBytes(32).toString('hex');
let live;
const exited = child => new Promise(r => child.once('exit', (code, signal) => r({code, signal})));
async function start() {
  const child = spawn(binary, ['serve', '--state', root, '--port', '0'], {env: {...process.env, WORKER_LOCAL_TOKEN: token}, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']});
  let output = '', diagnostic = '';
  child.stdout.on('data', b => output += b);
  child.stderr.on('data', b => diagnostic += b);
  const until = Date.now() + 60000;
  while (Date.now() < until) {
    const m = /LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output);
    if (m) { live = child; return m[1]; }
    if (child.exitCode !== null) throw new Error('HOST_START_FAILED ' + diagnostic.trim());
    await delay(25);
  }
  child.kill();
  throw new Error('HOST_START_TIMEOUT ' + diagnostic.trim());
}
async function request(base, method, tail, body, extra = {}) {
  const r = await fetch(base + tail, {method, headers: {authorization: 'Bearer ' + token, 'content-type': 'application/json', ...extra}, body: body === undefined ? undefined : JSON.stringify(body)});
  return {status: r.status, body: await r.json()};
}
async function detail(base, taskId) { return (await request(base, 'GET', '/local/v1/tasks/' + taskId)).body; }
async function allEvents(base, taskId) {
  let cursor = 0, items = [];
  for (;;) {
    const p = await request(base, 'GET', '/local/v1/tasks/' + taskId + '/events?limit=100&cursor=' + cursor);
    const batch = p.body.items ?? [];
    items.push(...batch);
    if (batch.length < 100) return items;
    cursor = p.body.cursor ?? batch[batch.length - 1].cursor;
  }
}
async function waitTask(base, taskId, states, ms) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    const d = await detail(base, taskId);
    if (states.includes(d.state) && (!['CANCELLED', 'BUDGET_EXCEEDED'].includes(d.state) || d.quiescent === true)) return d;
    assert(!['FAILED', 'REJECTED'].includes(d.state), 'NATIVE_TASK_FAILED ' + d.state + ' ' + (d.reasonCode ?? ''));
    await delay(500);
  }
  const last = await detail(base, taskId);
  throw new Error('NATIVE_DEADLINE_EXCEEDED ' + states.join('/') + ' last=' + JSON.stringify({state: last.state, reasonCode: last.reasonCode}));
}
async function collabGet(base, taskId, collabId) { return request(base, 'GET', `/local/v1/tasks/${taskId}/collaborations/${collabId}`); }
const childStates = c => Object.fromEntries((c.children ?? []).map(ch => [ch.childId, ch]));
const TERMINAL = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'BLOCKED'];
async function waitCollab(base, taskId, collabId, ms) {
  const until = Date.now() + ms;
  let last;
  while (Date.now() < until) {
    last = (await collabGet(base, taskId, collabId)).body;
    const children = last.children ?? [];
    if (children.length && children.every(ch => TERMINAL.includes(ch.state))) return last;
    await delay(500);
  }
  throw new Error('COLLAB_DEADLINE ' + collabId + ' last=' + JSON.stringify(last));
}
async function waitChild(base, taskId, collabId, childId, states, ms) {
  const until = Date.now() + ms;
  let last;
  while (Date.now() < until) {
    last = (await collabGet(base, taskId, collabId)).body;
    const st = childStates(last)[childId];
    if (st && states.includes(st.state)) return st;
    await delay(400);
  }
  throw new Error('COLLAB_CHILD_DEADLINE ' + collabId + '/' + childId + ' last=' + JSON.stringify(last));
}
let checks = 0;
try {
  await mkdir(join(root, 'profile'));
  await writeFile(join(root, 'registry.json'), await readFile(join(template, 'registry.json')));
  await writeFile(join(root, 'profile', 'config.toml'), await readFile(join(template, 'profile', 'config.toml')));
  const registry = JSON.parse(await readFile(join(root, 'registry.json'), 'utf8'));
  const base = await start();
  const checked = (await request(base, 'POST', '/local/v1/task-preflights', {projectId: registry.project_id, engineId: 'codex'})).body;
  const mk = (requestKey, extra = {}) => ({requestKey, projectId: registry.project_id, engineId: 'codex', optionsRevision: checked.optionsRevision, instruction: 'placeholder', ...extra});
  // Capability surface: W5b records IMPLEMENTED (PARTIAL qualification).
  const caps = (await request(base, 'GET', '/worker-host/v1/capabilities')).body.items;
  const cap = id => caps.find(c => c.id === id);
  for (const id of ['execution.collaborations', 'execution.adoptions']) {
    assert.equal(cap(id)?.implementation, 'IMPLEMENTED', 'CAPABILITY_MISSING ' + id);
  }
  const engines = (await request(base, 'GET', '/local/v1/engines')).body.items;
  assert.equal(engines[0].capabilities.collaborations.dag, true);
  assert.equal(engines[0].capabilities.collaborations.isolatedCheckout, true);
  assert.equal(engines[0].capabilities.adoptions.conflictCheck, true);
  assert.equal(engines[0].capabilities.adoptions.partial, true);
  checks++;
  // Parent task: one trivial real turn so the collaboration has a live parent.
  const t1key = 'w5b-t1-' + randomUUID().slice(0, 8);
  const marker = 'NWF_W5B_' + randomUUID().replaceAll('-', '');
  const t1 = await request(base, 'POST', '/local/v1/tasks', mk(t1key, {
    instruction: 'Reply with exactly the token W5B-T1-OK and nothing else. Do not run tools or commands.',
  }), {'Idempotency-Key': t1key});
  assert.equal(t1.status, 202, JSON.stringify(t1.body));
  const t1Id = t1.body.taskId;
  const t1Done = await waitTask(base, t1Id, ['SUCCEEDED'], 420000);
  const ws = t1Done.workspace;
  const d1 = await detail(base, t1Id);
  assert.equal(d1.capabilities.collaborations, true, 'TASK_COLLAB_CAPABILITY_MISSING');
  assert.equal(d1.capabilities.adoptions, true, 'TASK_ADOPT_CAPABILITY_MISSING');
  checks++;
  // ---- Intake validation: every refusal is deterministic (no model spend) ----
  const collabPost = (body, key) => request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations`, body, {'Idempotency-Key': key ?? body.requestKey});
  const badChildren = [
    ['w5b-bad-cycle', {name: 'x', children: [{childId: 'a', goal: 'g', dependsOn: ['b']}, {childId: 'b', goal: 'g', dependsOn: ['a']}]}, 'COLLABORATION_DEPENDENCY_CYCLE'],
    ['w5b-bad-self', {children: [{childId: 'a', goal: 'g', dependsOn: ['a']}]}, 'COLLABORATION_DEPENDENCY_CYCLE'],
    ['w5b-bad-unknown', {children: [{childId: 'a', goal: 'g', dependsOn: ['ghost']}]}, 'COLLABORATION_DEPENDENCY_UNKNOWN'],
    ['w5b-bad-id', {children: [{childId: 'BAD_ID', goal: 'g'}]}, 'INVALID_COLLABORATION_CHILD_ID'],
    ['w5b-bad-dup', {children: [{childId: 'a', goal: 'g'}, {childId: 'a', goal: 'h'}]}, 'INVALID_COLLABORATION_CHILD_ID'],
    ['w5b-bad-empty', {children: []}, 'INVALID_COLLABORATION_CHILDREN'],
    ['w5b-bad-many', {children: Array.from({length: 9}, (_, i) => ({childId: 'c' + i, goal: 'g'}))}, 'INVALID_COLLABORATION_CHILDREN'],
    ['w5b-bad-goal', {children: [{childId: 'a', goal: '   '}]}, 'INVALID_COLLABORATION_GOAL'],
    ['w5b-bad-name', {name: 'n'.repeat(65), children: [{childId: 'a', goal: 'g'}]}, 'INVALID_COLLABORATION_NAME'],
    ['w5b-bad-pkgs', {children: [{childId: 'a', goal: 'g', contextPackageIds: ['pkg-missing']}]}, 'PACKAGE_NOT_FOUND'],
  ];
  for (const [key, body, code] of badChildren) {
    const r = await collabPost({requestKey: key, ...body});
    assert.equal(r.status, code === 'PACKAGE_NOT_FOUND' ? 404 : 409, key + ' status=' + r.status + ' ' + JSON.stringify(r.body));
    assert.equal(r.body.code, code, key + ' body=' + JSON.stringify(r.body));
    checks++;
  }
  assert.equal((await collabGet(base, t1Id, 'collab-missing')).status, 404);
  checks++;
  // ---- N19 COLLABORATION_DAG_ISOLATED ----
  const childFile = 'nwf-collab-' + marker.toLowerCase().replace(/[^a-z0-9]/g, '') + '.txt';
  const c1key = 'w5b-collab-' + randomUUID().slice(0, 8);
  const c1body = {requestKey: c1key, name: 'nwf-w5b', children: [
    {childId: 'writer', goal: `Use your write tool to create the file ${childFile} in your workspace containing exactly the text ${marker} then reply done.`},
    {childId: 'after', goal: 'Reply with the single word ready.', dependsOn: ['writer']},
  ]};
  const made = await collabPost(c1body);
  assert.equal(made.status, 202, JSON.stringify(made.body));
  const collabId = made.body.collaborationId;
  assert(collabId, 'COLLABORATION_ID_MISSING');
  checks++;
  // Idempotent intake: the same key+body replays the stored acceptance; a
  // different body under the same key conflicts.
  const replay = await collabPost(c1body);
  assert.equal(replay.status, 202);
  assert.equal(replay.body.collaborationId, collabId);
  const iconflict = await collabPost({...c1body, name: 'changed'});
  assert.equal(iconflict.status, 409);
  assert.equal(iconflict.body.code, 'IDEMPOTENCY_CONFLICT');
  checks++;
  const c1 = await waitCollab(base, t1Id, collabId, 420000);
  const states = childStates(c1);
  assert.equal(states.writer?.state, 'SUCCEEDED', 'COLLAB_CHILD_WRITER_NOT_SUCCEEDED ' + JSON.stringify(states.writer));
  assert.equal(states.after?.state, 'SUCCEEDED', 'COLLAB_CHILD_AFTER_NOT_SUCCEEDED ' + JSON.stringify(states.after));
  assert(states.writer.startedAt && states.writer.finishedAt, 'WRITER_TIMES_MISSING');
  assert(states.after.startedAt && states.after.finishedAt, 'AFTER_TIMES_MISSING');
  assert(states.after.startedAt >= states.writer.finishedAt, 'DAG_ORDER_NOT_HONOURED ' + states.after.startedAt + ' < ' + states.writer.finishedAt);
  assert.equal(c1.state, 'COMPLETED');
  // The child wrote into its own checkout only: the parent workspace stays
  // clean until an explicit adoption.
  assert(!existsSync(join(ws, childFile)), 'CHILD_OUTPUT_LEAKED_TO_PARENT');
  const childWs = states.writer.workspace;
  assert(childWs && childWs.includes('collab'), 'CHILD_WORKSPACE_NOT_ISOLATED ' + childWs);
  assert.notEqual(resolve(childWs), resolve(ws), 'CHILD_WS_IS_PARENT_WS');
  assert.equal(await readFile(join(childWs, childFile), 'utf8'), marker, 'CHILD_FILE_MISSING');
  assert(states.writer.nativeSessionId, 'CHILD_THREAD_MISSING');
  assert.notEqual(states.writer.nativeSessionId, t1Done.nativeSessionId, 'CHILD_SHARED_MAIN_THREAD');
  checks++;
  // Collaboration lists on the task detail; events + audit are durable.
  const d2 = await detail(base, t1Id);
  assert((d2.collaborations ?? []).some(c => c.collaborationId === collabId), 'COLLAB_NOT_ON_TASK_DETAIL');
  const ev = await allEvents(base, t1Id);
  for (const type of ['collaboration.created', 'collaboration.child.started', 'collaboration.child.completed', 'collaboration.completed']) {
    assert(ev.some(e => e.type === type), 'COLLAB_EVENT_MISSING ' + type);
  }
  const audit = (await request(base, 'GET', `/worker-host/v1/tasks/${t1Id}/audit`)).body;
  const kinds = (audit.items ?? []).map(i => i.kind);
  assert(kinds.includes('collaboration') && kinds.includes('collaboration.child'), 'AUDIT_COLLAB_MISSING');
  assert(!JSON.stringify(audit).includes('Reply with the single word ready'), 'AUDIT_LEAKED_GOAL');
  checks++;
  // ---- N20 ADOPTION_EXPLICIT_GUARDED ----
  const a1key = 'w5b-adopt-' + randomUUID().slice(0, 8);
  const a1body = {requestKey: a1key, childId: 'writer', mode: 'FILES', paths: [childFile]};
  const adopt = await request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations/${collabId}/adoptions`, a1body, {'Idempotency-Key': a1key});
  assert.equal(adopt.status, 202, JSON.stringify(adopt.body));
  assert.equal(adopt.body.outcome, 'ADOPTED', 'ADOPTION_NOT_APPLIED');
  const adopted = join(ws, childFile);
  assert(existsSync(adopted), 'ADOPTED_FILE_MISSING');
  assert.equal(await readFile(adopted, 'utf8'), marker, 'ADOPTED_CONTENT_WRONG');
  checks++;
  // Replay: the same request serves the stored result, never recomputed.
  const adoptReplay = await request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations/${collabId}/adoptions`, a1body, {'Idempotency-Key': a1key});
  assert.equal(adoptReplay.status, 202);
  assert.equal(adoptReplay.body.outcome, 'ADOPTED');
  // Path discipline: escapes, non-candidate and absent paths refuse honestly.
  for (const [paths, code] of [
    [['../escape.txt'], 'INVALID_ADOPTION_PATHS'],
    [['D:/abs.txt'], 'INVALID_ADOPTION_PATHS'],
    [['not-a-candidate.txt'], 'ADOPTION_PATH_NOT_CANDIDATE'],
  ]) {
    const k = 'w5b-adoptbad-' + randomUUID().slice(0, 8);
    const r = await request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations/${collabId}/adoptions`, {requestKey: k, childId: 'writer', mode: 'FILES', paths}, {'Idempotency-Key': k});
    assert.equal(r.status, 409, JSON.stringify(r.body));
    assert.equal(r.body.code, code, 'body=' + JSON.stringify(r.body));
    checks++;
  }
  // Repeat adoption of identical content is idempotent: NO_CHANGES.
  const a2key = 'w5b-adopt2-' + randomUUID().slice(0, 8);
  const again = await request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations/${collabId}/adoptions`, {requestKey: a2key, childId: 'writer', mode: 'ALL'}, {'Idempotency-Key': a2key});
  assert.equal(again.status, 202);
  assert.equal(again.body.outcome, 'NO_CHANGES', 'REPEAT_ADOPTION_NOT_IDEMPOTENT');
  checks++;
  // Parent drift under the adopted path conflicts as a 409 with zero writes.
  const drift = 'parent drift ' + marker;
  await writeFile(adopted, drift);
  const a3key = 'w5b-adopt3-' + randomUUID().slice(0, 8);
  const conflict = await request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations/${collabId}/adoptions`, {requestKey: a3key, childId: 'writer', mode: 'ALL'}, {'Idempotency-Key': a3key});
  assert.equal(conflict.status, 409, 'PARENT_DRIFT_NOT_CONFLICT ' + JSON.stringify(conflict.body));
  assert.equal(conflict.body.outcome, 'CONFLICT');
  assert.equal(await readFile(adopted, 'utf8'), drift, 'CONFLICT_WRITE_LEAKED');
  const c1After = (await collabGet(base, t1Id, collabId)).body;
  assert(childStates(c1After).writer.adoptions.length >= 3, 'ADOPTION_RECORDS_MISSING');
  checks++;
  // ---- Collaboration stop: pending dependents block, the peer is killed ----
  const c2key = 'w5b-collab2-' + randomUUID().slice(0, 8);
  const made2 = await collabPost({requestKey: c2key, children: [
    {childId: 'slow', goal: 'Count slowly to five hundred, writing each number on its own line of your reply. Do not create files.'},
    {childId: 'after', goal: 'Reply with the single word ready.', dependsOn: ['slow']},
  ]});
  assert.equal(made2.status, 202);
  const collab2 = made2.body.collaborationId;
  await waitChild(base, t1Id, collab2, 'slow', ['RUNNING', 'SUCCEEDED'], 120000);
  const stopKey = 'w5b-stop-' + randomUUID().slice(0, 8);
  const stopped = await request(base, 'POST', `/local/v1/tasks/${t1Id}/collaborations/${collab2}/stop`, {requestKey: stopKey}, {'Idempotency-Key': stopKey});
  assert.equal(stopped.status, 202, JSON.stringify(stopped.body));
  const c2 = (await collabGet(base, t1Id, collab2)).body;
  assert(['CANCELLED', 'COMPLETED'].includes(c2.state), 'COLLAB_STOP_STATE ' + c2.state);
  if (c2.state === 'CANCELLED') {
    const s2 = childStates(c2);
    assert(TERMINAL.includes(s2.slow?.state), 'STOPPED_CHILD_STILL_OPEN ' + JSON.stringify(s2.slow));
    if (s2.after?.state === 'BLOCKED') assert.equal(s2.after.reasonCode, 'COLLABORATION_STOP');
  }
  checks++;
  // ---- Restart honesty: an in-flight child is never reported complete ----
  const c3key = 'w5b-collab3-' + randomUUID().slice(0, 8);
  const made3 = await collabPost({requestKey: c3key, children: [
    {childId: 'slow', goal: 'Count slowly to nine hundred, writing each number on its own line of your reply. Do not create files.'},
    {childId: 'after', goal: 'Reply with the single word ready.', dependsOn: ['slow']},
  ]});
  assert.equal(made3.status, 202);
  const collab3 = made3.body.collaborationId;
  const slow = await waitChild(base, t1Id, collab3, 'slow', ['RUNNING', 'SUCCEEDED'], 120000);
  if (slow.state === 'RUNNING') {
    live.kill();
    await exited(live);
    const base2 = await start();
    const c3 = (await collabGet(base2, t1Id, collab3)).body;
    assert.equal(c3.state, 'CANCELLED', 'RESTART_COLLAB_NOT_CANCELLED ' + c3.state);
    const s3 = childStates(c3);
    assert.equal(s3.slow?.state, 'CANCELLED', 'RESTART_CHILD_NOT_HONEST ' + JSON.stringify(s3.slow));
    assert.equal(s3.slow?.reasonCode, 'HOST_RESTART_JOB_CLOSED');
    assert.equal(s3.after?.state, 'BLOCKED', 'RESTART_DEPENDENT_NOT_BLOCKED ' + JSON.stringify(s3.after));
    checks++;
  }
  if (live && live.exitCode === null) live.kill();
  console.log('W5B_OK checks=' + checks);
} catch (e) {
  if (live && live.exitCode === null) live.kill();
  throw e;
}
