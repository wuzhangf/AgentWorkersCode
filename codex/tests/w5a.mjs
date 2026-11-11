// Real host process + real pinned app-server + real model (where noted).
// W5a slice coverage: frozen durable context packages (frozen bytes, sha256
// digest, lexical+realpath workspace scope, package limit/idempotency),
// auxiliary research sessions on a separate managed peer with an isolated
// workspace snapshot (real model Q&A against inlined package content, stop,
// session cap, guarded one-shot deliver into the main thread), and same-engine
// assignment transitions (STOP-machinery quiesce -> thread/resume + turn/start
// on the same native thread with a new generation/segment/epoch, stale-owner
// fencing, incompatible-target refusal) plus restart persistence.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, randomUUID} from 'node:crypto';
import {mkdtemp, mkdir, readFile, writeFile, rm, stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';

const [binaryArg, templateArg] = process.argv.slice(2);
assert(binaryArg && templateArg, 'Usage: node tests/w5a.mjs <host executable> <initialized state template>');
const binary = resolve(binaryArg), template = resolve(templateArg);
assert(process.env.MODEL_API_KEY, 'REAL_MODEL_BINDING_REQUIRED');
const here = fileURLToPath(new URL('.', import.meta.url));
const root = await mkdtemp(join(tmpdir(), 'nwf-codex-w5a-'));
assert(root.startsWith(join(tmpdir(), 'nwf-codex-w5a-')), 'TEST_CLEANUP_SCOPE');
const outside = await mkdtemp(join(tmpdir(), 'nwf-w5a-outside-'));
const token = randomBytes(32).toString('hex');
let live;
const exited = child => new Promise(r => child.once('exit', (code, signal) => r({code, signal})));
async function start(maxActive = '1') {
  const child = spawn(binary, ['serve', '--state', root, '--port', '0'], {env: {...process.env, WORKER_LOCAL_TOKEN: token, NWF_CODEX_MAX_ACTIVE_TASKS: maxActive}, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']});
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
const binding = d => ({generation: d.generation, nativeSessionId: d.nativeSessionId, nativeTurnId: d.nativeTurnId, targetCommandId: d.currentCommandId});
// Test-only scoped approver: only the exact fixture-write script or an
// apply_patch touching only the fixture file inside the task workspace.
function fixtureApproval(q, {workspace, targets}) {
  const method = q.nativeMethod ?? q.method;
  if (!(q.options ?? []).includes('accept')) return null;
  const params = q.params ?? {}; const scope = q.scope ?? {};
  const cwd = scope.cwd ?? params.cwd;
  if (typeof cwd === 'string' && resolve(cwd).toLowerCase() !== resolve(workspace).toLowerCase()) return null;
  if (method === 'item/commandExecution/requestApproval') {
    let actions = params.commandActions; if (!actions && typeof scope.commandActions === 'string') { try { actions = JSON.parse(scope.commandActions); } catch { return null; } }
    if (Array.isArray(actions) && actions.length >= 1 && actions.every(a => { const c = String(a?.command ?? ''); return targets.some(t => c.includes(t.file) && c.includes(t.marker)); })) return {decision: 'accept'};
    // The task workspace is a throwaway temp dir: any command that stays
    // inside it (cwd already checked, no `..`, every absolute path inside
    // the workspace, no piped/chained escapes) is safe to accept. Anything
    // referencing outside paths stays refused.
    const inScope = c => {
      const cmd = String(c);
      if (cmd.includes('..')) return false;
      const abs = cmd.match(/[A-Za-z]:[\\/][^\s"'|&;<>\]){}]*/g) ?? [];
      return abs.every(p => resolve(p).toLowerCase().startsWith(resolve(workspace).toLowerCase()));
    };
    if (Array.isArray(actions) && actions.length >= 1 && actions.every(a => inScope(a?.command ?? ''))) return {decision: 'accept'};
  }
  if (method === 'item/fileChange/requestApproval') {
    let paths = params.paths; if (!paths && typeof scope.paths === 'string') { try { paths = JSON.parse(scope.paths); } catch { return null; } }
    if (Array.isArray(paths) && paths.length === 1 && targets.some(t => resolve(workspace, t.file).toLowerCase() === resolve(workspace, paths[0]).toLowerCase())) return {decision: 'accept'};
  }
  return null;
}
const answered = new Set();
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
async function settle(base, taskId, targets, d) {
  const questions = await request(base, 'GET', '/local/v1/tasks/' + taskId + '/input-requests');
  for (const q of questions.body.items ?? []) {
    if (q.state !== 'PENDING' || answered.has(q.inputRequestId)) continue;
    const response = fixtureApproval(q, {workspace: d.workspace, targets}) ?? (q.nativeMethod === 'item/tool/requestUserInput' ? {value: 'Proceed with the original instruction'} : null);
    assert(response, 'OUT_OF_SCOPE_INPUT_REQUEST ' + (q.nativeMethod ?? q.method) + ' ' + JSON.stringify(q).slice(0, 900));
    const answer = {requestKey: 'nwf-answer-' + q.inputRequestId, response, generation: q.generation, nativeSessionId: q.nativeSessionId, nativeTurnId: q.nativeTurnId, targetCommandId: q.targetCommandId};
    const sent = await request(base, 'POST', '/local/v1/tasks/' + taskId + '/input-requests/' + q.inputRequestId + '/responses', answer, {'Idempotency-Key': answer.requestKey});
    assert.equal(sent.status, 202);
    answered.add(q.inputRequestId);
  }
}
async function waitTask(base, taskId, states, ms, targets = []) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    const d = await detail(base, taskId);
    if (states.includes(d.state) && (!['CANCELLED', 'BUDGET_EXCEEDED'].includes(d.state) || d.quiescent === true)) return d;
    assert(!['FAILED', 'REJECTED'].includes(d.state), 'NATIVE_TASK_FAILED ' + d.state + ' ' + (d.reasonCode ?? ''));
    await settle(base, taskId, targets, d);
    await delay(500);
  }
  const last = await detail(base, taskId);
  const ev = (await request(base, 'GET', '/local/v1/tasks/' + taskId + '/events?limit=50')).body.items.map(e => e.type);
  throw new Error('NATIVE_DEADLINE_EXCEEDED ' + states.join('/') + ' last=' + JSON.stringify({state: last.state, reasonCode: last.reasonCode, launchState: last.launchState}) + ' events=' + ev.join(','));
}
async function auxList(base, taskId) { return (await request(base, 'GET', `/local/v1/tasks/${taskId}/auxiliary-sessions`)).body.items ?? []; }
async function auxGet(base, taskId, auxId) { return request(base, 'GET', `/local/v1/tasks/${taskId}/auxiliary-sessions/${auxId}`); }
async function waitAux(base, taskId, auxId, states, ms) {
  const until = Date.now() + ms;
  let last;
  while (Date.now() < until) {
    last = (await auxGet(base, taskId, auxId)).body;
    if (states.includes(last.state)) return last;
    await delay(500);
  }
  throw new Error('AUX_DEADLINE ' + auxId + ' wanted=' + states.join('/') + ' last=' + JSON.stringify(last));
}
let checks = 0;
try {
  await mkdir(join(root, 'profile'));
  await writeFile(join(root, 'registry.json'), await readFile(join(template, 'registry.json')));
  await writeFile(join(root, 'profile', 'config.toml'), await readFile(join(template, 'profile', 'config.toml')));
  const registry = JSON.parse(await readFile(join(root, 'registry.json'), 'utf8'));
  const base = await start('2');
  const checked = (await request(base, 'POST', '/local/v1/task-preflights', {projectId: registry.project_id, engineId: 'codex'})).body;
  const mk = (requestKey, extra = {}) => ({requestKey, projectId: registry.project_id, engineId: 'codex', optionsRevision: checked.optionsRevision, instruction: 'placeholder', ...extra});
  // Capability surface: W5a records IMPLEMENTED, cross-engine an honest GAP.
  const caps = (await request(base, 'GET', '/worker-host/v1/capabilities')).body.items;
  const cap = id => caps.find(c => c.id === id);
  for (const id of ['execution.contextPackages', 'execution.auxiliarySessions', 'execution.transitions']) {
    assert.equal(cap(id)?.implementation, 'IMPLEMENTED', 'CAPABILITY_MISSING ' + id);
  }
  assert.equal(cap('execution.transitions').limits.crossEngine, false);
  assert.equal(cap('execution.crossEngineTransition').implementation, 'GAP');
  const engines = (await request(base, 'GET', '/local/v1/engines')).body.items;
  assert.equal(engines[0].capabilities.contextPackages, true);
  assert.equal(engines[0].capabilities.auxiliarySessions, true);
  assert.equal(engines[0].capabilities.transitions.sameEngine, true);
  assert.equal(engines[0].capabilities.transitions.crossEngine, false);
  checks++;
  // T1: a trivial real turn binds the thread; the fixture file the package
  // freezes is written host-side so the slice never depends on model I/O.
  const srcFile = 'w5a-source.txt';
  const marker1 = 'NWF_W5A_SRC_' + randomUUID().replaceAll('-', '');
  const t1key = 'w5a-t1-' + randomUUID().slice(0, 8);
  const t1 = await request(base, 'POST', '/local/v1/tasks', mk(t1key, {
    instruction: 'Reply with exactly the token W5A-T1-OK and nothing else. Do not run tools or commands.',
  }), {'Idempotency-Key': t1key});
  assert.equal(t1.status, 202, JSON.stringify(t1.body));
  const t1Id = t1.body.taskId;
  const t1Done = await waitTask(base, t1Id, ['SUCCEEDED'], 420000);
  const thread = t1Done.nativeSessionId, gen1 = t1Done.generation, seg1 = t1Done.segmentId;
  assert(thread && t1Done.nativeTurnId, 'NATIVE_BINDING_MISSING');
  assert.equal(t1Done.ownerEpoch, '1');
  const ws = t1Done.workspace;
  await writeFile(join(ws, srcFile), marker1 + '\n');
  checks++;
  // -- Context packages -----------------------------------------------------
  const pkgKey = 'w5a-pkg-' + randomUUID().slice(0, 8);
  const pkgBody = {requestKey: pkgKey, name: 'w5a-frozen', sources: [
    {kind: 'file', path: srcFile},
    {kind: 'text', text: 'frozen inline note w5a'},
    {kind: 'summary', text: 'summary: the marker file was created by the task'},
  ]};
  const pkg = await request(base, 'POST', `/local/v1/tasks/${t1Id}/context-packages`, pkgBody, {'Idempotency-Key': pkgKey});
  assert.equal(pkg.status, 200, JSON.stringify(pkg.body));
  const pkgId = pkg.body.packageId;
  assert.match(pkg.body.digest, /^[0-9a-f]{64}$/);
  assert.equal(pkg.body.sources.length, 3);
  assert(pkg.body.sources.every(s => /^[0-9a-f]{64}$/.test(s.sha256)), 'SOURCE_HASH_MISSING');
  checks++;
  // Same key + body replays the stored package; a different body conflicts.
  const replay = await request(base, 'POST', `/local/v1/tasks/${t1Id}/context-packages`, pkgBody, {'Idempotency-Key': pkgKey});
  assert.equal(replay.status, 200);
  assert.equal(replay.body.packageId, pkgId);
  const conflict = await request(base, 'POST', `/local/v1/tasks/${t1Id}/context-packages`, {...pkgBody, name: 'changed'}, {'Idempotency-Key': pkgKey});
  assert.equal(conflict.status, 409);
  assert.equal(conflict.body.code, 'IDEMPOTENCY_CONFLICT');
  checks++;
  // List + detail: file content rides base64 and matches the frozen bytes.
  const pkgList = (await request(base, 'GET', `/local/v1/tasks/${t1Id}/context-packages`)).body.items;
  assert(pkgList.some(p => p.packageId === pkgId), 'PACKAGE_NOT_LISTED');
  assert(!pkgList.find(p => p.packageId === pkgId).sources[0].content && !pkgList.find(p => p.packageId === pkgId).sources[0].text, 'LIST_LEAKED_CONTENT');
  const pkgDetail = (await request(base, 'GET', `/local/v1/tasks/${t1Id}/context-packages/${pkgId}`)).body;
  const fileSrc = pkgDetail.sources.find(s => s.kind === 'file');
  assert.equal(fileSrc.path, srcFile);
  assert.equal(Buffer.from(fileSrc.content, 'base64').toString('utf8'), marker1 + '\n');
  const textSrc = pkgDetail.sources.find(s => s.kind === 'text');
  assert.equal(textSrc.text, 'frozen inline note w5a');
  checks++;
  // Frozen proof: rewriting the workspace file cannot mutate the package.
  const markerGone = 'NWF_W5A_GONE_' + randomUUID().replaceAll('-', '');
  await writeFile(join(ws, srcFile), markerGone + '\n');
  const pkgAfter = (await request(base, 'GET', `/local/v1/tasks/${t1Id}/context-packages/${pkgId}`)).body;
  assert.equal(Buffer.from(pkgAfter.sources.find(s => s.kind === 'file').content, 'base64').toString('utf8'), marker1 + '\n', 'PACKAGE_NOT_FROZEN');
  assert.equal(pkgAfter.digest, pkgDetail.digest);
  checks++;
  // Scope enforcement: escaping paths, missing files and oversize are refused.
  // Size limits ride FILE sources so the request stays under the 64 KiB body cap.
  const outsideFile = join(outside, 'w5a-escape.txt');
  await writeFile(outsideFile, 'outside content\n');
  const bigFile = 'w5a-big.txt';
  await writeFile(join(ws, bigFile), 'x'.repeat(300 * 1024));
  const chunk = 'y'.repeat(250 * 1024); // each under 256 KiB, 5 x 250KiB > 1 MiB total
  const manyBig = [];
  for (let i = 0; i < 5; i++) { const f = `w5a-chunk-${i}.txt`; await writeFile(join(ws, f), chunk); manyBig.push({kind: 'file', path: f}); }
  for (const [name, body, code] of [
    ['w5a-pkg-rel-escape', {requestKey: 'k-esc-' + randomUUID().slice(0, 8), sources: [{kind: 'file', path: '../outside.txt'}]}, 'PACKAGE_SOURCE_OUT_OF_SCOPE'],
    ['w5a-pkg-abs-escape', {requestKey: 'k-abs-' + randomUUID().slice(0, 8), sources: [{kind: 'file', path: outsideFile}]}, 'PACKAGE_SOURCE_OUT_OF_SCOPE'],
    ['w5a-pkg-missing', {requestKey: 'k-mis-' + randomUUID().slice(0, 8), sources: [{kind: 'file', path: 'no-such-file.txt'}]}, 'PACKAGE_SOURCE_UNAVAILABLE'],
    ['w5a-pkg-oversize', {requestKey: 'k-big-' + randomUUID().slice(0, 8), sources: [{kind: 'file', path: bigFile}]}, 'PACKAGE_SOURCE_TOO_LARGE'],
    ['w5a-pkg-total', {requestKey: 'k-tot-' + randomUUID().slice(0, 8), sources: manyBig}, 'PACKAGE_SOURCE_TOO_LARGE'],
    ['w5a-pkg-toomany', {requestKey: 'k-many-' + randomUUID().slice(0, 8), sources: Array.from({length: 21}, (_, i) => ({kind: 'text', text: 's' + i}))}, 'INVALID_PACKAGE_SOURCES'],
    ['w5a-pkg-badkind', {requestKey: 'k-kind-' + randomUUID().slice(0, 8), sources: [{kind: 'url', text: 'http://x'}]}, 'INVALID_PACKAGE_SOURCES'],
    ['w5a-pkg-empty', {requestKey: 'k-empty-' + randomUUID().slice(0, 8), sources: []}, 'INVALID_PACKAGE_SOURCES'],
  ]) {
    const r = await request(base, 'POST', `/local/v1/tasks/${t1Id}/context-packages`, body, {'Idempotency-Key': body.requestKey});
    assert.equal(r.status, 409, name + ' ' + JSON.stringify(r.body));
    assert.equal(r.body.code, code, name);
    checks++;
  }
  assert.equal((await request(base, 'GET', `/local/v1/tasks/${t1Id}/context-packages/pkg-missing`)).status, 404);
  checks++;
  // -- Auxiliary sessions ---------------------------------------------------
  // marker1 only survives in the frozen package (the workspace file was
  // overwritten): an aux answer that reports it proves inline context delivery.
  const auxQuestion = `A frozen context source shows an earlier file content. Reply with exactly the marker token it contains (it starts with NWF_W5A_SRC_). Output the token and nothing else.`;
  const aux1Key = 'w5a-aux1-' + randomUUID().slice(0, 8);
  const aux1 = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions`, {requestKey: aux1Key, question: auxQuestion, contextPackageIds: [pkgId]}, {'Idempotency-Key': aux1Key});
  assert.equal(aux1.status, 202, JSON.stringify(aux1.body));
  const aux1Id = aux1.body.auxSessionId;
  assert.equal(aux1.body.state, 'RUNNING');
  checks++;
  const aux1Done = await waitAux(base, t1Id, aux1Id, ['SUCCEEDED'], 240000);
  assert.equal(aux1Done.state, 'SUCCEEDED');
  assert(aux1Done.answer.includes(marker1), 'AUX_ANSWER_MISSING_FROZEN_MARKER ' + JSON.stringify(aux1Done.answer).slice(0, 400));
  assert(aux1Done.nativeSessionId, 'AUX_THREAD_MISSING');
  assert.notEqual(aux1Done.nativeSessionId, thread, 'AUX_SHARED_MAIN_THREAD');
  assert(aux1Done.finishedAt, 'AUX_FINISHED_MISSING');
  assert((await auxList(base, t1Id)).some(a => a.auxSessionId === aux1Id && a.state === 'SUCCEEDED'), 'AUX_NOT_LISTED');
  // A named-but-missing package refuses intake before any spawn.
  const badKey = 'w5a-auxbad-' + randomUUID().slice(0, 8);
  const badPkgRef = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions`, {requestKey: badKey, question: 'x', contextPackageIds: ['pkg-missing']}, {'Idempotency-Key': badKey});
  assert.equal(badPkgRef.status, 404);
  assert.equal(badPkgRef.body.code, 'PACKAGE_NOT_FOUND');
  assert.equal((await auxGet(base, t1Id, 'aux-missing')).status, 404);
  checks++;
  // Isolated workspace: the aux snapshot holds the post-overwrite content, not
  // the frozen package content and not the live file afterwards.
  const auxWs = aux1Done.workspace;
  assert(auxWs && auxWs.includes('auxiliary'), 'AUX_WS_OUTSIDE_AUXILIARY ' + auxWs);
  assert.notEqual(resolve(auxWs), resolve(ws), 'AUX_WS_IS_MAIN_WS');
  const snapContent = await readFile(join(auxWs, srcFile), 'utf8');
  assert.equal(snapContent, markerGone + '\n', 'AUX_SNAPSHOT_WRONG ' + snapContent.slice(0, 200));
  await writeFile(join(ws, srcFile), 'NWF_W5A_LATER\n');
  assert.equal(await readFile(join(auxWs, srcFile), 'utf8'), markerGone + '\n', 'AUX_SNAPSHOT_NOT_ISOLATED');
  checks++;
  // Session events exist and the main thread was untouched by aux work.
  const ev1 = await allEvents(base, t1Id);
  const auxStarted = ev1.find(e => e.type === 'aux-session.started' && e.payload?.auxSessionId === aux1Id);
  const auxCompleted = ev1.find(e => e.type === 'aux-session.completed' && e.payload?.auxSessionId === aux1Id);
  assert(auxStarted && auxCompleted, 'AUX_EVENTS_MISSING');
  assert.equal(auxStarted.payload.auxSessionId, aux1Id);
  assert.equal((await detail(base, t1Id)).nativeSessionId, thread, 'MAIN_THREAD_CHANGED_BY_AUX');
  checks++;
  // Stop: kills only the aux peer; the parent task stays SUCCEEDED.
  const aux2Key = 'w5a-aux2-' + randomUUID().slice(0, 8);
  const aux2 = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions`, {requestKey: aux2Key, question: 'Count slowly to one thousand, reporting each number.'}, {'Idempotency-Key': aux2Key});
  assert.equal(aux2.status, 202);
  const aux2Id = aux2.body.auxSessionId;
  const aux2Stop = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions/${aux2Id}/stop`, {}, {'Idempotency-Key': 'w5a-aux2-stop'});
  assert.equal(aux2Stop.status, 200, JSON.stringify(aux2Stop.body));
  const aux2Done = await waitAux(base, t1Id, aux2Id, ['CANCELLED', 'SUCCEEDED', 'FAILED'], 60000);
  assert.notEqual(aux2Done.state, 'RUNNING');
  assert.equal((await detail(base, t1Id)).state, 'SUCCEEDED', 'AUX_STOP_TOUCHED_MAIN_TASK');
  checks++;
  // Independent SOFT budget: a tiny cap trips at a real usage boundary on the
  // aux ledger only — the parent task's usageTotals are untouched.
  const parentUsage = (await detail(base, t1Id)).usageTotals;
  const aux3Key = 'w5a-aux3-' + randomUUID().slice(0, 8);
  const aux3 = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions`, {requestKey: aux3Key, question: 'Count slowly from one to five hundred, printing every number on its own line.', budget: {maxTotalTokens: '150'}}, {'Idempotency-Key': aux3Key});
  assert.equal(aux3.status, 202);
  const aux3Done = await waitAux(base, t1Id, aux3.body.auxSessionId, ['FAILED', 'SUCCEEDED'], 240000);
  assert.equal(aux3Done.state, 'FAILED', 'AUX_SOFT_CAP_NOT_ENFORCED ' + aux3Done.state);
  assert.equal(aux3Done.reasonCode, 'BUDGET_EXCEEDED');
  assert(aux3Done.usage?.totalTokens, 'AUX_LEDGER_MISSING');
  assert.equal(aux3Done.budget?.maxTotalTokens, '150');
  assert.equal(aux3Done.budget?.enforcement, 'SOFT');
  assert.deepEqual((await detail(base, t1Id)).usageTotals, parentUsage, 'AUX_USAGE_CONSUMED_PARENT_LEDGER');
  checks++;
  // The four-session cap is durable; a fifth intake is refused.
  {
    const k = 'w5a-aux4-' + randomUUID().slice(0, 8);
    const r = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions`, {requestKey: k, question: 'Reply with exactly: OK'}, {'Idempotency-Key': k});
    assert.equal(r.status, 202, 'w5a-aux4');
    const rStop = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions/${r.body.auxSessionId}/stop`, {});
    assert.equal(rStop.status, 200, 'w5a-aux4 stop');
    checks++;
  }
  const aux5Key = 'w5a-aux5-' + randomUUID().slice(0, 8);
  const aux5 = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions`, {requestKey: aux5Key, question: 'x'}, {'Idempotency-Key': aux5Key});
  assert.equal(aux5.status, 409, JSON.stringify(aux5.body));
  assert.equal(aux5.body.code, 'AUX_SESSION_LIMIT');
  checks++;
  // Intake/state refusals are honest.
  const aux2DlKey = 'w5a-dl-aux2-' + randomUUID().slice(0, 8);
  const aux2Deliver = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions/${aux2Id}/deliver`, {requestKey: aux2DlKey}, {'Idempotency-Key': aux2DlKey});
  assert.equal(aux2Deliver.status, 409);
  assert.equal(aux2Deliver.body.code, 'AUX_NOT_FINISHED');
  checks++;
  // Deliver the completed aux answer into the MAIN task: the existing
  // follow-up admission binds a new turn on the same live thread.
  const deliverKey = 'w5a-deliver-' + randomUUID().slice(0, 8);
  const deliver = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions/${aux1Id}/deliver`, {requestKey: deliverKey}, {'Idempotency-Key': deliverKey});
  assert.equal(deliver.status, 202, JSON.stringify(deliver.body));
  const afterDeliver = await waitTask(base, t1Id, ['SUCCEEDED'], 420000, [{file: srcFile, marker: marker1}]);
  assert.equal(afterDeliver.nativeSessionId, thread, 'DELIVERY_CHANGED_THREAD');
  assert.notEqual(afterDeliver.nativeTurnId, t1Done.nativeTurnId, 'DELIVERY_NO_NEW_TURN');
  assert.equal(afterDeliver.generation, gen1, 'LIVE_DELIVERY_ROTATED_GENERATION');
  assert.equal(afterDeliver.currentCommandId, deliverKey);
  checks++;
  const aux1Delivered = (await auxGet(base, t1Id, aux1Id)).body;
  assert.equal(aux1Delivered.state, 'SUCCEEDED', 'delivered row keeps its terminal fact');
  assert.equal(aux1Delivered.deliverRequestKey, deliverKey);
  assert(aux1Delivered.deliveredAt, 'DELIVERED_AT_MISSING');
  const deliverEvents = await allEvents(base, t1Id);
  assert(deliverEvents.some(e => e.type === 'aux-session.delivered' && e.payload?.auxSessionId === aux1Id), 'DELIVERED_EVENT_MISSING');
  assert(deliverEvents.some(e => e.type === 'turn.bound' && e.payload?.requestKey === deliverKey), 'DELIVERY_TURN_FACT_MISSING');
  // One-shot: a second delivery of the same answer is refused atomically.
  const redeliverKey = 'w5a-dl2-' + randomUUID().slice(0, 8);
  const redeliver = await request(base, 'POST', `/local/v1/tasks/${t1Id}/auxiliary-sessions/${aux1Id}/deliver`, {requestKey: redeliverKey}, {'Idempotency-Key': redeliverKey});
  assert.equal(redeliver.status, 409);
  assert.equal(redeliver.body.code, 'AUX_ALREADY_DELIVERED');
  const deliverReceipt = (await request(base, 'GET', `/local/v1/tasks/${t1Id}/interaction-requests/${deliverKey}`)).body;
  assert.equal(deliverReceipt.state, 'APPLIED');
  checks++;
  // -- Assignment transitions ----------------------------------------------
  // Incompatible targets are refused before any quiesce: nothing changes.
  for (const [name, target] of [
    ['w5a-tr-eng', {engineId: 'other-engine'}],
    ['w5a-tr-model', {model: 'not-' + registry.model}],
    ['w5a-tr-tools', {toolAllowlist: ['shell']}],
  ]) {
    const k = name + '-' + randomUUID().slice(0, 8);
    const r = await request(base, 'POST', `/local/v1/tasks/${t1Id}/transitions`, {requestKey: k, targetProfile: target, ...binding(await detail(base, t1Id))}, {'Idempotency-Key': k});
    assert.equal(r.status, 409, name + ' ' + JSON.stringify(r.body));
    assert.equal(r.body.code, 'INCOMPATIBLE_TRANSITION', name);
    checks++;
  }
  assert.equal((await detail(base, t1Id)).state, 'SUCCEEDED', 'INCOMPATIBLE_TRANSITION_TOUCHED_STATE');
  assert.equal((await request(base, 'GET', `/local/v1/tasks/${t1Id}/transitions`)).body.items.length, 0, 'INCOMPATIBLE_WROTE_ROW');
  checks++;
  // Same-engine transition on the LIVE task: the host quiesces the current
  // owner through the STOP machinery itself, then reopens the same native
  // thread under a new generation/segment/epoch.
  const trKey = 'w5a-transition-' + randomUUID().slice(0, 8);
  const tr = await request(base, 'POST', `/local/v1/tasks/${t1Id}/transitions`, {requestKey: trKey, ...binding(await detail(base, t1Id))}, {'Idempotency-Key': trKey});
  assert.equal(tr.status, 202, JSON.stringify(tr.body));
  const transitioned = await waitTask(base, t1Id, ['SUCCEEDED'], 420000, [{file: srcFile, marker: marker1}]);
  assert.equal(transitioned.nativeSessionId, thread, 'TRANSITION_CHANGED_THREAD');
  assert.notEqual(transitioned.generation, gen1, 'TRANSITION_GENERATION_NOT_ROTATED');
  assert.notEqual(transitioned.segmentId, seg1, 'TRANSITION_SEGMENT_NOT_ROTATED');
  assert.equal(transitioned.ownerEpoch, '2', 'TRANSITION_EPOCH_NOT_INCREMENTED');
  assert.equal(transitioned.currentCommandId, trKey);
  assert.equal(transitioned.transitionedFrom?.fromEpoch, '1');
  checks++;
  const trEvents = await allEvents(base, t1Id);
  const quiescedEv = trEvents.find(e => e.type === 'transition.quiesced' && e.payload?.requestKey === trKey);
  assert(quiescedEv, 'QUIESCED_EVENT_MISSING');
  assert.equal(quiescedEv.payload.alreadyQuiescent, false, 'LIVE_QUIESCE_PATH_NOT_TAKEN');
  const admitted = trEvents.find(e => e.type === 'transition.admitted' && (e.requestKey === trKey || e.payload?.requestKey === trKey));
  assert(admitted, 'ADMITTED_EVENT_MISSING');
  assert.equal(admitted.payload.compatibility?.engineId, 'codex');
  assert.equal(admitted.payload.compatibility?.sameEngine, true);
  assert.equal(admitted.payload.fromEpoch, '1');
  assert.equal(admitted.payload.toEpoch, '2');
  checks++;
  const trList = (await request(base, 'GET', `/local/v1/tasks/${t1Id}/transitions`)).body.items;
  assert.equal(trList.length, 1);
  assert.equal(trList[0].state, 'ADMITTED');
  assert.equal(trList[0].fromEpoch, '1');
  assert.equal(trList[0].toEpoch, '2');
  assert.equal(trList[0].fromSegment, seg1);
  assert.equal(trList[0].toSegment, transitioned.segmentId);
  checks++;
  // Stale-owner fencing: a follow-up bound to the pre-transition generation is
  // refused; the admitted owner is fenced by generation/session/turn/command.
  const staleKey = 'w5a-stale-' + randomUUID().slice(0, 8);
  const stale = await request(base, 'POST', `/local/v1/tasks/${t1Id}/messages`, {requestKey: staleKey, kind: 'NATIVE_FOLLOW_UP', content: 'stale write attempt', ...binding(afterDeliver)}, {'Idempotency-Key': staleKey});
  assert.equal(stale.status, 409);
  assert.equal(stale.body.code, 'NATIVE_TARGET_CHANGED');
  const trReceipt = (await request(base, 'GET', `/local/v1/tasks/${t1Id}/interaction-requests/${trKey}`)).body;
  assert.equal(trReceipt.state, 'APPLIED');
  checks++;
  // -- Restart persistence --------------------------------------------------
  // T2 hosts an in-flight aux session at kill time: it must reconcile LOST.
  const t2key = 'w5a-t2-' + randomUUID().slice(0, 8);
  const t2 = await request(base, 'POST', '/local/v1/tasks', mk(t2key, {instruction: 'Reply with exactly the token W5A-T2-OK and nothing else. Do not run tools or commands.'}), {'Idempotency-Key': t2key});
  assert.equal(t2.status, 202);
  const t2Id = t2.body.taskId;
  await waitTask(base, t2Id, ['SUCCEEDED'], 420000);
  const auxRK = 'w5a-auxR-' + randomUUID().slice(0, 8);
  const auxR = await request(base, 'POST', `/local/v1/tasks/${t2Id}/auxiliary-sessions`, {requestKey: auxRK, question: 'List every file in this workspace with a one-line description of each.'}, {'Idempotency-Key': auxRK});
  assert.equal(auxR.status, 202);
  const auxRId = auxR.body.auxSessionId;
  {const dead = exited(live); live.kill(); await dead; live = null;}
  const base2 = await start('1');
  // Durable W5a state survives the crash; live aux work honestly does not.
  const pkgRestart = (await request(base2, 'GET', `/local/v1/tasks/${t1Id}/context-packages/${pkgId}`)).body;
  assert.equal(pkgRestart.packageId, pkgId);
  assert.equal(pkgRestart.digest, pkg.body.digest);
  assert.equal(Buffer.from(pkgRestart.sources.find(s => s.kind === 'file').content, 'base64').toString('utf8'), marker1 + '\n');
  const trRestart = (await request(base2, 'GET', `/local/v1/tasks/${t1Id}/transitions`)).body.items;
  assert.equal(trRestart.length, 1);
  assert.equal(trRestart[0].state, 'ADMITTED');
  assert.equal(trRestart[0].toEpoch, '2');
  checks++;
  const auxRAfter = (await auxGet(base2, t2Id, auxRId)).body;
  assert(['CANCELLED', 'SUCCEEDED', 'FAILED'].includes(auxRAfter.state), 'AUX_RESTART_STATE ' + auxRAfter.state);
  if (auxRAfter.state === 'CANCELLED') assert.ok(['HOST_RESTART_JOB_CLOSED', 'HOST_CLOSE'].includes(auxRAfter.reasonCode), 'AUX_RESTART_REASON ' + auxRAfter.reasonCode);
  assert(!['STARTING', 'RUNNING'].includes(auxRAfter.state), 'AUX_CLAIMED_LIVE_AFTER_RESTART');
  const aux1After = (await auxGet(base2, t1Id, aux1Id)).body;
  assert.equal(aux1After.state, 'SUCCEEDED', 'AUX_TERMINAL_FACT_REWRITTEN');
  assert(aux1After.deliveredAt, 'AUX_DELIVERY_MARK_LOST');
  checks++;
  // A task whose owner died is UNKNOWN; its sessions stay terminal records.
  const t1After = (await request(base2, 'GET', `/local/v1/tasks/${t1Id}`)).body;
  assert.equal(t1After.state, 'UNKNOWN');
  const trDeadKey = 'w5a-trdead-' + randomUUID().slice(0, 8);
  const trOnDead = await request(base2, 'POST', `/local/v1/tasks/${t1Id}/transitions`, {requestKey: trDeadKey, ...binding(t1After)}, {'Idempotency-Key': trDeadKey});
  assert.equal(trOnDead.status, 409);
  assert.equal(trOnDead.body.code, 'TRANSITION_STATE_INCOMPATIBLE');
  checks++;
  // Durable SQL spot-check: the W5a tables carry rows under schema v5.
  const db = new DatabaseSync(join(root, 'worker.sqlite'), {readOnly: true});
  const prow = db.prepare('SELECT digest,size FROM context_packages WHERE task=? AND id=?').get(t1Id, pkgId);
  const srcRows = db.prepare('SELECT count(*) c, sum(length(bytes)) b FROM context_package_sources WHERE task=? AND package_id=?').get(t1Id, pkgId);
  const auxRow = db.prepare('SELECT state,stop_reason FROM aux_sessions WHERE task=? AND id=?').get(t2Id, auxRId);
  const trRow = db.prepare('SELECT from_epoch,to_epoch,compatibility FROM transitions WHERE task=?').get(t1Id);
  const version = db.prepare('PRAGMA user_version').get().user_version;
  db.close();
  assert.equal(prow.digest, pkg.body.digest);
  assert.equal(srcRows.c, 3);
  assert(srcRows.b > 0, 'FROZEN_BYTES_MISSING');
  assert.equal(version, 5);
  assert(auxRow, 'AUX_ROW_MISSING');
  assert(['CANCELLED', 'SUCCEEDED', 'FAILED'].includes(auxRow.state), 'AUX_ROW_STATE ' + auxRow.state);
  assert.equal(trRow.from_epoch, '1');
  assert.equal(trRow.to_epoch, '2');
  assert.equal(JSON.parse(trRow.compatibility).engineId, 'codex');
  assert.equal(JSON.parse(trRow.compatibility).sameEngine, true);
  checks++;
  console.log(JSON.stringify({result: 'PASS', checks, scope: 'W5a frozen context packages (freeze proof + scope/digest/limits/idempotency), isolated aux sessions (real-model Q&A on inlined frozen context, snapshot workspace, stop, 4-session cap, one-shot deliver on same thread), same-engine transitions (STOP quiesce -> same thread resume with new generation/segment/epoch, stale fencing, incompatible refusals), restart persistence with CANCELLED aux reconciliation'}));
} finally {
  if (live) { let done = exited(live); live.kill(); await done; }
  for (let attempt = 0; attempt < 40; attempt++) {
    try { await rm(root, {recursive: true, force: true}); await rm(outside, {recursive: true, force: true}); break; }
    catch (e) { if (attempt === 39) console.error('CLEANUP_INCOMPLETE ' + e.message); else await delay(500); }
  }
}
