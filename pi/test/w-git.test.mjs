import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { PiHost } from '../dist/host/server.js';
import { hash } from '../dist/host/store.js';

const token = 'deterministic-git-baseline-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-git-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup() { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-git-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture }); host.register('demo', p); const port = await host.listen(); return { host, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { taskId: result.body.taskId }; }
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });
const gb = (ws, args, opt = {}) => execFileSync('git', ['-c', 'core.longpaths=true', '-C', ws, ...args], { stdio: ['ignore', 'pipe', 'ignore'], ...opt });
const g = (ws, args) => gb(ws, args, { encoding: 'utf8' }).trimEnd();

// GAP-04: export.gitBaseline carries repo facts + layered binary payloads;
// payload parts flow through the digest-bound chunk route verbatim.
test('gitBaseline: repo facts, layered patches, bundle, untracked manifest, binary chunk reads', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const ws = d.workspace;
    // stash first (it restores tracked files to HEAD), then build layers
    writeFileSync(join(ws, 'stashme.txt'), 'stash body');
    gb(ws, ['add', 'stashme.txt']);
    gb(ws, ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'stash', 'push', '-m', 'wip-stash']);
    // local-only commit
    writeFileSync(join(ws, 'committed.txt'), 'local commit body');
    gb(ws, ['add', 'committed.txt']);
    gb(ws, ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'local work']);
    // staged text + staged binary (NUL bytes prove the payload is not utf8-mangled)
    writeFileSync(join(ws, 'staged.txt'), 'staged body');
    writeFileSync(join(ws, 'staged.bin'), Buffer.from([0, 1, 2, 250, 251, 0, 128]));
    gb(ws, ['add', 'staged.txt', 'staged.bin']);
    // unstaged modification + untracked file
    writeFileSync(join(ws, 'README.md'), 'baseline\nunstaged drift');
    writeFileSync(join(ws, 'untracked.txt'), 'untracked body');
    // in-flight markers a real merge/rebase leaves behind
    const gitDir = g(ws, ['rev-parse', '--absolute-git-dir']);
    writeFileSync(join(gitDir, 'MERGE_HEAD'), g(ws, ['rev-parse', 'HEAD']) + '\n');
    mkdirSync(join(gitDir, 'rebase-merge'));
    const head = g(ws, ['rev-parse', 'HEAD']);
    const branch = (() => { try { return g(ws, ['symbolic-ref', '-q', '--short', 'HEAD']) || null; } catch { return null; } })();
    const localCommits = Number(g(ws, ['rev-list', '--count', 'HEAD', '--not', '--remotes']));
    const stagedBytes = gb(ws, ['diff', '--cached', '--binary', '--full-index', '--no-color', '--no-ext-diff']);
    const unstagedBytes = gb(ws, ['diff', '--binary', '--full-index', '--no-color', '--no-ext-diff']);
    assert.ok(stagedBytes.length > 0 && unstagedBytes.length > 0);
    assert.ok(localCommits >= 1, 'local commit counted');

    const ex = (await s.call(`/tasks/${c.taskId}/handoff-export?limit=100`)).body;
    assert.equal(ex.schemaVersion, 'handoff-export/1');
    assert.ok(Array.isArray(ex.gitBaseline), 'gitBaseline array present');
    const byKind = Object.fromEntries(ex.gitBaseline.map(e => [e.kind, e]));
    const repo = byKind.REPOSITORY;
    assert.equal(repo.present, true); assert.equal(repo.head, head); assert.equal(repo.branch, branch); assert.equal(repo.detached, branch === null);
    assert.ok(repo.inFlight.includes('MERGE') && repo.inFlight.includes('REBASE'), 'in-flight ops reported: ' + JSON.stringify(repo.inFlight));
    assert.equal(byKind.PATCH_STAGED.state, 'AVAILABLE');
    assert.equal(byKind.PATCH_STAGED.sha256, hash(stagedBytes));
    assert.equal(byKind.PATCH_STAGED.part, 'git-staged');
    assert.equal(byKind.PATCH_UNSTAGED.state, 'AVAILABLE');
    assert.equal(byKind.PATCH_UNSTAGED.sha256, hash(unstagedBytes));
    assert.equal(byKind.BUNDLE.state, 'AVAILABLE');
    assert.equal(byKind.BUNDLE.part, 'git-bundle');
    assert.equal(byKind.BUNDLE.localCommits, localCommits);
    // stash produced a fact row; objects are honestly not bundled
    const stashRow = byKind.STASHES;
    assert.ok(stashRow, 'STASHES row'); assert.equal(stashRow.items.length, 1);
    assert.equal(stashRow.items[0].ref, 'stash@{0}');
    assert.ok(/^[0-9a-f]{40}$/.test(stashRow.items[0].commit), 'stash commit resolved');
    // path-level summary rides the patch entry — no patch parsing needed
    assert.ok(byKind.PATCH_STAGED.files.some(f => f.path === 'staged.txt' && f.status === 'A'));
    const untracked = byKind.UNTRACKED_MANIFEST;
    assert.equal(untracked.truncated, false);
    assert.ok(untracked.files.some(f => f.path === 'untracked.txt'), 'untracked manifest lists untracked.txt');
    const expectedUntracked = g(ws, ['ls-files', '--others', '--exclude-standard', '-z']).split('\0').filter(Boolean).sort();
    assert.equal(untracked.total, expectedUntracked.length);
    // payload items appear in the projection with digests PAC can fetch
    const gitItems = Object.fromEntries(ex.items.filter(i => i.kind === 'GIT_PART').map(i => [i.id, i]));
    for (const [id, bytes] of [['git-staged', stagedBytes], ['git-unstaged', unstagedBytes]]) {
      const it = gitItems[id];
      assert.ok(it, id + ' item'); assert.equal(it.sha256, hash(bytes));
      assert.equal(it.body.textRef.part, id);
      // chunk read returns the exact binary payload — digest-verified
      const chunk = (await s.call(`/tasks/${c.taskId}/handoff-export/${id}/chunks/${it.sha256}/0`)).body;
      assert.deepEqual(Buffer.from(chunk.bytes, 'base64'), bytes, id + ' bytes verbatim');
      assert.equal((await s.call(`/tasks/${c.taskId}/handoff-export/${id}/chunks/${'0'.repeat(64)}/0`)).body.code, 'HANDOFF_CHUNK_DIGEST_MISMATCH');
    }
    const bundle = gitItems['git-bundle'];
    assert.ok(bundle, 'git-bundle item');
    const bundleChunk = (await s.call(`/tasks/${c.taskId}/handoff-export/git-bundle/chunks/${bundle.sha256}/0`)).body;
    const bundleBytes = Buffer.from(bundleChunk.bytes, 'base64');
    assert.equal(hash(bundleBytes), bundle.sha256);
    assert.ok(bundleBytes.subarray(0, 15).toString('latin1').startsWith('# v2 git bundle'), 'real git bundle header');
    // a second export re-collects consistently — same digests
    const ex2 = (await s.call(`/tasks/${c.taskId}/handoff-export?limit=100`)).body;
    assert.deepEqual(ex2.gitBaseline, ex.gitBaseline, 'deterministic re-collect');
  } finally { await s.host.close(); }
});

// Clean clone: every layer reports an honest EMPTY/NO_LOCAL_COMMITS state.
test('gitBaseline: clean clone reports EMPTY layers and NO_LOCAL_COMMITS bundle', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const ex = (await s.call(`/tasks/${c.taskId}/handoff-export?limit=100`)).body;
    const byKind = Object.fromEntries(ex.gitBaseline.map(e => [e.kind, e]));
    assert.equal(byKind.REPOSITORY.present, true);
    assert.equal(byKind.PATCH_STAGED.state, 'EMPTY');
    assert.equal(byKind.PATCH_UNSTAGED.state, 'EMPTY');
    const localCommits = Number(g(d.workspace, ['rev-list', '--count', 'HEAD', '--not', '--remotes']));
    if (localCommits === 0) { assert.equal(byKind.BUNDLE.state, 'EMPTY'); assert.equal(byKind.BUNDLE.reason, 'NO_LOCAL_COMMITS'); }
    else { assert.equal(byKind.BUNDLE.state, 'AVAILABLE'); assert.equal(byKind.BUNDLE.localCommits, localCommits); }
    // the fixture child writes its own untracked files — assert the manifest
    // enumerates exactly what `ls-files --others` reports
    const expected = g(d.workspace, ['ls-files', '--others', '--exclude-standard', '-z']).split('\0').filter(Boolean).sort();
    assert.deepEqual(byKind.UNTRACKED_MANIFEST.files.map(f => f.path), expected);
    assert.ok(byKind.UNTRACKED_MANIFEST.files.every(f => f.status === 'UNSEALED' || f.sha256), 'honest per-file status');
    assert.equal(ex.items.filter(i => i.kind === 'GIT_PART').length, 0, 'no payload parts when empty');
    // unknown git part names still refuse like any other part
    assert.equal((await s.call(`/tasks/${c.taskId}/handoff-export/git-staged/chunks/${'0'.repeat(64)}/0`)).body.code, 'HANDOFF_PART_NOT_FOUND');
  } finally { await s.host.close(); }
});
