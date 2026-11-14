import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import http from 'node:http';
import { PiHost } from '../dist/host/server.js';

/**
 * worker-events/1: assistant text_delta increments ride IPC as assistant_delta,
 * land as durable OUTPUT_DELTA rows on the events spine (and as child-message
 * rows for aux/collab streams), and surface through the events SSE read path.
 */
const token = 'deterministic-w7-test-token-not-real';
function project() { const path = mkdtempSync(join(tmpdir(), 'nwf-pi-w7-project-')); execFileSync('git', ['init', path], { stdio: 'ignore' }); writeFileSync(join(path, 'README.md'), 'baseline'); for (const args of [['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'baseline']]) execFileSync('git', ['-C', path, ...args], { stdio: 'ignore' }); return path; }
const fixture = fileURLToPath(new URL('./fixture-child.mjs', import.meta.url));
async function wait(fn, timeout = 12000) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)); } throw new Error('WAIT_TIMEOUT'); }
async function setup(extra = {}) { const state = mkdtempSync(join(tmpdir(), 'nwf-pi-w7-state-')); const p = project(); const host = new PiHost({ state, token, childModule: fixture, ...extra }); host.register('demo', p); const port = await host.listen(); return { host, state, project: p, port, call: client(port) }; }
function client(port) { return async (path, body, headers = {}, method) => { const response = await fetch(`http://127.0.0.1:${port}${path.startsWith('/worker-host') ? '' : '/local/v1'}${path}`, { method: method ?? (body ? 'POST' : 'GET'), headers: { Authorization: 'Bearer ' + token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined }); return { status: response.status, body: await response.json() }; }; }
async function create(call, instruction = 'hello', extra = {}) { const pf = (await call('/task-preflights', { projectId: 'demo', engineId: 'sage-pi' })).body; const body = { requestKey: 'create-' + Math.random().toString(36).slice(2), projectId: 'demo', engineId: 'sage-pi', optionsRevision: pf.optionsRevision, instruction, ...extra }; const result = await call('/tasks', body); return { request: body, taskId: result.body.taskId, status: result.status }; }
const guard = t => ({ generation: t.generation, nativeSessionId: t.nativeSessionId, targetCommandId: t.currentCommandId });
const events = async (call, taskId) => (await call(`/tasks/${taskId}/events?limit=100`)).body.items;
const succeeded = async (call, taskId) => wait(async () => { const d = (await call('/tasks/' + taskId)).body; return d.state === 'SUCCEEDED' && d; });
const auxGet = async (call, taskId, auxId) => (await call(`/tasks/${taskId}/auxiliary-sessions/${auxId}`)).body;
const collabGet = async (call, taskId, collabId) => (await call(`/tasks/${taskId}/collaborations/${collabId}`)).body;

/** Raw SSE reader over node:http — the assertion target is the wire format, so
 *  frames are collected straight off the socket, not through a buffered fetch. */
function sseOpen(port, path, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: '/local/v1' + path, method: 'GET', headers: { Authorization: 'Bearer ' + token, ...headers } }, res => {
      let buffer = ''; const frames = [];
      res.on('data', c => { buffer += c.toString('utf8'); let i; while ((i = buffer.indexOf('\n\n')) >= 0) { frames.push(buffer.slice(0, i)); buffer = buffer.slice(i + 2); } });
      resolve({ res, frames, close: () => res.destroy() });
    });
    req.on('error', reject); req.end();
  });
}
const frameField = (f, name) => f.split('\n').find(l => l.startsWith(name + ': '))?.slice(name.length + 2);
const frameData = f => JSON.parse(frameField(f, 'data'));
const dataFrames = frames => frames.filter(f => frameField(f, 'id') !== undefined);

test('OUTPUT_DELTA: assistant deltas land on the events spine and child-message streams', async () => {
  const s = await setup(); try {
    const c = await create(s.call, 'hello'); await succeeded(s.call, c.taskId);
    const items = await events(s.call, c.taskId);
    const deltas = items.filter(e => e.kind === 'OUTPUT_DELTA');
    assert.equal(deltas.length, 2, 'fixture emits two text_delta chunks');
    assert.equal(deltas.map(d => d.delta).join(''), 'completed hello');
    for (const d of deltas) {
      assert.equal(d.contentIndex, 0); assert.equal(d.nativeTurnId, 'fixture-turn-1');
      assert.match(d.sequence, /^\d+$/); assert.match(d.cursor, /^\d+$/);
      assert.equal(d.taskId, c.taskId); assert.ok(d.occurredAt);
    }
    // Deltas are additive; the finalized ASSISTANT_MESSAGE row is unchanged.
    assert.ok(items.some(e => e.kind === 'ASSISTANT_MESSAGE' && e.text === 'completed hello'));
    // Auxiliary session stream: OUTPUT_DELTA rows precede the final message.
    const aux = await s.call(`/tasks/${c.taskId}/auxiliary-sessions`, { requestKey: 'ax-d1', question: 'delta question' });
    const auxId = aux.body.auxSessionId;
    await wait(async () => (await auxGet(s.call, c.taskId, auxId)).state === 'SUCCEEDED');
    const am = (await s.call(`/tasks/${c.taskId}/auxiliary-sessions/${auxId}/messages`)).body;
    const ad = am.items.filter(i => i.kind === 'OUTPUT_DELTA');
    const auxFull = am.items.find(i => i.kind === 'ASSISTANT_MESSAGE').text;
    assert.ok(ad.length >= 1, 'aux stream carries OUTPUT_DELTA rows');
    assert.equal(ad.map(i => i.delta).join(''), auxFull);
    for (const i of ad) { assert.equal(i.contentIndex, 0); assert.match(i.nativeTurnId, /^fixture-turn-/); assert.match(i.seq, /^\d+$/); }
    // Collaboration child stream: same durable delta capture.
    const made = await s.call(`/tasks/${c.taskId}/collaborations`, { requestKey: 'cb-d1', children: [{ childId: 'writer', goal: 'child-write dx.txt' }] });
    const cid = made.body.collaborationId;
    await wait(async () => ['COMPLETED', 'CANCELLED'].includes((await collabGet(s.call, c.taskId, cid)).state));
    const cm = (await s.call(`/tasks/${c.taskId}/collaborations/${cid}/children/writer/messages`)).body;
    const cd = cm.items.filter(i => i.kind === 'OUTPUT_DELTA');
    assert.ok(cd.length >= 1, 'collab stream carries OUTPUT_DELTA rows');
    assert.equal(cd.map(i => i.delta).join(''), 'collab child done');
  } finally { await s.host.close(); }
});

test('events SSE: replay, live follow, Last-Event-ID resume, heartbeat, validation', async () => {
  const s = await setup({ ssePollMs: 40, sseHeartbeatMs: 250 }); try {
    const c = await create(s.call, 'hello'); const d = await succeeded(s.call, c.taskId);
    const json = (await s.call(`/tasks/${c.taskId}/events?limit=100`)).body;
    assert.deepEqual(Object.keys(json).sort(), ['cursor', 'cursorExpired', 'items'], 'JSON pagination surface unchanged');
    const stream = await sseOpen(s.port, `/tasks/${c.taskId}/events`, { Accept: 'text/event-stream' });
    try {
      assert.equal(stream.res.statusCode, 200);
      assert.equal(stream.res.headers['content-type'], 'text/event-stream; charset=utf-8');
      assert.equal(stream.res.headers['cache-control'], 'no-cache');
      await wait(() => stream.frames.some(f => frameField(f, 'event') === 'OUTPUT_DELTA'));
      // Frame format: id/event/data triples; data is the spine row serialized as-is.
      const deltaFrame = stream.frames.find(f => frameField(f, 'event') === 'OUTPUT_DELTA');
      assert.match(frameField(deltaFrame, 'id'), /^\d+$/);
      const row = frameData(deltaFrame);
      assert.equal(row.kind, 'OUTPUT_DELTA'); assert.equal(row.taskId, c.taskId); assert.equal(typeof row.delta, 'string');
      assert.deepEqual(row, json.items.find(e => e.cursor === frameField(deltaFrame, 'id')), 'data line is the store row verbatim');
      const ids = dataFrames(stream.frames).map(f => Number(frameField(f, 'id')));
      assert.deepEqual(ids, [...ids].sort((a, b) => a - b), 'replay is in spine order');
      // Live follow: a follow-up prompt pushes new OUTPUT_DELTA frames on the
      // same socket through the poll — no reconnect needed.
      const mark = stream.frames.length;
      const follow = await s.call(`/tasks/${c.taskId}/messages`, { requestKey: 'fu-d1', kind: 'NATIVE_FOLLOW_UP', content: 'again', ...guard(d) });
      assert.equal(follow.status, 202);
      await wait(() => dataFrames(stream.frames.slice(mark)).some(f => frameField(f, 'event') === 'NATIVE_SETTLED'));
      const live = dataFrames(stream.frames.slice(mark)).filter(f => frameField(f, 'event') === 'OUTPUT_DELTA').map(f => frameData(f).delta).join('');
      assert.equal(live, 'completed again');
      // Last-Event-ID resume replays strictly later rows.
      const lastId = frameField(dataFrames(stream.frames).at(-1), 'id');
      const resume = await sseOpen(s.port, `/tasks/${c.taskId}/events`, { Accept: 'text/event-stream', 'Last-Event-ID': lastId });
      try {
        await new Promise(r => setTimeout(r, 300));
        assert.ok(dataFrames(resume.frames).every(f => Number(frameField(f, 'id')) > Number(lastId)), 'resume starts strictly after last id');
        assert.equal(dataFrames(resume.frames).filter(f => frameField(f, 'event') === 'OUTPUT_DELTA').length, 0, 'no stale deltas replayed');
      } finally { resume.close(); }
      // The query cursor is equivalent to the header.
      const viaCursor = await sseOpen(s.port, `/tasks/${c.taskId}/events?cursor=${lastId}`, { Accept: 'text/event-stream' });
      try {
        await new Promise(r => setTimeout(r, 300));
        assert.equal(dataFrames(viaCursor.frames).length, 0, 'tail cursor replays nothing');
      } finally { viaCursor.close(); }
      // Idle keep-alive: a comment heartbeat lands on a quiet stream.
      const idle = await sseOpen(s.port, `/tasks/${c.taskId}/events?cursor=${lastId}`, { Accept: 'text/event-stream' });
      try { await wait(() => idle.frames.some(f => f.startsWith(':')), 5000); } finally { idle.close(); }
      // ?follow=1 triggers the same surface for clients that cannot set Accept.
      const followed = await sseOpen(s.port, `/tasks/${c.taskId}/events?follow=1`, {});
      try {
        assert.equal(followed.res.headers['content-type'], 'text/event-stream; charset=utf-8');
        await wait(() => dataFrames(followed.frames).length >= 1);
      } finally { followed.close(); }
      // Conflicting cursor sources refuse with 400, malformed cursors get the
      // JSON surface's validation error, and the auth gates still apply.
      const conflict = await fetch(`http://127.0.0.1:${s.port}/local/v1/tasks/${c.taskId}/events?cursor=1`, { headers: { Authorization: 'Bearer ' + token, Accept: 'text/event-stream', 'Last-Event-ID': '7' } });
      assert.equal(conflict.status, 400); assert.equal((await conflict.json()).code, 'EVENT_CURSOR_CONFLICT');
      const bad = await fetch(`http://127.0.0.1:${s.port}/local/v1/tasks/${c.taskId}/events?cursor=abc&follow=1`, { headers: { Authorization: 'Bearer ' + token } });
      assert.equal(bad.status, 409); assert.equal((await bad.json()).code, 'INVALID_EVENT_LIMIT');
      const unauth = await fetch(`http://127.0.0.1:${s.port}/local/v1/tasks/${c.taskId}/events`, { headers: { Accept: 'text/event-stream' } });
      assert.equal(unauth.status, 401);
      const missing = await fetch(`http://127.0.0.1:${s.port}/local/v1/tasks/task-nope/events`, { headers: { Authorization: 'Bearer ' + token, Accept: 'text/event-stream' } });
      assert.equal(missing.status, 404);
    } finally { stream.close(); }
  } finally { await s.host.close(); }
});
