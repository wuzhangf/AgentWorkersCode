import { createServer } from 'node:http';
import { createHash, randomUUID } from 'node:crypto';
import { canonicalDigest } from '../dist/wire/protocol.js';

/**
 * W6: deterministic in-process /worker/v1 control plane. Every endpoint
 * the wire adapter speaks is implemented with the SAME response bodies
 * the Java control plane returns; every request (method, path, body,
 * headers) lands in `recorded` so tests assert the exact wire contract.
 * Fault knobs are counters/booleans the test flips — no timers that leak,
 * no randomness in the protocol itself.
 */

const json = (res, status, body) => {
  const bytes = Buffer.from(JSON.stringify(body), 'utf8');
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': bytes.length });
  res.end(bytes);
};
const problem = (res, status, code, message, retryable = false) => json(res, status, { code, message, retryable });

export function createControlPlane(opts = {}) {
  const recorded = [];
  const state = {
    registrations: [],
    workers: new Map(),         // workerId -> {instanceId, streamId, epoch, bootId, generation}
    commands: new Map(),        // instanceId -> CommandEnvelope[]
    receipts: [],               // {instanceId, commandId, receiptRevision, state, payloadDigest, body}
    events: [],                 // accepted event envelopes
    eventResults: [],           // every batch result returned
    artifacts: new Map(),       // artifactId -> {digest,sizeBytes,status,bytes,attemptId,receiptRef}
    uploadIntents: [],          // every intent request body
    uploads: [],                // {path, sha256, bytes, authorization, query}
    finishes: new Map(),        // finishId -> {request, attemptId, state, receiptDigest, submissions}
    finishReceipts: [],         // every receipt poll
    recoveries: [],             // every recovery grant request
    approvals: [],
    heartbeats: [],
  };
  const knobs = {
    desiredState: opts.desiredState ?? 'READY',
    leaseEpoch: 1,
    /** when >0, the next N heartbeats fail with LEASE_EXPIRED (410). */
    leaseExpireHeartbeats: 0,
    /** when >0, the next N receipt posts fail 503 (retryable). */
    dropReceipts: 0,
    /** 'accept' | 'reject' | 'drop' — event batch disposition. */
    eventMode: 'accept',
    /** when >0, the next N event batches fail 503. */
    dropEventBatches: 0,
    /** finish receipt state override; 'auto' resolves to DELIVERY_ACCEPTED once declared; 'manual' stays PENDING until the test flips it. */
    finishMode: opts.finishMode ?? 'auto',
    /** when >0, the next N finish posts fail 503. */
    dropFinishPosts: 0,
    /** upload URL origin: 'same' | 'cross' | 'redirect'. */
    uploadOrigin: 'same',
    /** cross-origin peer URL (another fixture listener) when uploadOrigin==='cross'. */
    crossOriginUrl: null,
    /** when true, intents answer alreadyReceipted and skip the PUT. */
    alreadyReceipted: false,
    /** recovery grants: attemptIds/streamIds this plane will scope; 'allowAll' grants whatever was requested. */
    recoveryScope: 'all',     // 'all' | 'none' | {streamIds:[], attemptIds:[]}
    denyRecoveries: 0,
    /** when true, commands are replayed (same commandId, same envelope) on the next poll. */
    replayNextPoll: null,
    /** next poll also returns this extra envelope verbatim (duplicate/conflict probes). */
    extraPollCommands: [],
    maxWaitMillis: 2000,
  };

  let server = null;
  let port = 0;
  let commandSeq = 0;
  const waiters = new Map(); // instanceId -> Set<resolve>
  const seenAttemptIds = new Set();
  const seenStreamIds = new Set();

  const nextSeq = () => String(++commandSeq);

  function command(type, payload, over = {}) {
    const workerId = over.workerId ?? 'worker-w6';
    const instanceId = over.instanceId ?? state.workers.get(workerId)?.instanceId ?? 'inst-pending';
    const env = {
      protocol: over.protocol ?? 'agent-worker/1.4',
      commandId: over.commandId ?? 'cmd-' + randomUUID().replace(/-/g, '').slice(0, 16),
      commandSequence: over.commandSequence ?? nextSeq(),
      type,
      organizationId: over.organizationId ?? 'org-w6',
      projectId: over.projectId ?? 'proj-wire-demo',
      workerId,
      instanceId,
      workerLeaseEpoch: over.workerLeaseEpoch ?? String(knobs.leaseEpoch),
      issuedAt: over.issuedAt ?? new Date().toISOString(),
      payloadDigest: over.payloadDigest ?? canonicalDigest(payload),
      payload,
    };
    if (over.executeBefore !== undefined) env.executeBefore = over.executeBefore;
    return env;
  }
  function enqueue(env) {
    if (env?.payload?.attemptId) seenAttemptIds.add(env.payload.attemptId);
    const queue = state.commands.get(env.instanceId) ?? [];
    queue.push(env);
    state.commands.set(env.instanceId, queue);
    for (const w of waiters.get(env.instanceId) ?? []) w();
    waiters.delete(env.instanceId);
    return env;
  }

  async function route(req, res, body) {
    const url = new URL(req.url, 'http://fixture');
    const path = url.pathname;
    const segs = path.split('/').filter(Boolean); // ['worker','v1',...]
    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) headers[k] = v;
    recorded.push({ method: req.method, path: path + (url.search || ''), body, headers });

    if (req.method === 'POST' && path === '/worker/v1/registrations') {
      const workerId = body.workerId;
      if (!workerId || !body.protocol || !String(body.protocol).startsWith('agent-worker/')) {
        return problem(res, 400, 'INVALID_SCHEMA', 'registration malformed');
      }
      // Every registration is a NEW live incarnation under the durable
      // worker+boot identity: fresh instanceId/streamId/epoch each time.
      const w = {
        workerId, bootId: body.bootId,
        instanceId: 'inst-' + randomUUID().replace(/-/g, '').slice(0, 12),
        streamId: 'stream-' + randomUUID().replace(/-/g, '').slice(0, 12),
        epoch: String(knobs.leaseEpoch++), generation: state.registrations.length + 1,
      };
      state.workers.set(workerId, w);
      seenStreamIds.add(w.streamId);
      state.registrations.push({ ...body, granted: { instanceId: w.instanceId, streamId: w.streamId, workerLeaseEpoch: w.epoch } });
      return json(res, 200, {
        instanceId: w.instanceId, streamId: w.streamId, workerLeaseEpoch: w.epoch,
        leaseTtlMillis: 45000, leaseExpiresAt: new Date(Date.now() + 45000).toISOString(),
        serverTime: new Date().toISOString(), protocolAccepted: body.protocol,
        desiredState: knobs.desiredState, effectiveCapabilities: null,
      });
    }

    const inst = segs[0] === 'worker' && segs[1] === 'v1' && segs[2] === 'instances' ? segs[3] : null;
    const worker = [...state.workers.values()].find(w => w.instanceId === inst);

    if (req.method === 'POST' && inst && segs[4] === 'heartbeats') {
      state.heartbeats.push(body);
      if (knobs.leaseExpireHeartbeats > 0) {
        knobs.leaseExpireHeartbeats--;
        return problem(res, 410, 'LEASE_EXPIRED', 'lease expired', false);
      }
      if (!worker) return problem(res, 404, 'INSTANCE_UNKNOWN', 'unknown instance');
      if (body.workerLeaseEpoch !== worker.epoch) {
        return problem(res, 409, 'LEASE_EPOCH_MISMATCH', 'stale epoch ' + body.workerLeaseEpoch);
      }
      return json(res, 200, {
        workerLeaseEpoch: worker.epoch, leaseExpiresAt: new Date(Date.now() + 45000).toISOString(),
        serverTime: new Date().toISOString(), desiredState: knobs.desiredState,
      });
    }

    if (req.method === 'GET' && inst && segs[4] === 'commands') {
      if (!worker) return problem(res, 404, 'INSTANCE_UNKNOWN', 'unknown instance');
      const after = BigInt(url.searchParams.get('after') ?? '0');
      const waitMillis = Math.min(Number(url.searchParams.get('waitSeconds') ?? '0') * 1000, knobs.maxWaitMillis);
      let queue = () => (state.commands.get(inst) ?? []).filter(c => BigInt(c.commandSequence) > after);
      const deadline = Date.now() + waitMillis;
      while (queue().length === 0 && Date.now() < deadline) {
        await new Promise(done => {
          const s = waiters.get(inst) ?? new Set(); s.add(done); waiters.set(inst, s);
          setTimeout(done, Math.max(10, deadline - Date.now()));
        });
      }
      let commands = queue().slice(0, 32);
      if (knobs.replayNextPoll) { commands = [knobs.replayNextPoll, ...commands]; knobs.replayNextPoll = null; }
      if (knobs.extraPollCommands.length) { commands = [...commands, ...knobs.extraPollCommands]; knobs.extraPollCommands = []; }
      const maxSeq = commands.reduce((m, c) => BigInt(c.commandSequence) > m ? BigInt(c.commandSequence) : m, after);
      // Popped only on successful delivery — the durable cursor stays with the worker.
      state.commands.set(inst, (state.commands.get(inst) ?? []).filter(c => BigInt(c.commandSequence) > maxSeq));
      return json(res, 200, { commands, nextAfter: String(maxSeq) });
    }

    if (req.method === 'POST' && inst && segs[4] === 'commands' && segs[6] === 'receipt') {
      if (knobs.dropReceipts > 0) { knobs.dropReceipts--; return problem(res, 503, 'RECEIPT_DROP', 'transient receipt failure', true); }
      state.receipts.push({ instanceId: inst, commandId: segs[5], ...body });
      return json(res, 202, {});
    }

    if (req.method === 'POST' && inst && segs[4] === 'events:batch') {
      if (knobs.dropEventBatches > 0) { knobs.dropEventBatches--; return problem(res, 503, 'EVENT_DROP', 'transient event failure', true); }
      const results = (body.events ?? []).map(e => {
        const status = knobs.eventMode === 'reject' ? 'REJECTED' : 'ACCEPTED';
        if (e.streamId) seenStreamIds.add(e.streamId);
        if (e.attemptId) seenAttemptIds.add(e.attemptId);
        if (status === 'ACCEPTED' || status === 'DUPLICATE') state.events.push(e);
        return { eventId: e.eventId, status, code: status === 'REJECTED' ? 'FIXTURE_REJECTED' : null, receiptRef: null, receiptDigest: null };
      });
      state.eventResults.push(results);
      const max = (body.events ?? []).reduce((m, e) => BigInt(e.sequence) > m ? BigInt(e.sequence) : m, 0n);
      return json(res, 200, { results, contiguousAckSequence: String(max) });
    }

    const attemptIdx = segs[0] === 'worker' && segs[1] === 'v1' && segs[2] === 'attempts' ? segs[3] : null;

    if (req.method === 'POST' && attemptIdx && segs[4] === 'artifacts' && segs[5] === 'upload-intents') {
      seenAttemptIds.add(attemptIdx);
      state.uploadIntents.push(body);
      const artifactId = body.artifactId;
      if (knobs.alreadyReceipted) {
        const a = state.artifacts.get(artifactId) ?? { status: 'RECEIPTED', bytes: null };
        a.status = 'RECEIPTED'; state.artifacts.set(artifactId, a);
        return json(res, 200, { uploadUrl: '', method: 'PUT', expiresAt: new Date(Date.now() + 60000).toISOString(), alreadyReceipted: true });
      }
      const blobPath = '/worker/v1/uploads/' + artifactId;
      let uploadUrl;
      if (knobs.uploadOrigin === 'cross' && knobs.crossOriginUrl) uploadUrl = knobs.crossOriginUrl + blobPath;
      else if (knobs.uploadOrigin === 'redirect') uploadUrl = '/worker/v1/redirect-to-upload/' + artifactId;
      else uploadUrl = blobPath;
      state.artifacts.set(artifactId, { ...(state.artifacts.get(artifactId) ?? {}), attemptId: attemptIdx, digest: body.digest, sizeBytes: body.sizeBytes, kind: body.kind ?? null, status: 'INTENT_GRANTED' });
      return json(res, 200, { uploadUrl, method: 'PUT', expiresAt: new Date(Date.now() + 60000).toISOString(), maxSizeBytes: 8388608 });
    }

    if (req.method === 'PUT' && segs[0] === 'worker' && segs[1] === 'v1' && segs[2] === 'uploads') {
      const artifactId = segs[3];
      const a = state.artifacts.get(artifactId);
      if (!a) return problem(res, 404, 'ARTIFACT_UNKNOWN', 'no intent');
      const sha256 = 'sha256:' + createHash('sha256').update(body ?? Buffer.alloc(0)).digest('hex');
      state.uploads.push({ path, bytes: (body ?? Buffer.alloc(0)).length, sha256, authorization: req.headers.authorization ?? null, query: url.search });
      if (a.digest && sha256 !== a.digest) return problem(res, 409, 'ARTIFACT_DIGEST_MISMATCH', 'uploaded bytes diverge from declared digest');
      a.status = 'UPLOADED'; a.bytes = body; state.artifacts.set(artifactId, a);
      return json(res, 200, {});
    }

    if (segs[0] === 'worker' && segs[1] === 'v1' && segs[2] === 'redirect-to-upload') {
      res.writeHead(302, { location: '/worker/v1/uploads/' + segs[3] }); return res.end();
    }

    if (req.method === 'POST' && attemptIdx && segs[4] === 'artifacts' && segs[6] === 'complete') {
      const artifactId = segs[5];
      const a = state.artifacts.get(artifactId);
      if (a && a.status === 'RECEIPTED' && body.digest === a.digest && body.sizeBytes === a.sizeBytes) {
        return json(res, 200, { receiptRef: a.receiptRef, receiptDigest: 'sha256:' + createHash('sha256').update(String(a.receiptRef)).digest('hex') });
      }
      if (!a || a.status !== 'UPLOADED') return problem(res, 409, 'ARTIFACT_NOT_UPLOADED', 'no verified upload for ' + artifactId);
      if (body.digest !== a.digest || body.sizeBytes !== a.sizeBytes) return problem(res, 409, 'ARTIFACT_DIGEST_MISMATCH', 'completion disagrees with intent');
      a.status = 'RECEIPTED'; a.receiptRef = 'rcpt-' + artifactId; state.artifacts.set(artifactId, a);
      return json(res, 200, { receiptRef: a.receiptRef, receiptDigest: 'sha256:' + createHash('sha256').update(String(a.receiptRef)).digest('hex') });
    }

    if (req.method === 'POST' && attemptIdx && segs[4] === 'finish') {
      if (knobs.dropFinishPosts > 0) { knobs.dropFinishPosts--; return problem(res, 503, 'FINISH_DROP', 'transient finish failure', true); }
      const { recoverySessionId, recoveryTicket, ...payloadOnly } = body;
      const existing = state.finishes.get(body.finishId);
      const canon = canonicalDigest(payloadOnly);
      if (existing) {
        if (existing.canonicalDigest !== canon) {
          return problem(res, 409, 'IDEMPOTENCY_PAYLOAD_MISMATCH', 'finishId rebound to a different declaration');
        }
        existing.submissions++;
        if (existing.state === 'PENDING' && knobs.finishMode === 'auto') existing.state = 'DELIVERY_ACCEPTED';
        return json(res, 202, { finishId: body.finishId, state: existing.state });
      }
      const f = {
        finishId: body.finishId, attemptId: body.attemptId, request: payloadOnly,
        canonicalDigest: canon, state: 'PENDING', submissions: 1, receiptDigest: null,
        recoveryScope: recoverySessionId ? { recoverySessionId, recoveryTicket } : null,
      };
      if (body.attemptId) seenAttemptIds.add(body.attemptId);
      if (knobs.finishMode === 'auto') f.state = 'DELIVERY_ACCEPTED';
      state.finishes.set(body.finishId, f);
      return json(res, 202, { finishId: body.finishId, state: f.state });
    }

    if (req.method === 'GET' && attemptIdx && segs[4] === 'finish-receipts') {
      const f = state.finishes.get(segs[5]);
      state.finishReceipts.push({ attemptId: attemptIdx, finishId: segs[5], found: !!f });
      if (!f) return problem(res, 404, 'FINISH_UNKNOWN', 'no such finish');
      if (f.state === 'PENDING' && knobs.finishMode === 'auto') f.state = 'DELIVERY_ACCEPTED';
      return json(res, 200, {
        finishId: f.finishId, attemptId: f.attemptId, state: f.state,
        pendingReasons: f.state === 'PENDING' ? ['ARTIFACTS_UNCONFIRMED'] : [],
        receiptDigest: f.state === 'DELIVERY_ACCEPTED' ? 'sha256:' + createHash('sha256').update(f.finishId + '|ok').digest('hex') : null,
      });
    }

    if (req.method === 'POST' && path === '/worker/v1/recoveries') {
      state.recoveries.push(body);
      if (knobs.denyRecoveries > 0) { knobs.denyRecoveries--; return problem(res, 403, 'RECOVERY_DENIED', 'fixture denies recovery'); }
      const sessionId = 'rec-' + randomUUID().replace(/-/g, '').slice(0, 12);
      const scope = knobs.recoveryScope;
      const granted = scope === 'all'
        ? { streamIds: [...seenStreamIds], attemptIds: [...seenAttemptIds] }
        : scope === 'none' ? { streamIds: [], attemptIds: [] } : scope;
      return json(res, 200, {
        recoverySessionId: sessionId, ticket: 'ticket-' + sessionId,
        oldInstanceId: body.oldInstanceId ?? null,
        allowedStreamIds: granted.streamIds,
        allowedAttemptIds: granted.attemptIds,
        expiresAtEpochMs: Date.now() + 60000, expiresAt: new Date(Date.now() + 60000).toISOString(),
      });
    }

    if (req.method === 'POST' && attemptIdx && segs[4] === 'approval-requests') {
      state.approvals.push(body);
      return json(res, 202, {});
    }

    return problem(res, 404, 'ROUTE_UNKNOWN', req.method + ' ' + path);
  }

  async function handle(req, res) {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks);
    let body = raw;
    if (req.method === 'POST' || req.method === 'PATCH') {
      try { body = raw.length ? JSON.parse(raw.toString('utf8')) : {}; }
      catch { return problem(res, 400, 'BAD_JSON', 'unparseable body'); }
    }
    try { await route(req, res, body); }
    catch (e) { problem(res, 500, 'FIXTURE_ERROR', String(e?.message ?? e), true); }
  }

  return {
    state, knobs, recorded, command, enqueue,
    get port() { return port; },
    get url() { return 'http://127.0.0.1:' + port; },
    worker(workerId = 'worker-w6') { return state.workers.get(workerId); },
    async start() {
      server = createServer((req, res) => { void handle(req, res); });
      await new Promise(done => server.listen(0, '127.0.0.1', done));
      port = server.address().port;
    },
    async close() { if (server) await new Promise(done => server.close(done)); },
    /** second bare listener acting as a foreign origin for R11 tests. */
    async startForeignOrigin() {
      const foreign = { recorded: [], server: null, port: 0 };
      foreign.server = createServer((req, res) => {
        void (async () => {
          const chunks = [];
          for await (const c of req) chunks.push(c);
          const raw = Buffer.concat(chunks);
          foreign.recorded.push({
            method: req.method, path: req.url,
            authorization: req.headers.authorization ?? null,
            bytes: raw.length,
            sha256: 'sha256:' + createHash('sha256').update(raw).digest('hex'),
          });
          res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}');
        })();
      });
      await new Promise(done => foreign.server.listen(0, '127.0.0.1', done));
      foreign.port = foreign.server.address().port;
      foreign.url = 'http://127.0.0.1:' + foreign.port;
      foreign.close = () => new Promise(done => foreign.server.close(done));
      return foreign;
    },
  };
}
