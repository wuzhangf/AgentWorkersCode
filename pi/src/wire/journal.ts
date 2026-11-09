import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { canonicalString } from './protocol.js';
import { commandDigest, type CommandEnvelope } from './dto.js';
import type { Obj } from './fields.js';

/**
 * W6: durable wire journal — the SQLite ledger that makes every outbound
 * obligation survive an adapter restart: bootstrap identity, command
 * inbox (dedupe + receipt retransmission), event outbox (per-stream
 * monotone sequence), attempt rows, artifact/finish declarations and the
 * scoped-recovery task schedule. Rows are canonical-JSON values; the few
 * columns needed for set queries are materialized.
 */
export type Row = Record<string, any>;

export interface Bootstrap { workerId: string; deploymentId: string; imageDigest: string; bootId: string | null; instanceId: string | null; streamId: string | null; workerLeaseEpoch: string | null }

const ATTEMPT_STATES = ['ACCEPTED', 'PREPARING', 'STARTING', 'EXECUTING', 'QUIESCING', 'VERIFYING', 'COLLECTING', 'FINISHING', 'FINISHED', 'FAILED', 'CANCELLED', 'AMBIGUOUS'] as const;
const LEGAL: Record<string, string[]> = {
  ACCEPTED: ['PREPARING', 'FAILED', 'CANCELLED'],
  PREPARING: ['STARTING', 'FAILED', 'CANCELLED'],
  STARTING: ['EXECUTING', 'AMBIGUOUS', 'FAILED', 'CANCELLED'],
  AMBIGUOUS: ['STARTING', 'EXECUTING', 'FAILED', 'CANCELLED'],
  EXECUTING: ['QUIESCING', 'COLLECTING', 'FAILED', 'CANCELLED'],
  QUIESCING: ['VERIFYING', 'COLLECTING', 'FAILED', 'CANCELLED'],
  VERIFYING: ['COLLECTING', 'FAILED'],
  COLLECTING: ['FINISHING', 'FAILED'],
  FINISHING: ['FINISHED', 'FAILED'],
  FINISHED: [], FAILED: [], CANCELLED: [],
};

export class WireJournal {
  readonly db: DatabaseSync;
  private readonly owner: DatabaseSync;
  constructor(readonly directory: string) {
    mkdirSync(directory, { recursive: true });
    this.owner = new DatabaseSync(join(directory, 'wire-owner.sqlite'));
    try { this.owner.exec('PRAGMA busy_timeout=0; CREATE TABLE IF NOT EXISTS ownership (id INTEGER PRIMARY KEY); BEGIN EXCLUSIVE'); }
    catch { this.owner.close(); throw new Error('WIRE_ADAPTER_ALREADY_OWNS_STATE'); }
    this.db = new DatabaseSync(join(directory, 'wire.sqlite'));
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS inbox (command_id TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS outbox (stream_id TEXT NOT NULL, seq INTEGER NOT NULL, status TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(stream_id, seq));
      CREATE TABLE IF NOT EXISTS attempts (attempt_id TEXT PRIMARY KEY, run_command_id TEXT NOT NULL, state TEXT NOT NULL, value TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS attempts_run ON attempts(run_command_id);
      CREATE TABLE IF NOT EXISTS facts (attempt_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(attempt_id, key));
      CREATE TABLE IF NOT EXISTS artifacts (artifact_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, status TEXT NOT NULL, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS finishes (attempt_id TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS recovery_tasks (task_id TEXT PRIMARY KEY, value TEXT NOT NULL);`);
    const old = this.getMeta('identity');
    if (old?.schemaVersion && old.schemaVersion !== 'pi-wire/1') throw new Error('SCHEMA_INCOMPATIBLE');
    if (!old) this.setMeta('identity', { schemaVersion: 'pi-wire/1' });
  }
  close() { this.db.close(); this.owner.exec('ROLLBACK'); this.owner.close(); }

  getMeta(key: string): any { const row = this.db.prepare('SELECT value FROM meta WHERE key=?').get(key); return row ? JSON.parse(row.value as string) : null; }
  setMeta(key: string, value: any) { this.db.prepare('INSERT OR REPLACE INTO meta VALUES (?,?)').run(key, JSON.stringify(value)); }
  private txDepth = 0;
  transaction<T>(action: () => T): T {
    if (this.txDepth > 0) return action();
    this.db.exec('BEGIN IMMEDIATE'); this.txDepth++;
    try { const r = action(); this.db.exec('COMMIT'); return r; }
    catch (e) { this.db.exec('ROLLBACK'); throw e; }
    finally { this.txDepth--; }
  }

  // ---------------- bootstrap identity ----------------
  bootstrap(): Bootstrap | null { return this.getMeta('bootstrap'); }
  saveBootstrap(workerId: string, deploymentId: string, imageDigest: string, bootId: string) {
    this.setMeta('bootstrap', { workerId, deploymentId, imageDigest, bootId, instanceId: null, streamId: null, workerLeaseEpoch: null });
  }
  bindInstance(instanceId: string, streamId: string, workerLeaseEpoch: string) {
    const b = this.bootstrap(); if (!b) throw new Error('BOOTSTRAP_REQUIRED');
    b.instanceId = instanceId; b.streamId = streamId; b.workerLeaseEpoch = workerLeaseEpoch;
    this.setMeta('bootstrap', b);
  }

  // ---------------- command inbox ----------------
  private inboxRow(commandId: string): Row | null {
    const r = this.db.prepare('SELECT value FROM inbox WHERE command_id=?').get(commandId);
    return r ? JSON.parse(r.value as string) : null;
  }
  private inboxPut(row: Row) { this.db.prepare('INSERT OR REPLACE INTO inbox VALUES (?,?)').run(row.commandId, JSON.stringify(row)); }
  /**
   * CommandInbox.receive — durable intake + whole-envelope dedupe. The
   * stored commandDigest binds the whole envelope (R10): a redelivery
   * with a swapped epoch/scope under the same payload is a conflict.
   */
  inboxReceive(env: CommandEnvelope, attemptId: string | null): { outcome: 'RECEIVED' | 'DUPLICATE_SAME' | 'DUPLICATE_CONFLICT'; receiptRevision: number; sequenceGap: boolean } {
    return this.transaction(() => {
      const existing = this.inboxRow(env.commandId);
      if (existing) {
        const stored = existing.commandDigest ?? commandDigest(existing.envelope as CommandEnvelope);
        return { outcome: stored === commandDigest(env) ? 'DUPLICATE_SAME' : 'DUPLICATE_CONFLICT', receiptRevision: existing.receiptRevision, sequenceGap: false };
      }
      const seq = this.inboxMaxSequence(env.instanceId);
      const gap = seq != null && BigInt(env.commandSequence) !== BigInt(seq) + 1n;
      this.inboxPut({
        commandId: env.commandId, commandSequence: env.commandSequence, type: env.type,
        payloadDigest: env.payloadDigest, commandDigest: commandDigest(env),
        envelope: JSON.parse(JSON.stringify({ ...env })),
        state: 'DURABLY_ACCEPTED', receiptRevision: 0,
        organizationId: env.organizationId, instanceId: env.instanceId,
        workerLeaseEpoch: env.workerLeaseEpoch, attemptId,
        receivedAt: new Date().toISOString(), ackedAt: null, receiptDeliveryState: null, lastError: null,
      });
      return { outcome: 'RECEIVED', receiptRevision: 0, sequenceGap: gap };
    });
  }
  inboxAdvance(commandId: string, next: string, error: string | null): number {
    return this.transaction(() => {
      const row = this.inboxRow(commandId);
      if (!row) throw new Error('COMMAND_NOT_IN_INBOX:' + commandId);
      const rev = row.receiptRevision + 1;
      row.state = next; row.receiptRevision = rev; row.lastError = error;
      this.inboxPut(row);
      return rev;
    });
  }
  inboxMarkAcked(commandId: string) {
    const row = this.inboxRow(commandId); if (!row) return;
    if (row.ackedAt == null) { row.ackedAt = new Date().toISOString(); this.inboxPut(row); }
  }
  inboxMarkDelivery(commandId: string, delivered: boolean) {
    const row = this.inboxRow(commandId); if (!row) return;
    row.receiptDeliveryState = delivered ? 'SENT' : 'FAILED'; this.inboxPut(row);
  }
  inboxMarkUndeliverable(commandId: string) {
    const row = this.inboxRow(commandId); if (!row) return;
    row.receiptDeliveryState = 'UNDELIVERABLE'; this.inboxPut(row);
  }
  inboxFind(commandId: string): Row | null { return this.inboxRow(commandId); }
  /** R08: rows whose latest receipt was never acknowledged re-post their stored state. */
  inboxUnacked(): Row[] {
    return this.db.prepare('SELECT value FROM inbox').all()
      .map(r => JSON.parse((r as any).value as string))
      .sort((a: Row, b: Row) => String(a.receivedAt).localeCompare(String(b.receivedAt)))
      .filter((row: Row) => (row.ackedAt == null || ['PENDING', 'FAILED'].includes(row.receiptDeliveryState)) && row.receiptDeliveryState !== 'UNDELIVERABLE')
      .slice(0, 100);
  }
  /** Commands accepted durably but never driven to a decision — the crash-between-intake-and-dispatch window. */
  inboxUndecided(): Row[] {
    return this.db.prepare('SELECT value FROM inbox').all()
      .map(r => JSON.parse((r as any).value as string))
      .filter((row: Row) => row.state === 'DURABLY_ACCEPTED')
      .sort((a: Row, b: Row) => String(a.receivedAt).localeCompare(String(b.receivedAt)));
  }
  inboxMaxSequence(instanceId: string): string | null {
    const rows = this.db.prepare('SELECT value FROM inbox').all().map(r => JSON.parse((r as any).value as string)).filter((r: Row) => r.instanceId === instanceId);
    if (!rows.length) return null;
    return String(rows.map((r: Row) => BigInt(r.commandSequence)).reduce((a, b) => (a > b ? a : b)));
  }
  inboxFindStartTaskFor(attemptId: string): Row | null {
    return this.db.prepare('SELECT value FROM inbox').all().map(r => JSON.parse((r as any).value as string))
      .find((r: Row) => r.attemptId === attemptId && r.type === 'startTask') ?? null;
  }

  // ---------------- event outbox ----------------
  /**
   * EventOutbox.append — sequence assignment inside the transaction:
   * (stream_id, seq) is unique so replay never allocates a new sequence.
   * `envelopeJson` is the canonical event object with sequence already set.
   */
  outboxAppendInTx(streamId: string, envelopeJson: Obj): Row {
    const row = this.db.prepare('SELECT COALESCE(MAX(seq),0) AS m FROM outbox WHERE stream_id=?').get(streamId) as any;
    const seq = Number(row.m) + 1;
    const value = { ...envelopeJson, sequence: String(seq) };
    this.db.prepare('INSERT INTO outbox VALUES (?,?,?,?)').run(streamId, seq, 'PENDING', JSON.stringify(value));
    return { streamId, seq, value };
  }
  outboxPending(streamId: string, limit: number): Row[] {
    return this.db.prepare("SELECT seq, value FROM outbox WHERE stream_id=? AND status <> 'ACKED' ORDER BY seq LIMIT ?")
      .all(streamId, limit).map(r => ({ seq: (r as any).seq, value: JSON.parse((r as any).value as string) }));
  }
  outboxPendingStreams(): string[] {
    return this.db.prepare("SELECT DISTINCT stream_id FROM outbox WHERE status <> 'ACKED'").all().map(r => (r as any).stream_id as string);
  }
  outboxPendingCount(streamId: string): number {
    const r = this.db.prepare("SELECT COUNT(*) AS c FROM outbox WHERE stream_id=? AND status <> 'ACKED'").get(streamId) as any;
    return Number(r.c);
  }
  /** applyAcks — ACCEPTED/DUPLICATE acks the row; REJECTED fails durably; RETRYABLE stays pending. */
  outboxApplyAcks(acks: { eventId: string; status: string; code?: string | null; receiptRef?: string | null; receiptDigest?: string | null }[]) {
    this.transaction(() => {
      for (const ack of acks) {
        const status = ack.status === 'ACCEPTED' || ack.status === 'DUPLICATE' ? 'ACKED' : ack.status === 'REJECTED' ? 'FAILED' : 'PENDING';
        this.db.prepare('UPDATE outbox SET status=? WHERE value LIKE ?').run(status, '%"eventId":"' + ack.eventId + '"%');
      }
    });
  }

  // ---------------- attempts + facts ----------------
  attemptFind(attemptId: string): Row | null {
    const r = this.db.prepare('SELECT value FROM attempts WHERE attempt_id=?').get(attemptId);
    return r ? JSON.parse(r.value as string) : null;
  }
  attemptFindByRunCommand(runCommandId: string): Row | null {
    const r = this.db.prepare('SELECT value FROM attempts WHERE run_command_id=?').get(runCommandId);
    return r ? JSON.parse(r.value as string) : null;
  }
  attemptPut(rec: Row) {
    this.db.prepare('INSERT OR REPLACE INTO attempts VALUES (?,?,?,?)').run(rec.attemptId, rec.runCommandId, rec.state, JSON.stringify(rec));
  }
  /** ExecutionJournal.advance — the state table is the only legal path. */
  attemptAdvance(rec: Row, next: string) {
    const legal = LEGAL[rec.state];
    if (!legal || !legal.includes(next)) throw new Error('ILLEGAL_ATTEMPT_TRANSITION:' + rec.state + '->' + next);
    rec.state = next; rec.updatedAt = new Date().toISOString();
    this.attemptPut(rec);
  }
  attemptsUnfinished(): Row[] {
    return this.db.prepare("SELECT value FROM attempts WHERE state <> 'FINISHED'").all().map(r => JSON.parse((r as any).value as string));
  }
  fact(attemptId: string, key: string): string | null {
    const r = this.db.prepare('SELECT value FROM facts WHERE attempt_id=? AND key=?').get(attemptId, key);
    return r ? (r as any).value as string : null;
  }
  /** putIfAbsent — a durable fact is first-write-wins (R04 deadlines). */
  factPutIfAbsent(attemptId: string, key: string, value: string): string {
    const existing = this.fact(attemptId, key);
    if (existing != null) return existing;
    this.db.prepare('INSERT INTO facts VALUES (?,?,?)').run(attemptId, key, value);
    return value;
  }

  // ---------------- artifacts ----------------
  artifactRecord(a: Row) {
    this.db.prepare('INSERT INTO artifacts VALUES (?,?,?,?)').run(a.artifactId, a.attemptId, a.status ?? 'PENDING', JSON.stringify(a));
  }
  artifactFind(artifactId: string): Row | null {
    const r = this.db.prepare('SELECT value FROM artifacts WHERE artifact_id=?').get(artifactId);
    return r ? JSON.parse(r.value as string) : null;
  }
  artifactAdvance(artifactId: string, status: string, receiptRef: string | null) {
    const row = this.artifactFind(artifactId); if (!row) return;
    row.status = status; if (receiptRef != null) row.receiptRef = receiptRef;
    this.db.prepare('UPDATE artifacts SET status=?, value=? WHERE artifact_id=?').run(status, JSON.stringify(row), artifactId);
  }
  artifactsPending(): Row[] {
    return this.db.prepare("SELECT value FROM artifacts WHERE status <> 'RECEIPTED'").all().map(r => JSON.parse((r as any).value as string));
  }
  artifactsForAttempt(attemptId: string): Row[] {
    return this.db.prepare('SELECT value FROM artifacts WHERE attempt_id=?').all(attemptId).map(r => JSON.parse((r as any).value as string));
  }

  // ---------------- finishes ----------------
  finishFind(attemptId: string): Row | null {
    const r = this.db.prepare('SELECT value FROM finishes WHERE attempt_id=?').get(attemptId);
    return r ? JSON.parse(r.value as string) : null;
  }
  finishRecordInTx(attemptId: string, finishId: string, payloadDigest: string, canonicalJson: string, boundInstanceId: string) {
    this.db.prepare('INSERT INTO finishes VALUES (?,?)').run(attemptId, JSON.stringify({
      attemptId, finishId, payloadDigest, canonicalJson, boundInstanceId,
      submitted: false, serviceState: null, receiptDigest: null, declaredAt: new Date().toISOString(),
    }));
  }
  finishMarkSubmitted(attemptId: string) {
    const row = this.finishFind(attemptId); if (!row) return;
    row.submitted = true; this.db.prepare('UPDATE finishes SET value=? WHERE attempt_id=?').run(JSON.stringify(row), attemptId);
  }
  finishMarkServiceState(attemptId: string, serviceState: string, receiptDigest: string | null) {
    const row = this.finishFind(attemptId); if (!row) return;
    row.serviceState = serviceState; row.receiptDigest = receiptDigest;
    this.db.prepare('UPDATE finishes SET value=? WHERE attempt_id=?').run(JSON.stringify(row), attemptId);
  }
  finishesPending(): Row[] {
    return this.db.prepare('SELECT value FROM finishes').all().map(r => JSON.parse((r as any).value as string))
      .filter((r: Row) => r.serviceState !== 'DELIVERY_ACCEPTED' && r.serviceState !== 'AUDIT_CONFIRMED');
  }
  finishBoundInstanceFor(attemptId: string): string | null { return this.finishFind(attemptId)?.boundInstanceId ?? null; }

  // ---------------- recovery task schedule ----------------
  recoverySeed(kind: string, targetId: string, oldInstanceId: string) {
    const taskId = 'rec-' + kind + '-' + targetId;
    this.db.prepare('INSERT OR IGNORE INTO recovery_tasks VALUES (?,?)').run(taskId, JSON.stringify({
      taskId, kind, targetId, oldInstanceId, attempts: 0, nextRetryAt: 0, lastError: null, parked: false,
    }));
  }
  recoveryDue(nowMs: number): Row[] {
    return this.db.prepare('SELECT value FROM recovery_tasks').all().map(r => JSON.parse((r as any).value as string))
      .filter((r: Row) => !r.parked && r.nextRetryAt <= nowMs).slice(0, 50);
  }
  recoveryRetry(taskId: string, error: string, nextRetryAt: number) {
    const row = this.recoveryFind(taskId); if (!row) return;
    row.attempts += 1; row.lastError = error; row.nextRetryAt = nextRetryAt;
    this.db.prepare('UPDATE recovery_tasks SET value=? WHERE task_id=?').run(JSON.stringify(row), taskId);
  }
  recoveryDone(taskId: string) { this.db.prepare('DELETE FROM recovery_tasks WHERE task_id=?').run(taskId); }
  recoveryPark(taskId: string, error: string) {
    const row = this.recoveryFind(taskId); if (!row) return;
    row.parked = true; row.lastError = error;
    this.db.prepare('UPDATE recovery_tasks SET value=? WHERE task_id=?').run(JSON.stringify(row), taskId);
  }
  recoveryFind(taskId: string): Row | null {
    const r = this.db.prepare('SELECT value FROM recovery_tasks WHERE task_id=?').get(taskId);
    return r ? JSON.parse(r.value as string) : null;
  }
  recoveryAll(): Row[] { return this.db.prepare('SELECT value FROM recovery_tasks').all().map(r => JSON.parse((r as any).value as string)); }
  /** streamTargetFor — the dead instance's stream for the recovery grant. */
  streamTargetFor(oldInstanceId: string): string | null {
    const streams = this.outboxPendingStreams();
    // We store bound stream ids on attempts; fall back to any pending stream.
    const rows = this.db.prepare('SELECT value FROM attempts').all().map(r => JSON.parse((r as any).value as string));
    const bound = rows.find((r: Row) => r.boundInstanceId === oldInstanceId && r.boundStreamId);
    if (bound) return bound.boundStreamId;
    return streams[0] ?? null;
  }
}
