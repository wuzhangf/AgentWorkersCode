import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { canonicalBytes, canonicalDigest, localKey, newEventId, newId, nowRfc3339, PROTOCOL_NAME } from './protocol.js';
import { ControlPlaneError, HttpControlPlanePort, isLeaseLoss } from './port.js';
import { LocalError, LocalHostClient } from './local.js';
import { WireJournal, type Row } from './journal.js';
import * as dto from './dto.js';
import * as Fields from './fields.js';
import type { Obj } from './fields.js';

/**
 * W6/P25: enterprise wire adapter — WorkerDaemonMain + AttemptRunner +
 * CommandPump + EventOutboxDriver + ArtifactDriver + FinishDriver +
 * RecoveryLane, all durable in the WireJournal, driving any host that
 * serves the /local/v1 contract (embedded Pi host or an external engine
 * host such as the codex-native-worker — select via localEngineId). The
 * host is the executor; the adapter is the
 * protocol-correct worker: durable-first intake, whole-envelope dedupe,
 * revisioned receipts retransmitted until acked, per-stream monotone
 * event sequence, quiescence-proven stop, sealed evidence upload,
 * immutable finish declarations with receipt polling, and scoped
 * same-origin recovery.
 *
 * Attempt lanes not yet mapped to a real host capability are declared
 * honestly at the gate (REJECTED INVALID_WORKSPACE_SCOPE /
 * UNKNOWN_EXECUTION_PROFILE / INVALID_SCHEMA) — never faked.
 */

/**
 * The host's task `state` vs. the attempt lifecycle are different
 * machines. A live `UNKNOWN` verdict is NOT terminal — it is the
 * dispatch-in-flight uncertainty window (and the honest unprovable
 * verdict after a lost child). The live driver keeps reconciling it
 * under an unknown budget, exactly like Java's CommandObservation
 * UNKNOWN handling; only the observer lane treats a durable UNKNOWN as
 * a settled-uncertain verdict.
 */
const LIVE_TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED', 'BINDING_UNAVAILABLE', 'BUDGET_EXCEEDED']);
const OBSERVED_TERMINAL = new Set([...LIVE_TERMINAL, 'UNKNOWN']);
const ATTEMPT_TERMINAL = new Set(['FINISHED', 'FAILED', 'CANCELLED']);
const STOP_GUARD_RETRY_MS = 30_000;

export class BudgetExceeded extends Error { constructor(readonly detailCode: string) { super(detailCode); this.name = 'BudgetExceeded'; } }

export interface ProjectMap { [wireId: string]: string }
export interface WireAdapterConfig {
  stateDir: string;
  controlPlaneUrl: string;
  workerToken: string;
  localUrl: string;
  localToken: string;
  workerId: string;
  deploymentId: string;
  imageDigest?: string;
  /** engineId sent to the /local/v1 host in preflight/create ('pi' or 'codex'). */
  localEngineId?: string;
  /** wire projectId / workspaceId → local Pi project id; '*' is the fallback. */
  projects?: ProjectMap;
  executionProfiles?: string[];
  capabilities?: Partial<dto.WorkerCapabilities>;
  heartbeatMillis?: number;
  pumpMillis?: number;
  taskPollMillis?: number;
  pollWaitSeconds?: number;
  quiesceWaitMillis?: number;
  /** How long a live UNKNOWN verdict is reconciled before declaring it honestly. */
  unknownBudgetMillis?: number;
  finishPollMaxAttempts?: number;
  recoveryMaxAttempts?: number;
  recoveryGrantTtlMillis?: number;
  fetchImpl?: typeof fetch;
  log?: (line: string) => void;
}

interface Driver { cancelRequested: boolean; cancelReason: string | null; cancelCommandId: string | null }

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));
const scrub = (s: string) => s.replace(/(recoverySessionId|recoveryTicket)=[^&\s"']+/g, '$1=REDACTED');

export class EnterpriseWireAdapter {
  private readonly journal: WireJournal;
  private readonly plane: HttpControlPlanePort;
  private readonly local: LocalHostClient;
  private readonly cfg: Required<Omit<WireAdapterConfig, 'capabilities' | 'imageDigest' | 'projects' | 'executionProfiles' | 'fetchImpl' | 'log'>> & Pick<WireAdapterConfig, 'projects' | 'executionProfiles' | 'fetchImpl' | 'log' | 'imageDigest'>;
  private readonly capabilities: dto.WorkerCapabilities;
  registration: dto.RegisterResponse | null = null;
  private priorInstanceId: string | null = null;
  private readonly drivers = new Map<string, Driver>();
  private readonly observersInFlight = new Set<string>();
  private readonly recoverySessions = new Map<string, dto.RecoverySession>();
  private draining = false;
  private stopWhenIdle = false;
  private running = false;
  private pollAbort = new AbortController();
  private loopHandles: Promise<void>[] = [];
  private lastPollError: string | null = null;
  private lastPumpError: string | null = null;

  constructor(cfg: WireAdapterConfig) {
    this.cfg = {
      heartbeatMillis: cfg.heartbeatMillis ?? 10_000,
      pumpMillis: cfg.pumpMillis ?? 1_000,
      taskPollMillis: cfg.taskPollMillis ?? 200,
      pollWaitSeconds: cfg.pollWaitSeconds ?? 20,
      quiesceWaitMillis: cfg.quiesceWaitMillis ?? 30_000,
      unknownBudgetMillis: cfg.unknownBudgetMillis ?? 60_000,
      finishPollMaxAttempts: cfg.finishPollMaxAttempts ?? 5,
      recoveryMaxAttempts: cfg.recoveryMaxAttempts ?? 8,
      recoveryGrantTtlMillis: cfg.recoveryGrantTtlMillis ?? 60_000,
      stateDir: cfg.stateDir, controlPlaneUrl: cfg.controlPlaneUrl, workerToken: cfg.workerToken,
      localUrl: cfg.localUrl, localToken: cfg.localToken, localEngineId: cfg.localEngineId ?? 'pi',
      workerId: cfg.workerId, deploymentId: cfg.deploymentId,
      projects: cfg.projects, executionProfiles: cfg.executionProfiles,
      fetchImpl: cfg.fetchImpl, log: cfg.log, imageDigest: cfg.imageDigest,
    };
    this.journal = new WireJournal(cfg.stateDir);
    this.plane = new HttpControlPlanePort({ baseUrl: cfg.controlPlaneUrl, bearerToken: cfg.workerToken, fetchImpl: cfg.fetchImpl });
    this.local = new LocalHostClient({ baseUrl: cfg.localUrl, token: cfg.localToken, fetchImpl: cfg.fetchImpl });
    this.capabilities = {
      providers: ['pi'], toolchains: ['typescript'],
      maxConcurrentTasks: 1, cancel: true, approval: false,
      changeCapture: 'FINAL_TREE',
      executionProfiles: cfg.executionProfiles ?? ['pi.standard'],
      commercialAdmissionV1: false, projectSourceV1: false,
      ...cfg.capabilities,
    };
  }

  private log(line: string) { try { this.cfg.log?.('[pi-wire] ' + line); } catch { /* never break on logging */ } }

  // ======================================================================
  // lifecycle — WorkerDaemonMain.start / runLoop
  // ======================================================================
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.pollAbort = new AbortController();
    await this.register();
    await this.performRecovery();
    await this.redispatchStuckIntake();
    await this.resendUnackedReceipts();
    await this.resendPendingArtifacts();
    await this.resendPendingFinishes();
    // Java waits for recovery first: recovery before dispatch.
    for (const rec of this.journal.attemptsUnfinished()) {
      if (rec.boundInstanceId === this.registration?.instanceId) {
        this.spawnAttemptDriver(rec);
      } else {
        this.spawnObserver(rec);
      }
    }
    this.loopHandles = [
      this.heartbeatLoop(),
      this.commandPollLoop(),
      this.pumpLoop(),
    ];
  }

  /** drainAndStop: stop the loops, let drivers observe the stop flag, close the journal. */
  async stop(graceMillis = 5_000): Promise<void> {
    this.running = false;
    this.pollAbort.abort();
    await Promise.allSettled(this.loopHandles);
    const deadline = Date.now() + graceMillis;
    while (this.drivers.size && Date.now() < deadline) await sleep(25);
    try { await this.flushOutbox(); } catch { /* durable — next boot retries */ }
    try { await this.runRecoveryTasksOnce(); } catch { /* durable — next boot retries */ }
    this.journal.close();
  }

  status() {
    const reg = this.registration;
    return {
      running: this.running,
      instanceId: reg?.instanceId ?? null,
      streamId: reg?.streamId ?? null,
      workerLeaseEpoch: reg?.workerLeaseEpoch ?? null,
      draining: this.draining,
      activeAttempts: [...this.drivers.keys()],
      lastPollError: this.lastPollError,
      lastPumpError: this.lastPumpError,
    };
  }

  // ======================================================================
  // registration (03 §6.3) — durable bootId replay; bound ids stored
  // ======================================================================
  private async register(): Promise<dto.RegisterResponse> {
    let boot = this.journal.bootstrap();
    let bootId = boot?.bootId;
    if (bootId == null) {
      bootId = newEventId();
      this.journal.saveBootstrap(this.cfg.workerId, this.cfg.deploymentId, this.cfg.imageDigest ?? '', bootId);
      boot = this.journal.bootstrap();
    }
    if (boot && boot.workerId !== this.cfg.workerId) {
      throw new Error('DURABLE_BOOT_IDENTITY_MISMATCH: durable workerId=' + boot.workerId + ' configured=' + this.cfg.workerId);
    }
    if (boot?.instanceId && boot.instanceId !== this.registration?.instanceId) {
      this.priorInstanceId = boot.instanceId;
    }
    this.log('registering workerId=' + this.cfg.workerId + ' bootId=' + bootId + (this.priorInstanceId ? ' (old instance ' + this.priorInstanceId + ')' : ''));
    const resp = await this.plane.register(dto.registerRequestToJson({
      protocol: PROTOCOL_NAME, workerId: this.cfg.workerId, bootId,
      deploymentId: this.cfg.deploymentId, reportedImageDigest: this.cfg.imageDigest ?? null,
      capabilities: this.capabilities,
      recoverySummary: this.priorInstanceId ? { pendingEventCount: this.totalPendingEvents(), pendingArtifactCount: this.journal.artifactsPending().length } : null,
    }));
    this.registration = resp;
    this.journal.bindInstance(resp.instanceId, resp.streamId, resp.workerLeaseEpoch);
    this.applyDesiredState(resp.desiredState);
    this.log('registered instanceId=' + resp.instanceId + ' streamId=' + resp.streamId + ' epoch=' + resp.workerLeaseEpoch);
    return resp;
  }

  /** desired-state application — REGISTERED/READY/DRAINING/STOP. */
  private applyDesiredState(desired: string) {
    switch (desired) {
      case 'READY': this.draining = false; this.stopWhenIdle = false; break;
      case 'DRAINING': this.draining = true; break;
      case 'STOP': this.draining = true; this.stopWhenIdle = true; break;
      default: break;
    }
  }

  private currentAttemptId(): string | null {
    for (const attemptId of this.drivers.keys()) {
      const rec = this.journal.attemptFind(attemptId);
      if (rec && !ATTEMPT_TERMINAL.has(rec.state)) return attemptId;
    }
    return null;
  }
  private totalPendingEvents(): number {
    return this.journal.outboxPendingStreams().reduce((n, s) => n + this.journal.outboxPendingCount(s), 0);
  }

  // ======================================================================
  // heartbeat — lease/epoch tracking + lease-loss re-registration
  // ======================================================================
  private async heartbeatLoop(): Promise<void> {
    while (this.running) {
      let nextSleep = this.cfg.heartbeatMillis;
      try {
        const resp = await this.plane.heartbeat(this.registration!.instanceId, dto.heartbeatRequestToJson({
          heartbeatId: 'hb-' + newEventId(),
          workerLeaseEpoch: this.registration!.workerLeaseEpoch,
          state: this.draining ? 'DRAINING' : this.currentAttemptId() != null ? 'BUSY' : 'READY',
          currentAttemptId: this.currentAttemptId(),
          pendingEventCount: this.totalPendingEvents(),
          pendingArtifactCount: this.journal.artifactsPending().length,
        }));
        this.applyDesiredState(resp.desiredState);
        if (resp.renewAfterMillis != null) {
          const renew = Number(resp.renewAfterMillis);
          if (Number.isFinite(renew) && renew > 0) nextSleep = Math.min(Math.max(renew, 100), this.cfg.heartbeatMillis * 4);
        }
      } catch (e) {
        if (isLeaseLoss(e)) { try { await this.register(); } catch (re) { this.log('rebind failed: ' + scrub(String(re))); } }
        else this.log('heartbeat failed: ' + scrub(String(e)));
      }
      await sleep(nextSleep);
    }
  }

  // ======================================================================
  // command pump — CommandPump.pollOnce semantics
  // ======================================================================
  private async commandPollLoop(): Promise<void> {
    const reg0 = this.registration!;
    let after: string | null = this.journal.inboxMaxSequence(reg0.instanceId);
    while (this.running) {
      if (this.stopWhenIdle && this.drivers.size === 0) break;
      try {
        const page = await this.plane.pollCommands(this.registration!.instanceId, after, this.cfg.pollWaitSeconds, this.pollAbort.signal);
        for (const env of page.commands) {
          if (after == null || BigInt(env.commandSequence) > BigInt(after)) after = env.commandSequence;
          await this.handleCommand(env);
        }
        if (page.nextAfter != null) after = page.nextAfter;
        this.lastPollError = null;
      } catch (e) {
        if (!this.running) break;
        if (isLeaseLoss(e)) {
          try { await this.register(); await this.resendUnackedReceipts(); this.spawnRecoveryForOldInstance(); }
          catch (re) { this.log('rebind failed: ' + scrub(String(re))); }
          after = this.journal.inboxMaxSequence(this.registration!.instanceId);
        } else if (e instanceof ControlPlaneError && !e.retryable) {
          this.lastPollError = e.code;
          throw e;
        } else {
          this.lastPollError = e instanceof Error ? e.message : String(e);
          await sleep(2_000);
        }
      }
    }
  }

  /** CommandPump.handleCommand — durable intake first; duplicates replay their receipt. */
  private async handleCommand(env: dto.CommandEnvelope): Promise<void> {
    if (env.instanceId !== this.registration!.instanceId) {
      this.log('command ' + env.commandId + ' for foreign instance ' + env.instanceId + ' — skipped');
      return;
    }
    const attemptId = this.attemptIdFor(env);
    const stored = this.journal.inboxReceive(env, attemptId);
    if (stored.outcome === 'DUPLICATE_SAME') {
      // Duplicate redelivery: replay the stored decision, never re-dispatch.
      const row = this.journal.inboxFind(env.commandId)!;
      await this.resendReceipt(env, row.state as dto.CommandState, row.receiptRevision);
      return;
    }
    if (stored.outcome === 'DUPLICATE_CONFLICT') {
      const row = this.journal.inboxFind(env.commandId)!;
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', 'IDEMPOTENCY_PAYLOAD_MISMATCH');
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'IDEMPOTENCY_PAYLOAD_MISMATCH' });
      this.log('command ' + env.commandId + ' conflict — envelope digest differs (prior rev ' + row.receiptRevision + ')');
      return;
    }
    // RECEIVED → DURABLY_ACCEPTED receipt first, then durable dispatch decision.
    await this.sendReceipt({ commandId: env.commandId, receiptRevision: '0', state: 'DURABLY_ACCEPTED', payloadDigest: env.payloadDigest });
    try {
      await this.dispatchCommand(env);
    } catch (e) {
      const code = e instanceof Error ? e.message.slice(0, 120) : String(e).slice(0, 120);
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', code);
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'DISPATCH_FAILURE' });
    }
  }

  private attemptIdFor(env: dto.CommandEnvelope): string | null {
    const p = env.payload;
    if (env.type === 'startTask') {
      const existing = typeof p?.runCommandId === 'string' ? this.journal.attemptFindByRunCommand(p.runCommandId) : null;
      if (existing) return existing.attemptId;
      return typeof p?.attemptId === 'string' ? p.attemptId : null;
    }
    if (env.type === 'cancelTask' || env.type === 'approvalDecision') {
      return typeof p?.attemptId === 'string' ? p.attemptId : null;
    }
    return null;
  }

  private async dispatchCommand(env: dto.CommandEnvelope): Promise<void> {
    switch (env.type) {
      case 'startTask': return this.dispatchStartTask(env);
      case 'cancelTask': return this.dispatchCancelTask(env);
      case 'approvalDecision': return this.dispatchApprovalDecision(env);
      case 'drain': return this.dispatchDrain(env);
      default:
        this.journal.inboxAdvance(env.commandId, 'REJECTED', 'unsupported type ' + env.type);
        await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(this.journal.inboxFind(env.commandId)!.receiptRevision), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'INVALID_SCHEMA' });
    }
  }

  /** startTask — admission checks are durable decisions, then the durable record exists before any local effect. */
  private async dispatchStartTask(env: dto.CommandEnvelope): Promise<void> {
    let payload: dto.StartTask;
    try { payload = dto.parseStartTask(env.payload); }
    catch (e) {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', String(e).slice(0, 120));
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'INVALID_SCHEMA' });
      return;
    }
    const existing = this.journal.attemptFindByRunCommand(payload.runCommandId);
    if (existing && existing.attemptId !== payload.attemptId) {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', 'runCommandId bound to ' + existing.attemptId);
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'IDEMPOTENCY_PAYLOAD_MISMATCH' });
      return;
    }
    const reject = await this.admissionRejection(payload);
    if (reject != null) {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', reject.detail);
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: reject.code });
      return;
    }
    if (!existing) {
      const rec: Row = {
        attemptId: payload.attemptId, attemptEpoch: payload.attemptEpoch,
        taskId: payload.taskId, runCommandId: payload.runCommandId, commandId: env.commandId,
        organizationId: env.organizationId, projectId: payload.projectId ?? env.projectId,
        workerId: this.cfg.workerId,
        workspaceId: payload.workspaceId, workspaceGeneration: payload.workspaceGeneration,
        goal: payload.goal, repository: payload.repository, scope: payload.scope,
        maxDurationMillis: payload.maxDurationMillis, executionProfile: payload.executionProfile,
        acceptanceBindingRef: payload.acceptanceBindingRef,
        state: 'ACCEPTED', cancelRequested: false, cancelReason: null, cancelCommandId: null,
        localTaskId: null, engineRunId: null, engineOutcome: null, engineDetail: null,
        boundInstanceId: this.registration!.instanceId, boundStreamId: this.registration!.streamId,
        boundEpoch: this.registration!.workerLeaseEpoch,
        createdAt: nowRfc3339(), updatedAt: nowRfc3339(), startedAt: null, finishedAt: null,
      };
      this.journal.attemptPut(rec);
      this.spawnAttemptDriver(rec, env);
    }
  }

  /** Honest capability gate — what this host cannot prove, it does not fake. */
  private async admissionRejection(p: dto.StartTask): Promise<{ code: string; detail: string } | null> {
    if (this.draining) return { code: 'WORKER_DRAINING', detail: 'worker is draining' };
    if (this.drivers.size >= this.capabilities.maxConcurrentTasks) {
      return { code: 'ADMISSION_CAPACITY_FULL', detail: 'single-attempt host occupied' };
    }
    if (p.sourcePlanRef != null || p.sourcePlan != null) {
      return { code: 'INVALID_WORKSPACE_SCOPE', detail: 'projectSourceV1 undeclared' };
    }
    if (p.repository != null) {
      return { code: 'INVALID_WORKSPACE_SCOPE', detail: 'repository pinning (directGitV1) undeclared' };
    }
    if (p.scope != null && p.scope.writePaths.length > 0) {
      return { code: 'INVALID_WORKSPACE_SCOPE', detail: 'workspace write-path scoping not implemented by pi host' };
    }
    if (p.executionProfile != null && !this.capabilities.executionProfiles.includes(p.executionProfile)) {
      return { code: 'UNKNOWN_EXECUTION_PROFILE', detail: 'unsupported profile ' + p.executionProfile };
    }
    if (!this.resolveLocalProject(p)) {
      return { code: 'INVALID_WORKSPACE_SCOPE', detail: 'no local project mapping for ' + (p.projectId ?? 'unspecified') };
    }
    return null;
  }

  private resolveLocalProject(p: dto.StartTask): string | null {
    const map = this.cfg.projects ?? {};
    if (p.projectId && map[p.projectId]) return map[p.projectId];
    if (map['*']) return map['*'];
    if (p.projectId) return p.projectId; // identity fallback — the host validates existence itself
    return null;
  }

  private async dispatchCancelTask(env: dto.CommandEnvelope): Promise<void> {
    let c: dto.CancelTask;
    try { c = dto.parseCancelTask(env.payload); }
    catch (e) {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', String(e).slice(0, 120));
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'INVALID_SCHEMA' });
      return;
    }
    const rec = this.journal.attemptFind(c.attemptId);
    if (!rec || rec.attemptEpoch !== c.attemptEpoch) {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', 'unknown attempt/epoch');
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'INVALID_SCHEMA' });
      return;
    }
    const driver = this.drivers.get(c.attemptId);
    if (driver) {
      driver.cancelRequested = true;
      driver.cancelReason = c.reason;
      driver.cancelCommandId = c.cancelCommandId;
    }
    rec.cancelRequested = true; rec.cancelReason = c.reason; rec.cancelCommandId = c.cancelCommandId;
    rec.updatedAt = nowRfc3339();
    this.journal.attemptPut(rec);
    // CANCELLED receipt is the durable decision acknowledgement; the
    // attempt's own finish declaration is the real proof-of-stop.
    const rev = this.journal.inboxAdvance(env.commandId, 'CANCELLED', null);
    await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'CANCELLED', payloadDigest: env.payloadDigest });
  }

  private async dispatchApprovalDecision(env: dto.CommandEnvelope): Promise<void> {
    try { dto.parseApprovalDecision(env.payload); }
    catch (e) {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', String(e).slice(0, 120));
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'INVALID_SCHEMA' });
      return;
    }
    // The pi host's provider ask_user lane is not yet bridged (P09); the
    // decision is recorded durably and receipted STARTED — capability is
    // not overclaimed (approval:false in WorkerCapabilities).
    const rev = this.journal.inboxAdvance(env.commandId, 'STARTED', 'decision recorded; provider approval lane unimplemented');
    await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'STARTED', payloadDigest: env.payloadDigest });
  }

  private async dispatchDrain(env: dto.CommandEnvelope): Promise<void> {
    try { dto.parseDrain(env.payload); } catch { /* a drain with a malformed payload still drains */ }
    this.draining = true;
    const rev = this.journal.inboxAdvance(env.commandId, 'STARTED', 'draining');
    await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'STARTED', payloadDigest: env.payloadDigest });
  }

  /**
   * Boot-time safety net: commands durably accepted but never decided
   * (crash between intake and dispatch) are re-dispatched from the
   * stored envelope — the inbox digest makes a redelivery with a
   * different envelope a conflict, never silent work.
   */
  private async redispatchStuckIntake(): Promise<void> {
    for (const row of this.journal.inboxUndecided()) {
      if (row.envelope) {
        try { await this.dispatchCommand(row.envelope as dto.CommandEnvelope); }
        catch (e) { this.log('redispatch ' + row.commandId + ': ' + scrub(String(e))); }
      }
    }
  }

  private async sendReceipt(r: dto.ReceiptRequest): Promise<void> {
    try {
      await this.plane.postReceipt(this.registration!.instanceId, dto.receiptRequestToJson(r));
      this.journal.inboxMarkDelivery(r.commandId, true);
      this.journal.inboxMarkAcked(r.commandId);
    } catch (e) {
      this.journal.inboxMarkDelivery(r.commandId, false);
      if (e instanceof ControlPlaneError && !e.retryable) this.journal.inboxMarkUndeliverable(r.commandId);
      this.log('receipt ' + r.commandId + '/' + r.state + ' not delivered: ' + scrub(String(e)));
    }
  }
  /** Duplicate replay: re-send the stored state at its stored revision (R08). */
  private async resendReceipt(env: dto.CommandEnvelope, state: dto.CommandState, revision: number): Promise<void> {
    const rec = attemptIdForEnvelope(env, this.journal);
    const req: dto.ReceiptRequest = { commandId: env.commandId, receiptRevision: String(revision), state, payloadDigest: env.payloadDigest };
    if (state === 'STARTED' && rec) { req.attemptId = rec.attemptId; req.engineRunId = rec.engineRunId ?? undefined; }
    if (state === 'REJECTED') req.detailCode = this.journal.inboxFind(env.commandId)?.lastError ?? 'DUPLICATE';
    await this.sendReceipt(req);
  }
  /** R08 — receipts the plane never acknowledged are reposted with stored state+revision. */
  private async resendUnackedReceipts(): Promise<void> {
    for (const row of this.journal.inboxUnacked()) {
      const req: dto.ReceiptRequest = { commandId: row.commandId, receiptRevision: String(row.receiptRevision), state: row.state, payloadDigest: row.payloadDigest };
      if (row.state === 'STARTED' && row.attemptId) {
        const rec = this.journal.attemptFind(row.attemptId);
        if (rec) { req.attemptId = rec.attemptId; req.engineRunId = rec.engineRunId ?? undefined; }
      }
      if (row.state === 'REJECTED') req.detailCode = row.lastError ?? 'REDELIVERED';
      else req.detailCode = 'REDELIVERED';
      await this.sendReceipt(req);
    }
  }

  // ======================================================================
  // attempt driver — AttemptRunner phases over the /local/v1 host
  // ======================================================================
  private spawnAttemptDriver(rec: Row, env?: dto.CommandEnvelope) {
    if (this.drivers.has(rec.attemptId)) return;
    const driver: Driver = { cancelRequested: rec.cancelRequested === true, cancelReason: rec.cancelReason ?? null, cancelCommandId: rec.cancelCommandId ?? null };
    this.drivers.set(rec.attemptId, driver);
    const envelope = env ?? this.envelopeForStart(rec);
    void this.runAttempt(rec.attemptId, envelope, driver)
      .catch(e => this.log('attempt ' + rec.attemptId + ' driver error: ' + scrub(String(e))))
      .finally(() => { this.drivers.delete(rec.attemptId); });
  }
  private envelopeForStart(rec: Row): dto.CommandEnvelope | null {
    const row = this.journal.inboxFind(rec.commandId);
    return (row?.envelope as dto.CommandEnvelope) ?? null;
  }

  private async runAttempt(attemptId: string, env: dto.CommandEnvelope | null, driver: Driver): Promise<void> {
    let rec = this.journal.attemptFind(attemptId);
    if (!rec) return;
    try {
      while (this.running) {
        rec = this.journal.attemptFind(attemptId);
        if (!rec) return;
        switch (rec.state) {
          case 'ACCEPTED':
            this.journal.attemptAdvance(rec, 'PREPARING');
            continue;
          case 'PREPARING': {
            const ok = await this.preparePhase(rec);
            if (!ok) {
              await this.failAndFinish(rec, 'PREPARING');
              await this.postRunSafety(env);
              return;
            }
            continue;
          }
          case 'STARTING':
          case 'AMBIGUOUS':
            await this.startPhase(rec, driver);
            continue;
          case 'EXECUTING':
            if (driver.cancelRequested || rec.cancelRequested) {
              await this.cancelPhase(rec, 'CANCELLED', driver.cancelReason ?? rec.cancelReason);
              await this.finalizeDelivery(this.journal.attemptFind(attemptId)!, 'CANCELLED');
              await this.postRunSafety(env);
              return;
            }
            await this.executePhase(rec, driver);
            continue;
          case 'CANCELLED': {
            // Durable cancel replays cancelPhase idempotently, then declares.
            await this.cancelPhase(rec, 'CANCELLED', rec.cancelReason);
            await this.finalizeDelivery(this.journal.attemptFind(attemptId)!, 'CANCELLED');
            await this.postRunSafety(env);
            return;
          }
          case 'FAILED': {
            // Terminal engine state with an unfinished declaration: finish
            // from durable facts — never a fabricated outcome (04 §12.1.1).
            const declared = this.journal.finishFind(attemptId);
            if (declared) await this.submitFinish(rec, declared);
            else await this.finalizeDelivery(rec, rec.engineOutcome ?? 'UNKNOWN');
            await this.postRunSafety(env);
            return;
          }
          case 'FINISHED':
            await this.postRunSafety(env);
            return;
          case 'QUIESCING':
            await this.proveQuiescence(rec);
            this.journal.attemptAdvance(this.journal.attemptFind(attemptId)!, 'COLLECTING');
            continue;
          case 'VERIFYING':
            this.journal.attemptAdvance(rec, 'COLLECTING');
            continue;
          case 'COLLECTING':
            await this.collectPhase(rec);
            this.journal.attemptAdvance(this.journal.attemptFind(attemptId)!, 'FINISHING');
            continue;
          case 'FINISHING': {
            const idempotentRef = this.journal.fact(attemptId, 'finishId');
            this.emit(rec, 'task.finished', { outcome: 'SUCCEEDED', finishId: idempotentRef ?? null });
            await this.finalizeDelivery(this.journal.attemptFind(attemptId)!, 'SUCCEEDED');
            this.journal.attemptAdvance(this.journal.attemptFind(attemptId)!, 'FINISHED');
            await this.postRunSafety(env);
            return;
          }
          default:
            return;
        }
      }
    } catch (e) {
      if (!this.running) return;
      const fresh = this.journal.attemptFind(attemptId);
      if (!fresh) return;
      if (e instanceof BudgetExceeded) {
        await this.cancelPhase(fresh, 'FAILED', e.detailCode).catch(() => undefined);
        await this.finalizeDelivery(this.journal.attemptFind(attemptId)!, 'FAILED').catch(err => this.log(String(err)));
      } else {
        await this.failPhase(fresh, e).catch(() => undefined);
        await this.finalizeDelivery(this.journal.attemptFind(attemptId)!, 'FAILED').catch(err => this.log(String(err)));
      }
      await this.postRunSafety(env);
    }
  }

  /** prepare: resolve mapping → preflight → pin deadline → create → bind localTaskId. */
  private async preparePhase(rec: Row): Promise<boolean> {
    if (rec.localTaskId) { this.journal.attemptAdvance(rec, 'STARTING'); return true; }
    const env = this.envelopeForStart(rec);
    const payload: dto.StartTask = env?.payload ? dto.parseStartTask(env.payload) : ({} as dto.StartTask);
    const localProject = this.resolveLocalProject(payload);
    try {
      const pf = await this.local.preflight(localProject!, this.cfg.localEngineId);
      // The requestKey is the replay binding: same wire runCommandId →
      // same local intent, so an adapter restart re-reads the same
      // operation instead of double-creating a task.
      const requestKey = localKey('wire-', rec.runCommandId);
      const body: Row = {
        requestKey, projectId: localProject, engineId: this.cfg.localEngineId,
        optionsRevision: pf.optionsRevision,
        instruction: payload.goal ?? rec.goal,
      };
      if (rec.maxDurationMillis != null) {
        const deadline = new Date(Date.now() + rec.maxDurationMillis).toISOString();
        this.journal.factPutIfAbsent(rec.attemptId, 'executionDeadlineEpochMs', deadline);
      }
      const created = await this.local.createTask(body);
      rec.localTaskId = created.taskId;
      rec.updatedAt = nowRfc3339();
      this.journal.attemptPut(rec);
      this.journal.transaction(() => {
        this.emit(rec, 'task.prepared', { workspace: localProject, launchState: created.launchState });
        this.journal.attemptAdvance(this.journal.attemptFind(rec.attemptId)!, 'STARTING');
      });
      return true;
    } catch (e) {
      const code = e instanceof LocalError ? e.code : 'PREPARE_FAILURE';
      this.markEngineDone(rec, 'FAILED', code);
      this.emit(rec, 'task.failed', { phase: 'PREPARING', reasonCode: code, detail: scrub(String(e)).slice(0, 512) });
      const fresh = this.journal.attemptFind(rec.attemptId)!;
      this.journal.attemptAdvance(fresh, 'FAILED');
      return false;
    }
  }

  /** start: poll detail until RUNNING with a nativeSessionId (or terminal), under the admission deadline. */
  private async startPhase(rec: Row, driver: Driver): Promise<void> {
    let unknownSince = 0;
    while (this.running) {
      if (driver.cancelRequested || rec.cancelRequested) return;
      const deadline = this.journal.fact(rec.attemptId, 'executionDeadlineEpochMs');
      if (deadline != null && Date.now() > Date.parse(deadline)) throw new BudgetExceeded('EXECUTION_BUDGET_EXCEEDED');
      let d: Row;
      try { d = await this.local.taskDetail(rec.localTaskId); }
      catch (e) {
        if (e instanceof LocalError && e.code === 'TASK_NOT_FOUND') {
          this.markEngineDone(rec, 'FAILED', 'LOCAL_TASK_LOST');
          this.emit(rec, 'task.failed', { phase: 'STARTING', reasonCode: 'LOCAL_TASK_LOST' });
          this.journal.attemptAdvance(this.journal.attemptFind(rec.attemptId)!, 'FAILED');
          return;
        }
        await sleep(this.cfg.taskPollMillis); continue;
      }
      if (d.state === 'UNKNOWN') {
        // Verdict-uncertain window: keep reconciling under a budget —
        // never fabricate SUCCEEDED, never abandon early.
        if (unknownSince === 0) unknownSince = Date.now();
        if (Date.now() - unknownSince > this.cfg.unknownBudgetMillis) {
          this.markEngineDone(rec, 'UNKNOWN', 'OUTCOME_UNKNOWN');
          this.emit(rec, 'task.failed', { phase: 'STARTING', reasonCode: 'OUTCOME_UNKNOWN' });
          this.journal.attemptAdvance(this.journal.attemptFind(rec.attemptId)!, 'FAILED');
          return;
        }
        await sleep(this.cfg.taskPollMillis); continue;
      }
      unknownSince = 0;
      if (d.state === 'RUNNING' && d.nativeSessionId) {
        this.markStarted(rec, rec.localTaskId);
        this.journal.attemptAdvance(this.journal.attemptFind(rec.attemptId)!, 'EXECUTING');
        this.emit(rec, 'execution.started', { engineRunId: rec.engineRunId, nativeSessionId: d.nativeSessionId });
        await this.fireStarted(rec);
        return;
      }
      if (LIVE_TERMINAL.has(d.state)) {
        const outcome = localOutcome(d);
        if (d.nativeSessionId) {
          // The engine ran to completion between create and first
          // observation — bind the run, land EXECUTING legally, receipt
          // STARTED, then settle through the same evidence path.
          this.markStarted(rec, rec.localTaskId);
          this.emit(rec, 'execution.started', { engineRunId: rec.engineRunId, nativeSessionId: d.nativeSessionId, observedAtTerminal: true });
          const mid = this.journal.attemptFind(rec.attemptId)!;
          if (mid.state === 'STARTING' || mid.state === 'AMBIGUOUS') this.journal.attemptAdvance(mid, 'EXECUTING');
          await this.fireStarted(this.journal.attemptFind(rec.attemptId)!);
          this.markEngineDone(this.journal.attemptFind(rec.attemptId)!, outcome, d.reasonCode ?? null);
          this.emit(rec, 'execution.finished', { outcome, engineRunId: rec.engineRunId, terminalReason: d.reasonCode ?? null });
          const fresh = this.journal.attemptFind(rec.attemptId)!;
          if (outcome === 'SUCCEEDED') this.journal.attemptAdvance(fresh, 'QUIESCING');
          else this.journal.attemptAdvance(fresh, outcome === 'CANCELLED' ? 'CANCELLED' : 'FAILED');
        } else {
          this.markEngineDone(rec, outcome, d.reasonCode ?? null);
          this.emit(rec, 'task.failed', { phase: 'STARTING', reasonCode: d.reasonCode ?? 'NEVER_STARTED' });
          const fresh = this.journal.attemptFind(rec.attemptId)!;
          this.journal.attemptAdvance(fresh, outcome === 'CANCELLED' ? 'CANCELLED' : 'FAILED');
        }
        return;
      }
      await sleep(this.cfg.taskPollMillis);
    }
  }

  /** execute: poll detail for the terminal verdict; cancel flips to cancelPhase. */
  private async executePhase(rec: Row, driver: Driver): Promise<void> {
    let unknownSince = 0;
    while (this.running) {
      if (driver.cancelRequested || rec.cancelRequested) return;
      const deadline = this.journal.fact(rec.attemptId, 'executionDeadlineEpochMs');
      if (deadline != null && Date.now() > Date.parse(deadline)) throw new BudgetExceeded('EXECUTION_BUDGET_EXCEEDED');
      let d: Row;
      try { d = await this.local.taskDetail(rec.localTaskId); }
      catch { await sleep(this.cfg.taskPollMillis); continue; }
      if (d.state === 'UNKNOWN') {
        if (unknownSince === 0) unknownSince = Date.now();
        if (Date.now() - unknownSince > this.cfg.unknownBudgetMillis) {
          this.markEngineDone(rec, 'UNKNOWN', 'OUTCOME_UNKNOWN');
          this.emit(rec, 'task.failed', { phase: 'EXECUTING', reasonCode: 'OUTCOME_UNKNOWN' });
          this.journal.attemptAdvance(this.journal.attemptFind(rec.attemptId)!, 'FAILED');
          return;
        }
        await sleep(this.cfg.taskPollMillis); continue;
      }
      unknownSince = 0;
      if (LIVE_TERMINAL.has(d.state)) {
        const outcome = localOutcome(d);
        this.markEngineDone(rec, outcome, d.reasonCode ?? null);
        this.emit(rec, 'execution.finished', { outcome, engineRunId: rec.engineRunId ?? rec.localTaskId, terminalReason: d.reasonCode ?? null });
        const fresh = this.journal.attemptFind(rec.attemptId)!;
        if (outcome === 'SUCCEEDED') this.journal.attemptAdvance(fresh, 'QUIESCING');
        else this.journal.attemptAdvance(fresh, outcome === 'CANCELLED' ? 'CANCELLED' : 'FAILED');
        return;
      }
      await sleep(this.cfg.taskPollMillis);
    }
  }

  /**
   * cancel — the Java cancelPhase: proof-of-stop comes from provider stop
   * or terminal observation, never from the receipt alone. The persisted
   * cancelCommandId makes the cancellation idempotent across replays.
   */
  private async cancelPhase(rec: Row, outcome: 'CANCELLED' | 'FAILED', reason: string | null): Promise<void> {
    rec.cancelRequested = true;
    if (rec.cancelCommandId == null) {
      rec.cancelCommandId = newId('cancel');
      this.journal.factPutIfAbsent(rec.attemptId, 'cancelCommandId', rec.cancelCommandId);
    } else {
      this.journal.factPutIfAbsent(rec.attemptId, 'cancelCommandId', rec.cancelCommandId);
    }
    this.journal.attemptPut(rec);
    if (rec.localTaskId != null && this.journal.fact(rec.attemptId, 'quiescenceReceiptRef') == null) {
      this.emit(rec, 'lifecycle.transition', { to: 'STOPPING', reason: reason ?? 'cancel' });
      await this.issueLocalStop(rec);
      await this.proveQuiescence(rec);
    } else if (this.journal.fact(rec.attemptId, 'quiescenceReceiptRef') == null) {
      const ev = this.emit(rec, 'lifecycle.transition', { to: 'QUIESCED', receipt: 'engine-never-started' });
      this.journal.factPutIfAbsent(rec.attemptId, 'quiescenceReceiptRef', 'event:' + ev.eventId);
    }
    this.markEngineDone(rec, outcome, reason ?? null);
    this.emit(rec, 'task.cancelled', { cancelCommandId: rec.cancelCommandId, reason: reason ?? null });
    const fresh = this.journal.attemptFind(rec.attemptId)!;
    if (fresh.state !== 'FAILED' && fresh.state !== 'CANCELLED') this.journal.attemptAdvance(fresh, outcome === 'CANCELLED' ? 'CANCELLED' : 'FAILED');
  }

  /**
   * proveQuiescence — STOP until the local task reports `quiescent`
   * (intake closed + no live process + no open artifacts). Receipt alone
   * is never accepted as proof (Java isQuiescent contract).
   */
  private async proveQuiescence(rec: Row): Promise<void> {
    if (this.journal.fact(rec.attemptId, 'quiescenceReceiptRef') != null) return;
    const deadline = Date.now() + this.cfg.quiesceWaitMillis;
    while (Date.now() < deadline) {
      let d: Row | null = null;
      try { d = await this.local.taskDetail(rec.localTaskId); } catch { d = null; }
      if (d == null) break;
      if (d.quiescent === true) {
        const ev = this.emit(rec, 'lifecycle.transition', {
          to: 'QUIESCED',
          receipt: d.stopProof ? 'stop:' + d.stopProof.method + (d.stopProof.detail ? ':' + d.stopProof.detail : '') : 'terminal:' + d.state,
        });
        this.journal.factPutIfAbsent(rec.attemptId, 'quiescenceReceiptRef', 'event:' + ev.eventId);
        return;
      }
      await this.issueLocalStop(rec, d);
      await sleep(this.cfg.taskPollMillis);
    }
    this.emit(rec, 'lifecycle.transition', { to: 'QUIESCE_UNPROVEN', detail: 'stop not proven within ' + this.cfg.quiesceWaitMillis + 'ms' });
    this.journal.factPutIfAbsent(rec.attemptId, 'quiescenceReceiptRef', 'unproven');
  }

  private async issueLocalStop(rec: Row, d?: Row): Promise<void> {
    try {
      const detail = d ?? await this.local.taskDetail(rec.localTaskId);
      await this.local.stop(rec.localTaskId, {
        generation: detail.generation ?? null,
        nativeSessionId: detail.nativeSessionId ?? null,
        nativeTurnId: detail.nativeTurnId ?? null,
        targetCommandId: detail.currentCommandId ?? null,
      }, 'wire-stop-' + rec.attemptId + '-' + (detail.generation ?? '0'));
    } catch (e) {
      this.log('stop ' + rec.localTaskId + ': ' + scrub(String(e)));
    }
  }

  /**
   * collect — sealed owned-tree evidence + manifest. The host only seals
   * after quiescence; a `STOP_TO_SEAL_RESULTS` response is surfaced
   * honestly (no manifest) rather than fabricating a digest.
   */
  private async collectPhase(rec: Row): Promise<void> {
    if (this.journal.fact(rec.attemptId, 'collectionSealed') != null) return;
    let manifest: Row;
    try { manifest = await this.local.artifacts(rec.localTaskId); }
    catch (e) {
      this.journal.factPutIfAbsent(rec.attemptId, 'collectionSealed', 'unavailable:' + scrub(String(e)).slice(0, 120));
      this.emit(rec, 'change.collected', { coverage: 'NONE', detail: 'artifact listing failed', entryCount: 0 });
      return;
    }
    const items: Row[] = Array.isArray(manifest.items) ? manifest.items : [];
    if (manifest.reasonCode === 'STOP_TO_SEAL_RESULTS') {
      this.journal.factPutIfAbsent(rec.attemptId, 'collectionSealed', 'unsealed:STOP_TO_SEAL_RESULTS');
      this.emit(rec, 'change.collected', { coverage: 'UNSEALED', detail: 'STOP_TO_SEAL_RESULTS', entryCount: 0 });
      return;
    }
    const dir = join(this.cfg.stateDir, 'uploads', rec.attemptId);
    let withheld = 0, blobCount = 0, deleted = 0;
    for (const item of items) {
      if (item.status === 'DELETED') { deleted++; continue; }
      if (item.status !== 'AVAILABLE') { withheld++; continue; }
      const artifact = await this.local.artifact(rec.localTaskId, item.artifactId);
      const bytes = Buffer.from(String(artifact.bytes ?? ''), 'base64');
      const digestHex = createHash('sha256').update(bytes).digest('hex');
      if (bytes.length !== Number(item.size) || digestHex !== item.sha256) {
        // Never ship bytes that don't match the sealed manifest claim.
        withheld++;
        continue;
      }
      const localPath = join(dir, item.artifactId + '.bin');
      mkdirSync(dirname(localPath), { recursive: true });
      writeFileSync(localPath, bytes);
      this.journal.artifactRecord({
        artifactId: 'blob-' + rec.attemptId + '-' + digestHex.slice(0, 16),
        attemptId: rec.attemptId, kind: 'blob', localPath,
        digest: 'sha256:' + digestHex,
        sizeBytes: bytes.length, status: 'PENDING', receiptRef: null,
      });
      blobCount++;
    }
    const manifestDoc: Obj = {
      schemaVersion: 'pi-artifact-manifest/1', taskId: rec.taskId, attemptId: rec.attemptId,
      baseline: manifest.baseline ?? null, segmentId: manifest.segmentId ?? null,
      coverage: manifest.coverage ?? null, bounded: manifest.bounded ?? false,
      items: items.map(i => ({ artifactId: i.artifactId, path: i.path, status: i.status, sha256: i.sha256 ?? null, size: i.size ?? 0 })),
      withheld: items.filter(i => !['AVAILABLE', 'DELETED'].includes(i.status)).map(i => ({ artifactId: i.artifactId, path: i.path, status: i.status })),
      deleted: items.filter(i => i.status === 'DELETED').map(i => ({ artifactId: i.artifactId, path: i.path })),
    };
    const manifestBytes = canonicalBytes(manifestDoc);
    const manifestPath = join(dir, 'manifest.json');
    mkdirSync(dirname(manifestPath), { recursive: true });
    writeFileSync(manifestPath, manifestBytes);
    const manifestDigest = 'sha256:' + createHash('sha256').update(manifestBytes).digest('hex');
    this.journal.artifactRecord({
      artifactId: 'manifest-' + rec.attemptId, attemptId: rec.attemptId, kind: 'manifest',
      localPath: manifestPath, digest: manifestDigest, sizeBytes: manifestBytes.length,
      status: 'PENDING', receiptRef: null,
    });
    this.journal.factPutIfAbsent(rec.attemptId, 'collectionSealed', manifestDigest);
    this.emit(rec, 'change.collected', {
      manifestRef: 'artifact:manifest-' + rec.attemptId, digest: manifestDigest,
      coverage: manifest.coverage ?? 'CHANGED_OWNED_FILES',
      entryCount: items.length, blobCount, withheldCount: withheld, deletedCount: deleted,
    });
  }

  /**
   * finalizeDelivery — the Java order: quiescence fact → best-effort
   * collection → pending artifact uploads → durable declaration → submit.
   */
  private async finalizeDelivery(rec: Row, outcome: dto.EngineOutcome): Promise<void> {
    if (this.journal.fact(rec.attemptId, 'quiescenceReceiptRef') == null && rec.localTaskId != null) {
      await this.proveQuiescence(rec).catch(() => undefined);
    }
    if (this.journal.fact(rec.attemptId, 'collectionSealed') == null && rec.localTaskId != null) {
      await this.collectPhase(rec).catch(() => undefined);
    }
    await this.uploadPendingArtifactsFor(rec.attemptId);
    const declared = this.journal.finishFind(rec.attemptId) ?? this.declareFinish(rec, outcome);
    await this.submitFinish(rec, declared);
  }

  /** declareFinish — final event + immutable finish body in ONE durable tx. */
  private declareFinish(rec: Row, outcome: dto.EngineOutcome): Row {
    const fresh = this.journal.attemptFind(rec.attemptId)!;
    const artifacts = this.journal.artifactsForAttempt(rec.attemptId);
    const manifest = artifacts.find(a => a.kind === 'manifest');
    const finishId = this.journal.fact(rec.attemptId, 'finishId') ?? ('finish-' + rec.attemptId + '-' + randomBytes(6).toString('hex'));
    return this.journal.transaction(() => {
      const closed = this.emit(fresh, 'attempt.delivery.closed', {
        attemptId: rec.attemptId, outcome,
        finalDurableSequence: null, // filled below by seq assignment
        artifactCount: artifacts.length,
      });
      const finalSeq = String(closed.sequence);
      const req: dto.FinishRequest = {
        finishId, attemptId: rec.attemptId, attemptEpoch: rec.attemptEpoch,
        workerLeaseEpoch: rec.boundEpoch, workspaceGeneration: rec.workspaceGeneration,
        engineOutcome: outcome, engineRunId: rec.engineRunId,
        finalDurableSequence: finalSeq,
        manifestRef: manifest ? 'artifact:' + manifest.artifactId : null,
        manifestDigest: manifest ? manifest.digest : null,
        quiescenceReceiptRef: this.journal.fact(rec.attemptId, 'quiescenceReceiptRef'),
        usageCompleteness: 'UNKNOWN', unresolvedActionIds: [],
      };
      const json = canonicalDigest(dto.finishRequestToJson(req));
      this.journal.factPutIfAbsent(rec.attemptId, 'finishId', finishId);
      this.journal.finishRecordInTx(rec.attemptId, finishId, json, JSON.stringify(dto.finishRequestToJson(req)), rec.boundInstanceId);
      return this.journal.finishFind(rec.attemptId)!;
    });
  }

  /** submitFinish — identical body on every retry (R08 immutability). */
  private async submitFinish(rec: Row, declared: Row): Promise<void> {
    const req = dto.parseFinishRequest(JSON.parse(declared.canonicalJson));
    const session = await this.recoveryScopeFor(rec.attemptId, rec.boundInstanceId);
    await this.plane.postFinish(rec.attemptId, req, session ?? undefined);
    this.journal.finishMarkSubmitted(rec.attemptId);
    await this.pollFinishReceipt(rec, req, session);
  }

  /** pollFinishReceipt — pending obligations are repaired mid-poll. */
  private async pollFinishReceipt(rec: Row, req: dto.FinishRequest, session: dto.RecoverySession | null): Promise<void> {
    for (let attempt = 0; attempt < this.cfg.finishPollMaxAttempts; attempt++) {
      const receipt = await this.plane.getFinishReceipt(rec.attemptId, req.finishId);
      this.journal.finishMarkServiceState(rec.attemptId, receipt.state, receipt.receiptDigest);
      if (dto.finishTerminalAccepted(receipt) || receipt.state === 'REJECTED' || receipt.state === 'OUTCOME_UNKNOWN') return;
      await this.uploadPendingArtifactsFor(rec.attemptId);
      await this.plane.postFinish(rec.attemptId, req, session ?? await this.recoveryScopeFor(rec.attemptId, rec.boundInstanceId) ?? undefined);
      await sleep(Math.min(2_000 * (attempt + 1), 8_000));
    }
  }

  private async failPhase(rec: Row, err: unknown): Promise<void> {
    const fresh = this.journal.attemptFind(rec.attemptId)!;
    if (fresh.state !== 'FAILED' && fresh.state !== 'CANCELLED') this.journal.attemptAdvance(fresh, 'FAILED');
    this.markEngineDone(this.journal.attemptFind(rec.attemptId)!, 'FAILED', scrub(String(err)).slice(0, 240));
    this.emit(rec, 'task.failed', { phase: 'EXECUTING', detail: scrub(String(err)).slice(0, 512) });
  }
  private async failAndFinish(rec: Row, phase: string): Promise<void> {
    await this.finalizeDelivery(this.journal.attemptFind(rec.attemptId)!, 'FAILED');
  }

  private async postRunSafety(env: dto.CommandEnvelope | null): Promise<void> {
    if (!env) return;
    const rec = this.journal.attemptFindByRunCommand(env.payload?.runCommandId as string ?? '');
    const row = this.journal.inboxFind(env.commandId);
    if (!rec || !row) return;
    if ((rec.engineRunId != null || rec.state === 'FINISHED') && row.state !== 'STARTED') {
      const rev = this.journal.inboxAdvance(env.commandId, 'STARTED', null);
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'STARTED', payloadDigest: env.payloadDigest, attemptId: rec.attemptId, engineRunId: rec.engineRunId });
    } else if ((rec.state === 'FAILED' || rec.state === 'CANCELLED') && rec.engineRunId == null && row.state !== 'REJECTED') {
      const rev = this.journal.inboxAdvance(env.commandId, 'REJECTED', 'never started');
      await this.sendReceipt({ commandId: env.commandId, receiptRevision: String(rev), state: 'REJECTED', payloadDigest: env.payloadDigest, detailCode: 'NEVER_STARTED' });
    }
  }

  // ======================================================================
  // observer lane — reconcileHistoricalTask for OLD-instance attempts
  // ======================================================================
  private spawnObserver(rec: Row) {
    void this.reconcileStranded(rec.attemptId)
      .catch(e => this.log('observer ' + rec.attemptId + ': ' + scrub(String(e))));
  }

  /** reconcileHistoricalTask — observe the surviving local task, then declare honestly. Single-flight per attempt. */
  private async reconcileStranded(attemptId: string): Promise<boolean> {
    if (this.observersInFlight.has(attemptId)) return false;
    this.observersInFlight.add(attemptId);
    try { return await this.reconcileStrandedInner(attemptId); }
    finally { this.observersInFlight.delete(attemptId); }
  }
  private async reconcileStrandedInner(attemptId: string): Promise<boolean> {
    const rec = this.journal.attemptFind(attemptId);
    if (!rec || rec.state === 'FINISHED') return true;
    if (rec.localTaskId == null) {
      // Never reached the local host: there is nothing to observe — park
      // the finish honestly instead of fabricating one.
      this.journal.recoveryPark('rec-QUERY_UNKNOWN-' + attemptId, 'NEVER_STARTED: no localTaskId bound');
      return true;
    }
    let d: Row;
    try { d = await this.local.taskDetail(rec.localTaskId); }
    catch (e) {
      if (e instanceof LocalError && e.code === 'TASK_NOT_FOUND') {
        this.journal.factPutIfAbsent(attemptId, 'historicalRecoveryIssue', 'LOCAL_TASK_LOST');
        this.journal.recoveryPark('rec-QUERY_UNKNOWN-' + attemptId, 'TASK_DISAPPEARED');
        return true;
      }
      throw e;
    }
    if (!OBSERVED_TERMINAL.has(d.state)) {
      const deadline = this.journal.fact(attemptId, 'executionDeadlineEpochMs');
      if (deadline != null && Date.now() > Date.parse(deadline)) {
        await this.issueLocalStop(rec, d);
      }
      return false; // still running — retry later
    }
    const outcome = localOutcome(d);
    if (rec.engineRunId == null && d.nativeSessionId != null) {
      rec.engineRunId = rec.localTaskId;
      this.journal.attemptPut(rec);
    }
    // Settle once — markEngineDone is idempotent and so is our event.
    if (rec.engineOutcome == null) {
      this.markEngineDone(rec, outcome, d.reasonCode ?? null);
      this.emit(rec, 'execution.finished', { outcome, engineRunId: rec.engineRunId, historicalObservation: true, terminalReason: d.reasonCode ?? null });
    }
    const at = () => this.journal.attemptFind(attemptId)!;
    if (outcome === 'SUCCEEDED') {
      // Same ladder as the live driver, entered at whatever durable state
      // the dead instance left behind.
      let cur = at();
      if (cur.state === 'STARTING' || cur.state === 'AMBIGUOUS') { this.journal.attemptAdvance(cur, 'EXECUTING'); cur = at(); }
      if (cur.state === 'EXECUTING') { this.journal.attemptAdvance(cur, 'QUIESCING'); cur = at(); }
      if (cur.state === 'QUIESCING') { await this.proveQuiescence(cur); this.journal.attemptAdvance(at(), 'COLLECTING'); cur = at(); }
      if (cur.state === 'VERIFYING') { this.journal.attemptAdvance(cur, 'COLLECTING'); cur = at(); }
      if (cur.state === 'COLLECTING') { await this.collectPhase(cur); this.journal.attemptAdvance(at(), 'FINISHING'); cur = at(); }
      if (cur.state === 'FINISHING') {
        if (!this.journal.finishFind(attemptId)) this.emit(cur, 'task.finished', { outcome: 'SUCCEEDED', finishId: null });
        await this.finalizeDelivery(cur, 'SUCCEEDED');
        this.journal.attemptAdvance(at(), 'FINISHED');
      }
      if (at().state === 'FINISHED') return true;
      // ACCEPTED/PREPARING rows cannot be reconciled forward — the local
      // task exists, so the honest outcome is FAILED via the ladder below.
      cur = at();
      if (cur.state === 'ACCEPTED') this.journal.attemptAdvance(cur, 'FAILED');
      else if (cur.state === 'PREPARING') this.journal.attemptAdvance(cur, 'FAILED');
      if (at().state === 'FAILED') {
        const declared = this.journal.finishFind(attemptId);
        if (declared) await this.submitFinish(at(), declared); else await this.finalizeDelivery(at(), outcome);
      }
      return at().state === 'FINISHED' || this.journal.finishFind(attemptId)?.serviceState === 'DELIVERY_ACCEPTED';
    }
    // Terminal non-success: land the verdict, declare, done.
    const cur = at();
    if (!ATTEMPT_TERMINAL.has(cur.state)) {
      this.journal.attemptAdvance(cur, outcome === 'CANCELLED' ? 'CANCELLED' : 'FAILED');
    }
    const declared = this.journal.finishFind(attemptId);
    if (declared) await this.submitFinish(at(), declared);
    else await this.finalizeDelivery(at(), outcome === 'CANCELLED' ? 'CANCELLED' : outcome === 'UNKNOWN' ? 'UNKNOWN' : 'FAILED');
    return true;
  }

  // ======================================================================
  // evidence drivers — EventOutboxDriver / ArtifactDriver / FinishDriver
  // ======================================================================
  private async pumpLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.flushOutbox();
        await this.resendUnackedReceipts();
        await this.resendPendingArtifacts();
        await this.resendPendingFinishes();
        await this.runRecoveryTasksOnce();
        this.lastPumpError = null;
      } catch (e) {
        this.lastPumpError = scrub(String(e));
        this.log('pump: ' + this.lastPumpError);
      }
      await sleep(this.cfg.pumpMillis);
    }
  }

  /** EventOutboxDriver.sendPendingEvents — same-stream contiguous pages. */
  private async flushOutbox(): Promise<void> {
    if (!this.registration) return;
    const streams = new Set<string>([this.registration.streamId, ...this.journal.outboxPendingStreams()]);
    for (const streamId of streams) {
      const pending = this.journal.outboxPending(streamId, 50).map(r => dto.parseEvent(r.value as Obj));
      if (!pending.length) continue;
      let session: dto.RecoverySession | undefined;
      if (streamId !== this.registration.streamId) {
        session = [...this.recoverySessions.values()].find(s => s.streamIds.has(streamId) && s.expiresAtEpochMs > Date.now());
        if (!session) continue; // scoped only via a granted session
      }
      try {
        const resp = await this.plane.postEvents(this.registration.instanceId, pending, session);
        this.journal.outboxApplyAcks(resp.results);
      } catch (e) {
        this.log('events flush ' + streamId + ': ' + scrub(String(e)));
      }
    }
  }

  private emit(rec: Row, type: string, data: Obj): { eventId: string; sequence: string } {
    const env = dto.mintEvent({
      streamId: rec.boundStreamId, sequence: '0', type,
      organizationId: rec.organizationId, workerId: this.cfg.workerId,
      instanceId: rec.boundInstanceId, workerLeaseEpoch: rec.boundEpoch,
      taskId: rec.taskId, attemptId: rec.attemptId, attemptEpoch: rec.attemptEpoch,
      occurredAt: nowRfc3339(), data,
    });
    const obj = dto.eventToJson(env);
    delete obj.sequence;
    const stored = this.journal.transaction(() => this.journal.outboxAppendInTx(rec.boundStreamId, obj));
    return { eventId: env.eventId, sequence: String(stored.seq) };
  }

  // ---------------- artifacts ----------------
  private async uploadPendingArtifactsFor(attemptId: string): Promise<void> {
    for (const a of this.journal.artifactsForAttempt(attemptId)) {
      if (a.status === 'RECEIPTED') continue;
      try { await this.driveArtifactRow(a); } catch (e) { this.log('artifact ' + a.artifactId + ': ' + scrub(String(e))); }
    }
  }
  private async resendPendingArtifacts(): Promise<void> {
    for (const a of this.journal.artifactsPending()) {
      try { await this.driveArtifactRow(a); } catch (e) { this.log('artifact ' + a.artifactId + ': ' + scrub(String(e))); }
    }
  }
  /** ArtifactDriver.retryPending + ExecutionJournal.uploadArtifact — resume from the durable status; digest-verified stream, same-origin creds only. */
  private async driveArtifactRow(a: Row): Promise<void> {
    const rec = this.journal.attemptFind(a.attemptId);
    const session = await this.recoveryScopeFor(a.attemptId, rec?.boundInstanceId ?? null);
    if (!existsSync(a.localPath)) { this.journal.artifactAdvance(a.artifactId, 'FAILED', null); return; }
    if (a.status === 'PENDING' || a.status === 'FAILED' || a.status === 'INTENT_GRANTED') {
      const intent = await this.plane.requestUploadIntent(a.attemptId, {
        artifactId: a.artifactId, digest: a.digest, sizeBytes: a.sizeBytes, kind: a.kind,
      }, session ?? undefined);
      this.journal.artifactAdvance(a.artifactId, 'INTENT_GRANTED', null);
      if (intent.alreadyReceipted === true) {
        this.journal.artifactAdvance(a.artifactId, 'RECEIPTED', null);
        return;
      }
      await this.plane.putArtifact(intent.uploadUrl, a.localPath, a.digest, session ?? undefined);
      this.journal.artifactAdvance(a.artifactId, 'UPLOADED', null);
    }
    const receiptRef = await this.plane.completeArtifact(a.attemptId, {
      artifactId: a.artifactId, digest: a.digest, sizeBytes: a.sizeBytes,
    }, session ?? undefined);
    this.journal.artifactAdvance(a.artifactId, 'RECEIPTED', receiptRef);
    if (rec) {
      this.emit(rec, 'artifact.uploaded', { artifactId: a.artifactId, digest: a.digest, kind: a.kind, receiptRef });
    }
  }

  // ---------------- finishes ----------------
  private async resendPendingFinishes(): Promise<void> {
    for (const f of this.journal.finishesPending()) {
      try {
        const req = dto.parseFinishRequest(JSON.parse(f.canonicalJson));
        const rec = this.journal.attemptFind(f.attemptId);
        const session = await this.recoveryScopeFor(f.attemptId, rec?.boundInstanceId ?? f.boundInstanceId);
        await this.plane.postFinish(f.attemptId, req, session ?? undefined);
        this.journal.finishMarkSubmitted(f.attemptId);
        const receipt = await this.plane.getFinishReceipt(f.attemptId, req.finishId);
        this.journal.finishMarkServiceState(f.attemptId, receipt.state, receipt.receiptDigest);
      } catch (e) {
        this.log('finish resend ' + f.attemptId + ': ' + scrub(String(e)));
      }
    }
  }

  // ======================================================================
  // recovery lane — scoped sessions; creds never outside granted scope
  // ======================================================================
  private async performRecovery(): Promise<void> {
    const prior = this.priorInstanceId;
    if (!prior) return;
    let streamHint: string | null = null;
    try {
      await this.grantRecoverySession(prior);
      streamHint = this.journal.streamTargetFor(prior);
      this.log('recovery session for ' + prior + (streamHint ? ' stream=' + streamHint : ''));
    } catch (e) {
      this.log('recovery session denied for ' + prior + ': ' + scrub(String(e)));
    }
    this.seedRecoveryTasks(prior);
  }
  private spawnRecoveryForOldInstance() {
    const prior = this.priorInstanceId;
    if (prior) this.seedRecoveryTasks(prior);
  }

  private seedRecoveryTasks(oldInstanceId: string): void {
    for (const streamId of this.journal.outboxPendingStreams()) {
      if (streamId !== this.registration?.streamId) this.journal.recoverySeed('STREAM_EVENTS', streamId, oldInstanceId);
    }
    for (const a of this.journal.artifactsPending()) this.journal.recoverySeed('ARTIFACT', a.artifactId, oldInstanceId);
    for (const f of this.journal.finishesPending()) {
      if (f.serviceState !== 'DELIVERY_ACCEPTED' && f.serviceState !== 'AUDIT_CONFIRMED') {
        this.journal.recoverySeed('FINISH', f.attemptId, oldInstanceId);
      }
    }
    for (const rec of this.journal.attemptsUnfinished()) {
      if (rec.boundInstanceId === oldInstanceId && !this.journal.finishFind(rec.attemptId)) {
        this.journal.recoverySeed('QUERY_UNKNOWN', rec.attemptId, oldInstanceId);
      }
    }
  }

  private async grantRecoverySession(oldInstanceId: string): Promise<dto.RecoverySession> {
    const session = await this.plane.postRecovery(this.cfg.workerId, oldInstanceId, this.journal.streamTargetFor(oldInstanceId));
    this.recoverySessions.set(oldInstanceId, session);
    return session;
  }
  private async sessionFor(oldInstanceId: string): Promise<dto.RecoverySession> {
    const cached = this.recoverySessions.get(oldInstanceId);
    if (cached && cached.expiresAtEpochMs - 15_000 > Date.now()) return cached;
    return this.grantRecoverySession(oldInstanceId);
  }
  /** recoveryScopeFor — a granted session covering this attempt, or null for own-instance attempts. */
  private async recoveryScopeFor(attemptId: string, boundInstanceId: string | null): Promise<dto.RecoverySession | null> {
    const scoped = [...this.recoverySessions.values()].find(
      s => s.attemptIds.has(attemptId) && s.expiresAtEpochMs - 15_000 > Date.now());
    if (scoped) return scoped;
    const bound = boundInstanceId ?? this.journal.finishBoundInstanceFor(attemptId);
    if (bound && bound !== this.registration?.instanceId) {
      try { return await this.sessionFor(bound); } catch { return null; }
    }
    return null;
  }

  private async runRecoveryTasksOnce(): Promise<void> {
    for (const task of this.journal.recoveryDue(Date.now())) {
      try {
        const done = await this.driveRecoveryTask(task);
        if (done) this.journal.recoveryDone(task.taskId);
      } catch (e) {
        const err = scrub(String(e));
        const attempts = (task.attempts ?? 0) + 1;
        if (attempts >= this.cfg.recoveryMaxAttempts || (e instanceof ControlPlaneError && !e.retryable && e.code !== 'TRANSPORT')) {
          this.journal.recoveryPark(task.taskId, err);
        } else {
          this.journal.recoveryRetry(task.taskId, err, Date.now() + Math.min(60_000, 2_000 * attempts));
        }
      }
    }
  }

  private async driveRecoveryTask(task: Row): Promise<boolean> {
    const session = await this.sessionFor(task.oldInstanceId);
    switch (task.kind) {
      case 'STREAM_EVENTS': {
        if (task.targetId !== session.streamIds.values().next().value && !session.streamIds.has(task.targetId)) {
          throw new Error('STREAM_MISMATCH:' + task.targetId);
        }
        const pending = this.journal.outboxPending(task.targetId, 50).map(r => dto.parseEvent(r.value as Obj));
        if (!pending.length) return true;
        const resp = await this.plane.postEvents(this.registration!.instanceId, pending, session);
        this.journal.outboxApplyAcks(resp.results);
        return this.journal.outboxPending(task.targetId, 1).length === 0;
      }
      case 'ARTIFACT': {
        const a = this.journal.artifactFind(task.targetId);
        if (!a) return true;
        if (!session.attemptIds.has(a.attemptId)) throw new Error('SCOPE_DENIED:' + a.attemptId);
        await this.driveArtifactRow(a);
        return this.journal.artifactFind(task.targetId)?.status === 'RECEIPTED';
      }
      case 'FINISH': {
        const f = this.journal.finishFind(task.targetId);
        if (!f) return true;
        if (dto.finishTerminalAccepted({ ...f, state: f.serviceState } as any)) return true;
        const req = dto.parseFinishRequest(JSON.parse(f.canonicalJson));
        await this.plane.postFinish(f.attemptId, req, session);
        this.journal.finishMarkSubmitted(f.attemptId);
        const receipt = await this.plane.getFinishReceipt(f.attemptId, f.finishId);
        this.journal.finishMarkServiceState(f.attemptId, receipt.state, receipt.receiptDigest);
        return receipt.state !== 'PENDING';
      }
      case 'QUERY_UNKNOWN': {
        const f = this.journal.finishFind(task.targetId);
        if (f) {
          const receipt = await this.plane.getFinishReceipt(f.attemptId, f.finishId);
          this.journal.finishMarkServiceState(f.attemptId, receipt.state, receipt.receiptDigest);
          return receipt.state !== 'PENDING';
        }
        const rec = this.journal.attemptFind(task.targetId);
        if (!rec || rec.state === 'FINISHED') return true;
        return this.reconcileStranded(task.targetId);
      }
      default:
        return true;
    }
  }

  private markStarted(rec: Row, engineRunId: string) {
    if (rec.engineRunId == null) {
      rec.engineRunId = engineRunId; rec.startedAt = nowRfc3339();
      this.journal.attemptPut(rec);
    }
  }
  private markEngineDone(rec: Row, outcome: dto.EngineOutcome, detail: string | null) {
    rec.engineOutcome = outcome; rec.engineDetail = detail;
    rec.finishedAt = nowRfc3339(); rec.updatedAt = nowRfc3339();
    this.journal.attemptPut(rec);
  }
  private async fireStarted(rec: Row): Promise<void> {
    const rev = this.journal.inboxAdvance(rec.commandId, 'STARTED', null);
    await this.sendReceipt({
      commandId: rec.commandId, receiptRevision: String(rev), state: 'STARTED',
      payloadDigest: this.journal.inboxFind(rec.commandId)!.payloadDigest,
      attemptId: rec.attemptId, engineRunId: rec.engineRunId,
    });
  }
}

function localOutcome(d: Row): dto.EngineOutcome {
  switch (d.state) {
    case 'SUCCEEDED': return 'SUCCEEDED';
    case 'CANCELLED': return 'CANCELLED';
    case 'UNKNOWN': return 'UNKNOWN';
    default: return 'FAILED';
  }
}
function attemptIdForEnvelope(env: dto.CommandEnvelope, journal: WireJournal): Row | null {
  const p = env.payload ?? {};
  const attemptId = typeof p.attemptId === 'string' ? p.attemptId : null;
  if (attemptId) return journal.attemptFind(attemptId);
  if (env.type === 'startTask' && typeof p.runCommandId === 'string') return journal.attemptFindByRunCommand(p.runCommandId);
  return null;
}
