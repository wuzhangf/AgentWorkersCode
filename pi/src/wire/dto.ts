import { canonicalBytes, canonicalDigest, EVENT_BATCH_MAX_EVENTS, EVENT_SCHEMA_VERSION, isSupportedProtocol, newEventId, PROTOCOL_NAME } from './protocol.js';
import * as Fields from './fields.js';
import type { Obj } from './fields.js';

/**
 * W6: DTO mirrors of agent-worker-protocol. Only the attempt lane is
 * implemented (design 03 §6.2–§6.8): registration, heartbeat, command
 * poll/receipt, event batch, artifacts, finish + receipt, recovery.
 * Field names and optionality match the Java DTOs byte-for-byte.
 */

// ------------------------------------------------------------------
// enums
// ------------------------------------------------------------------
export const COMMAND_STATES = ['DURABLY_ACCEPTED', 'STARTED', 'REJECTED', 'EXPIRED', 'CANCELLED'] as const;
export type CommandState = typeof COMMAND_STATES[number];

/** Attempt-lane command types; stage types are parsed but never dispatched. */
export const COMMAND_TYPES = ['startTask', 'cancelTask', 'approvalDecision', 'drain',
  'devopsStageTask', 'devopsStageInput', 'devopsStageControl', 'visualStageTask'] as const;
export type CommandType = typeof COMMAND_TYPES[number];

export const ENGINE_OUTCOMES = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'UNKNOWN'] as const;
export type EngineOutcome = typeof ENGINE_OUTCOMES[number];

export const FINISH_STATES = ['PENDING', 'FINALIZING', 'DELIVERY_ACCEPTED', 'AUDIT_CONFIRMED', 'OUTCOME_UNKNOWN', 'REJECTED'] as const;
export type FinishState = typeof FINISH_STATES[number];

export type DeliveryClass = 'DURABLE' | 'EPHEMERAL';
/** DeliveryClass.forEventType — progress/engine.output deltas ride EPHEMERAL. */
export const deliveryClassFor = (type: string): DeliveryClass =>
  type.startsWith('engine.output') || type.startsWith('progress.') ? 'EPHEMERAL' : 'DURABLE';

const put = (o: Obj, k: string, v: unknown) => { if (v !== null && v !== undefined) o[k] = v; };

// ------------------------------------------------------------------
// WorkerCapabilities (design 03 §6.3)
// ------------------------------------------------------------------
export interface WorkerCapabilities {
  providers: string[];
  toolchains: string[];
  maxConcurrentTasks: number;
  cancel: boolean;
  approval: boolean;
  changeCapture: 'FINAL_TREE' | 'FULL_TRACE' | 'NONE';
  executionProfiles: string[];
  commercialAdmissionV1?: boolean;
  projectSourceV1?: boolean;
  routingContractVersion?: number;
  sourceObjectFormats?: string[];
  sourceBundleVersions?: string[];
  maxSourceBundleBytes?: number;
  runtimeWorkspaceEvidence?: boolean;
  directGitV1?: boolean;
  sourceFetchModes?: string[];
  stage?: Obj;
  projectEnvironmentPrepareV1?: boolean;
  environmentBoundExecutionV1?: boolean;
  adaptiveActionV1?: boolean;
}
export function capabilitiesToJson(c: WorkerCapabilities): Obj {
  const o: Obj = {};
  o.providers = c.providers; o.toolchains = c.toolchains;
  o.maxConcurrentTasks = c.maxConcurrentTasks;
  o.cancel = c.cancel; o.approval = c.approval;
  o.changeCapture = c.changeCapture;
  o.executionProfiles = c.executionProfiles;
  o.commercialAdmissionV1 = c.commercialAdmissionV1 === true;
  o.projectSourceV1 = c.projectSourceV1 === true;
  if (c.routingContractVersion != null && c.routingContractVersion > 0) o.routingContractVersion = c.routingContractVersion;
  if (c.sourceObjectFormats?.length) o.sourceObjectFormats = c.sourceObjectFormats;
  if (c.sourceBundleVersions?.length) o.sourceBundleVersions = c.sourceBundleVersions;
  if (c.maxSourceBundleBytes != null) o.maxSourceBundleBytes = c.maxSourceBundleBytes;
  if (c.runtimeWorkspaceEvidence) o.runtimeWorkspaceEvidence = true;
  if (c.directGitV1) o.directGitV1 = true;
  if (c.sourceFetchModes?.length) o.sourceFetchModes = c.sourceFetchModes;
  if (c.stage) o.stage = c.stage;
  if (c.projectEnvironmentPrepareV1) o.projectEnvironmentPrepareV1 = true;
  if (c.environmentBoundExecutionV1) o.environmentBoundExecutionV1 = true;
  if (c.adaptiveActionV1) o.adaptiveActionV1 = true;
  return o;
}
export function parseCapabilities(o: Obj): WorkerCapabilities {
  return {
    providers: Fields.stringList(Fields.reqArr(o, 'providers'), 'providers'),
    toolchains: Fields.stringList(Fields.optArr(o, 'toolchains') ?? [], 'toolchains'),
    maxConcurrentTasks: Fields.reqLong(o, 'maxConcurrentTasks'),
    cancel: Fields.reqBool(o, 'cancel'),
    approval: Fields.reqBool(o, 'approval'),
    changeCapture: (Fields.optEnum(o, 'changeCapture', ['FINAL_TREE', 'FULL_TRACE', 'NONE']) ?? 'NONE') as WorkerCapabilities['changeCapture'],
    executionProfiles: Fields.stringList(Fields.optArr(o, 'executionProfiles') ?? [], 'executionProfiles'),
    commercialAdmissionV1: Fields.optBool(o, 'commercialAdmissionV1') === true,
    projectSourceV1: Fields.optBool(o, 'projectSourceV1') === true,
    routingContractVersion: Fields.optLong(o, 'routingContractVersion') ?? 0,
    sourceObjectFormats: Fields.optArr(o, 'sourceObjectFormats') ? Fields.stringList(Fields.optArr(o, 'sourceObjectFormats')!, 'sourceObjectFormats') : undefined,
    sourceBundleVersions: Fields.optArr(o, 'sourceBundleVersions') ? Fields.stringList(Fields.optArr(o, 'sourceBundleVersions')!, 'sourceBundleVersions') : undefined,
    maxSourceBundleBytes: Fields.optLong(o, 'maxSourceBundleBytes') ?? undefined,
    runtimeWorkspaceEvidence: Fields.optBool(o, 'runtimeWorkspaceEvidence') === true,
    directGitV1: Fields.optBool(o, 'directGitV1') === true,
    sourceFetchModes: Fields.optArr(o, 'sourceFetchModes') ? Fields.stringList(Fields.optArr(o, 'sourceFetchModes')!, 'sourceFetchModes') : undefined,
    stage: Fields.optObj(o, 'stage') ?? undefined,
    projectEnvironmentPrepareV1: Fields.optBool(o, 'projectEnvironmentPrepareV1') === true,
    environmentBoundExecutionV1: Fields.optBool(o, 'environmentBoundExecutionV1') === true,
    adaptiveActionV1: Fields.optBool(o, 'adaptiveActionV1') === true,
  };
}

// ------------------------------------------------------------------
// Registration (03 §6.3)
// ------------------------------------------------------------------
export interface RegisterRequest {
  protocol: string; workerId: string; bootId: string; deploymentId: string;
  reportedImageDigest?: string | null;
  capabilities: WorkerCapabilities;
  recoverySummary?: { pendingEventCount: number; pendingArtifactCount: number } | null;
}
export function registerRequestToJson(r: RegisterRequest): Obj {
  const o: Obj = {};
  o.protocol = r.protocol; o.workerId = r.workerId; o.bootId = r.bootId; o.deploymentId = r.deploymentId;
  put(o, 'reportedImageDigest', r.reportedImageDigest);
  o.capabilities = capabilitiesToJson(r.capabilities);
  if (r.recoverySummary) o.recoverySummary = { pendingEventCount: r.recoverySummary.pendingEventCount, pendingArtifactCount: r.recoverySummary.pendingArtifactCount };
  return o;
}
export interface RegisterResponse {
  instanceId: string; streamId: string; workerLeaseEpoch: string;
  leaseTtlMillis: number; leaseExpiresAt: string; serverTime: string;
  protocolAccepted: string; desiredState: string;
  effectiveCapabilities: WorkerCapabilities | null;
}
export function parseRegisterResponse(o: Obj): RegisterResponse {
  return {
    instanceId: Fields.reqId(o, 'instanceId'),
    streamId: Fields.reqId(o, 'streamId'),
    workerLeaseEpoch: Fields.reqDecimalString(o, 'workerLeaseEpoch'),
    leaseTtlMillis: Fields.reqLong(o, 'leaseTtlMillis'),
    leaseExpiresAt: Fields.reqTime(o, 'leaseExpiresAt'),
    serverTime: Fields.reqTime(o, 'serverTime'),
    protocolAccepted: Fields.reqString(o, 'protocolAccepted'),
    desiredState: Fields.reqString(o, 'desiredState'),
    effectiveCapabilities: Fields.optObj(o, 'effectiveCapabilities') ? parseCapabilities(Fields.optObj(o, 'effectiveCapabilities')!) : null,
  };
}

// ------------------------------------------------------------------
// Heartbeat (03 §6.2)
// ------------------------------------------------------------------
export interface HeartbeatRequest {
  heartbeatId: string; workerLeaseEpoch: string; state: string;
  currentAttemptId?: string | null; pendingEventCount?: number | null; pendingArtifactCount?: number | null;
}
export function heartbeatRequestToJson(r: HeartbeatRequest): Obj {
  const o: Obj = {};
  o.heartbeatId = r.heartbeatId; o.workerLeaseEpoch = r.workerLeaseEpoch; o.state = r.state;
  put(o, 'currentAttemptId', r.currentAttemptId);
  if (r.pendingEventCount != null) o.pendingEventCount = r.pendingEventCount;
  if (r.pendingArtifactCount != null) o.pendingArtifactCount = r.pendingArtifactCount;
  return o;
}
export interface HeartbeatResponse {
  workerLeaseEpoch: string; leaseExpiresAt: string; serverTime: string;
  desiredState: string; renewAfterMillis: string | null; commercial: Obj | null;
}
export function parseHeartbeatResponse(o: Obj): HeartbeatResponse {
  return {
    workerLeaseEpoch: Fields.reqDecimalString(o, 'workerLeaseEpoch'),
    leaseExpiresAt: Fields.reqTime(o, 'leaseExpiresAt'),
    serverTime: Fields.reqTime(o, 'serverTime'),
    desiredState: Fields.reqString(o, 'desiredState'),
    renewAfterMillis: Fields.optDecimalString(o, 'renewAfterMillis'),
    commercial: Fields.optObj(o, 'commercial'),
  };
}

// ------------------------------------------------------------------
// CommandEnvelope + typed payloads (03 §6.4–§6.8)
// ------------------------------------------------------------------
export interface CommandEnvelope {
  protocol: string; commandId: string; commandSequence: string; type: CommandType;
  organizationId: string; projectId: string | null; workerId: string; instanceId: string;
  workerLeaseEpoch: string; issuedAt: string; executeBefore: string | null;
  payloadDigest: string; payload: Obj;
}
export function commandEnvelopeToJson(env: CommandEnvelope): Obj {
  const o: Obj = {};
  o.protocol = env.protocol; o.commandId = env.commandId; o.commandSequence = env.commandSequence;
  o.type = env.type; o.organizationId = env.organizationId;
  put(o, 'projectId', env.projectId);
  o.workerId = env.workerId; o.instanceId = env.instanceId; o.workerLeaseEpoch = env.workerLeaseEpoch;
  o.issuedAt = env.issuedAt; put(o, 'executeBefore', env.executeBefore);
  o.payloadDigest = env.payloadDigest; o.payload = env.payload;
  return o;
}
export function commandEnvelopeBytes(env: CommandEnvelope): Buffer { return canonicalBytes(commandEnvelopeToJson(env)); }
/**
 * commandDigest(): the envelope WITHOUT payloadDigest — R10 binds the
 * whole envelope so a redelivery carrying a swapped epoch/scope under the
 * same payload is a conflict, never a duplicate.
 */
export function commandDigest(env: CommandEnvelope): string {
  const copy = commandEnvelopeToJson(env);
  delete copy.payloadDigest;
  return canonicalDigest(copy);
}
export function parseCommandEnvelope(o: Obj): CommandEnvelope {
  const protocol = Fields.reqString(o, 'protocol');
  if (!isSupportedProtocol(protocol)) throw new Error('BAD_FIELD:protocol');
  const env: CommandEnvelope = {
    protocol,
    commandId: Fields.reqId(o, 'commandId'),
    commandSequence: Fields.reqDecimalString(o, 'commandSequence'),
    type: Fields.reqEnum(o, 'type', COMMAND_TYPES) as CommandType,
    organizationId: Fields.reqId(o, 'organizationId'),
    projectId: Fields.optId(o, 'projectId'),
    workerId: Fields.reqId(o, 'workerId'),
    instanceId: Fields.reqId(o, 'instanceId'),
    workerLeaseEpoch: Fields.reqDecimalString(o, 'workerLeaseEpoch'),
    issuedAt: Fields.reqTime(o, 'issuedAt'),
    executeBefore: Fields.optTime(o, 'executeBefore'),
    payloadDigest: Fields.reqDigest(o, 'payloadDigest'),
    payload: Fields.reqObj(o, 'payload'),
  };
  if (env.payloadDigest !== canonicalDigest(env.payload)) throw new Error('BAD_FIELD:payloadDigest');
  return env;
}

/** startTask payload (03 §6.4). */
export interface StartTask {
  taskId: string; attemptId: string; attemptEpoch: string; runCommandId: string;
  workspaceId: string; workspaceGeneration: string; goal: string;
  contextManifestRef: string | null;
  repository: { repositoryId: string; objectFormat: 'sha1' | 'sha256'; baseCommit: string } | null;
  scope: { writePaths: string[] } | null;
  acceptanceBindingRef: string | null; providerProfileRef: string | null;
  policySnapshotRef: string | null; executionProfile: string | null;
  deliveryRequirementRef: string | null; maxDurationMillis: number | null;
  projectId: string | null; repositoryId: string | null; workId: string | null;
  sourcePlanRef: string | null; sourcePlan: Obj | null;
}
export function parseStartTask(o: Obj): StartTask {
  const repo = Fields.optObj(o, 'repository');
  let repository: StartTask['repository'] = null;
  if (repo) {
    const format = Fields.reqEnum(repo, 'objectFormat', ['sha1', 'sha256']) as 'sha1' | 'sha256';
    const oid = Fields.reqString(repo, 'baseCommit', 64);
    if (!new RegExp('^[0-9a-f]{' + (format === 'sha1' ? 40 : 64) + '}$').test(oid)) throw new Error('BAD_FIELD:baseCommit');
    repository = { repositoryId: Fields.reqId(repo, 'repositoryId'), objectFormat: format, baseCommit: oid };
  }
  const scopeObj = Fields.optObj(o, 'scope');
  let scope: StartTask['scope'] = null;
  if (scopeObj) {
    const writePaths = Fields.stringList(Fields.reqArr(scopeObj, 'writePaths'), 'writePaths');
    for (const p of writePaths) {
      if (!p || p.length > 1024 || p.startsWith('/') || p.includes('..') || p.includes('\\') || p.includes(':')) throw new Error('BAD_FIELD:writePaths');
    }
    scope = { writePaths };
  }
  return {
    taskId: Fields.reqId(o, 'taskId'),
    attemptId: Fields.reqId(o, 'attemptId'),
    attemptEpoch: Fields.reqDecimalString(o, 'attemptEpoch'),
    runCommandId: Fields.reqId(o, 'runCommandId'),
    workspaceId: Fields.reqId(o, 'workspaceId'),
    workspaceGeneration: Fields.reqDecimalString(o, 'workspaceGeneration'),
    goal: Fields.reqString(o, 'goal', 8192),
    contextManifestRef: Fields.optString(o, 'contextManifestRef'),
    repository, scope,
    acceptanceBindingRef: Fields.optString(o, 'acceptanceBindingRef'),
    providerProfileRef: Fields.optString(o, 'providerProfileRef'),
    policySnapshotRef: Fields.optString(o, 'policySnapshotRef'),
    executionProfile: Fields.optString(o, 'executionProfile'),
    deliveryRequirementRef: Fields.optString(o, 'deliveryRequirementRef'),
    maxDurationMillis: Fields.optLong(o, 'maxDurationMillis'),
    projectId: Fields.optId(o, 'projectId'),
    repositoryId: Fields.optId(o, 'repositoryId'),
    workId: Fields.optId(o, 'workId'),
    sourcePlanRef: Fields.optString(o, 'sourcePlanRef'),
    sourcePlan: Fields.optObj(o, 'sourcePlan'),
  };
}

/** cancelTask payload (03 §6.8). */
export interface CancelTask {
  cancelCommandId: string; attemptId: string; attemptEpoch: string;
  workerLeaseEpoch: string; reason: string | null; requestedAt: string | null;
}
export function parseCancelTask(o: Obj): CancelTask {
  return {
    cancelCommandId: Fields.reqId(o, 'cancelCommandId'),
    attemptId: Fields.reqId(o, 'attemptId'),
    attemptEpoch: Fields.reqDecimalString(o, 'attemptEpoch'),
    workerLeaseEpoch: Fields.reqDecimalString(o, 'workerLeaseEpoch'),
    reason: Fields.optString(o, 'reason'),
    requestedAt: Fields.optTime(o, 'requestedAt'),
  };
}

/** approvalDecision payload (03 §6.7). */
export interface ApprovalDecision {
  approvalRequestId: string; decisionId: string; decision: 'APPROVE' | 'DENY' | 'EXPIRED';
  proposalDigest: string; grantRef: string | null;
}
export function parseApprovalDecision(o: Obj): ApprovalDecision {
  return {
    approvalRequestId: Fields.reqId(o, 'approvalRequestId'),
    decisionId: Fields.reqId(o, 'decisionId'),
    decision: Fields.reqEnum(o, 'decision', ['APPROVE', 'DENY', 'EXPIRED']) as ApprovalDecision['decision'],
    proposalDigest: Fields.reqDigest(o, 'proposalDigest'),
    grantRef: Fields.optString(o, 'grantRef'),
  };
}

/** drain payload (03 §6.8). */
export interface Drain { reason: string | null; deadlineMillis: number | null }
export function parseDrain(o: Obj): Drain {
  return { reason: Fields.optString(o, 'reason'), deadlineMillis: Fields.optLong(o, 'deadlineMillis') };
}

// ------------------------------------------------------------------
// Command poll + receipts (03 §6.2/§6.4)
// ------------------------------------------------------------------
export interface PollResponse { commands: CommandEnvelope[]; nextAfter: string | null }
export function parsePollResponse(o: Obj): PollResponse {
  const arr = Fields.reqArr(o, 'commands');
  const commands: CommandEnvelope[] = [];
  for (const v of arr) {
    if (!Fields.isObj(v)) throw new Error('BAD_FIELD:commands');
    commands.push(parseCommandEnvelope(v));
  }
  return { commands, nextAfter: Fields.optDecimalString(o, 'nextAfter') };
}
export interface ReceiptRequest {
  commandId: string; receiptRevision: string; state: CommandState; payloadDigest: string;
  attemptId?: string | null; engineRunId?: string | null; detailCode?: string | null;
}
export function receiptRequestToJson(r: ReceiptRequest): Obj {
  const o: Obj = {};
  o.commandId = r.commandId; o.receiptRevision = r.receiptRevision; o.state = r.state; o.payloadDigest = r.payloadDigest;
  put(o, 'attemptId', r.attemptId); put(o, 'engineRunId', r.engineRunId); put(o, 'detailCode', r.detailCode);
  return o;
}

// ------------------------------------------------------------------
// EventEnvelope + EventBatch (03 §6.5)
// ------------------------------------------------------------------
export interface EventEnvelopeData {
  streamId: string; sequence: string; type: string;
  organizationId: string; workerId: string; instanceId: string; workerLeaseEpoch: string;
  taskId?: string | null; attemptId?: string | null; attemptEpoch?: string | null;
  occurredAt: string; data: Obj; deliveryClass?: DeliveryClass;
}
export interface EventEnvelope extends EventEnvelopeData {
  eventId: string; schemaVersion: string; deliveryClass: DeliveryClass;
}
/** Mint a schema-1.0 attempt-lane envelope; the outbox assigns `sequence`. */
export function mintEvent(e: EventEnvelopeData): EventEnvelope {
  return {
    eventId: newEventId(), schemaVersion: EVENT_SCHEMA_VERSION,
    streamId: e.streamId, sequence: e.sequence, type: e.type,
    organizationId: e.organizationId, workerId: e.workerId,
    instanceId: e.instanceId, workerLeaseEpoch: e.workerLeaseEpoch,
    taskId: e.taskId ?? null, attemptId: e.attemptId ?? null, attemptEpoch: e.attemptEpoch ?? null,
    occurredAt: e.occurredAt, data: e.data,
    deliveryClass: e.deliveryClass ?? deliveryClassFor(e.type),
  };
}
export function eventToJson(e: EventEnvelope): Obj {
  const o: Obj = {};
  o.eventId = e.eventId; o.streamId = e.streamId; o.sequence = e.sequence;
  o.type = e.type; o.schemaVersion = e.schemaVersion;
  o.organizationId = e.organizationId; o.workerId = e.workerId; o.instanceId = e.instanceId;
  o.workerLeaseEpoch = e.workerLeaseEpoch;
  put(o, 'taskId', e.taskId); put(o, 'attemptId', e.attemptId); put(o, 'attemptEpoch', e.attemptEpoch);
  o.occurredAt = e.occurredAt; o.deliveryClass = e.deliveryClass; o.data = e.data;
  return o;
}
export function eventBytes(e: EventEnvelope): Buffer { return canonicalBytes(eventToJson(e)); }
export function eventDigest(e: EventEnvelope): string { return canonicalDigest(eventToJson(e)); }
export function parseEvent(o: Obj): EventEnvelope {
  const schemaVersion = Fields.reqString(o, 'schemaVersion');
  if (schemaVersion !== EVENT_SCHEMA_VERSION) throw new Error('BAD_FIELD:schemaVersion');
  const eventId = Fields.reqString(o, 'eventId', 64);
  if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(eventId)) throw new Error('BAD_FIELD:eventId');
  return {
    schemaVersion, eventId,
    streamId: Fields.reqId(o, 'streamId'),
    sequence: Fields.reqDecimalString(o, 'sequence'),
    type: Fields.reqString(o, 'type', 128),
    organizationId: Fields.reqId(o, 'organizationId'),
    workerId: Fields.reqId(o, 'workerId'),
    instanceId: Fields.reqId(o, 'instanceId'),
    workerLeaseEpoch: Fields.reqDecimalString(o, 'workerLeaseEpoch'),
    taskId: Fields.optId(o, 'taskId'),
    attemptId: Fields.optId(o, 'attemptId'),
    attemptEpoch: Fields.optDecimalString(o, 'attemptEpoch'),
    occurredAt: Fields.reqTime(o, 'occurredAt'),
    data: Fields.optObj(o, 'data') ?? {},
    deliveryClass: (Fields.optEnum(o, 'deliveryClass', ['DURABLE', 'EPHEMERAL']) ?? deliveryClassFor(Fields.reqString(o, 'type', 128))) as DeliveryClass,
  };
}

export type AckStatus = 'ACCEPTED' | 'DUPLICATE' | 'REJECTED' | 'RETRYABLE';
export interface EventAck { eventId: string; status: AckStatus; code: string | null; receiptRef: string | null; receiptDigest: string | null }
export interface EventBatchResponse { results: EventAck[]; contiguousAckSequence: string | null; streamAcks: { streamId: string; contiguousAckSequence: string }[] | null }
export function eventBatchRequestToJson(events: EventEnvelope[]): Obj {
  if (events.length > EVENT_BATCH_MAX_EVENTS) throw new Error('EVENT_BATCH_TOO_LARGE');
  return { events: events.map(eventToJson) };
}
export function parseEventBatchResponse(o: Obj): EventBatchResponse {
  const arr = Fields.reqArr(o, 'results');
  const results: EventAck[] = [];
  for (const v of arr) {
    if (!Fields.isObj(v)) throw new Error('BAD_FIELD:results');
    results.push({
      eventId: Fields.reqString(v, 'eventId'),
      status: Fields.reqEnum(v, 'status', ['ACCEPTED', 'DUPLICATE', 'REJECTED', 'RETRYABLE']) as AckStatus,
      code: Fields.optString(v, 'code'),
      receiptRef: Fields.optString(v, 'receiptRef'),
      receiptDigest: Fields.optDigest(v, 'receiptDigest'),
    });
  }
  const streamAcks = Fields.optArr(o, 'streamAcks')?.map(v => {
    if (!Fields.isObj(v)) throw new Error('BAD_FIELD:streamAcks');
    return { streamId: Fields.reqId(v, 'streamId'), contiguousAckSequence: Fields.reqDecimalString(v, 'contiguousAckSequence') };
  }) ?? null;
  return { results, contiguousAckSequence: Fields.optDecimalString(o, 'contiguousAckSequence'), streamAcks };
}

// ------------------------------------------------------------------
// Finish / artifacts / recovery (03 §6.8, §8.3)
// ------------------------------------------------------------------
export interface FinishRequest {
  finishId: string; attemptId: string; attemptEpoch: string; workerLeaseEpoch: string;
  workspaceGeneration: string; engineOutcome: EngineOutcome; engineRunId: string | null;
  finalDurableSequence: string; manifestRef: string | null; manifestDigest: string | null;
  quiescenceReceiptRef: string | null; usageCompleteness: 'UNKNOWN' | 'COMPLETE' | 'INCOMPLETE';
  unresolvedActionIds: string[];
}
export function finishRequestToJson(r: FinishRequest): Obj {
  const o: Obj = {};
  o.finishId = r.finishId; o.attemptId = r.attemptId; o.attemptEpoch = r.attemptEpoch;
  o.workerLeaseEpoch = r.workerLeaseEpoch; o.workspaceGeneration = r.workspaceGeneration;
  o.engineOutcome = r.engineOutcome;
  put(o, 'engineRunId', r.engineRunId);
  o.finalDurableSequence = r.finalDurableSequence;
  put(o, 'manifestRef', r.manifestRef); put(o, 'manifestDigest', r.manifestDigest);
  put(o, 'quiescenceReceiptRef', r.quiescenceReceiptRef);
  put(o, 'usageCompleteness', r.usageCompleteness);
  o.unresolvedActionIds = r.unresolvedActionIds;
  return o;
}
/** Immutable finish identity — digest of the canonical declaration body. */
export function finishPayloadDigest(r: FinishRequest): string { return canonicalDigest(finishRequestToJson(r)); }
export function parseFinishRequest(o: Obj): FinishRequest {
  const completeness = Fields.optString(o, 'usageCompleteness');
  if (completeness != null && !['UNKNOWN', 'COMPLETE', 'INCOMPLETE'].includes(completeness)) throw new Error('BAD_FIELD:usageCompleteness');
  return {
    finishId: Fields.reqId(o, 'finishId'),
    attemptId: Fields.reqId(o, 'attemptId'),
    attemptEpoch: Fields.reqDecimalString(o, 'attemptEpoch'),
    workerLeaseEpoch: Fields.reqDecimalString(o, 'workerLeaseEpoch'),
    workspaceGeneration: Fields.reqDecimalString(o, 'workspaceGeneration'),
    engineOutcome: Fields.reqEnum(o, 'engineOutcome', ENGINE_OUTCOMES) as EngineOutcome,
    engineRunId: Fields.optId(o, 'engineRunId'),
    finalDurableSequence: Fields.reqDecimalString(o, 'finalDurableSequence'),
    manifestRef: Fields.optString(o, 'manifestRef'),
    manifestDigest: Fields.optDigest(o, 'manifestDigest'),
    quiescenceReceiptRef: Fields.optString(o, 'quiescenceReceiptRef'),
    usageCompleteness: (completeness ?? 'UNKNOWN') as FinishRequest['usageCompleteness'],
    unresolvedActionIds: Fields.optArr(o, 'unresolvedActionIds') ? Fields.stringList(Fields.optArr(o, 'unresolvedActionIds')!, 'unresolvedActionIds') : [],
  };
}
export interface FinishReceipt {
  finishId: string; attemptId: string; state: FinishState;
  pendingReasons: string[]; receiptDigest: string | null;
}
export const finishTerminalAccepted = (r: FinishReceipt) => r.state === 'DELIVERY_ACCEPTED' || r.state === 'AUDIT_CONFIRMED';
export function parseFinishReceipt(o: Obj): FinishReceipt {
  return {
    finishId: Fields.reqId(o, 'finishId'),
    attemptId: Fields.reqId(o, 'attemptId'),
    state: Fields.reqEnum(o, 'state', FINISH_STATES) as FinishState,
    pendingReasons: Fields.optArr(o, 'pendingReasons') ? Fields.stringList(Fields.optArr(o, 'pendingReasons')!, 'pendingReasons') : [],
    receiptDigest: Fields.optString(o, 'receiptDigest'),
  };
}

export interface UploadIntentRequest { artifactId: string; digest: string; sizeBytes: number; kind: string }
export function uploadIntentRequestToJson(r: UploadIntentRequest): Obj {
  const o: Obj = { artifactId: r.artifactId, digest: r.digest, sizeBytes: r.sizeBytes };
  put(o, 'kind', r.kind);
  return o;
}
export interface UploadIntentResponse { uploadUrl: string; method: 'PUT'; expiresAt: string; maxSizeBytes: number | null; alreadyReceipted: boolean | null }
export function parseUploadIntentResponse(o: Obj): UploadIntentResponse {
  const method = Fields.reqString(o, 'method');
  if (method !== 'PUT') throw new Error('BAD_FIELD:method');
  return {
    uploadUrl: Fields.reqString(o, 'uploadUrl', 2048),
    method: 'PUT',
    expiresAt: Fields.reqTime(o, 'expiresAt'),
    maxSizeBytes: Fields.optLong(o, 'maxSizeBytes'),
    alreadyReceipted: Fields.optBool(o, 'alreadyReceipted'),
  };
}
export interface ArtifactCompleteRequest { artifactId: string; digest: string; sizeBytes: number }
export function artifactCompleteToJson(r: ArtifactCompleteRequest): Obj {
  return { artifactId: r.artifactId, digest: r.digest, sizeBytes: r.sizeBytes };
}

export interface ApprovalRequest {
  approvalRequestId: string; actionProposalDigest: string; policySnapshotRef: string | null;
  grantScope: string | null; attemptEpoch: string; workerLeaseEpoch: string; expiresAt: string;
}
export function approvalRequestToJson(r: ApprovalRequest): Obj {
  const o: Obj = {};
  o.approvalRequestId = r.approvalRequestId; o.actionProposalDigest = r.actionProposalDigest;
  put(o, 'policySnapshotRef', r.policySnapshotRef); put(o, 'grantScope', r.grantScope);
  o.attemptEpoch = r.attemptEpoch; o.workerLeaseEpoch = r.workerLeaseEpoch; o.expiresAt = r.expiresAt;
  return o;
}

/**
 * A granted scoped-recovery session (design 04 §14.1). The port sends
 * {workerId, oldInstanceId?, priorStreamId?} — note `oldInstanceId`, the
 * key the plane actually reads — and parses the richer service response
 * (ticket + frozen stream/attempt scope).
 */
export interface RecoverySession {
  sessionId: string; ticket: string; oldInstanceId: string;
  streamIds: Set<string>; attemptIds: Set<string>; expiresAtEpochMs: number;
}
export function recoverySessionCovers(s: RecoverySession, streamId: string | null, attemptId: string | null) {
  return (streamId == null || s.streamIds.has(streamId)) && (attemptId == null || s.attemptIds.has(attemptId));
}
export function parseRecoveryResponse(o: Obj, requestedOldInstanceId: string | null): RecoverySession {
  const streamIds = new Set<string>();
  const streams = o.allowedStreamIds;
  if (Array.isArray(streams)) for (const v of streams) if (typeof v === 'string') streamIds.add(v);
  const attemptIds = new Set<string>();
  const attempts = o.allowedAttemptIds;
  if (Array.isArray(attempts)) for (const v of attempts) if (typeof v === 'string') attemptIds.add(v);
  let expires = 0;
  const expMs = o.expiresAtEpochMs;
  if (typeof expMs === 'number' && Number.isSafeInteger(expMs)) expires = expMs;
  if (!expires && typeof o.expiresAt === 'string') { const t = Date.parse(o.expiresAt as string); if (!Number.isNaN(t)) expires = t; }
  const str = (k: string) => (typeof o[k] === 'string' ? o[k] as string : null);
  return {
    sessionId: Fields.reqId(o, 'recoverySessionId'),
    ticket: str('ticket') ?? '',
    oldInstanceId: str('oldInstanceId') ?? requestedOldInstanceId ?? '',
    streamIds, attemptIds, expiresAtEpochMs: expires,
  };
}
