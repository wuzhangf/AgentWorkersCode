import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import * as Paths from './paths.js';
import { canonicalBytes, canonicalDigest, rfc3339 } from './protocol.js';
import * as dto from './dto.js';
import type { Obj } from './fields.js';

/**
 * W6: HttpControlPlanePort mirror — canonical-JSON bodies (the same bytes
 * that produced the digests), problem-body error codes, no redirect
 * following (R11), and origin-scoped credentials for artifact uploads.
 * Recovery creds for the attempt lane ride the JSON body / query exactly
 * like the Java overloads — never the headers (those are stage-only).
 */
export class ControlPlaneError extends Error {
  constructor(readonly code: string, message: string, readonly retryable: boolean) { super(message); this.name = 'ControlPlaneError'; }
}
/** Lease-loss signals the watchdog re-registers on (WorkerDaemonMain.isLeaseLoss). */
export const isLeaseLoss = (e: unknown): e is ControlPlaneError =>
  e instanceof ControlPlaneError && !e.retryable && ['LEASE_EXPIRED', 'LEASE_EPOCH_MISMATCH', 'STALE_LEASE'].includes(e.code);

const scrubCredentials = (message: string) => message.replace(/(recoverySessionId|recoveryTicket)=[^&\s"']+/g, '$1=REDACTED');

export interface PortOptions { baseUrl: string; bearerToken?: string; fetchImpl?: typeof fetch }

export class HttpControlPlanePort {
  private readonly baseUrl: string;
  private readonly bearerToken: string;
  private readonly baseOrigin: URL;
  private readonly fetchImpl: typeof fetch;
  constructor(opts: PortOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.bearerToken = opts.bearerToken ?? '';
    this.baseOrigin = new URL(this.baseUrl);
    this.fetchImpl = opts.fetchImpl ?? fetch;
  }

  register(request: Obj): Promise<dto.RegisterResponse> {
    return this.post(Paths.registrations(), request).then(dto.parseRegisterResponse);
  }
  heartbeat(instanceId: string, request: Obj): Promise<dto.HeartbeatResponse> {
    return this.post(Paths.heartbeatPath(instanceId), request).then(dto.parseHeartbeatResponse);
  }
  async postReceipt(instanceId: string, receipt: Obj): Promise<void> {
    await this.post(Paths.receiptPath(instanceId, receipt.commandId as string), receipt);
  }
  pollCommands(instanceId: string, after: string | null, waitSeconds: number, signal?: AbortSignal): Promise<dto.PollResponse> {
    return this.get(Paths.commandsQuery(instanceId, after, waitSeconds), (waitSeconds + 10) * 1000, signal).then(dto.parsePollResponse);
  }
  async postEvents(instanceId: string, events: dto.EventEnvelope[], session?: dto.RecoverySession): Promise<dto.EventBatchResponse> {
    const body: Obj = { events: events.map(dto.eventToJson) };
    if (session) { body.recoverySessionId = session.sessionId; body.recoveryTicket = session.ticket; }
    return dto.parseEventBatchResponse(await this.post(Paths.eventsPath(instanceId), body));
  }
  async postApprovalRequest(attemptId: string, request: Obj): Promise<void> {
    await this.post(Paths.approvalRequestsPath(attemptId), request);
  }
  async requestUploadIntent(attemptId: string, request: dto.UploadIntentRequest, session?: dto.RecoverySession): Promise<dto.UploadIntentResponse> {
    const body = dto.uploadIntentRequestToJson(request);
    if (session) { body.recoverySessionId = session.sessionId; body.recoveryTicket = session.ticket; }
    return dto.parseUploadIntentResponse(await this.post(Paths.uploadIntentsPath(attemptId), body));
  }
  /**
   * R11: uploads land where the control plane says — credentials follow
   * the ORIGIN, not the URL. A same-origin upload keeps the worker
   * bearer (and, when scoped, the recovery ticket as query params); a
   * cross-origin presigned URL is itself the credential and receives
   * neither. The local file is sha256-verified WHILE streaming — a
   * divergence means zero bytes on the wire.
   */
  async putArtifact(uploadUrl: string, localFile: string, digest: string, session?: dto.RecoverySession): Promise<void> {
    await this.requireLocalDigest(localFile, digest);
    let target: URL;
    try { target = new URL(this.resolve(uploadUrl)); }
    catch { throw new ControlPlaneError('UPLOAD_URL_REJECTED', 'upload url is not http(s)', false); }
    if (!['http:', 'https:'].includes(target.protocol)) {
      throw new ControlPlaneError('UPLOAD_URL_REJECTED', 'upload url is not http(s)', false);
    }
    // Same-origin recovery: the ticket is appended ONLY for control-plane
    // endpoints — on a cross-origin presigned URL the URL is the credential.
    if (session && this.isControlPlaneOrigin(target)) {
      target.searchParams.set('recoverySessionId', session.sessionId);
      target.searchParams.set('recoveryTicket', session.ticket);
    }
    const headers: Record<string, string> = { 'Content-Type': 'application/octet-stream' };
    if (this.isControlPlaneOrigin(target)) headers.Authorization = 'Bearer ' + this.bearerToken;
    let resp: Response;
    try {
      resp = await this.fetchImpl(target.toString(), {
        method: 'PUT', headers, redirect: 'manual',
        body: createReadStream(localFile) as any, duplex: 'half',
        signal: AbortSignal.timeout(5 * 60 * 1000),
      } as RequestInit);
    } catch (e) {
      throw new ControlPlaneError('UPLOAD_IO', scrubCredentials(String(e)), true);
    }
    await this.requireSuccess(resp, 'PUT artifact ' + digest.slice(0, 24));
  }
  async completeArtifact(attemptId: string, request: dto.ArtifactCompleteRequest, session?: dto.RecoverySession): Promise<string | null> {
    const path = Paths.artifactCompletePath(attemptId, request.artifactId)
      + (session ? '?recoverySessionId=' + encodeURIComponent(session.sessionId) + '&recoveryTicket=' + encodeURIComponent(session.ticket) : '');
    const resp = await this.post(path, dto.artifactCompleteToJson(request));
    const ref = resp.receiptRef;
    return typeof ref === 'string' ? ref : null;
  }
  async postFinish(attemptId: string, request: dto.FinishRequest, session?: dto.RecoverySession): Promise<void> {
    const body = dto.finishRequestToJson(request);
    // The server strips recovery creds from the payload digest — a rotated
    // ticket never forks a replay.
    if (session) { body.recoverySessionId = session.sessionId; body.recoveryTicket = session.ticket; }
    await this.post(Paths.finishPath(attemptId), body);
  }
  getFinishReceipt(attemptId: string, finishId: string): Promise<dto.FinishReceipt> {
    return this.get(Paths.finishReceiptPath(attemptId, finishId), 30_000).then(dto.parseFinishReceipt);
  }
  /** Grant a scoped recovery session for a dead instance's evidence (04 §14.1). */
  async postRecovery(workerId: string, oldInstanceId: string | null, priorStreamId: string | null): Promise<dto.RecoverySession> {
    const body: Obj = { workerId };
    if (oldInstanceId != null) body.oldInstanceId = oldInstanceId;
    if (priorStreamId != null) body.priorStreamId = priorStreamId;
    const resp = await this.post(Paths.recoveries(), body);
    return dto.parseRecoveryResponse(resp, oldInstanceId);
  }

  // ------------------------------------------------------------------

  private resolve(urlOrPath: string): string {
    if (urlOrPath.startsWith('http://') || urlOrPath.startsWith('https://')) return urlOrPath;
    return this.baseUrl + urlOrPath;
  }
  /** Scheme + host + effective port — the only origin that may receive worker credentials. */
  private isControlPlaneOrigin(target: URL): boolean {
    return target.protocol === this.baseOrigin.protocol
      && target.hostname.toLowerCase() === this.baseOrigin.hostname.toLowerCase()
      && this.effectivePort(target) === this.effectivePort(this.baseOrigin);
  }
  private effectivePort(u: URL): number {
    if (u.port) return Number(u.port);
    if (u.protocol === 'http:') return 80;
    if (u.protocol === 'https:') return 443;
    return -1;
  }
  private async requireLocalDigest(localFile: string, digest: string): Promise<void> {
    try {
      await stat(localFile);
    } catch (e) {
      throw new ControlPlaneError('UPLOAD_IO', 'local artifact unreadable: ' + String(e), true);
    }
    const md = createHash('sha256');
    await new Promise<void>((resolveP, reject) => {
      const stream = createReadStream(localFile);
      stream.on('data', chunk => md.update(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolveP());
    }).catch(e => { throw new ControlPlaneError('UPLOAD_IO', scrubCredentials(String(e)), true); });
    if ('sha256:' + md.digest('hex') !== digest) {
      throw new ControlPlaneError('ARTIFACT_DIGEST_MISMATCH', 'local bytes do not match declared digest', false);
    }
  }

  private async post(path: string, body: Obj): Promise<Obj> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json; charset=utf-8' };
    if (this.bearerToken) headers.Authorization = 'Bearer ' + this.bearerToken;
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.baseUrl + path, {
        method: 'POST', headers, redirect: 'manual',
        body: canonicalBytes(body) as unknown as BodyInit, signal: AbortSignal.timeout(60_000),
      });
    } catch (e) {
      throw new ControlPlaneError('TRANSPORT', scrubCredentials(String(e)), true);
    }
    return this.parseObject(await this.requireSuccess(resp, 'POST ' + path));
  }
  private async get(path: string, timeoutMs = 30_000, signal?: AbortSignal): Promise<Obj> {
    const headers: Record<string, string> = {};
    if (this.bearerToken) headers.Authorization = 'Bearer ' + this.bearerToken;
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.baseUrl + path, {
        method: 'GET', headers, redirect: 'manual',
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]) : AbortSignal.timeout(timeoutMs),
      });
    } catch (e) {
      throw new ControlPlaneError('TRANSPORT', scrubCredentials(String(e)), true);
    }
    return this.parseObject(await this.requireSuccess(resp, 'GET ' + path));
  }
  private async requireSuccess(resp: Response, what: string): Promise<Response> {
    const status = resp.status;
    if (status >= 300 && status < 400) {
      // R11: Redirect.NEVER semantics — a Location must never forward credentials.
      throw new ControlPlaneError('REDIRECT_REFUSED', 'redirect refused: ' + status + ' Location present=' + (resp.headers.get('location') != null), false);
    }
    if (status >= 200 && status < 300) return resp;
    let code = 'HTTP_' + status, message = what + ' -> ' + status;
    try {
      const problem = await resp.json() as Obj;
      if (typeof problem?.code === 'string') code = problem.code;
      if (typeof problem?.message === 'string') message = problem.message;
      const retryable = problem?.retryable === true;
      throw new ControlPlaneError(code, message, retryable || status >= 500);
    } catch (e) {
      if (e instanceof ControlPlaneError) throw e;
      throw new ControlPlaneError(code, message, status >= 500);
    }
  }
  private async parseObject(resp: Response): Promise<Obj> {
    try {
      const v = await resp.json();
      if (!v || typeof v !== 'object' || Array.isArray(v)) throw new Error('not an object');
      return v as Obj;
    } catch {
      throw new ControlPlaneError('BAD_RESPONSE', 'unparseable response', true);
    }
  }
}
