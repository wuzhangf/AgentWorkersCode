/**
 * W6: typed client for the existing Pi host's /local/v1 + /worker-host/v1
 * surface. The wire adapter never reimplements execution — it drives the
 * durable task ledger the host already owns: preflight → create →
 * observe detail → STOP with generation/session/turn/command guards →
 * sealed artifacts. requestKeys are deterministic per wire identity so
 * an adapter restart replays the SAME local intent idempotently.
 */
export class LocalError extends Error {
  constructor(readonly code: string, readonly status: number, message: string) { super(message); this.name = 'LocalError'; }
}
export type LocalRow = Record<string, any>;

export interface LocalClientOptions { baseUrl: string; token: string; fetchImpl?: typeof fetch; timeoutMillis?: number }

export class LocalHostClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMillis: number;
  constructor(opts: LocalClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.token = opts.token;
    this.fetchImpl = opts.fetchImpl ?? fetch;
    this.timeoutMillis = opts.timeoutMillis ?? 30_000;
  }

  private async call(method: 'GET' | 'POST', path: string, body?: LocalRow): Promise<LocalRow> {
    const headers: Record<string, string> = { Authorization: 'Bearer ' + this.token };
    if (body) {
      headers['Content-Type'] = 'application/json';
      if (body.requestKey) headers['Idempotency-Key'] = body.requestKey;
    }
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.baseUrl + path, {
        method, headers, redirect: 'manual',
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(this.timeoutMillis),
      });
    } catch (e) {
      throw new LocalError('LOCAL_TRANSPORT', 0, 'local host unreachable: ' + String(e));
    }
    let parsed: LocalRow;
    try { parsed = await resp.json() as LocalRow; } catch { throw new LocalError('LOCAL_BAD_RESPONSE', 0, 'local host returned non-JSON'); }
    if (resp.status < 200 || resp.status >= 300) {
      throw new LocalError(typeof parsed?.code === 'string' ? parsed.code : 'LOCAL_HTTP_' + resp.status, resp.status,
        typeof parsed?.message === 'string' ? parsed.message : 'local call failed ' + resp.status);
    }
    return parsed;
  }

  identity() { return this.call('GET', '/local/v1/identity'); }
  projects() { return this.call('GET', '/local/v1/projects'); }
  engines() { return this.call('GET', '/local/v1/engines'); }
  manifest() { return this.call('GET', '/worker-host/v1/manifest'); }
  preflight(projectId: string, engineId = 'pi') { return this.call('POST', '/local/v1/task-preflights', { projectId, engineId }); }
  /** 202 op receipt — {state:'ACCEPTED', taskId, launchState, ...}. */
  createTask(body: LocalRow) { return this.call('POST', '/local/v1/tasks', body); }
  taskDetail(taskId: string) { return this.call('GET', '/local/v1/tasks/' + encodeURIComponent(taskId)); }
  taskEvents(taskId: string, cursor = '0', limit = 100) { return this.call('GET', `/local/v1/tasks/${encodeURIComponent(taskId)}/events?cursor=${encodeURIComponent(cursor)}&limit=${limit}`); }
  artifacts(taskId: string) { return this.call('GET', `/local/v1/tasks/${encodeURIComponent(taskId)}/artifacts`); }
  artifact(taskId: string, artifactId: string) { return this.call('GET', `/local/v1/tasks/${encodeURIComponent(taskId)}/artifacts/${encodeURIComponent(artifactId)}`); }
  operation(requestKey: string) { return this.call('GET', '/local/v1/task-requests/' + encodeURIComponent(requestKey)); }
  /**
   * STOP with the host's durable guard: generation + nativeSessionId +
   * nativeTurnId + targetCommandId must match the current task identity —
   * the wire never cancels a rebinding it didn't pin.
   */
  stop(taskId: string, guards: { generation: string | null; nativeSessionId: string | null; nativeTurnId: string | null; targetCommandId: string | null }, requestKey: string) {
    return this.call('POST', `/local/v1/tasks/${encodeURIComponent(taskId)}/commands`, {
      requestKey, intent: 'STOP',
      generation: guards.generation ?? null,
      nativeSessionId: guards.nativeSessionId ?? null,
      nativeTurnId: guards.nativeTurnId ?? null,
      targetCommandId: guards.targetCommandId ?? null,
    });
  }
}
