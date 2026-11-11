import { digest, type Row } from './store.js';
export type MacTaskBinding = { workerRequestKey: string; origin: string; taskID: string; storeID: string; credentialEnvironment: string; ownsTask: true; ancestorReferences: string[] };
const id = (v: unknown) => typeof v === 'string' && /^[A-Za-z0-9._-]{1,128}$/.test(v);
const toolName = (id: string) => 'mac__' + id.replaceAll('.', '_');
/** Operator-owned task binding; the model receives only the original public capability schemas.
 * No MCP child process, owner token, arbitrary URL, or child-task inheritance is involved. */
export class MacTaskTools {
  constructor(private readonly bindings: MacTaskBinding[]) {
    const keys = new Set<string>(), tasks = new Set<string>();
    for (const b of bindings) {
      const url = new URL(b.origin);
      if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || url.pathname !== '/' || url.search || url.hash || url.username || url.password
        || !id(b.taskID) || !id(b.storeID) || !id(b.workerRequestKey) || !/^[A-Z][A-Z0-9_]{0,127}$/.test(b.credentialEnvironment)
        || b.ownsTask !== true || !Array.isArray(b.ancestorReferences) || b.ancestorReferences.length > 4
        || new Set(b.ancestorReferences).size !== b.ancestorReferences.length || b.ancestorReferences.some(ref => typeof ref !== 'string' || !ref || ref.length > 512 || ref === b.taskID || ref.endsWith(':' + b.taskID)) || keys.has(b.workerRequestKey) || tasks.has(b.origin + '/' + b.taskID)) throw new Error('MAC_TOOL_BINDING_INVALID');
      keys.add(b.workerRequestKey); tasks.add(b.origin + '/' + b.taskID);
    }
  }
  static fromEnvironment(): MacTaskTools | undefined {
    const value = process.env.NWF_MAC_TASK_BINDINGS;
    if (!value) return undefined;
    const rows = JSON.parse(value); if (!Array.isArray(rows) || rows.length > 16) throw new Error('MAC_TOOL_BINDING_INVALID');
    return new MacTaskTools(rows);
  }
  binding(requestKey: string) { return this.bindings.find(b => b.workerRequestKey === requestKey); }
  private checked(b: MacTaskBinding) {
    const actual = this.binding(b.workerRequestKey);
    if (!actual || digest(actual) !== digest(b)) throw new Error('MAC_TOOL_BINDING_CHANGED');
    return actual;
  }
  private async call(binding: MacTaskBinding, method: string, suffix: string, body?: Row): Promise<Row> {
    const b = this.checked(binding), secret = process.env[b.credentialEnvironment];
    if (!secret || /[\r\n]/.test(secret)) throw new Error('MAC_TASK_CREDENTIAL_UNAVAILABLE');
    const response = await fetch(new URL('/native/v1/tasks/' + b.taskID + suffix, b.origin), {
      method, redirect: 'error', signal: AbortSignal.timeout(20000),
      headers: { authorization: 'Bearer ' + secret, 'X-Worker-Local-Store': b.storeID, 'Content-Type': 'application/json', ...(body?.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    });
    let size = 0; const parts: Uint8Array[] = [];
    if (!response.body) throw new Error('MAC_OPERATION_UNCONFIRMED');
    for await (const chunk of response.body) { size += chunk.length; if (size > 1048576) { await response.body.cancel().catch(() => {}); throw new Error('MAC_RESPONSE_TOO_LARGE'); } parts.push(chunk); }
    if (!response.ok) throw new Error(response.status === 404 ? 'MAC_OPERATION_NOT_FOUND_UNCONFIRMED' : 'MAC_OPERATION_UNCONFIRMED');
    return JSON.parse(Buffer.concat(parts).toString('utf8'));
  }
  async capabilities(binding: MacTaskBinding) {
    const context = await this.call(binding, 'GET', '/context');
    if (context.taskId !== binding.taskID || context.schemaVersion !== 'mac-native-context/1' || !Array.isArray(context.capabilities)) throw new Error('MAC_CONTEXT_CHANGED');
    return context.capabilities.map((c: Row) => ({ name: toolName(c.capabilityId), label: c.capabilityId, description: c.description, parameters: c.inputSchema }));
  }
  async execute(binding: MacTaskBinding, task: Row, event: Row, save: (record: Row) => void): Promise<Row> {
    this.checked(binding);
    if (task.intakeClosed || task.macToolsClosed) throw new Error('MAC_TOOLS_CLOSED');
    const key = 'mac-' + digest({ taskId: task.taskId, segmentId: task.segmentId, callId: event.toolCallId });
    const old = task.macOperations?.[key];
    if (old) {
      if (old.inputDigest !== digest({ name: event.toolName, input: event.input })) throw new Error('IDEMPOTENCY_CONFLICT');
      return old.receipt ?? await this.call(binding, 'GET', '/operations/' + key);
    }
    const context = await this.call(binding, 'GET', '/context');
    const capability = context.capabilities?.find((c: Row) => toolName(c.capabilityId) === event.toolName);
    if (context.taskId !== binding.taskID || !capability || task.intakeClosed || task.macToolsClosed) throw new Error('MAC_CAPABILITY_UNAVAILABLE');
    const body = { schemaVersion: 'mac-native-call/1', requestKey: key, capabilityId: capability.capabilityId, capabilityVersion: capability.version, guard: context.guard, arguments: event.input };
    const record = { requestKey: key, inputDigest: digest({ name: event.toolName, input: event.input }), body, status: 'DISPATCHING' };
    save(record); // Host ledger must commit before POST. Unknown effects only use GET at the original key.
    const receipt = await this.call(binding, 'POST', '/operations', body);
    if (receipt.requestKey !== key || receipt.taskId !== binding.taskID) throw new Error('MAC_RECEIPT_BINDING_CHANGED');
    save({ ...record, status: receipt.state ?? 'UNKNOWN', receipt });
    return receipt;
  }
  async stop(binding: MacTaskBinding, original?: Row): Promise<Row> {
    const requestKey = 'mac-stop-' + digest({ requestKey: binding.workerRequestKey, taskID: binding.taskID });
    let receipt = original ? await this.call(binding, 'GET', '/requests/' + requestKey) : await this.call(binding, 'POST', '/stop', { requestKey });
    for (let attempt = 0; attempt < 80 && receipt.remoteQuiescent !== true && receipt.quiescent !== true; attempt++) {
      await new Promise(done => setTimeout(done, 50));
      receipt = await this.call(binding, 'GET', '/requests/' + requestKey);
    }
    if (receipt.requestKey !== requestKey || receipt.taskId !== binding.taskID) throw new Error('MAC_STOP_BINDING_CHANGED');
    return receipt;
  }
}
