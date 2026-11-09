import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import type { Row } from './store.js';

export const runnerEntry = fileURLToPath(new URL('../../../command-runner/main.mjs', import.meta.url));
const terminal = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'UNKNOWN']);
class RunnerHttpError extends Error { constructor(message: string, readonly status: number) { super(message); } }
export class RunnerRequestRejected extends Error {}

/** Each task generation has a private Runner, state and stdin owner lease. Neither
 * model credentials nor the Worker control token enter its environment. */
export class CommandRunner {
  private child!: ChildProcessWithoutNullStreams;
  private readonly token = randomBytes(32).toString('hex');
  private url = '';
  private closed = false;
  private active = new Set<Promise<Row>>();
  private stopResult?: Row;
  private constructor(readonly context: Row) {}
  static async start(state: string, context: Row) {
    const runner = new CommandRunner(context);
    const env: NodeJS.ProcessEnv = { WORKER_COMMAND_RUNNER_TOKEN: runner.token };
    for (const name of ['PATH', 'HOME', 'TMP', 'TEMP', ...context.envAllowlist]) if (process.env[name]) env[name] = process.env[name];
    runner.child = spawn(process.execPath, [runnerEntry, 'serve', '--state', state, '--port', '0'], { env, stdio: ['pipe', 'pipe', 'pipe'] });
    runner.child.stderr.resume(); // diagnostics never echo request bodies into the host console
    try {
      await new Promise<void>((done, reject) => {
        let raw = ''; const timer = setTimeout(() => finish(new Error('RUNNER_START_TIMEOUT')), 10000);
        const finish = (error?: Error) => { clearTimeout(timer); runner.child.stdout.off('data', data); runner.child.off('exit', exited); runner.child.off('error', failed); error ? reject(error) : done(); };
        const failed = () => finish(new Error('RUNNER_UNAVAILABLE'));
        const exited = () => failed();
        const data = (chunk: Buffer) => {
          raw += chunk.toString();
          for (const line of raw.split('\n')) if (line.startsWith('COMMAND_RUNNER_READY ')) {
            try { const ready = JSON.parse(line.slice('COMMAND_RUNNER_READY '.length)); const u = new URL(ready.url); if (u.protocol !== 'http:' || u.hostname !== '127.0.0.1' || !u.port) throw new Error(); runner.url = u.origin; finish(); } catch { failed(); } return;
          }
          if (raw.length > 16384) failed();
        };
        runner.child.stdout.on('data', data); runner.child.once('exit', exited); runner.child.once('error', failed);
      });
      runner.child.stdout.resume();
      await runner.call('/v1/contexts', context);
      return runner;
    } catch (error) { await runner.close(); throw error; }
  }
  private async call(path: string, body?: Row) {
    const response = await fetch(this.url + path, { method: body ? 'POST' : 'GET', headers: { Authorization: 'Bearer ' + this.token, ...(body ? { 'Content-Type': 'application/json', ...(body.requestKey ? { 'Idempotency-Key': body.requestKey } : {}) } : {}) }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(20000) });
    const value = await response.json() as Row;
    if (!response.ok) throw new RunnerHttpError(typeof value.code === 'string' && /^[A-Z_]+$/.test(value.code) ? value.code : 'RUNNER_HTTP_FAILED', response.status);
    return value;
  }
  execute(body: Row, admitted: (receipt: Row) => void) {
    const pending = this.executeOnce(body, admitted);
    this.active.add(pending); void pending.then(() => this.active.delete(pending), () => this.active.delete(pending));
    return pending;
  }
  lookup(requestKey: string) { return this.call('/v1/requests/' + encodeURIComponent(requestKey)); }
  private async executeOnce(body: Row, admitted: (receipt: Row) => void) {
    if (this.closed) throw new Error('RUNNER_CONTEXT_CLOSED');
    let receipt: Row;
    // Never resend after an uncertain POST. Recover the original key by lookup.
    try { receipt = await this.call('/v1/commands', body); }
    catch (error) {
      if (error instanceof RunnerHttpError && error.status >= 400 && error.status < 500) throw new RunnerRequestRejected(error.message);
      receipt = await this.call('/v1/requests/' + encodeURIComponent(body.requestKey));
    }
    admitted(receipt);
    const deadline = Date.now() + 660000;
    while (!terminal.has(receipt.status)) {
      if (Date.now() > deadline) throw new Error('RUNNER_SETTLEMENT_UNCONFIRMED');
      await new Promise(done => setTimeout(done, 50));
      receipt = await this.call('/v1/commands/' + encodeURIComponent(receipt.commandId));
    }
    return qualifyReceipt(receipt);
  }
  async stop(requestKey: string) {
    this.closed = true;
    if (this.stopResult) return this.stopResult;
    const context = await this.call('/v1/contexts/' + encodeURIComponent(this.context.contextId) + '/stop', { requestKey });
    await Promise.allSettled([...this.active]);
    this.stopResult = context; return context;
  }
  async close() {
    this.closed = true;
    if (!this.child || this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.stdin.end();
    await Promise.race([new Promise<void>(done => this.child.once('exit', () => done())), new Promise<void>(done => setTimeout(done, 16000).unref())]);
  }
}

export function qualifyReceipt(receipt: Row) {
  const result: Row = { ...receipt, buildSucceeded: receipt.kind === 'build' ? receipt.status === 'SUCCEEDED' && receipt.exitCode === 0 : null };
  if (receipt.kind === 'test') {
    const output = String(receipt.stdout ?? '') + '\n' + String(receipt.stderr ?? '');
    const count = (name: string) => { const values = [...output.matchAll(new RegExp('^# ' + name + '\\s+(\\d+)\\s*$', 'gm'))]; return values.length === 1 ? Number(values[0][1]) : null; };
    const executed = count('tests'), passed = count('pass'), failed = count('fail');
    const parsed = !receipt.outputTruncated && executed !== null && passed !== null && failed !== null && executed >= passed + failed;
    result.tests = parsed ? { evidence: 'NODE_TAP_SUMMARY', executed, passed, failed, nonzeroExecuted: executed > 0, passedWithTests: receipt.status === 'SUCCEEDED' && receipt.exitCode === 0 && executed > 0 && failed === 0 && passed > 0 } : { evidence: 'UNKNOWN', executed: null, nonzeroExecuted: null, passedWithTests: null };
  }
  return result;
}
