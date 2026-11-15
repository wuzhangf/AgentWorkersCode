import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, mkdtempSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomBytes } from 'node:crypto';
import { PiHost } from '../dist/host/server.js';

const [command, ...adapterArgs] = process.argv.slice(2);
assert.ok(command && adapterArgs.length, 'Expected Sage Link MCP executable and task grant');
for (const key of ['MODEL_BASE_URL', 'MODEL_DEFAULT_MODEL', 'MODEL_API_KEY']) {
  assert.ok(process.env[key], key + ' required');
}
const local = resolve('.local');
mkdirSync(local, { recursive: true });
const root = mkdtempSync(join(local, 'sage-link-mcp-'));
const project = join(root, 'project');
mkdirSync(project);
execFileSync('git', ['init', '-q', project]);
writeFileSync(join(project, 'README.md'), 'This isolated project contains no source data.\n');
execFileSync('git', ['-C', project, 'add', 'README.md']);
execFileSync('git', ['-C', project, '-c', 'user.name=wuzf', '-c', 'user.email=wuzhangf@gmail.com', 'commit', '-qm', 'test baseline']);
const token = randomBytes(32).toString('hex');
const host = new PiHost({ state: join(root, 'state'), token });
let taskId;
try {
  host.register('sage-link', project);
  const port = await host.listen();
  async function call(path, body) {
    const response = await fetch(`http://127.0.0.1:${port}/local/v1${path}`, {
      method: body ? 'POST' : 'GET',
      headers: {
        Authorization: 'Bearer ' + token,
        ...(body ? { 'Content-Type': 'application/json' } : {}),
        ...(body?.requestKey ? { 'Idempotency-Key': body.requestKey } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const value = await response.json();
    if (!response.ok) throw new Error(`Worker ${response.status}: ${value.code ?? 'unavailable'}`);
    return value;
  }
  const preflight = await call('/task-preflights', { engineId: 'sage-pi', projectId: 'sage-link' });
  const created = await call('/tasks', {
    requestKey: 'sage-link-mcp-scene', engineId: 'sage-pi', projectId: 'sage-link',
    optionsRevision: preflight.optionsRevision,
    instruction: [
      'Run a file exchange with the authorized phone. Call the Sage Link list tool, then use its receive tool on the incoming transferId.',
      'Use the read tool to read the returned relativePath. Write the exact file text (including its final newline) to report.txt.',
      'Use the Sage Link send tool with path report.txt and requestKey agent-report-1. Call the status tool for the returned transferId.',
      'Finish without asking a question. Do not access files outside the task directory.',
    ].join(' '),
    mcpServers: [{ id: 'sage-link', command: resolve(command), args: adapterArgs[0] === '--bridge' ? adapterArgs : [resolve(adapterArgs[0])] }],
  });
  taskId = created.taskId;
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    const task = await call('/tasks/' + taskId);
    if (task.state === 'SUCCEEDED') {
      const items = [];
      let cursor = '0';
      do {
        const page = await call('/tasks/' + taskId + '/events?limit=100&cursor=' + cursor);
        items.push(...page.items);
        cursor = page.items.length === 100 ? page.cursor : null;
      } while (cursor);
      const bound = items.find(e => e.kind === 'mcp.bound' && e.server === 'sage-link');
      assert.equal(bound?.toolCount, 5);
      const successful = new Set(items.filter(e => e.kind === 'TOOL_END' && !e.isError).map(e => e.toolCallId));
      for (const verb of ['list', 'receive', 'send', 'status']) {
        assert.ok(items.some(e => e.kind === 'TOOL_START' &&
          e.toolName === 'mcp__sage_link__link_files_' + verb && successful.has(e.toolCallId)), verb);
      }
      assert.ok(readFileSync(join(host.store.task(taskId).workspace, 'report.txt')).length);
      console.log('Pi model invoked Sage Link list, receive, send and status successfully');
      break;
    }
    if (['FAILED', 'STOPPED', 'BUDGET_EXCEEDED'].includes(task.state)) {
      throw new Error('Pi task ended: ' + task.state + ' ' + (task.reasonCode ?? ''));
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  assert.ok(Date.now() < deadline, 'Pi task timed out');
} finally {
  await host.close();
}
