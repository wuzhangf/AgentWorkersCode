import { MacTaskTools } from './mac-tools.js';
import { PiHost } from './server.js';
import { request as httpsRequest } from 'node:https';
import { request as httpRequest } from 'node:http';
import { mkdirSync, writeFileSync, renameSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
const args = process.argv.slice(2); const mode = args.shift() ?? 'serve'; const options: Record<string, string> = {};
for (let i = 0; i < args.length; i += 2) { if (!args[i].startsWith('--') || args[i + 1] === undefined) throw new Error('OPTION_VALUE_REQUIRED'); options[args[i]] = args[i + 1]; }
const workerToken = process.env[options['--token-env'] ?? 'WORKER_LOCAL_TOKEN'] ?? (mode === 'init' ? 'registration-only-no-server-token' : '');
const host = new PiHost({ macTools: MacTaskTools.fromEnvironment(), executionProfile: options['--execution-profile'], taskDeadlineMs: options['--task-deadline-ms'] === undefined ? undefined : Number(options['--task-deadline-ms']), state: options['--state'] ?? join(homedir(), '.agent-gateway', 'pi', 'state'), token: workerToken });
// Active registration: the owner mints a one-shot workerSecret on the gateway;
// this worker redeems it once, advertising its own url + bearer. Retried —
// a busy gateway must not kill the worker.
const registerToGateway = (port: number, attempt = 0): void => {
  const gw = process.env.GATEWAY_REGISTER_URL; const secret = process.env.GATEWAY_WORKER_SECRET;
  if (!gw || !secret) return;
  const body = JSON.stringify({ workerSecret: secret, connectionId: process.env.GATEWAY_CONNECTION_ID ?? ('sage-pi-' + host.identity().storeId.slice(0, 8)), url: process.env.GATEWAY_ADVERTISE_URL ?? ('http://127.0.0.1:' + port), token: workerToken, displayName: 'Sage Pi' });
  // LAN self-signed certs: the minted secret authenticates this call, so the
  // cert is transport-only here.
  const req = (gw.startsWith('https:') ? httpsRequest : httpRequest)(gw.replace(/\/$/, '') + '/personal/v1/worker-registrations', { method: 'POST', headers: { 'content-type': 'application/json' }, rejectUnauthorized: false }, (res) => {
    res.resume();
    if (res.statusCode === 202) { console.log('GATEWAY_REGISTERED ' + JSON.stringify({ gateway: gw })); return; }
    if (attempt < 5) setTimeout(() => registerToGateway(port, attempt + 1), 4000).unref();
    else console.log('GATEWAY_REGISTER_FAILED ' + JSON.stringify({ status: res.statusCode }));
  });
  req.on('error', () => { if (attempt < 5) setTimeout(() => registerToGateway(port, attempt + 1), 4000).unref(); });
  req.end(body);
};
if (mode === 'init') {
  const project = host.register(options['--project-id'], options['--project']); console.log(JSON.stringify({ registered: project.projectId, headRevision: project.headRevision, profileId: host.profileId, executionProfile: host.executionProfile })); await host.close();
} else if (mode === 'serve') {
  const port = await host.listen(Number(options['--port'] ?? 0)); console.log('LOCAL_TASK_READY ' + JSON.stringify({ url: 'http://127.0.0.1:' + port, ...host.identity() })); registerToGateway(port);
  // Drop a registration card any local AgentLink service can pick up —
  // the file itself is the credential on a loopback-only trust boundary.
  try {
    const dropDir = process.env.GATEWAY_WORKER_DROP_DIR ?? join(homedir(), '.agent-gateway', 'workers.d');
    const cardId = process.env.GATEWAY_CONNECTION_ID ?? ('sage-pi-' + host.identity().storeId.slice(0, 8));
    const card = join(dropDir, cardId + '.json');
    mkdirSync(dropDir, { recursive: true, mode: 0o700 });
    const tmp = card + '.' + process.pid + '.tmp';
    let projectPath: string | undefined;
    try { projectPath = host.project('pi-workspace').path; } catch { /* project not registered on this store */ }
    writeFileSync(tmp, JSON.stringify({ connectionId: cardId, url: 'http://127.0.0.1:' + port, token: workerToken, displayName: 'Sage Pi', engine: 'sage-pi', projectId: 'pi-workspace', project: projectPath }), { mode: 0o600 });
    renameSync(tmp, card);
    process.on('exit', () => { try { rmSync(card); } catch { /* already gone */ } });
  } catch (error) { console.log('WORKER_DROP_FAILED ' + String(error)); }
  let closing = false; const close = () => { if (closing) return; closing = true; void host.close().then(() => process.exit(0)); };
  process.on('SIGINT', close); process.on('SIGTERM', close);
  const timer = setInterval(() => { host.sweepAuxDeadlines(); host.sweepCollabDeadlines(); void host.sweepTaskDeadlines().catch(() => { /* retry from durable facts next tick */ }); }, 1000); timer.unref();
} else { await host.close(); throw new Error('MODE_UNSUPPORTED'); }
