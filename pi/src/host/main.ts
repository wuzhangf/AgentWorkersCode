import { MacTaskTools } from './mac-tools.js';
import { PiHost } from './server.js';
const args = process.argv.slice(2); const mode = args.shift() ?? 'serve'; const options: Record<string, string> = {};
for (let i = 0; i < args.length; i += 2) { if (!args[i].startsWith('--') || args[i + 1] === undefined) throw new Error('OPTION_VALUE_REQUIRED'); options[args[i]] = args[i + 1]; }
const host = new PiHost({ macTools: MacTaskTools.fromEnvironment(), executionProfile: options['--execution-profile'], taskDeadlineMs: options['--task-deadline-ms'] === undefined ? undefined : Number(options['--task-deadline-ms']), state: options['--state'] ?? '.local/pi-native', token: process.env[options['--token-env'] ?? 'WORKER_LOCAL_TOKEN'] ?? (mode === 'init' ? 'registration-only-no-server-token' : '') });
if (mode === 'init') {
  const project = host.register(options['--project-id'], options['--project']); console.log(JSON.stringify({ registered: project.projectId, headRevision: project.headRevision, profileId: host.profileId, executionProfile: host.executionProfile })); await host.close();
} else if (mode === 'serve') {
  const port = await host.listen(Number(options['--port'] ?? 0)); console.log('LOCAL_TASK_READY ' + JSON.stringify({ url: 'http://127.0.0.1:' + port, ...host.identity() }));
  let closing = false; const close = () => { if (closing) return; closing = true; void host.close().then(() => process.exit(0)); };
  process.on('SIGINT', close); process.on('SIGTERM', close);
  const timer = setInterval(() => { host.sweepAuxDeadlines(); host.sweepCollabDeadlines(); void host.sweepTaskDeadlines().catch(() => { /* retry from durable facts next tick */ }); }, 1000); timer.unref();
} else { await host.close(); throw new Error('MODE_UNSUPPORTED'); }
