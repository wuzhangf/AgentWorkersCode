import { PiHost } from '../dist/host/server.js';
const host = new PiHost({ state: process.argv[2], token: process.env.WORKER_LOCAL_TOKEN, executionProfile: 'macos-runner-v1' });
host.register('demo', process.argv[3]); console.log(await host.listen());
process.on('SIGTERM', () => void host.close().then(() => process.exit(0)));
