import { PiHost } from '../dist/host/server.js';
const fixture = new URL('./fixture-child.mjs', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1');
const host = new PiHost({ state: process.argv[2], token: process.argv[4], childModule: fixture, faultPoint: process.argv[5] || undefined, maxActiveTasks: Number(process.argv[6] || 0) || undefined });
host.register('demo', process.argv[3]);
console.log(await host.listen());
