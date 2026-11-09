import { PiHost } from '../dist/host/server.js';
const host = new PiHost({ state: process.argv[2], token: process.argv[4], childModule: new URL('./fixture-child.mjs', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'), faultPoint: 'dispatching' });
host.register('demo', process.argv[3]);
console.log(await host.listen());
