import { spawn } from 'node:child_process';
let guardianAuthorized = false;
process.once('disconnect', () => { if (process.platform !== 'darwin' || !guardianAuthorized) process.exit(1); });
process.once('message', (message: any) => {
  // The first configuration is released only after platform ownership is armed.
  guardianAuthorized = true;
  if (process.platform === 'darwin') setInterval(() => {}, 60000);
  const child = spawn('git', ['-c', 'core.longpaths=true', '-C', message.cwd, ...message.args], { stdio: 'ignore', windowsHide: true });
  child.once('error', () => process.exit(1)); child.once('exit', code => process.exit(code ?? 1));
});
