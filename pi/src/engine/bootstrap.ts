// No SDK or tool code executes before platform ownership is armed.
let guardianAuthorized = false;
process.once('disconnect', () => { if (process.platform !== 'darwin' || !guardianAuthorized) process.exit(0); });
process.once('message', async message => {
  guardianAuthorized = true;
  if (process.platform === 'darwin') setInterval(() => {}, 60000); // Only the armed guardian owns teardown now.
  await import('./child.js');
  process.emit('message', message as any, undefined);
});
