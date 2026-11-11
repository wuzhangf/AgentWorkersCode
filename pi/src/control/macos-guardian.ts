// Separate from the model runtime: an inherited host IPC channel is the lifetime lease.
// Darwin NOTE_FORK is deliberately a permanent proof veto: it cannot track children
// across setsid/double-fork (NOTE_TRACK has been unsupported since macOS 10.5).
import koffi from 'koffi';
const libc = koffi.load('/usr/lib/libSystem.B.dylib');
const kqueue = libc.func('int kqueue()');
const kevent = libc.func('int kevent64(int, void *, int, void *, int, uint32_t, void *)');
const close = libc.func('int close(int)');
let queue = -1, pid = 0, forked = false, exited = false, failed = false, stopping = false;
const timeout = Buffer.alloc(16), events = Buffer.alloc(48 * 8);
function observe() {
  if (queue < 0) return;
  for (;;) {
    const n = kevent(queue, null, 0, events, 8, 0, timeout);
    if (n < 0) { failed = true; return; }
    if (n === 0) return;
    for (let i = 0; i < n; i++) {
      const offset = i * 48, flags = events.readUInt32LE(offset + 12);
      if (events.readUInt16LE(offset + 10) & 0x4000) failed = true;
      if (flags & 0x40000000) forked = true;
      if (flags & 0x80000000) exited = true;
    }
  }
}
async function stop(reply: boolean) {
  if (stopping) return; stopping = true;
  observe();
  // While the gated root is still alive its PGID cannot be reused. If its exit
  // was already observed, do not signal a potentially recycled numeric group.
  if (!exited && !failed) { try { process.kill(-pid, 'SIGKILL'); } catch {} }
  let groupZero = false;
  for (let n = 0; n < 200; n++) {
    observe();
    try { process.kill(-pid, 0); } catch (e: any) { if (e.code === 'ESRCH') groupZero = true; }
    if (exited && groupZero) break;
    await new Promise(r => setTimeout(r, 10));
  }
  const proof = { method: 'DARWIN_KQUEUE_NO_FORK_GROUP_ZERO', quiescent: !failed && !forked && exited && groupZero, forkObserved: forked, rootExited: exited, groupZero, observationFailed: failed };
  if (reply && process.connected) process.send?.({ type: 'proof', proof }, undefined, {}, () => { if (queue >= 0) close(queue); if (process.connected) process.disconnect(); });
  else { if (queue >= 0) close(queue); process.exit(0); }
}
process.once('disconnect', () => { void stop(false); });
process.on('message', (message: any) => {
  if (message.type === 'arm') {
    pid = message.pid; queue = kqueue(); const change = Buffer.alloc(48);
    change.writeBigUInt64LE(BigInt(pid), 0); change.writeInt16LE(-5, 8); change.writeUInt16LE(0x21, 10); change.writeUInt32LE(0xc0000000, 12);
    if (queue < 0 || kevent(queue, change, 1, null, 0, 0, timeout) < 0) failed = true;
    observe(); process.send?.({ type: 'armed', ok: !failed });
  } else if (message.type === 'stop') void stop(true);
});
setInterval(observe, 5).unref();
