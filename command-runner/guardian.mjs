// A gated, single-job process group. The IPC connection is its owner's lease.
import {spawn, spawnSync} from 'node:child_process';
let child, configured = false, stopping = false, escaped = false, observationFailed = false;
const known = new Map([[process.pid,null]]);
const pgid = process.pid;
function census() {
  const result = spawnSync('/bin/ps', ['-axo', 'pid=,ppid=,pgid=,stat=,lstart='], {encoding: 'utf8', env: {PATH: '/usr/bin:/bin'}, timeout: 2000});
  if (result.status !== 0) {observationFailed=true;return;}
  const rows = result.stdout.trim().split('\n').map(line => {
    const [pid, ppid, group, state, ...start] = line.trim().split(/\s+/);
    return {pid: Number(pid), ppid: Number(ppid), group: Number(group), state, start:start.join(' ')};
  });
  if(!rows.some(r=>r.pid===process.pid)){observationFailed=true;return;}
  for(const [pid,start] of known){const row=rows.find(r=>r.pid===pid);if(!row||(start!==null&&row.start!==start))known.delete(pid);else known.set(pid,row.start);}
  // Best-effort observation, not a claim of exhaustive fork tracking.
  for (let pass = 0; pass < 3; pass++) for (const r of rows) {
    if (r.pid !== result.pid && (known.has(r.ppid) || r.group === pgid)) known.set(r.pid,r.start);
  }
  // A toolchain may temporarily put a waited-for compiler child in another
  // group. Only descendants still present outside our group remain unsettled.
  escaped = rows.some(r => known.has(r.pid) && r.pid !== process.pid && r.group !== pgid && !r.state?.startsWith('Z'));
}
function message(value) { if (process.connected) process.send(value, () => {}); }
function terminate() {
  if (stopping) return; stopping = true;
  census();
  // This process retains the group's identity until the kill, avoiding PID reuse.
  const kill=()=>{try { process.kill(-pgid, 'SIGKILL'); } catch { process.exit(1); }};
  if(process.connected){process.send({type:'observation',escaped,observationFailed},kill);setTimeout(kill,100).unref();}
  else kill();
}
process.once('disconnect', terminate);
process.on('message', m => {
  if (m.type === 'stop') return terminate();
  if (m.type !== 'run' || configured || stopping) return;
  configured = true;
  try {
    child = spawn(m.argv[0], m.argv.slice(1), {cwd: m.cwd, env: m.environment, stdio: ['ignore', 'pipe', 'pipe']});
    if(child.pid)known.set(child.pid,null);
    child.stdout.on('data', b => message({type: 'output', stream: 'stdout', bytes: b.toString('base64')}));
    child.stderr.on('data', b => message({type: 'output', stream: 'stderr', bytes: b.toString('base64')}));
    child.once('error', () => message({type: 'result', exitCode: null, signal: null, escaped, observationFailed, launchFailed: true}));
    child.once('close', (exitCode, signal) => { census(); message({type: 'result', exitCode, signal, escaped, observationFailed}); });
  } catch { message({type: 'result', exitCode: null, signal: null, escaped, observationFailed, launchFailed: true}); }
});
setInterval(census, 100).unref();
message({type: 'ready', pgid});
