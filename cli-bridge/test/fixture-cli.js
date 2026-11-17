#!/usr/bin/env node
// Deterministic stand-in for a stream-json vendor CLI (claude shape), used by
// host.test.mjs — no model, no network, no real agent.
// Behaviour is read per invocation from FIXTURE_CONF (JSON file, rewritten by
// the test between turns):
//   hold        — keep running silently while this file exists
//   delayMs     — wait this long before the terminal result row
//   pipeHoldMs  — spawn a detached survivor that inherits our stdout/stderr,
//                 delaying the host's 'close' after we are killed
//   sessionId   — session_id to report (default: fresh uuid)
//   argvLog     — append our argv (one JSON line) to this file
//   reply       — assistant text to emit (default 'fixture reply')
const { appendFileSync, existsSync, readFileSync } = require('node:fs');
const { spawn } = require('node:child_process');
const { randomUUID } = require('node:crypto');

let conf = {};
try { conf = JSON.parse(readFileSync(process.env.FIXTURE_CONF ?? '', 'utf8')); } catch {}
const argv = process.argv.slice(2);
// The bridge's startup engineVersion probe also runs us — keep the log to
// real turn invocations so tests can count dispatches.
if (conf.argvLog && !argv.includes('--version')) appendFileSync(conf.argvLog, JSON.stringify(argv) + '\n');
if (argv.includes('--version')) { console.log('fixture-cli 0.0.0'); process.exit(0); }
if (conf.pipeHoldMs) {
  spawn(process.execPath, ['-e', `setTimeout(() => {}, ${Number(conf.pipeHoldMs)})`],
    { stdio: ['ignore', 1, 2], detached: true, windowsHide: true }).unref();
}
const session = conf.sessionId || randomUUID();
// noSession — emit init/result rows without session_id so the bridge never
// gains a resumable native id (engine emitted none).
const sid = conf.noSession ? undefined : session;
const emit = (row) => process.stdout.write(JSON.stringify(row) + '\n');
const done = () => {
  emit({ type: 'assistant', message: { content: [{ type: 'text', text: conf.reply ?? 'fixture reply' }] } });
  emit({ type: 'result', subtype: 'success', is_error: false, session_id: sid, result: 'fixture done' });
  process.exit(0);
};
emit({ type: 'system', subtype: 'init', session_id: sid });
if (conf.hold) {
  setInterval(() => { if (!existsSync(conf.hold)) done(); }, 25);
} else if (conf.delayMs) {
  setTimeout(done, Number(conf.delayMs));
} else {
  done();
}
