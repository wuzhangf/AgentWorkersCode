import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const MAC_QUIESCENCE_METHOD = 'DARWIN_KQUEUE_NO_FORK_GROUP_ZERO';
export class MacOwnership {
  private guardian: ChildProcess;
  proof: any;
  private constructor() {
    this.guardian = fork(fileURLToPath(new URL('./macos-guardian.js', import.meta.url)), [], { detached: true, execArgv: [], env: {}, stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  }
  static async arm(pid: number) {
    const ownership = new MacOwnership();
    await new Promise<void>((done, reject) => {
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) return; settled = true; clearTimeout(timer);
        ownership.guardian.off('error', failed); ownership.guardian.off('exit', exited); ownership.guardian.off('message', message);
        if (error) { ownership.close(); reject(error); } else done();
      };
      const failed = () => finish(new Error('MAC_GUARDIAN_UNAVAILABLE'));
      const exited = () => failed();
      const message = (m: any) => finish(m.type === 'armed' && m.ok ? undefined : new Error('MAC_OBSERVATION_UNAVAILABLE'));
      const timer = setTimeout(failed, 5000);
      ownership.guardian.once('error', failed); ownership.guardian.once('exit', exited); ownership.guardian.once('message', message);
      ownership.guardian.send({ type: 'arm', pid }, error => { if (error) failed(); });
    });
    return ownership;
  }
  async stop() {
    if (this.proof) return this.proof.quiescent === true;
    if (!this.guardian.connected) return false;
    return new Promise<boolean>(done => {
      let settled = false;
      const finish = (value: boolean) => {
        if (settled) return; settled = true; clearTimeout(timer);
        this.guardian.off('error', failed); this.guardian.off('exit', failed); this.guardian.off('message', message);
        if (!value) this.close(); done(value);
      };
      const failed = () => finish(false);
      const message = (m: any) => { this.proof = m.proof; finish(m.type === 'proof' && m.proof.quiescent === true); };
      const timer = setTimeout(failed, 5000);
      this.guardian.once('message', message); this.guardian.once('error', failed); this.guardian.once('exit', failed);
      this.guardian.send({ type: 'stop' }, error => { if (error) failed(); });
    });
  }
  close() { if (this.guardian.connected) this.guardian.disconnect(); }
}
