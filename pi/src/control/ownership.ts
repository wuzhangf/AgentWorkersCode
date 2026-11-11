import koffi from 'koffi';

// Windows x64 ABI. The handle is never inherited: host death closes it and kills the job.
const kernel = process.platform === 'win32' ? koffi.load('kernel32.dll') : null;
const create = kernel?.func('void * __stdcall CreateJobObjectW(void *, void *)');
const setInfo = kernel?.func('int __stdcall SetInformationJobObject(void *, int, void *, uint32_t)');
const open = kernel?.func('void * __stdcall OpenProcess(uint32_t, int, uint32_t)');
const assign = kernel?.func('int __stdcall AssignProcessToJobObject(void *, void *)');
const terminate = kernel?.func('int __stdcall TerminateJobObject(void *, uint32_t)');
const query = kernel?.func('int __stdcall QueryInformationJobObject(void *, int, void *, uint32_t, void *)');
const close = kernel?.func('int __stdcall CloseHandle(void *)');

export class WindowsJob {
  private handle: any;
  constructor() {
    if (process.platform !== 'win32' || process.arch !== 'x64') throw new Error('WINDOWS_X64_JOB_REQUIRED');
    this.handle = create!(null, null); if (!this.handle) throw new Error('JOB_CREATE_FAILED');
    const limits = Buffer.alloc(144); limits.writeUInt32LE(0x2000, 16); // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if (!setInfo!(this.handle, 9, limits, limits.length)) { this.close(); throw new Error('JOB_LIMIT_FAILED'); }
  }
  assign(pid: number) {
    const processHandle = open!(0x0101 | 0x1000, 0, pid); if (!processHandle) throw new Error('OWNED_PROCESS_HANDLE_REQUIRED');
    try { if (!assign!(this.handle, processHandle)) throw new Error('JOB_ASSIGN_FAILED'); } finally { close!(processHandle); }
  }
  active() { const info = Buffer.alloc(48); if (!this.handle || !query!(this.handle, 1, info, info.length, null)) throw new Error('JOB_OBSERVATION_FAILED'); return info.readUInt32LE(40); }
  async stop() {
    if (!this.handle) return false;
    if (!terminate!(this.handle, 1)) return false;
    for (let n = 0; n < 200; n++) { if (this.active() === 0) return true; await new Promise(r => setTimeout(r, 10)); }
    return false;
  }
  close() { if (this.handle) { close!(this.handle); this.handle = null; } }
}
