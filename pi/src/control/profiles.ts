import { accessSync, constants } from 'node:fs';

export const IN_PROCESS_PROFILE = 'macos-in-process-v1';
export const RUNNER_PROFILE = 'macos-runner-v1';
export const isNoForkProfile = (value: string) => [IN_PROCESS_PROFILE, RUNNER_PROFILE].includes(value);
export const SANDBOX_EXEC = '/usr/bin/sandbox-exec';
// This bounds process creation only; filesystem/network access still uses HOST identity.
export const NO_FORK_POLICY = '(version 1)\n(allow default)\n(deny process-fork)\n';
export function executionProfile(value = 'native') {
  if (!['native', IN_PROCESS_PROFILE, RUNNER_PROFILE].includes(value)) throw new Error('EXECUTION_PROFILE_UNSUPPORTED');
  if (isNoForkProfile(value)) {
    if (process.platform !== 'darwin') throw new Error('MACOS_EXECUTION_PROFILE_REQUIRED');
    try { accessSync(SANDBOX_EXEC, constants.X_OK); } catch { throw new Error('MACOS_SANDBOX_UNAVAILABLE'); }
  }
  return value;
}
