import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { hash, digest } from './store.js';
import { isNoForkProfile, RUNNER_PROFILE, SANDBOX_EXEC, NO_FORK_POLICY } from '../control/profiles.js';
import { runnerEntry } from './runner.js';

/** Fingerprint explicitly enumerated installed bytes; this is not a full dependency attestation. */
export function engineBinding(executionProfile = 'native') {
  const sdkEntry = fileURLToPath(import.meta.resolve('@earendil-works/pi-coding-agent'));
  const sdkRoot = dirname(dirname(sdkEntry));
  const sdkManifest = JSON.parse(readFileSync(join(sdkRoot, 'package.json'), 'utf8'));
  if (sdkManifest.name !== '@earendil-works/pi-coding-agent' || sdkManifest.version !== '1.4.0') throw new Error('ENGINE_PACKAGE_CHANGED');
  const workerRoot = fileURLToPath(new URL('..', import.meta.url));
  const rows: { name: string; sha256: string; size: number }[] = [];
  const add = (name: string, file: string) => { const bytes = readFileSync(file); rows.push({ name, sha256: hash(bytes), size: bytes.length }); };
  const walkJs = (root: string, prefix: string, path = root) => {
    for (const item of readdirSync(path, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const file = join(path, item.name);
      if (item.isDirectory()) walkJs(root, prefix, file);
      else if (item.isFile() && item.name.endsWith('.js')) add(prefix + '/' + relative(root, file).replaceAll('\\', '/'), file);
    }
  };
  add('node/executable', process.execPath);
  add('sdk/package.json', join(sdkRoot, 'package.json'));
  add('worker/package-lock.json', join(workerRoot, '..', 'package-lock.json'));
  if (isNoForkProfile(executionProfile)) add('sandbox/executable', SANDBOX_EXEC);
  if (executionProfile === RUNNER_PROFILE) {
    for (const name of ['guardian.mjs', 'main.mjs', 'runner.mjs']) add('command-runner/' + name, join(dirname(runnerEntry), name));
  }
  walkJs(join(sdkRoot, 'dist'), 'sdk/dist'); walkJs(workerRoot, 'worker/dist');
  rows.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  const provenance = { schemaVersion: 'pi-engine-binding/1', kind: 'NODE_EXE_SDK_DIST_JS_WORKER_JS_NPM_LOCK_V1', nodeVersion: process.version, executionProfile, ...(isNoForkProfile(executionProfile) ? { processCreationPolicy: { wrapper: SANDBOX_EXEC, policySha256: hash(NO_FORK_POLICY), policy: 'DENY_PROCESS_FORK' } } : {}), sdk: { name: sdkManifest.name, version: sdkManifest.version }, coverage: 'Enumerated Node executable, Pi SDK manifest/dist JS, compiled Worker JS and npm lock bytes; selected no-fork profile additionally fingerprints sandbox-exec and its policy; selected Runner profile additionally fingerprints the fixed command-runner main/runner/guardian source bytes; transitive installed dependency verification remains GAP', files: rows };
  return { digest: digest(provenance), provenance };
}
