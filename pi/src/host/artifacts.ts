import { lstatSync, realpathSync, readFileSync } from 'node:fs';
import { resolve, relative, isAbsolute, sep } from 'node:path';
import { execFileSync } from 'node:child_process';
import { Store, hash, type Row } from './store.js';

export function git(cwd: string, args: string[]) { return execFileSync('git', ['-c', 'core.longpaths=true', '-C', cwd, ...args], { encoding: 'utf8', timeout: 30000, maxBuffer: 2 * 1024 * 1024, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] }).trimEnd(); }
/** Binary-capable git read: `--binary` diffs and bundles carry NUL bytes, so
 *  utf8 decoding would corrupt both content and digest. Bounded by maxBuffer —
 *  an oversized stream rejects rather than truncating mid-record. */
export function gitBytes(cwd: string, args: string[], max: number) { return execFileSync('git', ['-c', 'core.longpaths=true', '-C', cwd, ...args], { encoding: 'buffer', timeout: 30000, maxBuffer: max + 1, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] }) as Buffer; }
export function inspectProject(projectId: string, path: string) {
  const root = realpathSync(path);
  if (realpathSync(git(root, ['rev-parse', '--show-toplevel'])) !== root) throw new Error('SELECT_GIT_ROOT');
  return { projectId, displayName: projectId, path: root, headRevision: git(root, ['rev-parse', 'HEAD']), hasUncommittedChanges: git(root, ['status', '--porcelain']).length > 0 };
}
export function artifacts(store: Store, task: Row) {
  return store.transaction(() => sealArtifacts(store, task));
}
function sealArtifacts(store: Store, task: Row) {
  if (task.artifactManifest) return task.artifactManifest;
  if (!task.quiescent) return { items: [], reasonCode: 'STOP_TO_SEAL_RESULTS' };
  const root = realpathSync(task.workspace);
  if (root !== realpathSync(resolve(store.directory, 'workspaces', task.taskId))) throw new Error('OWNED_WORKSPACE_REQUIRED');
  const names = new Map<string, string>();
  for (const name of git(root, ['ls-files', '--others', '--exclude-standard', '-z']).split('\0')) if (name) names.set(name, 'A');
  const changes = git(root, ['diff', '--name-status', '-z', '--no-renames', task.baseCommit]).split('\0');
  for (let i = 0; i + 1 < changes.length; i += 2) if (changes[i + 1]) names.set(changes[i + 1], changes[i]);
  const items: Row[] = []; let total = 0;
  for (const [name, status] of [...names.entries()].sort().slice(0, 100)) {
    const row: Row = { artifactId: 'file-' + hash(name + '\0' + task.segmentId), path: name }; items.push(row);
    if (/[\x00-\x1f\x7f]/.test(name)) { row.status = 'WITHHELD_PATH'; continue; }
    const candidate = resolve(root, name); const rel = relative(root, candidate);
    if (isAbsolute(rel) || rel === '..' || rel.startsWith('..' + sep)) { row.status = 'WITHHELD_PATH'; continue; }
    if (status === 'D') { row.status = 'DELETED'; continue; }
    try {
      const before = lstatSync(candidate);
      if (!before.isFile() || before.isSymbolicLink() || !realpathSync(candidate).startsWith(root + sep)) { row.status = 'WITHHELD_PATH'; continue; }
      if (before.size > 524288 || total + before.size > 4194304) { row.status = 'WITHHELD_SIZE'; continue; }
      const bytes = readFileSync(candidate); const after = lstatSync(candidate);
      if (before.mtimeMs !== after.mtimeMs || before.size !== after.size || before.ino !== after.ino) { row.status = 'WITHHELD_CHANGED'; continue; }
      const text = bytes.toString('utf8');
      const knownSecrets = Object.entries(process.env).filter(([k, v]) => /(?:KEY|PASSWORD|TOKEN|SECRET)$/.test(k) && v && v.length >= 8).map(([, v]) => v!);
      if (knownSecrets.some(secret => text.includes(secret)) || /(?:sk-[A-Za-z0-9_-]{20,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:api[_-]?key|password|access[_-]?token)\s*[:=]\s*["']?[^\s"']{8,})/i.test(text)) { row.status = 'WITHHELD_SECRET'; continue; }
      total += bytes.length; row.status = 'AVAILABLE'; row.size = bytes.length; row.sha256 = hash(bytes); row.mediaType = 'application/octet-stream';
      // With no published current manifest, a same-segment row is unpublished crash residue.
      store.db.prepare('INSERT OR REPLACE INTO artifacts VALUES (?,?,?)').run(task.taskId, row.artifactId, bytes);
    } catch { row.status = 'WITHHELD_UNREADABLE'; }
  }
  const manifest = { items, bounded: names.size > items.length, coverage: 'CHANGED_OWNED_FILES', baseline: task.baseCommit, segmentId: task.segmentId };
  task.artifactManifest = manifest; store.save(task); return manifest;
}
export function artifact(store: Store, task: Row, artifactId: string) {
  const current = artifacts(store, task); const history = task.artifactHistory ?? [];
  const row = [...current.items, ...history.flatMap((manifest: Row) => manifest.items)].find((r: Row) => r.artifactId === artifactId && r.status === 'AVAILABLE');
  if (!row) throw new Error('ARTIFACT_NOT_AVAILABLE');
  const bytes = Buffer.from(store.db.prepare('SELECT bytes FROM artifacts WHERE task=? AND id=?').get(task.taskId, artifactId)!.bytes as Uint8Array);
  if (hash(bytes) !== row.sha256) throw new Error('ARTIFACT_CHANGED');
  return { artifactId, path: row.path, sha256: row.sha256, encoding: 'base64', bytes: bytes.toString('base64') };
}
