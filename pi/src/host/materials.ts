import { randomUUID } from 'node:crypto';
import { mkdirSync, openSync, closeSync, writeSync, readFileSync, existsSync, renameSync, rmSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { Store, type Row, id, digest, exact, now, hash } from './store.js';

/** Project-scoped material upload profile (CA-ANDROID-V2 §11.2). Bytes live
 *  under <state>/materials as <uploadKey>.part until sealed, then .sealed;
 *  displayName is presentation only and never becomes a host path. Chunks are
 *  raw bytes bounded to 256 KiB, idempotent by offset+digest; seal binds the
 *  declared whole sha256 before a materialRef may be referenced by messages. */
export const MATERIAL_CHUNK_MAX = 262144;
const num = (env: string, dflt: number) => { const n = Number(process.env[env]); return Number.isFinite(n) && n > 0 ? Math.floor(n) : dflt; };
const sha = (v: any) => typeof v === 'string' && /^[0-9a-f]{64}$/.test(v);

export class Materials {
  constructor(readonly store: Store, readonly directory: string) { mkdirSync(directory, { recursive: true }); }
  chunkSize() { return MATERIAL_CHUNK_MAX; }
  ttlMs() { return num('NWF_PI_MATERIAL_TTL_MS', 86400000); }
  imageMax() { return num('NWF_PI_MATERIAL_IMAGE_MAX_BYTES', 5242880); }
  textMax() { return num('NWF_PI_MATERIAL_TEXT_MAX_BYTES', 1048576); }
  bundleMax() { return num('NWF_PI_MATERIAL_BUNDLE_MAX_BYTES', 67108864); }
  maxUploads() { return num('NWF_PI_MATERIAL_MAX_UPLOADS', 256); }
  maxPerMessage() { return num('NWF_PI_MATERIAL_MAX_PER_MESSAGE', 4); }
  totalMax() { return num('NWF_PI_MATERIAL_TOTAL_MAX_BYTES', 12582912); }
  /** First material profile: PNG/JPEG images, UTF-8 TXT/MD/LOG/JSON reference
   *  text, plus GIT_BUNDLE/PATCH_BINARY for handoff delivery (not attachable
   *  to a chat message). Per-kind size ceilings are host-configured bounds. */
  media(mediaType: any): Row | null {
    const table: Record<string, Row> = {
      'image/png': { role: 'IMAGE', kind: 'IMAGE', maxBytes: this.imageMax(), messageAttach: true },
      'image/jpeg': { role: 'IMAGE', kind: 'IMAGE', maxBytes: this.imageMax(), messageAttach: true },
      'text/plain': { role: 'REFERENCE_TEXT', kind: 'TEXT', maxBytes: this.textMax(), messageAttach: true },
      'text/markdown': { role: 'REFERENCE_TEXT', kind: 'TEXT', maxBytes: this.textMax(), messageAttach: true },
      'text/x-markdown': { role: 'REFERENCE_TEXT', kind: 'TEXT', maxBytes: this.textMax(), messageAttach: true },
      'text/x-log': { role: 'REFERENCE_TEXT', kind: 'TEXT', maxBytes: this.textMax(), messageAttach: true },
      'text/json': { role: 'REFERENCE_TEXT', kind: 'TEXT', maxBytes: this.textMax(), messageAttach: true },
      'application/json': { role: 'REFERENCE_TEXT', kind: 'TEXT', maxBytes: this.textMax(), messageAttach: true },
      'application/x-git-bundle': { role: 'GIT_BUNDLE', kind: 'GIT_BUNDLE', maxBytes: this.bundleMax(), messageAttach: false },
      'application/x-binary-patch': { role: 'PATCH_BINARY', kind: 'PATCH_BINARY', maxBytes: this.bundleMax(), messageAttach: false },
    };
    return table[String(mediaType)] ?? null;
  }
  mediaLimits() { return { chunkBytes: this.chunkSize(), imageMaxBytes: this.imageMax(), textMaxBytes: this.textMax(), bundleMaxBytes: this.bundleMax(), perMessageMax: this.maxPerMessage(), perMessageTotalMaxBytes: this.totalMax(), ttlMs: this.ttlMs(), mediaTypes: ['image/png', 'image/jpeg', 'text/plain', 'text/markdown', 'text/x-markdown', 'text/x-log', 'text/json', 'application/json', 'application/x-git-bundle', 'application/x-binary-patch'] }; }
  private partPath(row: Row) { return join(this.directory, row.uploadKey + '.part'); }
  private sealedPath(row: Row) { return join(this.directory, row.uploadKey + '.sealed'); }
  public(row: Row) { return { schemaVersion: 'material-upload/1', uploadKey: row.uploadKey, projectId: row.projectId, target: row.target, displayName: row.displayName, mediaType: row.mediaType, mediaRole: row.mediaRole, sizeBytes: row.sizeBytes, sha256: row.sha256, nextOffset: row.nextOffset, state: row.state, chunkSize: row.chunkSize, materialRef: row.materialRef ?? null, expiresAt: row.expiresAt, createdAt: row.createdAt, sealedAt: row.sealedAt ?? null, boundAt: row.boundAt ?? null, consumedAt: row.consumedAt ?? null }; }
  private touch(row: Row): Row {
    if (['UPLOADING', 'SELECTED'].includes(row.state) && Date.parse(row.expiresAt) <= Date.now()) { row.state = 'EXPIRED'; this.store.saveMaterialUpload(row); }
    return row;
  }
  get(uploadKey: string): Row { const row = this.store.materialUpload(id(uploadKey)); if (!row) throw new Error('MATERIAL_NOT_FOUND'); return this.touch(row); }
  /** A message references the sealed materialRef (or, tolerantly, the raw
   *  uploadKey). Knowing either grants nothing: project/task binding is still
   *  checked at consumption. */
  resolve(ref: any): Row {
    const row = typeof ref === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(ref) ? this.store.materialUploads().find(r => r.uploadKey === ref || r.materialRef === ref) : null;
    if (!row) throw new Error('MATERIAL_NOT_FOUND'); return this.touch(row);
  }
  list(projectId: string): Row[] { return this.store.materialUploads().filter(r => r.projectId === projectId).map(r => this.public(this.touch(r))); }
  create(projectId: string, target: Row, body: Row) {
    exact(body, ['requestKey', 'target', 'displayName', 'mediaType', 'sizeBytes', 'sha256']);
    const previous = this.store.materialUploads().find(r => r.requestKey === body.requestKey);
    if (previous) { if (previous.bodyDigest !== digest(body)) throw new Error('IDEMPOTENCY_CONFLICT'); return this.public(this.touch(previous)); }
    const media = this.media(body.mediaType); if (!media) throw new Error('MATERIAL_MEDIA_UNSUPPORTED');
    if (typeof body.displayName !== 'string' || !body.displayName.trim() || body.displayName.length > 256 || /[\x00-\x1f/\\]/.test(body.displayName)) throw new Error('INVALID_MATERIAL_NAME');
    if (!Number.isSafeInteger(body.sizeBytes) || body.sizeBytes < 1 || body.sizeBytes > media.maxBytes) throw new Error('MATERIAL_SIZE_UNSUPPORTED');
    if (!sha(body.sha256)) throw new Error('INVALID_MATERIAL_SHA256');
    // Expired rows beyond the host cap are pruned before admitting a new upload.
    const live = this.store.materialUploads().filter(r => !['CANCELLED', 'EXPIRED', 'FAILED'].includes(this.touch(r).state));
    if (live.length >= this.maxUploads()) throw new Error('MATERIAL_UPLOAD_LIMIT');
    const uploadKey = 'mat-' + randomUUID();
    const row: Row = { schemaVersion: 'material-upload/1', uploadKey, projectId, target, requestKey: body.requestKey, bodyDigest: digest(body), displayName: body.displayName, mediaType: String(body.mediaType), mediaRole: media.role, mediaKind: media.kind, sizeBytes: body.sizeBytes, sha256: body.sha256, nextOffset: 0, state: 'UPLOADING', chunkSize: this.chunkSize(), chunks: {}, materialRef: null, expiresAt: new Date(Date.now() + this.ttlMs()).toISOString(), createdAt: now() };
    this.store.saveMaterialUpload(row);
    return this.public(row);
  }
  /** Raw chunk write: same offset + same digest confirms (idempotent resume);
   *  a differing payload at an already-stored offset is a hard conflict. New
   *  bytes must extend the contiguous prefix at nextOffset. */
  putChunk(row: Row, offset: number, bytes: Buffer, chunkDigest: any) {
    this.touch(row);
    if (row.state !== 'UPLOADING') throw new Error('MATERIAL_NOT_OPEN');
    if (!sha(chunkDigest)) throw new Error('INVALID_CHUNK_DIGEST');
    if (hash(bytes) !== chunkDigest) throw new Error('MATERIAL_CHUNK_DIGEST_MISMATCH');
    if (!Number.isSafeInteger(offset) || offset < 0 || offset + bytes.length > row.sizeBytes) throw new Error('MATERIAL_OFFSET_INVALID');
    if (bytes.length < 1 || bytes.length > this.chunkSize()) throw new Error('MATERIAL_CHUNK_TOO_LARGE');
    const stored = row.chunks?.[String(offset)];
    if (stored) {
      if (stored.size !== bytes.length || stored.digest !== chunkDigest) throw new Error('MATERIAL_CHUNK_CONFLICT');
      return this.public(row); // duplicate replay of an identical chunk
    }
    if (offset !== row.nextOffset) throw new Error('MATERIAL_OFFSET_INVALID');
    const fd = openSync(this.partPath(row), 'a');
    try { writeSync(fd, bytes, 0, bytes.length, offset); } finally { closeSync(fd); }
    (row.chunks ??= {})[String(offset)] = { size: bytes.length, digest: chunkDigest };
    row.nextOffset = offset + bytes.length;
    this.store.saveMaterialUpload(row);
    return this.public(row);
  }
  seal(row: Row, body: Row) {
    exact(body, ['requestKey', 'sha256']);
    if (row.sealRequestKey) {
      if (row.sealRequestKey !== body.requestKey || row.sealBodyDigest !== digest(body)) throw new Error('IDEMPOTENCY_CONFLICT');
      return this.public(this.touch(row));
    }
    this.touch(row);
    if (row.state !== 'UPLOADING') throw new Error('MATERIAL_NOT_OPEN');
    if (!sha(body.sha256) || body.sha256 !== row.sha256) throw new Error('MATERIAL_SHA256_MISMATCH');
    if (row.nextOffset !== row.sizeBytes) throw new Error('MATERIAL_INCOMPLETE');
    const bytes = readFileSync(this.partPath(row));
    if (bytes.length !== row.sizeBytes || hash(bytes) !== row.sha256) throw new Error('MATERIAL_SHA256_MISMATCH');
    renameSync(this.partPath(row), this.sealedPath(row));
    row.materialRef = 'matref-' + randomUUID();
    row.state = 'SEALED'; row.sealedAt = now(); row.sealRequestKey = body.requestKey; row.sealBodyDigest = digest(body);
    this.store.saveMaterialUpload(row);
    return this.public(row);
  }
  remove(row: Row) {
    this.touch(row);
    if (['BOUND', 'CONSUMED'].includes(row.state)) throw new Error('MATERIAL_BOUND');
    row.state = 'CANCELLED'; this.store.saveMaterialUpload(row);
    for (const p of [this.partPath(row), this.sealedPath(row)]) try { rmSync(p, { force: true }); } catch {}
    return this.public(row);
  }
  bytes(row: Row): Buffer {
    const file = this.sealedPath(row);
    if (!existsSync(file) || statSync(file).size !== row.sizeBytes) throw new Error('MATERIAL_CHANGED');
    const bytes = readFileSync(file);
    if (hash(bytes) !== row.sha256) throw new Error('MATERIAL_CHANGED');
    return bytes;
  }
  /** Validate a message materials[] whitelist and assemble the honest first-
   *  version consumption: REFERENCE_TEXT bytes are injected into the message
   *  content as bounded text (materialRef kept on the receipt); IMAGE is
   *  refused — the Pi SDK image-input mapping is unverified, so the host does
   *  not claim multimodal assembly. */
  attach(task: Row, materials: any, createKey: string | null) {
    if (!Array.isArray(materials) || !materials.length || materials.length > this.maxPerMessage()) throw new Error('MATERIALS_INVALID');
    const refs: Row[] = []; let injected = ''; let total = 0;
    for (const m of materials) {
      if (!m || typeof m !== 'object' || Array.isArray(m)) throw new Error('MATERIALS_INVALID');
      exact(m, ['materialRef', 'sha256', 'role']);
      const row = this.resolve(m.materialRef);
      if (!sha(m.sha256) || m.sha256 !== row.sha256) throw new Error('MATERIAL_SHA256_MISMATCH');
      if (!['IMAGE', 'REFERENCE_TEXT'].includes(m.role)) throw new Error('MATERIALS_INVALID');
      if (row.state !== 'SEALED') throw new Error('MATERIAL_NOT_SEALED');
      const bound = row.target?.taskId === task.taskId || (row.target?.createKey !== undefined && row.target.createKey === createKey);
      if (!bound) throw new Error('MATERIAL_TARGET_MISMATCH');
      const media = this.media(row.mediaType);
      if (!media || media.messageAttach === false) throw new Error('MATERIALS_UNSUPPORTED_INTENT');
      if (m.role !== media.role) throw new Error('MATERIALS_ROLE_MISMATCH');
      if (m.role === 'IMAGE') throw new Error('MODEL_IMAGE_INPUT_UNSUPPORTED');
      total += row.sizeBytes; if (total > this.totalMax()) throw new Error('MATERIAL_TOO_LARGE');
      const text = this.bytes(row).toString('utf8').slice(0, 65536);
      const name = String(row.displayName).replace(/[<>&"']/g, '_');
      injected += `\n\n<reference-material materialRef="${row.uploadKey}" sha256="${row.sha256}" name="${name}" mediaType="${row.mediaType}">\n${text}\n</reference-material>`;
      refs.push({ materialRef: row.materialRef ?? row.uploadKey, uploadKey: row.uploadKey, sha256: row.sha256, role: m.role, displayName: row.displayName, mediaType: row.mediaType, sizeBytes: row.sizeBytes, assembledAs: 'REFERENCE_TEXT_INJECTED' });
    }
    return { refs, injectedText: injected };
  }
  mark(refs: Row[], state: 'BOUND' | 'CONSUMED', fields: Row = {}) {
    for (const ref of refs) {
      const row = this.store.materialUpload(ref.uploadKey ?? ref.materialRef); if (!row) continue;
      if (state === 'BOUND' && row.state === 'SEALED') { row.state = 'BOUND'; row.boundAt = now(); }
      if (state === 'CONSUMED' && ['SEALED', 'BOUND'].includes(row.state)) { row.state = 'CONSUMED'; row.consumedAt = now(); }
      Object.assign(row, fields);
      this.store.saveMaterialUpload(row);
    }
  }
}
