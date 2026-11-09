import { randomUUID } from 'node:crypto';
import { canonical, digest, hash } from '../host/store.js';

/**
 * W6: wire-protocol constants + primitives mirrored from the Java
 * agent-worker-protocol module (design 03 §5–6). Identifier grammar,
 * digest format and canonical-JSON rules are byte-identical — the
 * canonical writer is the same implementation the host store uses,
 * which is already golden-vector tested against the Java CanonicalJson.
 */

export const PROTOCOL_NAME = 'agent-worker/1.4';
export const PROTOCOL_V1_3 = 'agent-worker/1.3';
export const PROTOCOL_V1_2 = 'agent-worker/1.2';
export const PROTOCOL_V1_1 = 'agent-worker/1.1';
export const PROTOCOL_V1_0 = 'agent-worker/1.0';
export const SUPPORTED_PROTOCOLS = [PROTOCOL_NAME, PROTOCOL_V1_3, PROTOCOL_V1_2, PROTOCOL_V1_1, PROTOCOL_V1_0] as const;
export const isSupportedProtocol = (name: unknown): name is string => typeof name === 'string' && (SUPPORTED_PROTOCOLS as readonly string[]).includes(name);

export const API_PREFIX = '/worker/v1';
/** Attempt-lane event schema (design 03 §6.5); stage facts use 1.1 — not implemented in W6. */
export const EVENT_SCHEMA_VERSION = '1.0';
export const COMMAND_POLL_MAX_WAIT_SECONDS = 20;
export const EVENT_MAX_BYTES = 32 * 1024;
export const EVENT_BATCH_MAX_EVENTS = 100;
export const EVENT_BATCH_MAX_BYTES = 1024 * 1024;
export const HEARTBEAT_INTERVAL_MILLIS = 10_000;
export const LEASE_TTL_MILLIS = 45_000;

const WIRE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
/** Ids.isValid — path-safe alphabet plus the `..` traversal guard. */
export const isWireId = (v: unknown): v is string => typeof v === 'string' && WIRE_ID.test(v) && !v.includes('..');
export const requireWireId = (field: string, v: unknown): string => { if (!isWireId(v)) throw new Error('BAD_FIELD:' + field); return v; };
/** Ids.newId — `prefix-<uuid16hex>`. */
export const newId = (prefix: string) => prefix + '-' + randomUUID().replace(/-/g, '').slice(0, 16);
/** EventEnvelope.newEventId — canonical UUID text. */
export const newEventId = () => randomUUID();

export const SHA256_PREFIX = 'sha256:';
export const isSha256Digest = (v: unknown): v is string => typeof v === 'string' && /^sha256:[0-9a-f]{64}$/.test(v);
export const sha256Bytes = (b: Buffer | Uint8Array) => SHA256_PREFIX + hash(Buffer.from(b));
export const sha256Utf8 = (s: string) => SHA256_PREFIX + hash(s);
/** CanonicalJson.digest — sha256 over canonical UTF-8 bytes. */
export const canonicalDigest = (v: unknown) => SHA256_PREFIX + digest(v);
export const canonicalBytes = (v: unknown): Buffer => Buffer.from(canonical(v), 'utf8');
export const canonicalString = (v: unknown): string => canonical(v);

/** RFC-3339 UTC instant truncated to milliseconds — Java Instant.truncatedTo(MILLIS). */
export const rfc3339 = (ms: number) => new Date(ms).toISOString();
export const nowRfc3339 = () => new Date().toISOString();
export const parseTime = (field: string, v: unknown): number => {
  if (typeof v !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$/.test(v)) throw new Error('BAD_FIELD:' + field);
  const t = Date.parse(v); if (Number.isNaN(t)) throw new Error('BAD_FIELD:' + field);
  return t;
};

/**
 * Map a wire identifier onto the local host's id grammar
 * (/local/v1 forbids ':'), deterministically — the same wire id always
 * yields the same local requestKey, which is what makes adapter
 * restart replay idempotent on the host's own idempotency store.
 */
export const localKey = (prefix: string, wireId: string): string => {
  const mapped = wireId.replace(/[^A-Za-z0-9._-]/g, '-');
  const candidate = (prefix + mapped).slice(0, 128);
  return /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(candidate) ? candidate : (prefix + hash(wireId).slice(0, 96)).slice(0, 128);
};
