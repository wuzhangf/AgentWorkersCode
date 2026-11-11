import { isSha256Digest, isWireId, parseTime } from './protocol.js';

/**
 * Mirror of protocol Fields.java: every parser throws BAD_FIELD:<name>
 * on a malformed value — the caller decides whether the failure is a
 * command reject or a plane-response bug. Longs are integer JSON
 * numbers; epochs/sequences stay decimal STRINGS end to end.
 */
export type Obj = Record<string, unknown>;

const bad = (field: string): never => { throw new Error('BAD_FIELD:' + field); };

export const isObj = (v: unknown): v is Obj => !!v && typeof v === 'object' && !Array.isArray(v);

export function reqString(o: Obj, field: string, maxLen = 1024): string {
  const v = o[field];
  if (typeof v !== 'string' || v.length > maxLen) bad(field);
  return v as string;
}
export function optString(o: Obj, field: string, maxLen = 1024): string | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  if (typeof v !== 'string' || v.length > maxLen) bad(field);
  return v as string;
}
export function reqId(o: Obj, field: string): string {
  const v = o[field];
  if (!isWireId(v)) bad(field);
  return v as string;
}
export function optId(o: Obj, field: string): string | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  if (!isWireId(v)) bad(field);
  return v as string;
}
/** Decimal grammar: an optional minus then digits — Long.parseLong shape. */
export const isDecimalString = (v: unknown): v is string => typeof v === 'string' && /^-?[0-9]+$/.test(v) && v.length <= 20;
export function reqDecimalString(o: Obj, field: string): string {
  const v = o[field];
  if (!isDecimalString(v)) bad(field);
  try { BigInt(v as string); } catch { bad(field); }
  return v as string;
}
export function optDecimalString(o: Obj, field: string): string | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  return reqDecimalString(o, field);
}
export function reqLong(o: Obj, field: string): number {
  const v = o[field];
  if (typeof v !== 'number' || !Number.isSafeInteger(v)) bad(field);
  return v as number;
}
export function optLong(o: Obj, field: string): number | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  return reqLong(o, field);
}
export function reqBool(o: Obj, field: string): boolean {
  const v = o[field];
  if (typeof v !== 'boolean') bad(field);
  return v as boolean;
}
export function optBool(o: Obj, field: string): boolean | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  if (typeof v !== 'boolean') bad(field);
  return v as boolean;
}
export function reqObj(o: Obj, field: string): Obj {
  const v = o[field];
  if (!isObj(v)) bad(field);
  return v as Obj;
}
export function optObj(o: Obj, field: string): Obj | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  if (!isObj(v)) bad(field);
  return v as Obj;
}
export function reqArr(o: Obj, field: string): unknown[] {
  const v = o[field];
  if (!Array.isArray(v)) bad(field);
  return v as unknown[];
}
export function optArr(o: Obj, field: string): unknown[] | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  if (!Array.isArray(v)) bad(field);
  return v as unknown[];
}
export function reqEnum(o: Obj, field: string, allowed: readonly string[]): string {
  const v = reqString(o, field);
  if (!allowed.includes(v)) bad(field);
  return v;
}
export function optEnum(o: Obj, field: string, allowed: readonly string[]): string | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  return reqEnum(o, field, allowed);
}
export function reqTime(o: Obj, field: string): string {
  const v = o[field];
  parseTime(field, v);
  return v as string;
}
export function optTime(o: Obj, field: string): string | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  parseTime(field, v);
  return v as string;
}
export function reqDigest(o: Obj, field: string): string {
  const v = o[field];
  if (!isSha256Digest(v)) bad(field);
  return v as string;
}
export function optDigest(o: Obj, field: string): string | null {
  const v = o[field];
  if (v === undefined || v === null) return null;
  if (!isSha256Digest(v)) bad(field);
  return v as string;
}
export function stringList(arr: unknown[], field: string, maxLen = 1024): string[] {
  const out: string[] = [];
  for (const v of arr) {
    if (typeof v !== 'string' || v.length > maxLen) bad(field);
    out.push(v as string);
  }
  return out;
}
