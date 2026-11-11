import { API_PREFIX } from './protocol.js';

/**
 * Mirror of agent-worker-protocol Paths.java — attempt-lane routes only.
 * Stage delivery paths (devopsStage*) are out of W6 scope and stay absent
 * rather than being sketched incorrectly.
 */
const enc = encodeURIComponent;

export const registrations = () => API_PREFIX + '/registrations';
export const heartbeatPath = (instanceId: string) => `${API_PREFIX}/instances/${enc(instanceId)}/heartbeats`;
export const commandsPath = (instanceId: string) => `${API_PREFIX}/instances/${enc(instanceId)}/commands`;
/** `?after={decimal}&waitSeconds={n}` — `after` omitted only when null. */
export const commandsQuery = (instanceId: string, after: string | null, waitSeconds: number) => {
  let q = commandsPath(instanceId) + '?';
  if (after != null) q += 'after=' + enc(after) + '&';
  return q + 'waitSeconds=' + enc(String(waitSeconds));
};
export const receiptPath = (instanceId: string, commandId: string) => `${commandsPath(instanceId)}/${enc(commandId)}/receipt`;
export const eventsPath = (instanceId: string) => `${API_PREFIX}/instances/${enc(instanceId)}/events:batch`;
export const approvalRequestsPath = (attemptId: string) => `${API_PREFIX}/attempts/${enc(attemptId)}/approval-requests`;
export const uploadIntentsPath = (attemptId: string) => `${API_PREFIX}/attempts/${enc(attemptId)}/artifacts/upload-intents`;
export const artifactCompletePath = (attemptId: string, artifactId: string) => `${API_PREFIX}/attempts/${enc(attemptId)}/artifacts/${enc(artifactId)}/complete`;
export const finishPath = (attemptId: string) => `${API_PREFIX}/attempts/${enc(attemptId)}/finish`;
export const finishReceiptPath = (attemptId: string, finishId: string) => `${API_PREFIX}/attempts/${enc(attemptId)}/finish-receipts/${enc(finishId)}`;
export const recoveries = () => API_PREFIX + '/recoveries';
