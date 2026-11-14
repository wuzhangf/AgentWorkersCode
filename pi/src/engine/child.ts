import { createAgentSession, createEventBus, createExtensionRuntime, createMcpExtension, createSyntheticSourceInfo, loadSkillsFromDir, ModelRuntime, SessionManager, SettingsManager, type AgentSession, type Extension, type ResourceLoader } from '@earendil-works/pi-coding-agent';
import { InMemoryCredentialStore, InMemoryModelsStore } from '@earendil-works/pi-ai';
import { Type } from 'typebox';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { isNoForkProfile, RUNNER_PROFILE } from '../control/profiles.js';

const send = (message: any) => { if (process.connected) process.send?.(message); };
let session: AgentSession | undefined;
const modelIndex = new Map<string, any>();
/** Assert the task's model binding on the live session — setModel/setThinkingLevel
 *  are SDK mutations recorded on the session, not a relaunch. */
async function applyModel(binding: any) {
  if (!binding || typeof binding.name !== 'string' || !session) return;
  const target = modelIndex.get(binding.name); if (!target) throw new Error('MODEL_NOT_FOUND');
  if (session.model?.id !== binding.name) await session.setModel(target);
  if (typeof binding.effort === 'string') session.setThinkingLevel(binding.effort);
}
let manager: SessionManager | undefined;
const questions = new Map<string, (v: any) => void>();
const macRequests = new Map<string, (v: any) => void>();
const runnerRequests = new Map<string, (v: any) => void>();
let stopping = false;
let directPrompt: { requestKey: string; waitingUser: boolean } | undefined;

/** Boundary gate: while closed, the next autonomous model request is held inside
 *  `before_provider_request` and new tool admission is held inside `tool_call`
 *  (both hooks are awaited by the SDK, so holding suspends the run without
 *  injecting a synthetic tool error into model context). `session_before_compact`
 *  is cancelled so compaction cannot bypass the gate. The gate is model-side; an
 *  in-flight provider request or already-executing tool still finishes before the
 *  run parks — that is the deterministic boundary the checkpoint seals. */
const gate = { closed: false, holds: new Map<() => void, string>(), heldCallIds: new Set<string>() };
/** toolCallId -> toolName for executions that started and have not ended. */
const inflight = new Map<string, string>();
const pauseWaiters: string[] = [];
/** Short public preview of a tool call's salient argument, bounded like a
 *  commandPreview — the start-side fact the durable activity row renders. */
function toolArgSummary(toolName: string, args: any): string | undefined {
  if (!args || typeof args !== 'object') return undefined;
  const pick = (key: string) => typeof args[key] === 'string' ? args[key] : undefined;
  const salient = pick('command') ?? pick('cmd') ?? pick('path') ?? pick('file_path') ?? pick('pattern') ?? pick('query') ?? pick('url');
  const raw = salient ?? JSON.stringify(args);
  return typeof raw === 'string' && raw ? raw.slice(0, 240) : undefined;
}
/** Bounded error-text preview for a failed tool result. */
function toolErrorSummary(result: any): string | undefined {
  const text = typeof result === 'string' ? result
    : Array.isArray(result?.content) ? result.content.filter((p: any) => p?.type === 'text').map((p: any) => p.text).join(' ')
    : result?.error ?? result?.message ?? (result ? JSON.stringify(result) : '');
  return typeof text === 'string' && text ? text.slice(0, 240) : undefined;
}
/** Bounded public preview of a successful tool result. The model sees the full
 *  body; the durable ledger keeps a small redacted slice so operators can tell
 *  what a command actually returned without storing arbitrary output. */
function toolResultSummary(result: any): string | undefined {
  const text = typeof result === 'string' ? result
    : Array.isArray(result?.content) ? result.content.filter((p: any) => p?.type === 'text').map((p: any) => p.text).join('\n')
    : result ? JSON.stringify(result) : '';
  return typeof text === 'string' && text ? text.slice(0, 4096) : undefined;
}
/** Admission checks in flight: the host decides allow/block/terminate inside the awaited gate. */
const hostChecks = new Map<string, (verdict: any) => void>();
let hostCheckSeq = 0;
function hostCheck(kind: 'PROVIDER_REQUEST' | 'TOOL_ADMISSION', fields: Record<string, unknown> = {}): Promise<any> {
  const checkId = 'chk-' + (++hostCheckSeq);
  return new Promise(resolve => {
    const complete = (verdict: any) => {
      clearTimeout(timer);
      hostChecks.delete(checkId);
      resolve(verdict);
    };
    const timer = setTimeout(() => complete({ action: 'unavailable', reason: 'HOST_GATE_TIMEOUT' }), 5000);
    hostChecks.set(checkId, complete);
    if (!process.connected) { complete({ action: 'unavailable', reason: 'HOST_GATE_DISCONNECTED' }); return; }
    process.send?.({ type: 'gate_check', checkId, kind, ...fields }, undefined, {}, error => {
      if (error) complete({ action: 'unavailable', reason: 'HOST_GATE_DISCONNECTED' });
    });
  });
}

let gateFailure: string | undefined;

function quietBoundary(): string | null {
  // A tool still executing (not merely held at admission) is real in-flight work;
  // ask_user stays in-flight while awaiting the bound answer, so it parks as input.
  const running = [...inflight.entries()].filter(([id]) => !gate.heldCallIds.has(id));
  if (running.some(([, name]) => name !== 'ask_user')) return null;
  if (gate.heldCallIds.size > 0) return 'TOOL_ADMISSION';
  if (gate.holds.size > 0) return 'PROVIDER_REQUEST';
  if (running.length > 0) return 'INPUT_REQUEST';
  return session?.isIdle ? 'IDLE' : null;
}
function tryQuiet() {
  const boundary = quietBoundary();
  if (!gate.closed || !boundary || pauseWaiters.length === 0) return;
  for (const requestKey of pauseWaiters.splice(0))
    send({ type: 'paused', requestKey, boundary, idle: session?.isIdle ?? true, pendingQuestions: [...questions.keys()], pendingToolCallIds: [...inflight.keys()], nativeSessionId: session?.sessionId, sessionFile: manager?.getSessionFile() });
}
/** Suspend inside an awaited SDK hook until the gate releases. */
async function gateHold(kind: 'PROVIDER_REQUEST' | 'TOOL_ADMISSION', toolCallId?: string): Promise<void> {
  let release!: () => void; const wait = new Promise<void>(resolve => { release = resolve; });
  gate.holds.set(release, kind); if (toolCallId) gate.heldCallIds.add(toolCallId);
  send({ type: 'gate_held', kind, toolCallId });
  tryQuiet();
  await wait;
  gate.holds.delete(release); if (toolCallId) gate.heldCallIds.delete(toolCallId);
}
function releaseGate() {
  gate.closed = false;
  for (const release of [...gate.holds.keys()]) release();
  gate.holds.clear(); gate.heldCallIds.clear();
}

async function stop() {
  if (stopping) return; stopping = true;
  releaseGate();
  for (const answer of questions.values()) answer({ cancelled: true });
  for (const answer of macRequests.values()) answer({ error: 'MAC_TOOLS_CLOSED' });
  for (const answer of runnerRequests.values()) answer({ error: 'RUNNER_CANCELLED' });
  try { await session?.abort(); } finally { session?.dispose(); send({ type: 'stopped' }); process.exit(0); }
}
if (process.platform !== 'darwin') process.on('disconnect', () => { void stop(); setTimeout(() => process.exit(1), 2500).unref(); });
process.on('SIGTERM', () => { void stop(); });

async function init(config: any) {
  if (config.executionMode === 'CONTEXT_ONLY' && ['macCapabilities','mcpServers','extensions'].some(k => (config[k] ?? []).length)) throw new Error('CONTEXT_ONLY_RESOURCE_DENIED');
  const credentials = new InMemoryCredentialStore();
  const modelRuntime = await ModelRuntime.create({ credentials, modelsStore: new InMemoryModelsStore(), modelsPath: null, refreshOnCreate: false, allowModelNetwork: false });
  // Every catalog entry is registered up front so prompt/steer commands can
  // switch the session model by name without another init round-trip.
  const catalog: any[] = Array.isArray(config.catalog) && config.catalog.length ? config.catalog : [config.model];
  catalog.forEach((binding, i) => {
    modelRuntime.registerProvider('worker-model-' + i, {
      baseUrl: binding.baseUrl, api: binding.api ?? 'openai-completions', apiKey: process.env[binding.apiKeyEnv ?? 'MODEL_API_KEY'],
      headers: Object.fromEntries((binding.headerEnvKeys ?? []).filter((key: string) => process.env[key]).map((key: string) => [key.slice('LLM_HEADER_'.length).replaceAll('_', '-'), process.env[key]!])),
      models: [{ id: binding.name, name: binding.name, reasoning: true, input: ['text'], contextWindow: binding.contextWindow ?? 131072, maxTokens: binding.maxTokens ?? 8192,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, compat: { supportsDeveloperRole: false, supportsStore: false, supportsUsageInStreaming: true, supportsReasoningEffort: binding.effort !== undefined || Array.isArray(binding.efforts), maxTokensField: 'max_tokens' } }],
    });
    const registered = modelRuntime.getModel('worker-model-' + i, binding.name);
    if (registered) modelIndex.set(binding.name, registered);
  });
  // Model credentials stay in runtime memory and never enter shell tool child environments.
  for (const binding of catalog) delete process.env[binding.apiKeyEnv ?? 'MODEL_API_KEY'];
  for (const key of Object.keys(process.env)) if (key.startsWith('LLM_HEADER_')) delete process.env[key];
  const model = modelIndex.get(config.model.name); if (!model) throw new Error('MODEL_NOT_FOUND');
  const agentDir = join(config.sessionDir, 'profile'); mkdirSync(agentDir, { recursive: true });
  const mcpNamespace = (id: string) => 'mcp__' + id.replace(/[^A-Za-z0-9_]/g, '_');
  // Inline boundary-gate extension. pi-coding-agent 1.4.0 does not re-export
  // loadExtensionFromFactory through the package root, so the Extension record is
  // built directly: the runner only reads handlers/tools and runtime bindings.
  const extensionRuntime = createExtensionRuntime();
  const boundaryGate: Extension = {
    path: '<inline:worker-boundary-gate>', resolvedPath: '<inline:worker-boundary-gate>', hidden: true,
    sourceInfo: createSyntheticSourceInfo('<inline:worker-boundary-gate>', { source: 'inline', scope: 'temporary' }),
    handlers: new Map<string, ((event: any, ctx: any) => any)[]>([
      ['before_provider_request', [async (_event: any, ctx: any) => {
        if (gate.closed) await gateHold('PROVIDER_REQUEST');
        if (!config.checkBudget) return undefined;
        const verdict = await hostCheck('PROVIDER_REQUEST');
        if (verdict.action === 'unavailable') gateFailure = verdict.reason;
        if (verdict.action !== 'allow') ctx.abort();
        return undefined;
      }]],
      // Held, not blocked, for PAUSE: the call's tool_execution_start already ran, so the
      // admission waits at the gate and executes for real after RESUME. After the hold the
      // host gate decides: block returns a real tool error (writeScope violation — the run
      // stays alive), terminate aborts the run (budget exceeded). Blocking on PAUSE would
      // fabricate a tool error into model context and corrupt its plan.
      ['tool_call', [async (event: any, ctx: any) => {
        if (gate.closed) await gateHold('TOOL_ADMISSION', event?.toolCallId);
        if (!config.checkBudget && !config.checkWriteScope) return undefined;
        const verdict = await hostCheck('TOOL_ADMISSION', { toolName: event?.toolName, toolCallId: event?.toolCallId, input: event?.input });
        if (verdict.action === 'unavailable') gateFailure = verdict.reason;
        if (verdict.action === 'block') return { block: true, reason: verdict.reason ?? 'TOOL_ADMISSION_DENIED' };
        if (verdict.action !== 'allow') { ctx.abort(); return { block: true, reason: verdict.reason ?? 'RUN_TERMINATED', terminate: true }; }
        return undefined;
      }]],
      ['session_before_compact', [() => gate.closed ? { cancel: true } : undefined]],
    ]) as any,
    tools: new Map(), messageRenderers: new Map(), entryRenderers: new Map(), commands: new Map(), flags: new Map(), shortcuts: new Map(),
  };
  // Managed resource surface: task-declared file extensions, skills and MCP servers.
  // 1.4.0 does not re-export loadExtensions/loadExtensionFromFactory at the package
  // root, so the pinned dist loader is imported by file URL — its bytes are part of
  // the host-recorded engineBinding digest, identical trust level to root exports.
  const sdkLoader: any = await import(new URL('./core/extensions/loader.js', import.meta.resolve('@earendil-works/pi-coding-agent')).href);
  const eventBus = createEventBus();
  const extensions: Extension[] = [boundaryGate];
  const extensionErrors: { path: string; error: string }[] = [];
  const resourceFacts: Record<string, any> = { mcp: [], extensions: [], skills: [], skillDiagnostics: [], extensionErrors };
  if (Array.isArray(config.mcpServers) && config.mcpServers.length) {
    // Only operator-declared servers: the extension's own mcp.json discovery (agent
    // dir and project file) is disabled by the empty loadConfig, so no unreviewed
    // server binding reaches the session. stdio only; http/OAuth is out of scope.
    try {
      extensions.push(await sdkLoader.loadExtensionFromFactory(createMcpExtension({
        startupWaitMs: 8000, logPath: join(agentDir, 'mcp.log'), loadConfig: () => ({ servers: [], errors: [] }),
      }), config.workspace, eventBus, extensionRuntime, '<inline:worker-mcp>'));
    } catch (error) { extensionErrors.push({ path: '<inline:worker-mcp>', error: error instanceof Error ? error.message : String(error) }); }
    try {
      // registerMcpServer validates each config and throws on invalid/duplicate names.
      extensions.push(await sdkLoader.loadExtensionFromFactory((pi: any) => { for (const s of config.mcpServers) pi.registerMcpServer(s.id, s.config); }, config.workspace, eventBus, extensionRuntime, '<inline:worker-mcp-servers>'));
    } catch (error) { extensionErrors.push({ path: '<inline:worker-mcp-servers>', error: error instanceof Error ? error.message : String(error) }); }
  }
  // Entry discovery mirrors the SDK rule: package.json "pi.extensions" entries, else
  // index.ts|index.js. Only the managed profile copies are scanned — never workspace .pi.
  const extensionEntries = (dir: string): string[] => {
    try {
      const pkg = join(dir, 'package.json');
      if (existsSync(pkg)) {
        const decl = JSON.parse(readFileSync(pkg, 'utf8'))?.pi?.extensions;
        if (Array.isArray(decl)) { const entries = decl.filter((e: any) => typeof e === 'string').map((e: string) => join(dir, e)).filter(existsSync); if (entries.length) return entries; }
      }
      for (const name of ['index.ts', 'index.js']) { const file = join(dir, name); if (existsSync(file)) return [file]; }
    } catch {}
    return [];
  };
  for (const declared of config.extensions ?? []) {
    const dir = join(agentDir, 'extensions', declared.name);
    const entries = extensionEntries(dir);
    if (!entries.length) { extensionErrors.push({ path: dir, error: 'EXTENSION_ENTRY_MISSING' }); continue; }
    try {
      const result = await sdkLoader.loadExtensions(entries, config.workspace, eventBus, extensionRuntime);
      extensions.push(...result.extensions); extensionErrors.push(...result.errors);
    } catch (error) { extensionErrors.push({ path: dir, error: error instanceof Error ? error.message : String(error) }); }
  }
  for (const ext of extensions.slice(1)) resourceFacts.extensions.push({ path: ext.path, handlers: [...ext.handlers.keys()], tools: [...ext.tools.keys()] });
  const skillsDir = join(agentDir, 'skills');
  const skillsResult = loadSkillsFromDir({ dir: skillsDir, source: 'worker-managed' });
  resourceFacts.skills = skillsResult.skills.map(s => ({ name: s.name, filePath: s.filePath }));
  resourceFacts.skillDiagnostics = skillsResult.diagnostics.map(d => ({ type: d.type, message: d.message, path: d.path }));
  // The declared tool allowlist must name extension tools and each MCP namespace
  // pattern or the SDK filters them out of the model-visible set.
  const profileTools = config.executionProfile === RUNNER_PROFILE ? ['read', 'write', 'edit', 'ask_user', 'bash', 'run_command', 'run_build', 'run_tests'] : isNoForkProfile(config.executionProfile) ? ['read', 'write', 'edit', 'ask_user'] : ['read', 'write', 'edit', 'bash', 'ask_user'];
  const toolAllowlist = [...profileTools, ...(config.macCapabilities ?? []).map((t: any) => t.name),
    ...(config.mcpServers ?? []).map((s: any) => mcpNamespace(s.id) + '__*'),
    ...extensions.flatMap(e => [...e.tools.keys()])];
  // A same-kernel assignment transition may narrow the model-visible set: the
  // host validated the requested entries against the session's active tools, and
  // this only ever intersects the computed allowlist — never adds to it.
  const effectiveAllowlist = config.executionMode === 'CONTEXT_ONLY' ? [] : Array.isArray(config.toolAllowlist) && config.toolAllowlist.length
    ? toolAllowlist.filter(t => config.toolAllowlist.includes(t) || config.toolAllowlist.some((w: string) => w.endsWith('*') && t.startsWith(w.slice(0, -1))))
    : toolAllowlist;
  // No global profile, models.json, project extension, or unreviewed executable resource discovery.
  const resources: ResourceLoader = {
    getExtensions: () => ({ extensions, errors: extensionErrors, runtime: extensionRuntime }),
    getSkills: () => skillsResult, getPrompts: () => ({ prompts: [], diagnostics: [] }), getThemes: () => ({ themes: [], diagnostics: [] }),
    getAgentsFiles: () => ({ agentsFiles: [] }), getSystemPrompt: () => undefined, getSystemPromptSource: () => undefined,
    getAppendSystemPrompt: () => [config.executionMode === 'CONTEXT_ONLY' ? 'Analyze only the explicitly supplied conversation and material text. No tools or workspace access are available. Do not claim any external observations or file effects.' : 'Work in the assigned workspace. Do not read or write outside this workspace. Never print credentials. Use ask_user when a user decision is necessary.'],
    getAppendSystemPromptSources: () => [], extendResources: () => {}, reload: async () => {},
  };
  manager = config.sessionFile ? SessionManager.open(config.sessionFile, config.sessionDir, config.workspace) : SessionManager.create(config.workspace, config.sessionDir);
  ({ session } = await createAgentSession({ cwd: config.workspace, agentDir, modelRuntime, model, thinkingLevel: config.model.effort ?? 'low', resourceLoader: resources,
    sessionManager: manager, settingsManager: SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } }),
    tools: effectiveAllowlist, customTools: config.executionMode === 'CONTEXT_ONLY' ? [] : [...(config.macCapabilities ?? []).map((tool: any) => ({
      ...tool,
      async execute(toolCallId: string, input: any, signal?: AbortSignal) {
        if (signal?.aborted || stopping) throw new Error('MAC_TOOLS_CLOSED');
        const result = await new Promise<any>((resolve, reject) => {
          macRequests.set(toolCallId, resolve);
          signal?.addEventListener('abort', () => { macRequests.delete(toolCallId); reject(new Error('MAC_OPERATION_UNCONFIRMED')); }, { once: true });
          send({ type: 'mac_tool_request', toolCallId, toolName: tool.name, input });
        });
        macRequests.delete(toolCallId);
        if (result.error) throw new Error(result.error);
        return { content: [{ type: 'text' as const, text: JSON.stringify(result.receipt) }], details: { macNative: result.receipt } };
      },
    })), ...(config.executionProfile === RUNNER_PROFILE ? ['bash', 'run_command', 'run_build', 'run_tests'].map(name => ({
      name, label: name, description: name === 'run_tests' ? 'Run a test command through the managed host Runner. Exit zero alone does not prove tests executed; independent test evidence is returned when available.' : 'Execute a shell command in the assigned workspace through the managed host Runner. Returns stdout, stderr, exit code and independent build/test facts.',
      parameters: Type.Object({ command: Type.String(), cwd: Type.Optional(Type.String()), timeoutMs: Type.Optional(Type.Integer({ minimum: 100, maximum: 600000 })) }),
      async execute(toolCallId: string, input: any, signal?: AbortSignal) {
        if (signal?.aborted || stopping) throw new Error('RUNNER_CANCELLED');
        const result = await new Promise<any>((resolve, reject) => {
          runnerRequests.set(toolCallId, resolve);
          signal?.addEventListener('abort', () => { runnerRequests.delete(toolCallId); reject(new Error('RUNNER_CANCELLED')); }, { once: true });
          send({ type: 'runner_request', toolCallId, toolName: name, input });
        });
        runnerRequests.delete(toolCallId);
        if (result.error) throw new Error(result.error);
        return { content: [{ type: 'text' as const, text: JSON.stringify(result.receipt) }], details: { runner: result.receipt } };
      },
    })) : []), {
      name: 'ask_user', label: 'Ask user', description: 'Ask the controlling user a confirmation, selection, or text question; wait for the bound response.',
      parameters: Type.Object({ method: Type.Union([Type.Literal('confirm'), Type.Literal('select'), Type.Literal('input')]), title: Type.String(), message: Type.Optional(Type.String()), options: Type.Optional(Type.Array(Type.String())) }),
      async execute(_id, params: any, signal) {
        if (params.method === 'select' && (!params.options?.length || params.options.length > 30)) throw new Error('INVALID_OPTIONS');
        const inputRequestId = randomUUID();
        const response = await new Promise<any>((resolve, reject) => {
          questions.set(inputRequestId, resolve);
          signal?.addEventListener('abort', () => { questions.delete(inputRequestId); reject(new Error('QUESTION_CANCELLED')); }, { once: true });
          send({ type: 'question', inputRequestId, method: params.method, title: params.title, message: params.message ?? '', options: params.options ?? [] });
        });
        questions.delete(inputRequestId);
        if (response.cancelled) throw new Error('QUESTION_CANCELLED');
        send({ type: 'question_consumed', inputRequestId });
        return { content: [{ type: 'text', text: JSON.stringify(response) }], details: { inputRequestId } };
      },
    }] }));
  session.subscribe(event => {
    if (event.type === 'message_end' && event.message.role === 'assistant') {
      const message = event.message as any;
      send({ type: 'assistant', text: message.content.filter((p: any) => p.type === 'text').map((p: any) => p.text).join(''), usage: message.usage, failed: message.stopReason === 'error', errorCode: message.stopReason === 'error' ? 'MODEL_REQUEST_FAILED' : null });
    } else if (event.type === 'message_end' && event.message.role === 'user') {
      const content = event.message.content;
      const requestKey = directPrompt?.waitingUser ? directPrompt.requestKey : undefined;
      if (directPrompt) directPrompt.waitingUser = false;
      send({ type: 'user_consumed', requestKey, text: typeof content === 'string' ? content : content.filter((p: any) => p.type === 'text').map((p: any) => p.text).join('') });
    } else if (event.type === 'message_update' && event.assistantMessageEvent?.type === 'text_delta') {
      // worker-events/1 OUTPUT_DELTA: the SDK already streams per-token text_delta
      // updates; forward just the increment so the host can keep the durable
      // typewriter spine. thinking_* and toolcall_* parts stay unclaimed for now.
      const update = event.assistantMessageEvent;
      const responseId = event.message?.role === 'assistant' ? event.message.responseId : undefined;
      send({ type: 'assistant_delta', delta: update.delta, contentIndex: update.contentIndex, nativeTurnId: typeof responseId === 'string' ? responseId : undefined });
    } else if (event.type === 'message_update' && event.assistantMessageEvent?.type === 'thinking_delta') {
      // worker-events/1 THINKING_DELTA: reasoning parts forward like the output
      // tail. Emitted only when the provider/model actually returns thinking
      // parts — absent parts stay absent, nothing is synthesized.
      const update = event.assistantMessageEvent;
      const responseId = event.message?.role === 'assistant' ? event.message.responseId : undefined;
      send({ type: 'thinking_delta', delta: update.delta, contentIndex: update.contentIndex, nativeTurnId: typeof responseId === 'string' ? responseId : undefined });
    } else if (event.type === 'tool_execution_start') { inflight.set(event.toolCallId, event.toolName); send({ type: 'tool_start', toolName: event.toolName, toolCallId: event.toolCallId, detail: toolArgSummary(event.toolName, event.args) }); }
    else if (event.type === 'tool_execution_end') { inflight.delete(event.toolCallId); send({ type: 'tool_end', toolName: event.toolName, toolCallId: event.toolCallId, isError: event.isError, detail: event.isError ? toolErrorSummary(event.result) : undefined, result: event.isError ? undefined : toolResultSummary(event.result) }); tryQuiet(); }
    else if (event.type === 'agent_end' || event.type === 'agent_settled') tryQuiet();
  });
  // Emits session_start: declared MCP servers connect in the background and task
  // extensions receive their real lifecycle. Extension runtime errors become facts.
  await session.bindExtensions({ onError: (error: any) => send({ type: 'extension_error', extensionPath: error?.extensionPath, event: error?.event, error: error?.error }) });
  if (Array.isArray(config.mcpServers) && config.mcpServers.length) {
    // Bound evidence is what the session itself observes: tools the server actually
    // registered under its namespace before the bounded wait expires. A server that
    // never registers tools stays UNVERIFIED; the first prompt's own startup wait
    // (startupWaitMs) still applies independently.
    const deadline = Date.now() + 8000;
    for (;;) {
      const names = new Set(session.getAllTools().map(t => t.name));
      if (config.mcpServers.every((s: any) => [...names].some(n => n.startsWith(mcpNamespace(s.id) + '__'))) || Date.now() > deadline) break;
      await new Promise(r => setTimeout(r, 100));
    }
    const names = session.getAllTools().map(t => t.name);
    resourceFacts.mcp = config.mcpServers.map((s: any) => {
      const tools = names.filter(n => n.startsWith(mcpNamespace(s.id) + '__'));
      return { id: s.id, state: tools.length ? 'BOUND' : 'UNVERIFIED', toolCount: tools.length, tools };
    });
  }
  resourceFacts.macNative = { transport: 'HOST_IPC', tools: session.getActiveToolNames().filter((name: string) => name.startsWith('mac__')) };
  send({ type: 'ready', nativeSessionId: session.sessionId, sessionFile: manager.getSessionFile(), model: session.model?.id, tools: session.getActiveToolNames(), envProbe: Object.fromEntries((config.probeEnv ?? []).map((k: string) => [k, process.env[k] !== undefined])), resources: resourceFacts });
}
process.on('message', (command: any) => {
  void (async () => {
    if (command.type === 'init') { await init(command); return; }
    if (command.type === 'stop') { await stop(); return; }
    if (command.type === 'gate_verdict') { const resolve = hostChecks.get(command.checkId); hostChecks.delete(command.checkId); resolve?.(command); return; }
    if (command.type === 'mac_tool_result') { const resolve = macRequests.get(command.toolCallId); macRequests.delete(command.toolCallId); resolve?.(command); return; }
    if (command.type === 'runner_result') { const resolve = runnerRequests.get(command.toolCallId); runnerRequests.delete(command.toolCallId); resolve?.(command); return; }
    if (!session || stopping) throw new Error('SESSION_UNAVAILABLE');
    if (command.type === 'pause') {
      gate.closed = true; pauseWaiters.push(command.requestKey); tryQuiet();
      return;
    }
    if (command.type === 'resume') {
      const cancelledPause = pauseWaiters.splice(0);
      releaseGate();
      for (const requestKey of cancelledPause) send({ type: 'pause_failed', requestKey, code: 'RESUME_SUPERSEDED' });
      send({ type: 'resumed', requestKey: command.requestKey, idle: session.isIdle });
      return;
    }
    if (command.type === 'prompt') {
      // The host re-asserts the task binding on every prompt — a requested
      // switch applies here, and any drift from a failed switch heals on the
      // next dispatch rather than silently diverging from the ledger.
      await applyModel(command.model);
      gateFailure = undefined;
      directPrompt = { requestKey: command.requestKey, waitingUser: true };
      send({ type: 'started', requestKey: command.requestKey, model: session.model?.id });
      await session.prompt(command.content, { expandPromptTemplates: false });
      await session.waitForIdle();
      if (gateFailure) send({ type: 'failure', requestKey: command.requestKey, code: gateFailure });
      else send({ type: 'settled', requestKey: command.requestKey });
    } else if (command.type === 'steer') {
      await applyModel(command.model);
      const disposition = await session.steer(command.content);
      send({ type: 'queued', requestKey: command.requestKey, disposition });
    } else if (command.type === 'answer') {
      const answer = questions.get(command.inputRequestId); if (!answer) throw new Error('NATIVE_INPUT_EXPIRED');
      answer(command.response); send({ type: 'answered', requestKey: command.requestKey });
    }
  })().catch(() => { send({ type: 'failure', requestKey: command.requestKey, code: gateFailure ?? 'PI_EXECUTION_FAILED' }); });
});
