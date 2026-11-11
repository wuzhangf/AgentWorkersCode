// Minimal stdio MCP server fixture: newline-delimited JSON-RPC.
// Implements initialize, notifications/initialized, ping, tools/list, tools/call.
import { createInterface } from 'node:readline';
const tools = [
  { name: 'echo', description: 'Echo the text argument back.', inputSchema: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] } },
  { name: 'workspace_marker', description: 'Return the fixture marker NWF_W4B_MCP_OK.', inputSchema: { type: 'object', properties: {} } },
];
const reply = (id, result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n');
const fail = (id, code, message) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } }) + '\n');
createInterface({ input: process.stdin }).on('line', line => {
  if (!line.trim()) return;
  let msg; try { msg = JSON.parse(line); } catch { return; }
  if (msg.id === undefined || msg.id === null) return; // notification
  if (msg.method === 'initialize') return reply(msg.id, { protocolVersion: msg.params?.protocolVersion ?? '2025-06-18', capabilities: { tools: {} }, serverInfo: { name: 'nwf-fixture-mcp', version: '1.0.0' }, instructions: 'Fixture MCP server for worker tests.' });
  if (msg.method === 'ping') return reply(msg.id, {});
  if (msg.method === 'tools/list') return reply(msg.id, { tools });
  if (msg.method === 'tools/call') {
    const name = msg.params?.name; const args = msg.params?.arguments ?? {};
    if (name === 'echo') return reply(msg.id, { content: [{ type: 'text', text: 'echo:' + (args.text ?? '') }] });
    if (name === 'workspace_marker') return reply(msg.id, { content: [{ type: 'text', text: 'NWF_W4B_MCP_OK' }] });
    return fail(msg.id, -32602, 'unknown tool');
  }
  return fail(msg.id, -32601, 'method not found');
});
