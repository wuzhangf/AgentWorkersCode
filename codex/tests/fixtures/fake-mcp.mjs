// Minimal stdio MCP server for W4b verification: reports its tool catalog,
// echoes the caller text, and reports whether the managed env marker arrived
// while the model credential stayed absent (the engine does not leak the
// peer's inherited environment into spawned MCP children).
import readline from 'node:readline';
const rl = readline.createInterface({ input: process.stdin });
const tools = [{ name: 'nwf_env_echo', description: 'Echo bound env marker', inputSchema: { type: 'object', properties: { text: { type: 'string' } } } }];
rl.on('line', (line) => {
  let v; try { v = JSON.parse(line); } catch { return; }
  const reply = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: v.id, result }) + '\n');
  const err = (code, message) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: v.id, error: { code, message } }) + '\n');
  switch (v.method) {
    case 'initialize': return reply({ protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'nwf-w4b-fake', version: '1.0.0' } });
    case 'notifications/initialized': case 'initialized': return;
    case 'ping': return reply({});
    case 'tools/list': return reply({ tools });
    case 'tools/call': {
      const text = (v.params && v.params.arguments && v.params.arguments.text) || '';
      return reply({ content: [{ type: 'text', text: 'ECHO:' + text + '|MARKER=' + (process.env.NWF_MCP_MARKER || 'none') + '|KEY=' + (process.env.MODEL_API_KEY ? 'PRESENT' : 'ABSENT') }], isError: false });
    }
    default: if (v.id !== undefined) return err(-32601, 'method not found');
  }
});
