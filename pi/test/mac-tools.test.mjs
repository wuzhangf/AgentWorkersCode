import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { MacTaskTools } from '../dist/host/mac-tools.js';
async function fixture(run) {
  let posts=0, gets=0, stops=0, lost=false, lostStop=false;
  const receipts=new Map();
  const server=createServer(async(req,res)=>{
    assert.equal(req.headers.authorization,'Bearer fixture-task-scope');
    assert.equal(req.headers['x-worker-local-store'],'fixture-store');
    const chunks=[]; for await (const chunk of req) chunks.push(chunk);
    const body=chunks.length?JSON.parse(Buffer.concat(chunks)):null;
    let value;
    if(req.url.endsWith('/context')) value={schemaVersion:'mac-native-context/1',taskId:'mac-task',guard:{generation:'g',nativeSessionId:'s',segmentId:'seg',ownerEpoch:'1'},capabilities:[{capabilityId:'mac.files.list',version:'1.0.0',description:'Original catalog description',inputSchema:{type:'object',properties:{},additionalProperties:false}}]};
    else if(req.method==='POST'&&req.url.endsWith('/operations')) { posts++; assert.equal(req.headers['idempotency-key'],body.requestKey); value={requestKey:body.requestKey,taskId:'mac-task',state:'APPLIED',result:{verification:'PASSED',data:{items:[]}}};receipts.set(body.requestKey,value); if(lost){lost=false;req.socket.destroy();return;} }
    else if(req.method==='GET'&&req.url.includes('/operations/')){gets++;value=receipts.get(req.url.split('/').at(-1));}
    else if(req.url.endsWith('/stop')){stops++;value={quiescent:true,taskId:'mac-task',requestKey:body.requestKey};if(lostStop){lostStop=false;req.socket.destroy();return;}}
    else if(req.method==='GET'&&req.url.includes('/requests/'))value={quiescent:true,taskId:'mac-task',requestKey:req.url.split('/').at(-1)};
    else throw new Error(req.url);
    res.writeHead(value?200:404,{'Content-Type':'application/json'});res.end(JSON.stringify(value??{code:'NOT_FOUND'}));
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const binding={workerRequestKey:'worker-create',origin:'http://127.0.0.1:'+server.address().port,taskID:'mac-task',storeID:'fixture-store',credentialEnvironment:'MAC_FIXTURE_TASK_TOKEN',ownsTask:true,ancestorReferences:[]};
  process.env.MAC_FIXTURE_TASK_TOKEN='fixture-task-scope';
  const bridge=new MacTaskTools([binding]);
  try { await run({bridge,binding,stats:()=>({posts,gets,stops}),lose:()=>{lost=true;},loseStop:()=>{lostStop=true;}}); }
  finally {delete process.env.MAC_FIXTURE_TASK_TOKEN;await new Promise(resolve=>server.close(resolve));}
}
test('Mac native tools reuse original schema, scoped auth and Host ledger before effects',async()=>fixture(async({bridge,binding,stats})=>{
 const capabilities=await bridge.capabilities(binding);assert.equal(capabilities[0].name,'mac__mac_files_list');assert.equal(capabilities[0].parameters.additionalProperties,false);assert.ok(!JSON.stringify(capabilities).includes('fixture-task-scope'));
 const task={taskId:'worker',segmentId:'seg',macOperations:{}};const event={toolName:'mac__mac_files_list',toolCallId:'call',input:{}};
 const save=record=>{task.macOperations[record.requestKey]=record;};
 const receipt=await bridge.execute(binding,task,event,save);assert.equal(receipt.state,'APPLIED');
 await bridge.execute(binding,task,event,save);assert.equal(stats().posts,1);
 await assert.rejects(bridge.execute(binding,task,{...event,input:{name:'changed'}},save),/IDEMPOTENCY_CONFLICT/);
 task.macToolsClosed=true;await assert.rejects(bridge.execute(binding,task,{...event,toolCallId:'new'},save),/MAC_TOOLS_CLOSED/);assert.equal(stats().posts,1);
}));
test('lost Mac operation response only queries original key',async()=>fixture(async({bridge,binding,stats,lose})=>{
 const task={taskId:'worker',segmentId:'seg',macOperations:{}};const event={toolName:'mac__mac_files_list',toolCallId:'call',input:{}};const save=record=>{task.macOperations[record.requestKey]=record;};lose();
 await assert.rejects(bridge.execute(binding,task,event,save));
 const receipt=await bridge.execute(binding,task,event,save);assert.equal(receipt.state,'APPLIED');assert.deepEqual(stats(),{posts:1,gets:1,stops:0});
 const stopped=await bridge.stop(binding);assert.equal(stopped.quiescent,true);assert.equal(stats().stops,1);
}));
test('external tools reject remote URL, referenced tasks, duplicate ownership and ancestor recursion',()=>{
 const b={workerRequestKey:'worker',origin:'http://127.0.0.1:1234',taskID:'mac-task',storeID:'store',credentialEnvironment:'MAC_TEST_TOKEN',ownsTask:true,ancestorReferences:[]};
 for(const change of [{origin:'https://remote.test'},{ownsTask:false},{ancestorReferences:['mac-task']},{ancestorReferences:['source:store:mac-task']},{credentialEnvironment:'TOKEN\r\nBAD'}])assert.throws(()=>new MacTaskTools([{...b,...change}]),/MAC_TOOL_BINDING_INVALID/);
 assert.throws(()=>new MacTaskTools([b,{...b,workerRequestKey:'other'}]),/MAC_TOOL_BINDING_INVALID/);
});
test('actual Pi SDK no-fork profile sees original Mac schema and returns a Host operation receipt', {skip:process.platform!=='darwin'}, async()=>fixture(async({bridge,binding,stats,loseStop})=>{
 const {PiHost}=await import('../dist/host/server.js');
 const {mkdtempSync,mkdirSync,writeFileSync,rmSync}=await import('node:fs');const {tmpdir}=await import('node:os');const {join}=await import('node:path');const {execFileSync}=await import('node:child_process');
 const root=mkdtempSync(join(tmpdir(),'pi-mac-tools-')),project=join(root,'project');mkdirSync(project);writeFileSync(join(project,'base.txt'),'fixture');execFileSync('git',['init',project],{stdio:'ignore'});execFileSync('git',['-C',project,'add','.'],{stdio:'ignore'});execFileSync('git',['-C',project,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','base'],{stdio:'ignore'});
 let calls=0;let sawTool=false,sawReceipt=false;
 const api=createServer(async(req,res)=>{let raw='';for await(const chunk of req)raw+=chunk;const body=JSON.parse(raw);assert.ok(!raw.includes('fixture-task-scope'));sawTool ||= body.tools.some(t=>t.function.name==='mac__mac_files_list');sawReceipt ||= raw.includes('APPLIED');const first=calls++===0;const delta=first?{role:'assistant',tool_calls:[{index:0,id:'mac-call',type:'function',function:{name:'mac__mac_files_list',arguments:'{}'}}]}:{role:'assistant',content:'MAC_TOOL_FIXTURE_DONE'};res.writeHead(200,{'Content-Type':'text/event-stream'});res.write('data: '+JSON.stringify({id:'mac-fixture',choices:[{index:0,delta,finish_reason:null}]})+'\n\n');res.write('data: '+JSON.stringify({id:'mac-fixture',choices:[{index:0,delta:{},finish_reason:first?'tool_calls':'stop'}]})+'\n\n');res.end('data: [DONE]\n\n');});await new Promise(done=>api.listen(0,'127.0.0.1',done));
 const env={MODEL_API_KEY:'fixture-model-key',MODEL_DEFAULT_MODEL:'fixture-model',MODEL_BASE_URL:'http://127.0.0.1:'+api.address().port+'/v1'},saved=Object.fromEntries(Object.keys(env).map(k=>[k,process.env[k]]));Object.assign(process.env,env);
 const host=new PiHost({state:join(root,'state'),token:'fixture-worker-owner-token-long',executionProfile:'macos-in-process-v1',macTools:bridge});
 try {host.register('demo',project);const pf=host.preflight({engineId: 'sage-pi',projectId:'demo'});const c=host.create({requestKey:binding.workerRequestKey,projectId:'demo',engineId: 'sage-pi',optionsRevision:pf.optionsRevision,instruction:'List fixed Mac materials'});let task;for(let i=0;i<400;i++){task=host.store.task(c.taskId);if(task.nativeIdle)break;await new Promise(done=>setTimeout(done,25));}assert.equal(task.state,'SUCCEEDED');assert.ok(task.nativeIdle);assert.ok(sawTool&&sawReceipt);assert.equal(stats().posts,1);assert.equal(Object.values(task.macOperations)[0].receipt.state,'APPLIED');const stopBody={requestKey:'worker-stop',intent:'STOP',generation:task.generation,nativeSessionId:task.nativeSessionId,targetCommandId:task.currentCommandId};loseStop();const unknown=await host.stop(task.taskId,stopBody);assert.equal(unknown.state,'UNKNOWN');const stopped=await host.stop(task.taskId,stopBody);assert.equal(stopped.state,'APPLIED');assert.equal(stats().stops,1);assert.equal(host.store.task(task.taskId).processOwnership.forkObserved,false);assert.equal(host.store.task(task.taskId).macRemoteQuiescent,true);}
 finally {await host.close();await new Promise(done=>api.close(done));for(const k of Object.keys(env))if(saved[k]===undefined)delete process.env[k];else process.env[k]=saved[k];rmSync(root,{recursive:true,force:true});}
}));
