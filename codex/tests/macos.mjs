// Real pinned macOS app-server + deterministic loopback Responses fixture.
// This proves native handoffs and lifecycle, never remote model access.
import assert from 'node:assert/strict';
import {spawn,execFileSync} from 'node:child_process';
import {createServer} from 'node:http';
import {mkdtemp,mkdir,readFile,writeFile,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {randomBytes,randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
const [binaryArg,engineArg]=process.argv.slice(2);
assert(binaryArg&&engineArg,'Usage: node tests/macos.mjs <host> <pinned-codex>');
assert.equal(process.platform,'darwin');
const binary=resolve(binaryArg),engine=resolve(engineArg),root=await mkdtemp(join(tmpdir(),'nwf-codex-macos-'));
const state=join(root,'state'),source=join(root,'source'),token=randomBytes(32).toString('hex');
let host,plan,requests=0;
const env={PATH:process.env.PATH,HOME:process.env.HOME,TMPDIR:process.env.TMPDIR,MODEL_API_KEY:'NWF_NON_SECRET_LOOPBACK_SENTINEL',WORKER_LOCAL_TOKEN:token};
const exit=child=>child.exitCode!==null?Promise.resolve():new Promise(r=>child.once('exit',r));
const model=createServer(async(req,res)=>{
 try{
  let raw='';for await(const bytes of req)raw+=bytes;
  const body=JSON.parse(raw);requests++;
  assert(req.url.endsWith('/responses'),'FIXTURE_ROUTE');
  assert.equal(body.model,'deepseek-v4.1-flash');
  assert(plan,'UNEXPECTED_INFERENCE');
  if(plan.hold){plan.requested=true;res.writeHead(200,{'content-type':'text/event-stream'});res.write('event: response.created\ndata: {"type":"response.created","response":{"id":"held","status":"in_progress","output":[]}}\n\n');return;}
  const tools=(body.tools??[]).map(t=>({type:t.type,name:t.name??t.function?.name}));
  let item;
  if(!plan.sent){
   plan.sent=true;
   const patch=`*** Begin Patch\n*** Add File: ${plan.file}\n+${plan.marker}\n*** End Patch`;
   const tool=tools.find(t=>t.name==='apply_patch');
   assert(tool,'APPLY_PATCH_TOOL_MISSING '+JSON.stringify(tools));
   item=tool.type==='custom'?{id:'ct_'+randomUUID(),type:'custom_tool_call',status:'completed',call_id:'call_'+randomUUID(),name:'apply_patch',input:patch}:{id:'fc_'+randomUUID(),type:'function_call',status:'completed',call_id:'call_'+randomUUID(),name:'apply_patch',arguments:JSON.stringify({patch})};
  }else item={id:'msg_'+randomUUID(),type:'message',status:'completed',role:'assistant',content:[{type:'output_text',text:plan.marker,annotations:[]}]};
  const response={id:'resp_'+randomUUID(),object:'response',status:'completed',model:body.model,output:[item],usage:{input_tokens:10,output_tokens:5,total_tokens:15,input_tokens_details:{cached_tokens:0}}};
  res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-cache'});
  const send=(type,data)=>res.write(`event: ${type}\ndata: ${JSON.stringify({type,...data})}\n\n`);
  send('response.created',{response:{...response,status:'in_progress',output:[]}});
  send('response.output_item.added',{output_index:0,item});
  send('response.output_item.done',{output_index:0,item});
  send('response.completed',{response});res.end();
 }catch(error){res.writeHead(500);res.end('FIXTURE_FAILURE');console.error(error.message);}
});
async function call(base,method,path,body){
 const r=await fetch(base+path,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...(body?.requestKey?{'Idempotency-Key':body.requestKey}:{})},body:body?JSON.stringify(body):undefined});
 const data=await r.json();assert(r.ok,JSON.stringify({path,status:r.status,body:data}));return data;
}
const binding=d=>({generation:d.generation,nativeSessionId:d.nativeSessionId,nativeTurnId:d.nativeTurnId,targetCommandId:d.currentCommandId});
async function waitFor(base,id,predicate){
 const until=Date.now()+60000;let d;
 while(Date.now()<until){d=await call(base,'GET','/local/v1/tasks/'+id);if(predicate(d))return d;
  const questions=await call(base,'GET',`/local/v1/tasks/${id}/input-requests`);
  for(const q of questions.items??[]){if(q.state!=='PENDING')continue;assert.equal(q.nativeMethod,'item/fileChange/requestApproval');
   // Only the exact loopback fixture file inside its owned workspace is allowed.
   const paths=q.params?.paths??JSON.parse(q.scope?.paths??'[]');assert(paths.length===1&&resolve(d.workspace,paths[0])===resolve(d.workspace,plan.file),'FIXTURE_APPROVAL_UNBOUND '+JSON.stringify(q));
   await call(base,'POST',`/local/v1/tasks/${id}/input-requests/${q.inputRequestId}/responses`,{requestKey:'answer-'+randomUUID(),response:{decision:'accept'},generation:q.generation,nativeSessionId:q.nativeSessionId,nativeTurnId:q.nativeTurnId,targetCommandId:q.targetCommandId});
  }
  assert(!['FAILED','REJECTED'].includes(d.state),JSON.stringify({state:d.state,reason:d.reasonCode}));await delay(100);
 }
 throw Error('TASK_TIMEOUT '+JSON.stringify({state:d?.state,reason:d?.reasonCode}));
}
async function startHost(){
 host=spawn(binary,['serve','--state',state,'--port','0'],{env,stdio:['ignore','pipe','pipe']});
 let output='',err='';host.stdout.on('data',b=>output+=b);host.stderr.on('data',b=>err+=b);
 for(let i=0;i<1600;i++){const base=/LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output)?.[1];if(base)return base;assert.equal(host.exitCode,null,err);await delay(25);}
 throw Error('HOST_TIMEOUT '+err);
}
try{
 await mkdir(source);await writeFile(join(source,'README.md'),'committed fixture\n');
 for(const args of [['init'],['config','user.name','NWF Fixture'],['config','user.email','nwf-fixture@example.invalid'],['add','.'],['commit','-m','fixture']])execFileSync('git',['-C',source,...args],{stdio:'ignore'});
 await new Promise(r=>model.listen(0,'127.0.0.1',r));
 execFileSync(binary,['init','--state',state,'--executable',engine,'--project',source,'--project-id','mac-fixture','--tool-profile','macos-in-process-v1','--model','deepseek-v4.1-flash','--base-url',`http://127.0.0.1:${model.address().port}/v1`],{env,stdio:'pipe'});
 let base=await startHost();
 const preflight=await call(base,'POST','/local/v1/task-preflights',{projectId:'mac-fixture',engineId:'codex'});
 plan={file:'first.txt',marker:'FIRST_MARKER'};
 const task=await call(base,'POST','/local/v1/tasks',{requestKey:'create-'+randomUUID(),projectId:'mac-fixture',engineId:'codex',optionsRevision:preflight.optionsRevision,instruction:'Use apply_patch to create first.txt with FIRST_MARKER and one newline. No shell or other tools.'});
 let done=await waitFor(base,task.taskId,d=>d.state==='SUCCEEDED');
 assert.equal(await readFile(join(done.workspace,plan.file),'utf8'),plan.marker+'\n');
 await call(base,'POST',`/local/v1/tasks/${task.taskId}/commands`,{requestKey:'stop-'+randomUUID(),intent:'STOP',...binding(done)});
 done=await waitFor(base,task.taskId,d=>(d.state==='CANCELLED'&&d.quiescent===true)||d.state==='UNKNOWN');
 const nativeDir=join(state,'native',task.taskId);const forks=(await readdir(nativeDir)).filter(n=>n.startsWith('.process-fork-')).length;
 assert.equal(done.quiescent,true,'REAL_FILE_CHANGE_NOT_QUIESCENT '+JSON.stringify({state:done.state,reason:done.reasonCode,proof:done.stopProof,forks}));
 const artifacts=await call(base,'GET',`/local/v1/tasks/${task.taskId}/artifacts`);assert(artifacts.items.some(a=>a.path==='first.txt'&&a.status==='AVAILABLE'));
 const firstThread=done.nativeSessionId,firstSegment=done.segmentId;
 plan={file:'second.txt',marker:'SECOND_MARKER'};
 await call(base,'POST',`/local/v1/tasks/${task.taskId}/messages`,{requestKey:'follow-'+randomUUID(),kind:'NATIVE_FOLLOW_UP',content:'Use apply_patch to create second.txt with SECOND_MARKER and one newline. No shell or other tools.',...binding(done)});
 done=await waitFor(base,task.taskId,d=>d.state==='SUCCEEDED');assert.equal(done.nativeSessionId,firstThread);assert.notEqual(done.segmentId,firstSegment);
 assert.equal(await readFile(join(done.workspace,plan.file),'utf8'),plan.marker+'\n');
 await call(base,'POST',`/local/v1/tasks/${task.taskId}/commands`,{requestKey:'stop-'+randomUUID(),intent:'STOP',...binding(done)});
 done=await waitFor(base,task.taskId,d=>(d.state==='CANCELLED'&&d.quiescent===true)||d.state==='UNKNOWN');assert.equal(done.quiescent,true);
 const final=await call(base,'GET',`/local/v1/tasks/${task.taskId}/artifacts`);assert.equal(final.segments.length,2);
 // Kill the real host while the real app-server is inside an unfinished turn.
 // Pipe EOF must stop the exact managed group, but restart cannot infer an outcome.
 plan={hold:true};
 const crashKey='crash-'+randomUUID();
 const uncertain=await call(base,'POST','/local/v1/tasks',{requestKey:crashKey,projectId:'mac-fixture',engineId:'codex',optionsRevision:preflight.optionsRevision,instruction:'Wait for the fixture response.'});
 await waitFor(base,uncertain.taskId,d=>d.state==='RUNNING'&&plan.requested);
 const lines=execFileSync('ps',['-axo','pid=,ppid=,pgid=,command='],{encoding:'utf8'}).trim().split('\n');
 const owned=lines.map(line=>/^\s*(\d+)\s+(\d+)\s+(\d+)\s+(.*)$/.exec(line)).filter(m=>m&&Number(m[2])===host.pid&&m[4].includes('--nwf-internal-guard'));
 assert.equal(owned.length,1,'OWNED_GUARD_NOT_UNIQUE');const group=Number(owned[0][3]);
 host.kill('SIGKILL');await exit(host);host=null;
 let empty=false;for(let i=0;i<400;i++){try{process.kill(-group,0);}catch(e){if(e.code==='ESRCH'){empty=true;break;}if(e.code!=='EPERM')throw e;}await delay(25);}assert(empty,'HOST_CRASH_LEFT_MANAGED_GROUP');
 const before= requests;base=await startHost();
 const unknown=await call(base,'GET',`/local/v1/tasks/${uncertain.taskId}`);assert.equal(unknown.state,'UNKNOWN');assert.equal(unknown.quiescent,false);assert.equal(unknown.capabilities.followUp,false);
 const receipt=await call(base,'GET',`/local/v1/task-requests/${crashKey}`);assert.equal(receipt.taskId,uncertain.taskId);assert.equal(receipt.nativeTurnId,unknown.nativeTurnId); // immutable proven native-acceptance receipt
 const unsealed=await call(base,'GET',`/local/v1/tasks/${uncertain.taskId}/artifacts`);assert.equal(unsealed.items.length,0);assert.equal(unsealed.reasonCode,'STOP_TO_SEAL_RESULTS');
 await delay(200);assert.equal(requests,before,'CRASHED_TURN_REPLAYED');
 host.kill('SIGKILL');await exit(host);host=null;
 // A modified non-secret model catalog must fail before listener/native launch.
 const catalog=join(state,'profile','model-catalog.json');await writeFile(catalog,(await readFile(catalog,'utf8'))+'\n');
 assert.throws(()=>execFileSync(binary,['serve','--state',state,'--port','0'],{env,stdio:'pipe'}),e=>e.stderr.toString().includes('ENGINE_CONFIG_CHANGED'));
 console.log(JSON.stringify({result:'PASS',scope:'real pinned app-server, loopback Responses fixture, apply_patch bytes, STOP no-fork proof, same-thread reopen, two immutable segment manifests, live-host SIGKILL group cleanup, durable UNKNOWN/no replay/no artifact, catalog tamper refusal',requests}));
}finally{if(host){host.kill('SIGKILL');await exit(host);}model.closeAllConnections();await new Promise(r=>model.close(r));await rm(root,{recursive:true,force:true});}
