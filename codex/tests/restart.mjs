// Real host process crash/restart test. No engine turn or model request is dispatched.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,rm,access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';

const [binaryArg,templateArg]=process.argv.slice(2);
assert(binaryArg&&templateArg,'Usage: node tests/restart.mjs <host executable> <initialized state template>');
const binary=resolve(binaryArg),template=resolve(templateArg);
const root=await mkdtemp(join(tmpdir(),'nwf-codex-restart-'));
assert(root.startsWith(join(tmpdir(),'nwf-codex-restart-')),'TEST_CLEANUP_SCOPE');
const token=randomBytes(32).toString('hex');
let live;
let modelEnvironment={MODEL_API_KEY:'NWF_TEST_NON_SECRET_SENTINEL'};
const exited=child=>new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
async function start(){
 const child=spawn(binary,['serve','--state',root,'--port','0'],{env:{...process.env,...modelEnvironment,WORKER_LOCAL_TOKEN:token},windowsHide:true,stdio:['ignore','pipe','pipe']});
 let output='',diagnostic='';child.stdout.on('data',b=>output+=b);child.stderr.on('data',b=>diagnostic+=b);
 const until=Date.now()+10000;
 while(Date.now()<until){const match=/LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output);if(match){live=child;return match[1]}if(child.exitCode!==null)throw new Error('HOST_START_FAILED '+diagnostic.trim());await delay(25)}
 child.kill();throw new Error('HOST_START_TIMEOUT');
}
async function request(base,method,tail,body,extra={}){const r=await fetch(base+tail,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...extra},body:body===undefined?undefined:JSON.stringify(body)});return {status:r.status,body:await r.json()}}
try{
 await mkdir(join(root,'profile'));
 await writeFile(join(root,'registry.json'),await readFile(join(template,'registry.json')));
 await writeFile(join(root,'profile','config.toml'),await readFile(join(template,'profile','config.toml')));
 const config=await readFile(join(root,'profile','config.toml'),'utf8');for(const match of config.matchAll(/"(LLM_HEADER_[A-Za-z0-9_]+)"/g))modelEnvironment[match[1]]='NWF_TEST_NON_SECRET_HEADER_SENTINEL';
 const originalRegistry=await readFile(join(root,'registry.json'),'utf8');const originalConfig=await readFile(join(root,'profile','config.toml'),'utf8');const initRegistry=JSON.parse(originalRegistry);
 const reinit=spawn(binary,['init','--state',root,'--executable',initRegistry.executable,'--project',root,'--project-id','changed-source','--model','changed-model','--base-url','http://127.0.0.1:1'],{windowsHide:true,stdio:['ignore','ignore','pipe']});let refusal='';reinit.stderr.on('data',b=>refusal+=b);assert.equal((await exited(reinit)).code,1);assert.match(refusal,/LOCAL_STATE_ALREADY_INITIALIZED/);assert.equal(await readFile(join(root,'registry.json'),'utf8'),originalRegistry);assert.equal(await readFile(join(root,'profile','config.toml'),'utf8'),originalConfig);
 let base=await start();const first=(await request(base,'GET','/local/v1/identity')).body;
 const contender=spawn(binary,['serve','--state',root,'--port','0'],{env:{...process.env,WORKER_LOCAL_TOKEN:token},windowsHide:true,stdio:['ignore','ignore','pipe']});let rejected='';contender.stderr.on('data',b=>rejected+=b);
 assert.equal((await exited(contender)).code,1);assert.match(rejected,/LOCAL_HOST_ALREADY_OWNS_STATE/);
 const registry=JSON.parse(await readFile(join(root,'registry.json'),'utf8'));
 const checked=(await request(base,'POST','/local/v1/task-preflights',{projectId:registry.project_id,engineId:'codex'})).body;
 const early={requestKey:'early-stop-start',projectId:registry.project_id,engineId:'codex',optionsRevision:checked.optionsRevision,instruction:'This task is cancelled immediately before any engine prompt.'};
 const admitted=await request(base,'POST','/local/v1/tasks',early,{'Idempotency-Key':early.requestKey});assert.equal(admitted.status,202);assert.equal(admitted.body.nativeSessionId,undefined);
 const stop={requestKey:'early-stop-command',intent:'STOP',generation:admitted.body.generation,targetCommandId:admitted.body.currentCommandId,nativeSessionId:null,nativeTurnId:null};
 const stopped=await request(base,'POST','/local/v1/tasks/'+admitted.body.taskId+'/commands',stop,{'Idempotency-Key':stop.requestKey});assert.equal(stopped.status,202);
 let stopDetail;const stopUntil=Date.now()+15000;while(Date.now()<stopUntil){stopDetail=(await request(base,'GET','/local/v1/tasks/'+admitted.body.taskId)).body;if(stopDetail.quiescent)break;await delay(25)}
 assert.equal(stopDetail.state,'CANCELLED');assert.equal(stopDetail.quiescent,true);
 const replay=await request(base,'GET','/local/v1/tasks/'+admitted.body.taskId+'/events?limit=100');assert(!replay.body.items.some(e=>e.type==='turn.bound'),'STOPPED_PREPARATION_MUST_NOT_DISPATCH_A_TURN');
 let missingExit=exited(live);live.kill();await missingExit;live=null;const configuredEnvironment=modelEnvironment;
 const headerName=Object.keys(configuredEnvironment).find(name=>name.startsWith('LLM_HEADER_'));
 if(headerName){modelEnvironment={...configuredEnvironment,[headerName]:''};base=await start();assert.equal((await request(base,'GET','/worker-host/v1/capabilities')).body.items.find(c=>c.id==='execution.messages').availability,'UNCONFIGURED');const missingHeader={...early,requestKey:'missing-model-header'};const denied=await request(base,'POST','/local/v1/tasks',missingHeader,{'Idempotency-Key':missingHeader.requestKey});assert.equal(denied.status,409);assert.equal(denied.body.code,'MODEL_CONFIGURATION_REQUIRED');let headerExit=exited(live);live.kill();await headerExit;live=null;}
 modelEnvironment=Object.fromEntries(Object.keys(configuredEnvironment).map(name=>[name,'']));
 base=await start();const missingCaps=(await request(base,'GET','/worker-host/v1/capabilities')).body.items;assert.equal(missingCaps.find(c=>c.id==='execution.messages').availability,'UNCONFIGURED');
 const missingRequest={...early,requestKey:'missing-model-auth'};const missing=await request(base,'POST','/local/v1/tasks',missingRequest,{'Idempotency-Key':missingRequest.requestKey});assert.equal(missing.status,409);assert.equal(missing.body.code,'MODEL_CONFIGURATION_REQUIRED');
 let missingDone=exited(live);live.kill();await missingDone;live=null;modelEnvironment=configuredEnvironment;
 base=await start();
 // Abrupt host death, then durable seed of the dispatch/reply loss window.
 let done=exited(live);live.kill();await done;live=null;
 const db=new DatabaseSync(join(root,'worker.sqlite'));
 const body={requestKey:'original-key',projectId:'fixture',engineId:'codex',optionsRevision:'original-guard',instruction:'This dispatch is uncertain and must never be replayed.'};
 const task={taskId:'native-crash-fixture',requestKey:'original-key',configuredModel:'frozen-historical-model',state:'RUNNING',quiescent:false,generation:'g1',ownerBootId:first.bootId,currentCommandId:'original-key',turns:{},capabilities:{stop:true,steer:true,followUp:true,pause:false,resume:false}};
 db.prepare('INSERT INTO tasks VALUES(?,?)').run(task.taskId,JSON.stringify(task));
 const canonical=JSON.stringify(Object.fromEntries(Object.entries(body).sort(([a],[b])=>a<b?-1:a>b?1:0)));
 db.prepare('INSERT INTO requests VALUES(?,?,?,?,?)').run('create',body.requestKey,canonical,'DISPATCHING',JSON.stringify(task));db.close();
 base=await start();const second=(await request(base,'GET','/local/v1/identity')).body;
 assert.equal(second.storeId,first.storeId);assert.notEqual(second.bootId,first.bootId);
 const unknown=await request(base,'GET','/local/v1/task-requests/original-key');assert.equal(unknown.status,200);assert.equal(unknown.body.state,'UNKNOWN');
 const lookup=await request(base,'POST','/worker-host/v1/operations/lookup',{requestKey:'original-key'});assert.equal(lookup.status,200);assert.equal(lookup.body.state,'UNKNOWN');
 const detail=(await request(base,'GET','/local/v1/tasks/'+task.taskId)).body;assert.equal(detail.state,'UNKNOWN');assert.equal(detail.quiescent,false);assert.equal(detail.capabilities.followUp,false);assert.equal(detail.capabilities.steer,false);
 assert.equal((await request(base,'GET','/local/v1/tasks/'+task.taskId+'/metrics')).body.configuredModel,'frozen-historical-model');
 const repeat=await request(base,'POST','/local/v1/tasks',body,{'Idempotency-Key':body.requestKey});assert.equal(repeat.status,202);assert.equal(repeat.body.state,'UNKNOWN');
 const conflict=await request(base,'POST','/local/v1/tasks',{...body,instruction:'different'},{'Idempotency-Key':body.requestKey});assert.equal(conflict.status,409);assert.equal(conflict.body.code,'IDEMPOTENCY_CONFLICT');
 assert.equal((await request(base,'GET','/local/v1/projects',undefined,{'X-Worker-Local-Store':'wrong-store'})).status,409);
 await assert.rejects(access(join(root,'native',task.taskId)));await assert.rejects(access(join(root,'workspaces',task.taskId)));
 console.log(JSON.stringify({result:'PASS',checks:headerName?33:30,scope:'immutable source init, frozen model metrics, missing key/header denial, real host process ownership, pre-session STOP fencing, durable identity, dispatch UNKNOWN, original-key lookup, no native replay'}));
}finally{if(live){let done=exited(live);live.kill();await done;}await rm(root,{recursive:true,force:true});}
