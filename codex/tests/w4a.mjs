// Real host process + real pinned app-server + real model (where noted).
// W4a slice coverage: intake validation refusals, environment binding
// terminal failure with no peer, FIFO slot queueing, soft budget exceed at a
// real boundary with an atomic guarded EXTEND_BUDGET, host-observed write
// scope violation facts, durable audit and content-redacted diagnostics.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,rm,access,mkdtemp as _tmp} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';

const [binaryArg,templateArg]=process.argv.slice(2);
assert(binaryArg&&templateArg,'Usage: node tests/w4a.mjs <host executable> <initialized state template>');
const binary=resolve(binaryArg),template=resolve(templateArg);
assert(process.env.MODEL_API_KEY,'REAL_MODEL_BINDING_REQUIRED');
const root=await mkdtemp(join(tmpdir(),'nwf-codex-w4a-'));
assert(root.startsWith(join(tmpdir(),'nwf-codex-w4a-')),'TEST_CLEANUP_SCOPE');
const extraDir=await mkdtemp(join(tmpdir(),'nwf-w4a-extra-'));
const token=randomBytes(32).toString('hex');
let live;
const exited=child=>new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
async function start(maxActive='1'){
 const child=spawn(binary,['serve','--state',root,'--port','0'],{env:{...process.env,WORKER_LOCAL_TOKEN:token,NWF_CODEX_MAX_ACTIVE_TASKS:maxActive},windowsHide:true,stdio:['ignore','pipe','pipe']});
 let output='',diagnostic='';child.stdout.on('data',b=>output+=b);child.stderr.on('data',b=>diagnostic+=b);
 const until=Date.now()+30000;
 while(Date.now()<until){const match=/LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output);if(match){live=child;return match[1]}if(child.exitCode!==null)throw new Error('HOST_START_FAILED '+diagnostic.trim());await delay(25)}
 child.kill();throw new Error('HOST_START_TIMEOUT '+diagnostic.trim());
}
async function request(base,method,tail,body,extra={}){const r=await fetch(base+tail,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...extra},body:body===undefined?undefined:JSON.stringify(body)});return {status:r.status,body:await r.json()}}
const binding=detail=>({generation:detail.generation,nativeSessionId:detail.nativeSessionId,nativeTurnId:detail.nativeTurnId,targetCommandId:detail.currentCommandId});
const writeScript=(file,marker)=>`[System.IO.File]::WriteAllText((Join-Path (Get-Location) '${file}'), "${marker}n", [System.Text.UTF8Encoding]::new($false))`;
// Test-only scoped approver: a command may run only when every proposed action
// writes the fixture file carrying the fixture marker (unique random ids), and
// an apply_patch/fileChange may touch exactly the named fixture path.
function fixtureApproval(q,{workspace,targets}){
 const method=q.nativeMethod??q.method;
 if(!(q.options??[]).includes('accept'))return null;
 const params=q.params??{};const scope=q.scope??{};
 const cwd=scope.cwd??params.cwd;
 if(typeof cwd==='string'&&resolve(cwd).toLowerCase()!==resolve(workspace).toLowerCase())return null;
 if(method==='item/commandExecution/requestApproval'){
  let actions=params.commandActions;if(!actions&&typeof scope.commandActions==='string'){try{actions=JSON.parse(scope.commandActions)}catch{return null}}
  if(Array.isArray(actions)&&actions.length>=1&&actions.every(a=>{const c=String(a?.command??'');return targets.some(t=>c.includes(t.file)&&c.includes(t.marker))}))return{decision:'accept'};
 }
 if(method==='item/fileChange/requestApproval'){
  let paths=params.paths;if(!paths&&typeof scope.paths==='string'){try{paths=JSON.parse(scope.paths)}catch{return null}}
  if(Array.isArray(paths)&&paths.length===1&&targets.some(t=>resolve(workspace,t.file).toLowerCase()===resolve(workspace,paths[0]).toLowerCase()))return{decision:'accept'};
 }
 return null;
}
const answered=new Set();
async function detail(base,taskId){return (await request(base,'GET','/local/v1/tasks/'+taskId)).body}
async function allEvents(base,taskId){let cursor=0,items=[];for(;;){const p=await request(base,'GET','/local/v1/tasks/'+taskId+'/events?limit=100&cursor='+cursor);const batch=p.body.items??[];items.push(...batch);if(batch.length<100)return items;cursor=p.body.cursor??batch[batch.length-1].cursor}}
async function settle(base,taskId,targets,d){
 const questions=await request(base,'GET','/local/v1/tasks/'+taskId+'/input-requests');
 for(const q of questions.body.items??[]){
  if(q.state!=='PENDING'||answered.has(q.inputRequestId))continue;
  const response=fixtureApproval(q,{workspace:d.workspace,targets})??(q.nativeMethod==='item/tool/requestUserInput'?{value:'Proceed with the original instruction'}:null);
  assert(response,'OUT_OF_SCOPE_INPUT_REQUEST '+(q.nativeMethod??q.method));
  const answer={requestKey:'nwf-answer-'+q.inputRequestId,response,generation:q.generation,nativeSessionId:q.nativeSessionId,nativeTurnId:q.nativeTurnId,targetCommandId:q.targetCommandId};
  const sent=await request(base,'POST','/local/v1/tasks/'+taskId+'/input-requests/'+q.inputRequestId+'/responses',answer,{'Idempotency-Key':answer.requestKey});
  assert.equal(sent.status,202);answered.add(q.inputRequestId);
 }
}
async function waitTask(base,taskId,states,ms,targets=[]){
 const until=Date.now()+ms;
 while(Date.now()<until){
  const d=await detail(base,taskId);
  if(states.includes(d.state)&&(!['CANCELLED','BUDGET_EXCEEDED'].includes(d.state)||d.quiescent===true))return d;
  assert(!['FAILED','REJECTED'].includes(d.state),'NATIVE_TASK_FAILED '+d.state+' '+(d.reasonCode??''));
  await settle(base,taskId,targets,d);await delay(500);
 }
 const last=await detail(base,taskId);const ev=(await request(base,'GET','/local/v1/tasks/'+taskId+'/events?limit=50')).body.items.map(e=>e.type);throw new Error('NATIVE_DEADLINE_EXCEEDED '+states.join('/')+' last='+JSON.stringify({state:last.state,reasonCode:last.reasonCode,launchState:last.launchState})+' events='+ev.join(','));
}
async function stop(base,taskId,d){
 const req={requestKey:'nwf-stop-'+randomUUID(),intent:'STOP',...binding(d)};
 const r=await request(base,'POST','/local/v1/tasks/'+taskId+'/commands',req,{'Idempotency-Key':req.requestKey});assert.equal(r.status,202);
 return waitTask(base,taskId,['CANCELLED'],60000);
}
let checks=0;
try{
 await mkdir(join(root,'profile'));
 await writeFile(join(root,'registry.json'),await readFile(join(template,'registry.json')));
 await writeFile(join(root,'profile','config.toml'),await readFile(join(template,'profile','config.toml')));
 const registry=JSON.parse(await readFile(join(root,'registry.json'),'utf8'));
 let base=await start("1");
 const checked=(await request(base,'POST','/local/v1/task-preflights',{projectId:registry.project_id,engineId:'codex'})).body;
 const mk=(requestKey,extra={})=>({requestKey,projectId:registry.project_id,engineId:'codex',optionsRevision:checked.optionsRevision,instruction:'placeholder',...extra});
 // Intake validation: malformed W4a fields are refused before any task binds.
 for(const [key,patch,code] of [
  ['w4-hard-budget',{budget:{maxTotalTokens:'10',enforcement:'HARD'}},'BUDGET_ENFORCEMENT_UNSUPPORTED'],
  ['w4-budget-no-cap',{budget:{enforcement:'SOFT'}},'INVALID_REQUEST'],
  ['w4-secret-allowlist',{environment:{envAllowlist:['MY_API_KEY']}},'ENV_ALLOWLIST_DENIED'],
  ['w4-bad-allowlist',{environment:{envAllowlist:['lower_case']}},'INVALID_ENV_NAME'],
  ['w4-relative-extra',{writeScope:{extraWriteDirs:['relative/dir']}},'INVALID_REQUEST'],
  ['w4-absolute-deny',{writeScope:{denyPaths:['D:/abs']}},'INVALID_REQUEST'],
  ['w4-deny-escape',{writeScope:{denyPaths:['../escape']}},'INVALID_REQUEST'],
 ]){
  const r=await request(base,'POST','/local/v1/tasks',mk(key,{instruction:'x',...patch}),{'Idempotency-Key':key});
  assert.equal(r.status,409,key);assert.equal(r.body.code,code,key+' '+JSON.stringify(r.body));checks++;
 }
 // Environment binding: a missing command is terminal before any peer exists.
 const missingCmd='nwf-missing-command-'+randomUUID().slice(0,8);
 const bindKey='w4-binding-'+randomUUID().slice(0,8);
 const bound=await request(base,'POST','/local/v1/tasks',mk(bindKey,{instruction:'never dispatched',environment:{requiresCommands:[missingCmd],requiresDirs:[join(root,'no-such-dir')]}}),{'Idempotency-Key':bindKey});
 assert.equal(bound.status,202);const boundId=bound.body.taskId;
 const boundDone=await waitTask(base,boundId,['BINDING_UNAVAILABLE'],30000);checks++;
 assert.equal(boundDone.reasonCode,'BINDING_UNAVAILABLE');assert.equal(boundDone.quiescent,true);
 assert(boundDone.bindingFailures.some(f=>f.kind==='command'&&f.name===missingCmd));
 assert(boundDone.bindingFailures.some(f=>f.kind==='dir'));
 const boundEvents=(await request(base,'GET','/local/v1/tasks/'+boundId+'/events?limit=50')).body.items;
 assert(boundEvents.some(e=>e.type==='binding.unavailable'),'BINDING_EVENT_MISSING');
 await assert.rejects(access(join(root,'native',boundId)),'BINDING_FAILURE_SPAWNED_PEER');
 assert.equal((await request(base,'GET','/local/v1/task-requests/'+bindKey)).body.state,'BINDING_UNAVAILABLE');checks++;
 // FIFO slots: the holder stays non-quiescent after SUCCEEDED, so two
 // queued intents wait durably until the slot frees, then drain in order.
 const holdFile='nwf-w4-holder-'+randomUUID().slice(0,8)+'.txt',holdMarker='NWF_W4_HOLD_'+randomUUID().replaceAll('-','');
 const holdInstruction=`Create ${holdFile} containing exactly ${holdMarker} followed by one newline. Use the shell tool to run exactly this PowerShell script: ${writeScript(holdFile,holdMarker)} . Then reply with ${holdMarker}. Do not run any other shell commands, verify via shell, modify any other file or run git commit.`;
 const holdKey='w4-holder-'+randomUUID().slice(0,8);
 const holder=await request(base,'POST','/local/v1/tasks',mk(holdKey,{instruction:holdInstruction,environment:{requiresCommands:['where.exe'],requiresDirs:[root],envAllowlist:['JAVA_HOME']}}),{'Idempotency-Key':holdKey});
 assert.equal(holder.status,202);const holderId=holder.body.taskId;
 const holderDone=await waitTask(base,holderId,['SUCCEEDED'],420000,[{file:holdFile,marker:holdMarker}]);checks++;
 assert.equal(holderDone.environment.envAllowlist[0],'JAVA_HOME');
 // Slot is still held (quiescent:false after SUCCEEDED): queue two intents.
 const q1key='w4-q1-'+randomUUID().slice(0,8),q2key='w4-q2-'+randomUUID().slice(0,8);
 const q1=await request(base,'POST','/local/v1/tasks',mk(q1key,{instruction:'queued one',environment:{requiresCommands:[missingCmd]}}),{'Idempotency-Key':q1key});
 const q2=await request(base,'POST','/local/v1/tasks',mk(q2key,{instruction:'queued two',environment:{requiresCommands:[missingCmd]}}),{'Idempotency-Key':q2key});
 assert.equal(q1.status,202);assert.equal(q2.status,202);
 const q1d=await detail(base,q1.body.taskId),q2d=await detail(base,q2.body.taskId);
 assert.equal(q1d.launchState,'QUEUED');assert.equal(q1d.queuePosition,'1');
 assert.equal(q2d.launchState,'QUEUED');assert.equal(q2d.queuePosition,'2');checks++;
 // Crash the host mid-queue: the ACCEPTED create receipts prove the queued
 // intents were never dispatched, so restart re-evaluates them exactly once in
 // FIFO order while the crashed holder's slot frees on its UNKNOWN outcome.
 {const dead=exited(live);live.kill();await dead;live=null;}
 base=await start("1");
 const holderAfter=await detail(base,holderId);
 assert.equal(holderAfter.state,'UNKNOWN','CRASHED_SLOT_HOLDER_NOT_UNKNOWN '+holderAfter.state);
 const q1done=await waitTask(base,q1.body.taskId,['BINDING_UNAVAILABLE'],60000);
 const q2done=await waitTask(base,q2.body.taskId,['BINDING_UNAVAILABLE'],60000);checks++;
 assert.equal(q1done.queueOrdinal,1);assert.equal(q2done.queueOrdinal,2);
 const q1ev=(await request(base,'GET','/local/v1/tasks/'+q1.body.taskId+'/events?limit=50')).body.items;
 const q2ev=(await request(base,'GET','/local/v1/tasks/'+q2.body.taskId+'/events?limit=50')).body.items;
 assert.equal(q1ev.filter(e=>e.type==='queue.admitted').length,1,'Q1_ADMITTED_TWICE_OR_NEVER');
 assert.equal(q2ev.filter(e=>e.type==='queue.admitted').length,1,'Q2_ADMITTED_TWICE_OR_NEVER');
 assert(!q1ev.some(e=>e.type==='turn.bound')&&!q2ev.some(e=>e.type==='turn.bound'),'QUEUED_INTENT_DISPATCHED_ENGINE');checks++;
 // Soft budget: a one-token cap trips at the first native usage boundary,
 // seals the turn interrupted, and refuses ordinary follow-up.
 const budgetFile='nwf-w4-budget-'+randomUUID().slice(0,8)+'.txt',budgetMarker='NWF_W4_BUDGET_'+randomUUID().replaceAll('-','');
 const deniedFile='secrets/denied-'+randomUUID().slice(0,8)+'.txt',deniedMarker='NWF_W4_DENY_'+randomUUID().replaceAll('-','');
 const bKey='w4-budget-'+randomUUID().slice(0,8);
 const budgetTask=await request(base,'POST','/local/v1/tasks',mk(bKey,{instruction:holdInstruction.replaceAll(holdFile,budgetFile).replaceAll(holdMarker,budgetMarker),budget:{maxTotalTokens:'1',enforcement:'SOFT'},writeScope:{extraWriteDirs:[extraDir],denyPaths:['secrets']}}),{'Idempotency-Key':bKey});
 assert.equal(budgetTask.status,202);const budgetId=budgetTask.body.taskId;
 const exceeded=await waitTask(base,budgetId,['BUDGET_EXCEEDED'],300000,[{file:budgetFile,marker:budgetMarker}]);checks++;
 assert.equal(exceeded.quiescent,true);assert.equal(exceeded.budgetStatus.exceeded,true);
 const bEvents=(await request(base,'GET','/local/v1/tasks/'+budgetId+'/events?limit=100')).body.items;
 const exceedFact=bEvents.find(e=>e.type==='budget.exceeded');assert(exceedFact,'BUDGET_EVENT_MISSING');
 assert(exceedFact.payload.usage.totalTokens,'BUDGET_COUNTERS_MISSING');checks++;
 // Follow-up is refused while the budget holds; a decrease is not an extension.
 const fuKey='w4-fu-'+randomUUID().slice(0,8);
 const fu=await request(base,'POST','/local/v1/tasks/'+budgetId+'/messages',{requestKey:fuKey,kind:'NATIVE_FOLLOW_UP',content:'go',...binding(exceeded)},{'Idempotency-Key':fuKey});
 assert.equal(fu.status,409);assert.equal(fu.body.code,'BUDGET_EXCEEDED');checks++;
 const shrinkKey='w4-shrink-'+randomUUID().slice(0,8);
 const shrink=await request(base,'POST','/local/v1/tasks/'+budgetId+'/commands',{requestKey:shrinkKey,intent:'EXTEND_BUDGET',budget:{maxTotalTokens:'1',enforcement:'SOFT'},content:'x',...binding(exceeded)},{'Idempotency-Key':shrinkKey});
 assert.equal(shrink.status,409);assert.equal(shrink.body.code,'BUDGET_STILL_EXCEEDED');checks++;
 // Guarded extension: strictly larger caps + the same binding reopen the
 // session; the resumed turn is asked to write into the deny prefix, and a
 // seeded deny-path file makes the boundary git-sweep detection deterministic.
 const deniedScript=writeScript(deniedFile,deniedMarker);
 const seeded='seeded-denied-'+randomUUID().slice(0,8)+'.txt';
 await mkdir(join(exceeded.workspace,'secrets'),{recursive:true});
 await writeFile(join(exceeded.workspace,'secrets',seeded),'host-seeded deny-path content\n');
 const extKey='w4-extend-'+randomUUID().slice(0,8);
 const extend=await request(base,'POST','/local/v1/tasks/'+budgetId+'/commands',{requestKey:extKey,intent:'EXTEND_BUDGET',budget:{maxTotalTokens:'4000000',enforcement:'SOFT'},content:`Create ${deniedFile} containing exactly ${deniedMarker} followed by one newline. Use the shell tool to run exactly this PowerShell script: ${deniedScript} . Then reply with ${deniedMarker}. Do not run any other shell commands, verify via shell, modify any other file or run git commit.`,...binding(exceeded)},{'Idempotency-Key':extKey});
 assert.equal(extend.status,202,JSON.stringify(extend.body));checks++;
 const extended=await waitTask(base,budgetId,['SUCCEEDED','CANCELLED','BUDGET_EXCEEDED'],420000,[{file:deniedFile,marker:deniedMarker}]);
 assert.notEqual(extended.state,'BUDGET_EXCEEDED','EXTENSION_STILL_TRIPPED');
 const extReceipt=(await request(base,'GET','/local/v1/tasks/'+budgetId+'/commands/'+extKey)).body;
 assert(['APPLIED','DISPATCHING','ACCEPTED','REQUESTED'].includes(extReceipt.state),'EXTENSION_RECEIPT '+extReceipt.state);
 const budgetRow=(await detail(base,budgetId)).budgetStatus;assert.equal(budgetRow.exceeded,false);assert.equal(budgetRow.extensions,'1');assert.equal(budgetRow.caps.maxTotalTokens,'4000000');checks++;
 // Host-observed write scope: the deny-prefixed write produced a durable fact.
 const scopeEvents=await allEvents(base,budgetId);
 const violation=scopeEvents.find(e=>e.type==='write.scope.violation'&&e.payload?.kind==='DENIED_PATH'&&((e.payload?.path??'').includes('denied-')||(e.payload?.path??'').includes('secrets')));
 assert(violation,'DENIED_PATH_VIOLATION_MISSING '+JSON.stringify({violations:scopeEvents.filter(e=>e.type==='write.scope.violation'),state:extended.state}));
 const scopedDetail=await detail(base,budgetId);
 assert(scopedDetail.writeScopeViolations.some(v=>v.kind==='DENIED_PATH'),'VIOLATION_PROJECTION_MISSING');checks++;
 // Audit: ordered durable chain with subject/keys/intents/dispatch/native ids
 // and no prompt bodies. Diagnostics: content-redacted sha256/size references.
 const audit=(await request(base,'GET','/worker-host/v1/tasks/'+budgetId+'/audit')).body;
 assert.equal(audit.schemaVersion,'worker-audit/1');
 const kinds=new Set(audit.items.map(i=>i.kind));
 for(const k of ['request','event','budget','usage','turn.seal'])assert(kinds.has(k),'AUDIT_KIND_MISSING '+k);
 const ats=audit.items.map(i=>i.at).filter(Boolean);assert.deepEqual(ats,[...ats].sort(),'AUDIT_NOT_ORDERED');
 const auditText=JSON.stringify(audit);const leaked=audit.items.filter(i=>JSON.stringify(i).includes(deniedMarker)||JSON.stringify(i).includes('PowerShell script'));assert(!leaked.length,'AUDIT_LEAKED_PROMPT '+JSON.stringify(leaked.slice(0,3)).slice(0,1500));
 assert(!auditText.includes('PowerShell script'),'AUDIT_LEAKED_INSTRUCTION');checks++;
 const diag=(await request(base,'GET','/worker-host/v1/tasks/'+budgetId+'/diagnostics?redact=content')).body;
 assert.equal(diag.schemaVersion,'worker-diagnostics/1');assert.equal(diag.redact,'content');
 const diagText=JSON.stringify(diag);assert(diagText.includes('"sha256"'),'REDACTION_MISSING');
 assert(!diagText.includes(deniedMarker),'DIAGNOSTICS_LEAKED_BODY');
 assert.equal((await request(base,'GET','/worker-host/v1/tasks/'+budgetId+'/diagnostics')).status,409,'UNREDACTED_DIAGNOSTICS_SERVED');checks++;
 // Durable spot-check: seal carries the budget reason; queue ordinals persist.
 const db=new DatabaseSync(join(root,'worker.sqlite'),{readOnly:true});
 const seal=db.prepare("SELECT status,reason FROM turn_seals WHERE task=? AND reason='BUDGET_EXCEEDED'").all(budgetId);
 const ledger=db.prepare('SELECT count(*) c FROM usage_ledger WHERE task=?').get(budgetId).c;
 const version=db.prepare('PRAGMA user_version').get().user_version;
 db.close();
 assert(seal.length>=1,'INTERRUPTED_SEAL_MISSING');assert(ledger>=1,'USAGE_LEDGER_EMPTY');assert.equal(version,5);checks++;
 console.log(JSON.stringify({result:'PASS',checks,scope:'W4a validation refusals, BINDING_UNAVAILABLE without peer, FIFO queued admission with queuePosition, soft budget exceed + interrupted seal + guarded EXTEND_BUDGET on same thread, DENIED_PATH write.scope.violation, audit without prompts, redacted diagnostics'}));
}finally{if(live){let done=exited(live);live.kill();await done;}for(let attempt=0;attempt<40;attempt++){try{await rm(root,{recursive:true,force:true});await rm(extraDir,{recursive:true,force:true});break}catch(e){if(attempt===39)console.error('CLEANUP_INCOMPLETE '+e.message);else await delay(500)}}}
