// Real host process + real model + real pinned app-server: a completed task is
// STOP'd to proven quiescence, then an explicit follow-up reopens the same
// persisted native thread as a fresh generation/segment. Both sealed segment
// manifests stay resolvable afterwards. A real model binding is required.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, createHash, randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';

const [binaryArg,templateArg]=process.argv.slice(2);
assert(binaryArg&&templateArg,'Usage: node tests/stopped-resume.mjs <host executable> <initialized state template>');
const binary=resolve(binaryArg),template=resolve(templateArg);
assert(process.env.MODEL_API_KEY,'REAL_MODEL_BINDING_REQUIRED');
const root=await mkdtemp(join(tmpdir(),'nwf-codex-reopen-'));
assert(root.startsWith(join(tmpdir(),'nwf-codex-reopen-')),'TEST_CLEANUP_SCOPE');
const token=randomBytes(32).toString('hex');
let live;
const exited=child=>new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
async function start(){
 const child=spawn(binary,['serve','--state',root,'--port','0'],{env:{...process.env,WORKER_LOCAL_TOKEN:token},windowsHide:true,stdio:['ignore','pipe','pipe']});
 let output='',diagnostic='';child.stdout.on('data',b=>output+=b);child.stderr.on('data',b=>diagnostic+=b);
 const until=Date.now()+30000;
 while(Date.now()<until){const match=/LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output);if(match){live=child;return match[1]}if(child.exitCode!==null)throw new Error('HOST_START_FAILED '+diagnostic.trim());await delay(25)}
 child.kill();throw new Error('HOST_START_TIMEOUT '+diagnostic.trim());
}
async function request(base,method,tail,body,extra={}){const r=await fetch(base+tail,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...extra},body:body===undefined?undefined:JSON.stringify(body)});return {status:r.status,body:await r.json()}}
const binding=detail=>({generation:detail.generation,nativeSessionId:detail.nativeSessionId,nativeTurnId:detail.nativeTurnId,targetCommandId:detail.currentCommandId});
const writeScript=(file,marker)=>`[System.IO.File]::WriteAllText((Join-Path (Get-Location) '${file}'), "${marker}\u0060n", [System.Text.UTF8Encoding]::new($false))`;
// Test-only scoped approver: it authorizes only the exact fixture write script or
// an apply_patch touching only the fixture file inside the task workspace.
function fixtureApproval(q,{workspace,file,script}){
 const method=q.nativeMethod??q.method;
 if(!(q.options??[]).includes('accept'))return null;
 const params=q.params??{};const scope=q.scope??{};
 const cwd=scope.cwd??params.cwd;
 if(typeof cwd!=='string'||resolve(cwd).toLowerCase()!==resolve(workspace).toLowerCase())return null;
 if(method==='item/commandExecution/requestApproval'){
  let actions=params.commandActions;if(!actions&&typeof scope.commandActions==='string'){try{actions=JSON.parse(scope.commandActions)}catch{return null}}
  let argv=params.proposedExecpolicyAmendment;if(!argv&&typeof scope.proposedExecpolicyAmendment==='string'){try{argv=JSON.parse(scope.proposedExecpolicyAmendment)}catch{return null}}
  if(Array.isArray(actions)&&actions.length===1&&actions[0].command?.trim()===script&&Array.isArray(argv)&&argv.at(-1)===script)return{decision:'accept'};
 }
 if(method==='item/fileChange/requestApproval'){
  let paths=params.paths;if(!paths&&typeof scope.paths==='string'){try{paths=JSON.parse(scope.paths)}catch{return null}}
  if(Array.isArray(paths)&&paths.length===1&&resolve(workspace,paths[0]).toLowerCase()===resolve(workspace,file).toLowerCase())return{decision:'accept'};
 }
 return null;
}
const answered=new Set();
async function detail(base,taskId){return (await request(base,'GET','/local/v1/tasks/'+taskId)).body}
async function settle(base,taskId,grant,d){
 const questions=await request(base,'GET','/local/v1/tasks/'+taskId+'/input-requests');
 for(const q of questions.body.items??[]){
  if(q.state!=='PENDING'||answered.has(q.inputRequestId))continue;
  const response=fixtureApproval(q,{...grant,workspace:d.workspace})??(q.nativeMethod==='item/tool/requestUserInput'?{value:'Proceed with the original instruction'}:null);
  assert(response,'OUT_OF_SCOPE_INPUT_REQUEST '+(q.nativeMethod??q.method));
  const answer={requestKey:'nwf-answer-'+q.inputRequestId,response,generation:q.generation,nativeSessionId:q.nativeSessionId,nativeTurnId:q.nativeTurnId,targetCommandId:q.targetCommandId};
  const sent=await request(base,'POST','/local/v1/tasks/'+taskId+'/input-requests/'+q.inputRequestId+'/responses',answer,{'Idempotency-Key':answer.requestKey});
  assert.equal(sent.status,202);answered.add(q.inputRequestId);
 }
}
async function waitTask(base,taskId,states,ms,grant){
 const until=Date.now()+ms;
 while(Date.now()<until){
  const d=await detail(base,taskId);
  if(states.includes(d.state)&&(!states.includes('CANCELLED')||d.quiescent===true))return d;
  assert(!['FAILED','REJECTED'].includes(d.state),'NATIVE_TASK_FAILED '+d.state+' '+(d.reasonCode??''));
  await settle(base,taskId,grant,d);await delay(500);
 }
 throw new Error('NATIVE_DEADLINE_EXCEEDED '+states.join('/'));
}
async function stop(base,taskId,d){
 const req={requestKey:'nwf-stop-'+randomUUID(),intent:'STOP',...binding(d)};
 const r=await request(base,'POST','/local/v1/tasks/'+taskId+'/commands',req,{'Idempotency-Key':req.requestKey});assert.equal(r.status,202);
 const expected=['SUCCEEDED','FAILED','BUDGET_EXCEEDED','BINDING_UNAVAILABLE'].includes(d.state)?d.state:'CANCELLED';
 const until=Date.now()+60000;
 while(Date.now()<until){const settled=await detail(base,taskId);if(settled.state===expected&&settled.quiescent===true)return settled;await delay(100);}
 throw new Error('STOP_PROOF_DEADLINE');
}
try{
 await mkdir(join(root,'profile'));
 await writeFile(join(root,'registry.json'),await readFile(join(template,'registry.json')));
 await writeFile(join(root,'profile','config.toml'),await readFile(join(template,'profile','config.toml')));
 const registry=JSON.parse(await readFile(join(root,'registry.json'),'utf8'));
 const base=await start();
 const checked=(await request(base,'POST','/local/v1/task-preflights',{projectId:registry.project_id,engineId:'codex'})).body;
 const marker1='NWF_SEG1_'+randomUUID().replaceAll('-',''),marker2='NWF_SEG2_'+randomUUID().replaceAll('-','');
 const file1='nwf-segment-one-'+randomUUID().slice(0,8)+'.txt',file2='nwf-segment-two-'+randomUUID().slice(0,8)+'.txt';
 const instruction=(file,marker)=>process.platform==='darwin'?`Create ${file} containing exactly ${marker} followed by one newline. Use only the native apply_patch tool with this exact patch:\n*** Begin Patch\n*** Add File: ${file}\n+${marker}\n*** End Patch\nThen reply with ${marker}. Do not use shell or any other tool, modify any other file or commit.`:`Create ${file} containing exactly ${marker} followed by one newline. Use the shell tool to run exactly this PowerShell script: ${writeScript(file,marker)} . Then reply with ${marker}. Do not run any other shell commands, verify via shell, modify any other file or run git commit.`;
 const grant1={file:file1,script:writeScript(file1,marker1)},grant2={file:file2,script:writeScript(file2,marker2)};
 const create={requestKey:'nwf-w3-'+randomUUID(),projectId:registry.project_id,engineId:'codex',optionsRevision:checked.optionsRevision,instruction:instruction(file1,marker1)};
 const admitted=await request(base,'POST','/local/v1/tasks',create,{'Idempotency-Key':create.requestKey});assert.equal(admitted.status,202);
 const taskId=admitted.body.taskId;
 const done=await waitTask(base,taskId,['SUCCEEDED'],420000,grant1);
 const thread=done.nativeSessionId,turn1=done.nativeTurnId;
 assert(thread&&turn1,'NATIVE_BINDING_MISSING');
 assert.equal(done.capabilities.stoppedSessionFollowUp,false,'SEAL_BEFORE_STOP');
 const first=await stop(base,taskId,done);
 assert.equal(first.state,'SUCCEEDED');assert.equal(first.quiescent,true);
 // The frozen completion fact now makes the stopped session reopenable.
 const stopped=await detail(base,taskId);
 assert.equal(stopped.capabilities.stoppedSessionFollowUp,true,'REOPEN_PREREQUISITES_NOT_HELD');
 const seg1=stopped.segmentId,gen1=stopped.generation;
 const artifacts1=(await request(base,'GET','/local/v1/tasks/'+taskId+'/artifacts')).body;
 const row1=artifacts1.items.find(a=>a.path===file1&&a.status==='AVAILABLE');assert(row1,'SEGMENT_ONE_ARTIFACT_MISSING');
 const bytes1=Buffer.from((await request(base,'GET','/local/v1/tasks/'+taskId+'/artifacts/'+row1.artifactId)).body.bytes,'base64');
 assert.equal(bytes1.toString('utf8'),marker1+'\n');
 // Explicit follow-up on the stopped session: old binding guards the admission.
 const follow={requestKey:'nwf-w3-reopen-'+randomUUID(),kind:'NATIVE_FOLLOW_UP',content:instruction(file2,marker2),...binding(stopped)};
 const sent=await request(base,'POST','/local/v1/tasks/'+taskId+'/messages',follow,{'Idempotency-Key':follow.requestKey});
 assert.equal(sent.status,202,JSON.stringify(sent.body));
 const reopened=await waitTask(base,taskId,['SUCCEEDED'],420000,grant2);
 assert.equal(reopened.nativeSessionId,thread,'NATIVE_THREAD_CHANGED');
 assert.notEqual(reopened.generation,gen1,'GENERATION_NOT_ROTATED');
 assert.notEqual(reopened.segmentId,seg1,'SEGMENT_NOT_ROTATED');
 assert.equal(reopened.ownerEpoch,'2','OWNER_EPOCH_NOT_INCREMENTED');
 assert.notEqual(reopened.nativeTurnId,turn1,'NATIVE_TURN_NOT_REBOUND');
 assert.equal(reopened.currentCommandId,follow.requestKey);
 assert.equal(reopened.reopenFrom?.generation,gen1);
 assert.equal(reopened.reopenFrom?.segmentId,seg1);
 assert.equal(reopened.reopenFrom?.nativeTurnId,turn1);
 const receipt=(await request(base,'GET','/local/v1/tasks/'+taskId+'/interaction-requests/'+follow.requestKey)).body;
 assert.equal(receipt.state,'APPLIED');
 // Same key+body replays the receipt; nothing is dispatched twice.
 const replay=await request(base,'POST','/local/v1/tasks/'+taskId+'/messages',follow,{'Idempotency-Key':follow.requestKey});
 assert.equal(replay.status,202);assert.equal(replay.body.state,'APPLIED');
 const bounds=(await request(base,'GET','/local/v1/tasks/'+taskId+'/events?limit=100')).body.items.filter(e=>e.type==='turn.bound'&&e.payload?.requestKey===follow.requestKey);
 assert.equal(bounds.length,1,'REOPEN_DISPATCHED_TWICE');
 const conflict=await request(base,'POST','/local/v1/tasks/'+taskId+'/messages',{...follow,content:'changed'},{'Idempotency-Key':follow.requestKey});
 assert.equal(conflict.status,409);assert.equal(conflict.body.code,'IDEMPOTENCY_CONFLICT');
 const staleKey='nwf-w3-stale-'+randomUUID();
 const stale=await request(base,'POST','/local/v1/tasks/'+taskId+'/messages',{...follow,requestKey:staleKey,generation:gen1},{'Idempotency-Key':staleKey});
 assert.equal(stale.status,409);assert.equal(stale.body.code,'NATIVE_TARGET_CHANGED');
 const second=await stop(base,taskId,reopened);
 assert.equal(second.quiescent,true);
 // Both sealed segment manifests remain listed and resolvable.
 const artifacts2=(await request(base,'GET','/local/v1/tasks/'+taskId+'/artifacts')).body;
 assert.equal(artifacts2.segmentId,second.segmentId);
 const listed=artifacts2.segments.map(m=>m.segmentId);
 assert(listed.includes(seg1)&&listed.includes(second.segmentId),'SEGMENT_MANIFESTS_INCOMPLETE '+JSON.stringify(listed));
 const row2=artifacts2.items.find(a=>a.path===file2&&a.status==='AVAILABLE');assert(row2,'SEGMENT_TWO_ARTIFACT_MISSING');
 const prior=artifacts2.segments.find(m=>m.segmentId===seg1);
 assert(prior.items.some(a=>a.artifactId===row1.artifactId),'SEGMENT_ONE_MANIFEST_REWRITTEN');
 const bytes2=Buffer.from((await request(base,'GET','/local/v1/tasks/'+taskId+'/artifacts/'+row2.artifactId)).body.bytes,'base64');
 assert.equal(bytes2.toString('utf8'),marker2+'\n');
 const resolved1=Buffer.from((await request(base,'GET','/local/v1/tasks/'+taskId+'/artifacts/'+row1.artifactId)).body.bytes,'base64');
 assert.equal(resolved1.toString('utf8'),marker1+'\n','SEGMENT_ONE_ARTIFACT_CORRUPTED');
 // Durable inventory: frozen turn seals per bound turn; effects resolved.
 const db=new DatabaseSync(join(root,'worker.sqlite'),{readOnly:true});
 const seals=db.prepare('SELECT native_turn_id,status,generation,segment_id FROM turn_seals WHERE task=? ORDER BY rowid').all(taskId);
 const open=db.prepare("SELECT count(*) c FROM native_effects WHERE task=? AND status IN ('STARTED','UNKNOWN')").get(taskId).c;
 const effects=db.prepare('SELECT status,count(*) c FROM native_effects WHERE task=? GROUP BY status').all(taskId);
 db.close();
 assert(seals.some(s=>s.native_turn_id===turn1&&s.status==='completed'&&s.generation===gen1&&s.segment_id===seg1),'TURN_SEAL_MISSING '+JSON.stringify(seals));
 assert.equal(open,0,'UNRESOLVED_EFFECTS_REMAIN');
 let cacheQualification;
 if(registry.tool_profile==='native'){
  // Advisory reads of unchanged runtime inputs must not rehash the executable.
  // Actual admission must still reject a changed launch config after that hint.
  const started=performance.now();
  const reads=await Promise.all(Array.from({length:6},async()=>{
   const d=await detail(base,taskId);
   assert.equal(d.capabilities.followUp,true);
   return Math.round(performance.now()-started);
  }));
  assert(Math.max(...reads)<2000,'UNCHANGED_CAPABILITY_READS_REHASHED '+JSON.stringify(reads));
  const before=(await request(base,'GET','/local/v1/tasks/'+taskId+'/messages')).body;
  const profile=join(root,'profile','config.toml'),original=await readFile(profile);
  try{
   await writeFile(profile,Buffer.concat([original,Buffer.from('\n# isolated admission verification test\n')]));
   const key='nwf-cached-hint-refusal-'+randomUUID();
   const refused=await request(base,'POST','/local/v1/tasks/'+taskId+'/messages',{
    requestKey:key,...binding(second),kind:'NATIVE_FOLLOW_UP',content:'This must never be dispatched.',
   },{'Idempotency-Key':key});
   assert.equal(refused.status,409);assert.equal(refused.body.code,'ENGINE_CONFIG_CHANGED');
   const after=(await request(base,'GET','/local/v1/tasks/'+taskId+'/messages')).body;
   assert.deepEqual(after.items,before.items,'REJECTED_ADMISSION_CHANGED_MESSAGES');
  }finally{await writeFile(profile,original);}
  cacheQualification={readMilliseconds:reads,admissionAfterCachedHint:'ENGINE_CONFIG_CHANGED'};
 }
 console.log(JSON.stringify({result:'PASS',checks:cacheQualification?33:30,scope:'real model turn, STOP quiescence, stopped-session reopen on same thread with new generation/segment/epoch, per-segment manifests resolvable, turn seals durable',effects,cacheQualification}));
}finally{if(live){let done=exited(live);live.kill();await done;}for(let attempt=0;attempt<40;attempt++){try{await rm(root,{recursive:true,force:true});break}catch(e){if(attempt===39)console.error('CLEANUP_INCOMPLETE '+e.message);else await delay(500)}}}
