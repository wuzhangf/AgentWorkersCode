// Real pinned app-server + real shared Runner + deterministic Responses fixture.
// --real additionally uses the configured model for the failing-project repair.
import assert from 'node:assert/strict';
import {spawn,execFileSync} from 'node:child_process';
import {createServer} from 'node:http';
import {mkdtemp,mkdir,readFile,writeFile,appendFile,rm,copyFile,stat,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';
const [binaryArg,engineArg,runnerArg,...flags]=process.argv.slice(2);
assert(binaryArg&&engineArg&&runnerArg,'Usage: node tests/macos-runner.mjs <host> <pinned-codex> <command-runner/main.mjs> [--real]');
assert.equal(process.platform,'darwin');
const real=flags.includes('--real');if(real)assert(process.env.MODEL_API_KEY&&process.env.MODEL_BASE_URL,'REAL_MODEL_CONFIGURATION_REQUIRED');
const binary=resolve(binaryArg),engine=resolve(engineArg),runner=resolve(runnerArg),root=await mkdtemp(join(tmpdir(),'nwf-codex-runner-'));
const source=join(root,'source'),state=join(root,'state'),runtime=join(root,'runtime'),token=randomBytes(32).toString('hex');
const env=real?{...process.env,WORKER_LOCAL_TOKEN:token}:{PATH:process.env.PATH,HOME:process.env.HOME,TMPDIR:process.env.TMPDIR,MODEL_API_KEY:'NWF_NON_SECRET_LOOPBACK_SENTINEL',WORKER_LOCAL_TOKEN:token};
let host,plan,requests=0,checks=0;
const runnerSourceHashes={};
const check=(condition,message)=>{assert(condition,message);checks++;};
const exited=child=>child.exitCode!==null||child.signalCode!==null?Promise.resolve():new Promise(r=>child.once('exit',r));
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const model=createServer(async(req,res)=>{
 try{
  let raw='';for await(const bytes of req)raw+=bytes;
  const body=JSON.parse(raw);requests++;
  assert(req.url.endsWith('/responses'));assert.equal(body.model,'deepseek-v4.1-flash');assert(plan,'UNEXPECTED_INFERENCE');
  const tools=(body.tools??[]).map(t=>({type:t.type,name:t.name??t.function?.name}));
  for(const name of ['apply_patch','run_command','run_build','run_tests'])assert(tools.some(t=>t.name===name),'TOOL_MISSING '+name);
  assert(!tools.some(t=>['exec_command','shell','shell_command'].includes(t.name)),'NATIVE_SHELL_ENABLED');
  let item;
  if(plan.kind==='repair'){
   const stages=[
    {name:'run_command',args:{command:'printf SHELL_STDOUT; printf SHELL_STDERR >&2; exit 7'}},
    {name:'run_tests',args:{command:'node --test --test-reporter=tap sum.test.mjs'}},
    {patch:'*** Begin Patch\n*** Update File: sum.mjs\n@@\n-export const sum = (a, b) => a - b;\n+export const sum = (a, b) => a + b;\n*** End Patch'},
    {name:'run_build',args:{command:'node build.mjs'}},
    {name:'run_tests',args:{command:'node --test --test-reporter=tap sum.test.mjs'}}
   ];
   if(plan.stage===1)assert(JSON.stringify(body.input).includes('SHELL_STDERR'),'ACTUAL_STDERR_NOT_CONSUMED');
   if(plan.stage===2)assert(JSON.stringify(body.input).includes('FAILED'),'FAILED_TEST_NOT_CONSUMED');
   if(plan.stage===4)assert(JSON.stringify(body.input).includes('BUILD_OK'),'ACTUAL_BUILD_NOT_CONSUMED');
   if(plan.stage===5)assert(JSON.stringify(body.input).includes('TEST_OK'),'ACTUAL_TEST_NOT_CONSUMED');
   const next=stages[plan.stage++];
   if(next?.patch){const t=tools.find(t=>t.name==='apply_patch');item=t.type==='custom'?{type:'custom_tool_call',id:'ct_'+randomUUID(),call_id:'call_'+randomUUID(),name:'apply_patch',input:next.patch}:{type:'function_call',id:'fc_'+randomUUID(),call_id:'call_'+randomUUID(),name:'apply_patch',arguments:JSON.stringify({patch:next.patch})};}
   else if(next)item={type:'function_call',id:'fc_'+randomUUID(),call_id:'call_'+randomUUID(),name:next.name,arguments:JSON.stringify(next.args)};
  }else if(plan.kind==='heartbeat'&&!plan.sent){
   plan.sent=true;item={type:'function_call',id:'fc_'+randomUUID(),call_id:'call_'+randomUUID(),name:'run_command',arguments:JSON.stringify({command:`node -e "setInterval(()=>require('fs').appendFileSync('${plan.file}','tick\\n'),50)"`,timeoutMs:600000})};
  }else if(plan.kind==='follow'&&!plan.sent){plan.sent=true;item={type:'function_call',id:'fc_'+randomUUID(),call_id:'call_'+randomUUID(),name:'run_tests',arguments:JSON.stringify({command:'node --test --test-reporter=tap sum.test.mjs'})};}
  item??={id:'msg_'+randomUUID(),type:'message',status:'completed',role:'assistant',content:[{type:'output_text',text:'FIXTURE_DONE',annotations:[]}]};
  const response={id:'resp_'+randomUUID(),object:'response',status:'completed',model:body.model,output:[item],usage:{input_tokens:10,output_tokens:5,total_tokens:15,input_tokens_details:{cached_tokens:0}}};
  res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-cache'});
  for(const [type,extra]of [['response.created',{response:{...response,status:'in_progress',output:[]}}],['response.output_item.added',{output_index:0,item}],['response.output_item.done',{output_index:0,item}],['response.completed',{response}]])res.write(`event: ${type}\ndata: ${JSON.stringify({type,...extra})}\n\n`);res.end();
 }catch(error){res.writeHead(500);res.end('FIXTURE_FAILURE');console.error(error.message);}
});
async function call(base,method,path,body,expected=200){
 const r=await fetch(base+path,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...(body?.requestKey&& !path.endsWith('/operations/lookup')?{'Idempotency-Key':body.requestKey}:{})},body:body?JSON.stringify(body):undefined});
 const data=await r.json();assert.equal(r.status,expected,JSON.stringify({path,status:r.status,body:data}));return data;
}
const binding=d=>({generation:d.generation,nativeSessionId:d.nativeSessionId,nativeTurnId:d.nativeTurnId,targetCommandId:d.currentCommandId});
async function waitFor(base,id,predicate,ms=90000){
 const until=Date.now()+ms;let d;
 while(Date.now()<until){d=await call(base,'GET','/local/v1/tasks/'+id);if(predicate(d))return d;
  const questions=await call(base,'GET',`/local/v1/tasks/${id}/input-requests`);
  for(const q of questions.items??[]){if(q.state!=='PENDING')continue;assert.equal(q.nativeMethod,'item/fileChange/requestApproval');
   const paths=q.params?.paths??JSON.parse(q.scope?.paths??'[]');assert(paths.length===1&&resolve(d.workspace,paths[0])===resolve(d.workspace,'sum.mjs'),'FIXTURE_APPROVAL_UNBOUND');
   await call(base,'POST',`/local/v1/tasks/${id}/input-requests/${q.inputRequestId}/responses`,{requestKey:'answer-'+randomUUID(),response:{decision:'accept'},generation:q.generation,nativeSessionId:q.nativeSessionId,nativeTurnId:q.nativeTurnId,targetCommandId:q.targetCommandId},202);
  }
  assert(!['FAILED','REJECTED'].includes(d.state)&&(d.state!=='UNKNOWN'||(d.launchState==='RESTORING'&&!d.reasonCode)),'TASK_FAILED '+JSON.stringify({state:d.state,reason:d.reasonCode}));await delay(100);
 }
 throw Error('TASK_TIMEOUT '+JSON.stringify({state:d?.state,reason:d?.reasonCode}));
}
async function startHost(){
 host=spawn(binary,['serve','--state',state,'--port','0'],{env,stdio:['ignore','pipe','pipe']});let output='',err='';host.stdout.on('data',b=>output+=b);host.stderr.on('data',b=>err+=b);
 for(let i=0;i<2000;i++){const base=/LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(output)?.[1];if(base)return base;assert.equal(host.exitCode,null,err);await delay(25);}throw Error('HOST_TIMEOUT '+err);
}
async function stop(base,task,d){await call(base,'POST',`/local/v1/tasks/${task}/commands`,{requestKey:'stop-'+randomUUID(),intent:'STOP',...binding(d)},202);return waitFor(base,task,d=>d.state==='CANCELLED'&&d.quiescent===true);}
async function runnerReceipts(base,task){const events=await call(base,'GET',`/local/v1/tasks/${task}/events?limit=100`);const keys=events.items.filter(e=>e.type==='runner.dispatching').map(e=>e.payload.requestKey);return Promise.all(keys.map(requestKey=>call(base,'POST','/worker-host/v1/operations/lookup',{requestKey,taskId:task})));}
async function heartbeat(base,preflight,file){
 plan={kind:'heartbeat',file};const create={requestKey:'heartbeat-'+randomUUID(),projectId:'runner-fixture',engineId:'codex',optionsRevision:preflight.optionsRevision,instruction:`Use run_command to execute this exact long command with timeoutMs 600000: node -e "setInterval(()=>require('fs').appendFileSync('${file}','tick\\n'),50)" . Do not modify any other file. Wait for cancellation.`};
 const task=await call(base,'POST','/local/v1/tasks',create,202);
 let current;for(let i=0;i<900;i++){current=await call(base,'GET','/local/v1/tasks/'+task.taskId);if(await stat(join(current.workspace,file)).then(s=>s.size>=10).catch(()=>false))return{task,detail:current,create};assert(!['FAILED','UNKNOWN'].includes(current.state),JSON.stringify(current));await delay(100);}throw Error('HEARTBEAT_NOT_STARTED');
}
async function stable(file){await delay(700);const before=(await stat(file)).size;await delay(700);check((await stat(file)).size===before,'HEARTBEAT_CONTINUED');}
try{
 await mkdir(source);await mkdir(runtime);
 await writeFile(join(source,'sum.mjs'),'export const sum = (a, b) => a - b;\n');
 await writeFile(join(source,'sum.test.mjs'),"import {test} from 'node:test';\nimport assert from 'node:assert/strict';\nimport {sum} from './sum.mjs';\ntest('adds positive integers',()=>assert.equal(sum(2,3),5));\ntest('adds negatives',()=>assert.equal(sum(-2,3),1));\nconsole.log('TEST_OK');\n");
 await writeFile(join(source,'build.mjs'),"import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';\nimport {sum} from './sum.mjs';\nif(typeof sum(1,2)!=='number')throw Error('BUILD_FAILED');\nmkdirSync('dist',{recursive:true});writeFileSync('dist/sum.mjs',readFileSync('sum.mjs'));console.log('BUILD_OK');\n");
 for(const args of [['init'],['config','user.name','NWF Fixture'],['config','user.email','nwf-fixture@example.invalid'],['add','.'],['commit','-m','fixture']])execFileSync('git',['-C',source,...args],{stdio:'ignore'});
 for(const file of (await readdir(dirname(runner))).filter(f=>f.endsWith('.mjs')).sort()){const bytes=await readFile(join(dirname(runner),file));runnerSourceHashes[file]=sha(bytes);await writeFile(join(runtime,file),bytes);}
 // Fault injection affects only the private test runtime: accept the first
 // command but drop its HTTP reply, and count every attempted command POST.
 const main=await readFile(join(runtime,'main.mjs'),'utf8');
 await writeFile(join(runtime,'main.mjs'),main.replace("const runner=new CommandRunner",`const originalHttp=CommandRunner.prototype.http;
CommandRunner.prototype.http=async function(req,res){
 if(req.method==='POST'&&req.url==='/v1/commands'){
  const {appendFileSync}=await import('node:fs');const {join}=await import('node:path');appendFileSync(join(this.state,'post-count.txt'),'POST\\n');
  if(!this.droppedReply){this.droppedReply=true;res.end=()=>{res.socket.destroy();};}
 }
 return originalHttp.call(this,req,res);
};
const runner=new CommandRunner`));
 await new Promise(r=>model.listen(0,'127.0.0.1',r));
 const baseUrl=real?process.env.MODEL_BASE_URL:`http://127.0.0.1:${model.address().port}/v1`;
 execFileSync(binary,['init','--state',state,'--executable',engine,'--project',source,'--project-id','runner-fixture','--tool-profile','macos-runner-v1','--command-runner',join(runtime,'main.mjs'),'--model','deepseek-v4.1-flash','--base-url',baseUrl],{env,stdio:'pipe'});
 let base=await startHost();const identity=await call(base,'GET','/local/v1/identity');check(identity.nativeToolProfile==='macos-runner-v1');check(identity.commandRunnerScope.arbitraryEscapedProcesses==='NOT_PROVEN');
 const preflight=await call(base,'POST','/local/v1/task-preflights',{projectId:'runner-fixture',engineId:'codex'});
 plan={kind:'repair',stage:0};
 const create={requestKey:'repair-'+randomUUID(),projectId:'runner-fixture',engineId:'codex',optionsRevision:preflight.optionsRevision,instruction:'Repair the small Node project in this workspace. First use run_command to run: printf SHELL_STDOUT; printf SHELL_STDERR >&2; exit 7 . This command deliberately fails; inspect stdout/stderr and exitCode. Then use run_tests to run node --test --test-reporter=tap sum.test.mjs and observe the failing assertions. Read files with run_command as needed. Fix only sum.mjs using native apply_patch. Run run_build with node build.mjs, then run_tests with node --test --test-reporter=tap sum.test.mjs. Both must exit 0. Do not commit. Report actual commands and results.'};
 const task=await call(base,'POST','/local/v1/tasks',create,202);let done=await waitFor(base,task.taskId,d=>d.state==='SUCCEEDED',real?420000:90000);
 const receipts=await runnerReceipts(base,task.taskId);check(receipts.length>=4);check(receipts.every(r=>r.state==='APPLIED'));
 check(receipts.some(r=>r.runner.exitCode===7&&r.runner.stdout.includes('SHELL_STDOUT')&&r.runner.stderr.includes('SHELL_STDERR')),'SHELL_OUTPUT_OR_EXIT_LOST');
 check(receipts.some(r=>r.runner.kind==='test'&&r.runner.status==='FAILED'&&r.runner.exitCode===1),'FAILING_TEST_NOT_EXECUTED');
 check(receipts.some(r=>r.runner.kind==='test'&&r.runner.exitCode===1&&/^# tests 2\s*$/m.test(r.runner.stdout)&&/^# fail [12]\s*$/m.test(r.runner.stdout)),'FAILING_TEST_COUNT_NOT_PROVEN');
 check(receipts.some(r=>r.runner.kind==='build'&&r.runner.status==='SUCCEEDED'&&r.runner.exitCode===0&&r.runner.stdout.includes('BUILD_OK')),'BUILD_NOT_EXECUTED');
 check(receipts.some(r=>r.runner.kind==='test'&&r.runner.status==='SUCCEEDED'&&r.runner.exitCode===0&&r.runner.stdout.includes('TEST_OK')),'PASSING_TEST_NOT_EXECUTED');
 check(receipts.some(r=>r.runner.kind==='test'&&r.runner.exitCode===0&&/^# tests 2\s*$/m.test(r.runner.stdout)&&/^# pass 2\s*$/m.test(r.runner.stdout)&&/^# fail 0\s*$/m.test(r.runner.stdout)),'PASSING_TEST_COUNT_NOT_PROVEN');
 check(await readFile(join(done.workspace,'sum.test.mjs'),'utf8')===await readFile(join(source,'sum.test.mjs'),'utf8'),'TEST_SOURCE_CHANGED');
 check((await readFile(join(done.workspace,'sum.mjs'),'utf8')).includes('a + b'),'MODEL_PATCH_NOT_PRESENT');
 const posts=(await readFile(join(state,'runners',task.taskId,'post-count.txt'),'utf8')).trim().split('\n').length;check(posts===receipts.length,'LOST_REPLY_REDISPATCHED');
 done=await stop(base,task.taskId,done);check(done.stopProof.commandRunner.scope==='REGISTERED_PROCESS_GROUP');check(done.stopProof.arbitraryEscapedProcesses==='NOT_PROVEN');
 const artifacts=await call(base,'GET',`/local/v1/tasks/${task.taskId}/artifacts`);check(artifacts.items.some(a=>a.path==='sum.mjs'&&a.status==='AVAILABLE'));
 const firstThread=done.nativeSessionId;plan={kind:'follow'};
 await call(base,'POST',`/local/v1/tasks/${task.taskId}/messages`,{requestKey:'follow-'+randomUUID(),kind:'NATIVE_FOLLOW_UP',content:'Use run_tests to run node --test --test-reporter=tap sum.test.mjs again. Do not edit anything. Report exitCode.',...binding(done)},202);
 done=await waitFor(base,task.taskId,d=>d.state==='SUCCEEDED',real?240000:90000);check(done.nativeSessionId===firstThread,'REOPEN_CHANGED_THREAD');done=await stop(base,task.taskId,done);
 const contexts=done.stopProof.commandRunner.contexts;check(contexts.length===2,'STOP_DID_NOT_RECONCILE_ALL_CONTEXTS');
 if(!real){
  const long=await heartbeat(base,preflight,'stop-heartbeat.log');const stopped=await stop(base,long.task.taskId,long.detail);check(stopped.quiescent===true);await stable(join(stopped.workspace,'stop-heartbeat.log'));
  const cancelled=await runnerReceipts(base,long.task.taskId);check(cancelled.length===1&&cancelled[0].runner.status==='CANCELLED'&&cancelled[0].state==='APPLIED','STOP_COMMAND_NOT_RECONCILED');
  const crash=await heartbeat(base,preflight,'crash-heartbeat.log');const pending=await runnerReceipts(base,crash.task.taskId);check(pending.length===1&&pending[0].state==='DISPATCHING');const original=pending[0].requestKey;
  host.kill('SIGKILL');await exited(host);host=null;await stable(join(crash.detail.workspace,'crash-heartbeat.log'));
  const before=requests;base=await startHost();const unknown=await call(base,'GET',`/local/v1/tasks/${crash.task.taskId}`);check(unknown.state==='UNKNOWN'&&unknown.quiescent===false);const lookup=await call(base,'POST','/worker-host/v1/operations/lookup',{taskId:crash.task.taskId,requestKey:original});check(lookup.state==='UNKNOWN','ORIGINAL_RUNNER_KEY_NOT_UNKNOWN');
  const unsealed=await call(base,'GET',`/local/v1/tasks/${crash.task.taskId}/artifacts`);check(unsealed.items.length===0);await delay(500);check(requests===before,'HOST_CRASH_REPLAYED_MODEL');
  const db=new DatabaseSync(join(state,'runners',crash.task.taskId,'commands.sqlite'),{readOnly:true});const rows=db.prepare('SELECT data FROM commands').all().map(r=>JSON.parse(r.data));db.close();check(rows.length===1&&rows[0].status==='UNKNOWN','RUNNER_OWNER_EOF_NOT_UNKNOWN');check(rows[0].stopProof?.groupExited===true,'CRASH_REGISTERED_GROUP_NOT_EXITED');let gone=false;try{process.kill(-rows[0].pgid,0);}catch(error){gone=error.code==='ESRCH';}check(gone,'CRASH_GROUP_STILL_LIVE');
 }
 host.kill('SIGKILL');await exited(host);host=null;
 await appendFile(join(runtime,'guardian.mjs'),'\n');check(await new Promise(resolve=>{const c=spawn(binary,['serve','--state',state,'--port','0'],{env,stdio:['ignore','ignore','pipe']});let err='';c.stderr.on('data',b=>err+=b);c.once('exit',code=>resolve(code===1&&err.includes('RUNNER_CONFIG_CHANGED')));}), 'RUNNER_TAMPER_NOT_REFUSED');
 const report={schemaVersion:'codex-macos-runner-qualification/1',testedAt:new Date().toISOString(),platform:process.platform,arch:process.arch,node:process.version,hostExecutableSha256:sha(await readFile(binary)),runnerSourceHashes,result:'PASS',mode:real?'deepseek-opencode-go':'pinned-app-server-loopback-fixture',checks,model:'deepseek-v4.1-flash',pin:'0.163.0-alpha.11.2',profile:'macos-runner-v1',repairCommands:receipts.map(r=>({kind:r.runner.kind,status:r.runner.status,exitCode:r.runner.exitCode,stdoutSha256:sha(r.runner.stdout),stderrSha256:sha(r.runner.stderr)})),scope:'native no-fork apply_patch; dynamic host command/build/test; actual failing and passing tests; original-key response-loss lookup without POST retry; same-thread reopen and all-context STOP; registered process groups, arbitrary escaped processes and remote effects not proven',...(real?{}:{lifecycle:'STOP heartbeat ceased; host SIGKILL owner EOF cleanup; original runner key UNKNOWN, no replay or artifacts'})};
 if(process.env.NWF_RUNNER_EVIDENCE)await writeFile(resolve(process.env.NWF_RUNNER_EVIDENCE),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
}finally{if(host){host.kill('SIGKILL');await exited(host);}model.closeAllConnections();await new Promise(r=>model.close(r));if(!process.env.NWF_KEEP_RUNNER_FIXTURE)await rm(root,{recursive:true,force:true});else console.log('FIXTURE_ROOT '+root);}
