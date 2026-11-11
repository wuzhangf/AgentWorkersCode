// Real host preparation path, with a PATH-local Git fixture that forks a live
// heartbeat writer before execing the real fixed clone command. No inference.
import assert from 'node:assert/strict';
import {spawn,execFileSync} from 'node:child_process';
import {mkdtemp,mkdir,readFile,writeFile,chmod,rm,stat,access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,delimiter} from 'node:path';
import {randomBytes,randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
const [hostArg,engineArg]=process.argv.slice(2);assert(hostArg&&engineArg,'Usage: node tests/macos-preparation.mjs <host> <pin>');
assert.equal(process.platform,'darwin');
const binary=resolve(hostArg),engine=resolve(engineArg),root=await mkdtemp(join(tmpdir(),'nwf-codex-preparation-'));
const source=join(root,'source'),bin=join(root,'bin'),token=randomBytes(32).toString('hex');
const realGit=execFileSync('which',['git'],{encoding:'utf8'}).trim();let host;
const env={PATH:bin+delimiter+process.env.PATH,HOME:process.env.HOME,MODEL_API_KEY:'NWF_NON_SECRET_PREPARATION_SENTINEL',WORKER_LOCAL_TOKEN:token};
const exited=child=>child.exitCode!==null?Promise.resolve():new Promise(r=>child.once('exit',r));
async function call(base,method,path,body){const r=await fetch(base+path,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...(body?.requestKey?{'Idempotency-Key':body.requestKey}:{})},body:body?JSON.stringify(body):undefined});const value=await r.json();assert(r.ok,JSON.stringify({status:r.status,value}));return value;}
async function start(state){host=spawn(binary,['serve','--state',state,'--port','0'],{env,stdio:['ignore','pipe','pipe']});let out='',err='';host.stdout.on('data',b=>out+=b);host.stderr.on('data',b=>err+=b);for(let i=0;i<1200;i++){const base=/LOCAL_TASK_READY (http:\/\/127\.0\.0\.1:\d+)/.exec(out)?.[1];if(base)return base;assert.equal(host.exitCode,null,err);await delay(25);}throw Error('START_TIMEOUT');}
async function until(test,code){for(let i=0;i<600;i++){if(await test())return;await delay(25);}throw Error(code);}
async function groupGone(group){await until(async()=>{try{process.kill(-group,0);return false;}catch(e){if(e.code==='ESRCH')return true;if(e.code==='EPERM')return false;throw e;}},'PREPARATION_GROUP_SURVIVED');}
async function create(base){const p=await call(base,'POST','/local/v1/task-preflights',{projectId:'fixture',engineId:'codex'});return call(base,'POST','/local/v1/tasks',{requestKey:'prepare-'+randomUUID(),projectId:'fixture',engineId:'codex',optionsRevision:p.optionsRevision,instruction:'Must never dispatch inference in this preparation test.'});}
try{
 await mkdir(source);await mkdir(bin);await writeFile(join(source,'README.md'),'fixture\n');
 for(const args of [['init'],['config','user.name','NWF Fixture'],['config','user.email','nwf-fixture@example.invalid'],['add','.'],['commit','-m','fixture']])execFileSync(realGit,['-C',source,...args],{stdio:'ignore'});
 const wrapper=`#!/usr/bin/python3
import os,sys,time,json,pathlib
args=sys.argv[1:]
if 'clone' in args:
    state=pathlib.Path(args[-1]).parent.parent
    fixture=state/'fixture-preparation';fixture.mkdir(exist_ok=True)
    child=os.fork()
    if child==0:
        os.close(0);os.close(1);os.close(2)
        with open(fixture/'heartbeat','ab',buffering=0) as stream:
            while True: stream.write(b'tick\\n');time.sleep(.02)
    with open(fixture/'entered','a') as stream:
        stream.write(json.dumps({'pid':os.getpid(),'group':os.getpgrp(),'child':child})+'\\n');stream.flush()
    while not (fixture/'release').exists(): time.sleep(.02)
os.execv(${JSON.stringify(realGit)},[${JSON.stringify(realGit)}]+args)
`;
 await writeFile(join(bin,'git'),wrapper);await chmod(join(bin,'git'),0o700);
 for(const scenario of ['stop','crash']){
  const state=join(root,scenario);
  execFileSync(binary,['init','--state',state,'--executable',engine,'--project',source,'--project-id','fixture','--model','deepseek-v4.1-flash','--base-url','http://127.0.0.1:1/v1','--tool-profile','macos-in-process-v1'],{env,stdio:'pipe'});
  let base=await start(state);const task=await create(base),fixture=join(state,'fixture-preparation');
  await until(async()=>{try{return (await stat(join(fixture,'heartbeat'))).size>0;}catch{return false;}},'PREPARATION_NOT_ENTERED');
  const entered=JSON.parse((await readFile(join(fixture,'entered'),'utf8')).trim().split('\n').at(-1));
  assert(entered.pid>1&&entered.child>1&&entered.group>1);
  if(scenario==='stop'){
   await call(base,'POST',`/local/v1/tasks/${task.taskId}/commands`,{requestKey:'stop-'+randomUUID(),intent:'STOP',generation:task.generation,targetCommandId:task.currentCommandId,nativeSessionId:null,nativeTurnId:null});
   await delay(100);assert.equal((await call(base,'GET',`/local/v1/tasks/${task.taskId}`)).quiescent,false,'PREPARATION_MUST_DRAIN_BEFORE_STOP');
   await writeFile(join(fixture,'release'),'release\n');
   await until(async()=>{const d=await call(base,'GET',`/local/v1/tasks/${task.taskId}`);return d.state==='CANCELLED'&&d.quiescent===true;},'STOP_DID_NOT_FENCE_PREPARATION');
   const events=await call(base,'GET',`/local/v1/tasks/${task.taskId}/events?limit=100`);assert(!events.items.some(e=>e.type==='turn.bound'));
   await groupGone(entered.group);
   // The trusted clone really ran, while STOP fenced checkout/native dispatch.
   await access(join(state,'workspaces',task.taskId,'.git'));
  }else{
   host.kill('SIGKILL');await exited(host);host=null;await groupGone(entered.group);
   const bytes=(await stat(join(fixture,'heartbeat'))).size;await delay(150);assert.equal((await stat(join(fixture,'heartbeat'))).size,bytes,'ORPHAN_PREPARATION_WRITER');
   const entries=await readFile(join(fixture,'entered'),'utf8');base=await start(state);
   const recovered=await call(base,'GET',`/local/v1/tasks/${task.taskId}`);assert.equal(recovered.state,'UNKNOWN');assert.equal(recovered.quiescent,false);assert.equal(recovered.capabilities.followUp,false);
   const receipt=await call(base,'GET',`/local/v1/task-requests/${task.requestKey}`);assert.equal(receipt.state,'UNKNOWN');
   await delay(100);assert.equal(await readFile(join(fixture,'entered'),'utf8'),entries,'PREPARATION_REPLAYED');
   const artifacts=await call(base,'GET',`/local/v1/tasks/${task.taskId}/artifacts`);assert.equal(artifacts.items.length,0);assert.equal(artifacts.reasonCode,'STOP_TO_SEAL_RESULTS');
  }
  host.kill('SIGKILL');await exited(host);host=null;
 }
 console.log(JSON.stringify({result:'PASS',scope:'real host fixed Git preparation path, actual forked heartbeat writer, STOP drain/fence with real local clone, SIGKILL group cleanup, durable UNKNOWN/no replay/no artifact'}));
}finally{if(host){host.kill('SIGKILL');await exited(host);}await rm(root,{recursive:true,force:true});}
