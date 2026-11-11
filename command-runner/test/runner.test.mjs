import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,readFileSync,existsSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {spawn} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {CommandRunner} from '../runner.mjs';
const token=randomBytes(32).toString('hex'),main=fileURLToPath(new URL('../main.mjs',import.meta.url));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function wait(fn,ms=15000){const until=Date.now()+ms;while(Date.now()<until){const v=await fn();if(v)return v;await sleep(20);}throw Error('TEST_DEADLINE');}
function fixture(){const root=mkdtempSync(join(tmpdir(),'command-runner-'));return{root,state:join(root,'state')};}
async function setup(){const f=fixture(),runner=new CommandRunner({state:f.state,token});runner.register({contextId:'ctx',workspace:f.root,binding:{taskId:'fixture',generation:'g1'}});return{...f,runner};}
const done=(r,id)=>wait(()=>{const c=r.command(id);return ['SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','UNKNOWN'].includes(c.status)&&c;});
test('real argv, stdout/stderr, nonzero exit and original-key replay',async()=>{
 const f=await setup();try{
  const request={requestKey:'argv',contextId:'ctx',kind:'command',argv:[process.execPath,'-e','process.stdout.write("hello");process.stderr.write("error");process.exitCode=7']};
  const c=f.runner.create(request);assert.equal(f.runner.create(request).commandId,c.commandId);
  assert.throws(()=>f.runner.create({...request,argv:[process.execPath,'--version']}),/IDEMPOTENCY_CONFLICT/);
  const r=await done(f.runner,c.commandId);assert.equal(r.status,'FAILED');assert.equal(r.exitCode,7);assert.equal(r.stdout,'hello');assert.equal(r.stderr,'error');assert.equal(r.stopProof.groupExited,true);assert.equal(f.runner.lookup('argv').commandId,c.commandId);
 }finally{await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
test('normal shell pipeline fork succeeds; build and real node tests retain their facts',async()=>{
 const f=await setup();try{
  const shell=f.runner.create({requestKey:'shell',contextId:'ctx',shell:{script:'printf hello | tr a-z A-Z'}});assert.equal((await done(f.runner,shell.commandId)).stdout,'HELLO');
  writeFileSync(join(f.root,'source.cjs'),'module.exports=(a,b)=>a+b;\n');
  writeFileSync(join(f.root,'test.cjs'),"const test=require('node:test'),assert=require('node:assert/strict');test('sum',()=>assert.equal(require('./source.cjs')(2,3),5));\n");
  const build=f.runner.create({requestKey:'build',contextId:'ctx',kind:'build',argv:[process.execPath,'--check','source.cjs']});assert.equal((await done(f.runner,build.commandId)).status,'SUCCEEDED');
  const testCommand=f.runner.create({requestKey:'test',contextId:'ctx',kind:'test',argv:[process.execPath,'--test','test.cjs']});const tested=await done(f.runner,testCommand.commandId);assert.equal(tested.status,'SUCCEEDED');assert.match(tested.stdout,/pass 1/);
 }finally{await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
test('STOP and timeout terminate the actual writer and close command intake',async()=>{
 const f=await setup();try{
  const script="const fs=require('node:fs');setInterval(()=>fs.appendFileSync('heartbeat','x'),15);";
  const c=f.runner.create({requestKey:'long',contextId:'ctx',argv:[process.execPath,'-e',script]});await wait(()=>existsSync(join(f.root,'heartbeat')));
  const stopped=await f.runner.stopContext('ctx','stop');assert.equal(stopped.closed,true);assert.equal(stopped.quiescent,true);assert.equal(f.runner.command(c.commandId).status,'CANCELLED');
  const size=readFileSync(join(f.root,'heartbeat')).length;await sleep(120);assert.equal(readFileSync(join(f.root,'heartbeat')).length,size);
  assert.throws(()=>f.runner.create({requestKey:'late',contextId:'ctx',argv:['true']}),/CONTEXT_CLOSED/);
  f.runner.register({contextId:'timeout',workspace:f.root,binding:{taskId:'fixture',generation:'g2'}});
  const timed=f.runner.create({requestKey:'timeout',contextId:'timeout',argv:[process.execPath,'-e','setInterval(()=>{},1000)'],timeoutMs:150});assert.equal((await done(f.runner,timed.commandId)).status,'TIMED_OUT');
 }finally{await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
test('STOP before dispatch has an explicit never-started proof and performs no command',async()=>{
 const f=await setup();try{
  const c=f.runner.create({requestKey:'early',contextId:'ctx',argv:[process.execPath,'-e',"require('node:fs').writeFileSync('must-not-exist','x')"]});
  const stop=await f.runner.stopContext('ctx','early-stop');const r=f.runner.command(c.commandId);
  assert.equal(r.status,'CANCELLED');assert.equal(r.stopProof.neverDispatched,true);assert.equal(r.stopProof.groupExited,true);assert.equal(stop.quiescent,true);await sleep(80);assert.equal(existsSync(join(f.root,'must-not-exist')),false);
 }finally{await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
test('a waited-for child may change groups and exit without leaving an unresolved writer',async()=>{
 const f=await setup();try{
  const script="const c=require('node:child_process').spawn(process.execPath,['-e','setTimeout(()=>{},180)'],{detached:true,stdio:'ignore'});c.on('exit',()=>setTimeout(()=>process.exit(0),180));";
  const c=f.runner.create({requestKey:'waited-group',contextId:'ctx',argv:[process.execPath,'-e',script]});assert.equal((await done(f.runner,c.commandId)).status,'SUCCEEDED');
 }finally{await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
test('HTTP auth, binding guards, output cursor and credentials excluded from tools',async()=>{
 const f=await setup();try{
  const base=await f.runner.listen();assert.equal((await fetch(base+'/v1/identity')).status,401);
  assert.throws(()=>f.runner.register({contextId:'ctx',workspace:f.root,binding:{generation:'other'}}),/CONTEXT_BINDING_CHANGED/);
  assert.throws(()=>f.runner.register({contextId:'secret',workspace:f.root,binding:{x:1},envAllowlist:['MODEL_API_KEY']}),/ENV_ALLOWLIST_DENIED/);
  process.env.MODEL_API_KEY='benign-should-not-cross';
  const c=f.runner.create({requestKey:'env',contextId:'ctx',argv:[process.execPath,'-e','console.log(JSON.stringify({model:!!process.env.MODEL_API_KEY,owner:!!process.env.WORKER_COMMAND_RUNNER_TOKEN,home:process.env.HOME}))']});const r=await done(f.runner,c.commandId);const env=JSON.parse(r.stdout);assert.equal(env.model,false);assert.equal(env.owner,false);assert(env.home.includes('contexts/ctx/home'));
  const page=await fetch(base+`/v1/commands/${c.commandId}/output?cursor=0`,{headers:{authorization:'Bearer '+token}});assert.equal(page.status,200);assert((await page.json()).items.length>0);
  assert.throws(()=>f.runner.create({requestKey:'outside',contextId:'ctx',cwd:'..',argv:['true']}),/CWD_OUTSIDE_WORKSPACE/);
 }finally{delete process.env.MODEL_API_KEY;await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
test('STOP records an observed detached writer as UNKNOWN without claiming full containment',async()=>{
 const f=await setup();let escaped;try{
  const childCode="const f=require('node:fs');f.writeFileSync('escaped.pid',String(process.pid));setInterval(()=>f.appendFileSync('escaped-heartbeat','x'),20)";
  const parentCode=`require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(childCode)}],{detached:true,stdio:'ignore'}).unref();setInterval(()=>{},1000)`;
  const c=f.runner.create({requestKey:'escape',contextId:'ctx',argv:[process.execPath,'-e',parentCode]});
  await wait(()=>existsSync(join(f.root,'escaped-heartbeat')));escaped=Number(readFileSync(join(f.root,'escaped.pid'),'utf8'));await sleep(350);
  const stopped=await f.runner.stopContext('ctx','stop-escape');const r=f.runner.command(c.commandId);
  assert.equal(r.status,'UNKNOWN');assert.equal(r.stopProof.observedDetached,true);assert.equal(stopped.quiescent,false);assert.equal(r.stopProof.arbitraryEscapedProcesses,'NOT_PROVEN');
  const n=readFileSync(join(f.root,'escaped-heartbeat')).length;await sleep(80);assert(readFileSync(join(f.root,'escaped-heartbeat')).length>n);
 }finally{if(escaped)try{process.kill(escaped,'SIGKILL');}catch{}await f.runner.close();rmSync(f.root,{recursive:true,force:true});}
});
async function start(state){
 const child=spawn(process.execPath,[main,'serve','--state',state,'--port','0'],{env:{PATH:process.env.PATH,WORKER_COMMAND_RUNNER_TOKEN:token},stdio:['pipe','pipe','pipe']});let out='',err='';child.stdout.on('data',b=>out+=b);child.stderr.on('data',b=>err+=b);
 const ready=await wait(()=>{const line=out.split('\n').find(l=>l.startsWith('COMMAND_RUNNER_READY '));if(child.exitCode!==null)throw Error(err);return line&&JSON.parse(line.slice(21));});return{child,...ready};
}
async function wire(base,method,path,body){const r=await fetch(base+path,{method,headers:{authorization:'Bearer '+token,'content-type':'application/json',...(body?.requestKey?{'idempotency-key':body.requestKey}:{})},body:body?JSON.stringify(body):undefined});assert(r.ok,'WIRE '+r.status);return r.json();}
test('real Runner SIGKILL and owner EOF clean groups; restart keeps original UNKNOWN with no replay',async()=>{
 const f=fixture();let live;try{
  live=await start(f.state);const store=live.storeId,boot=live.bootId;
  await wire(live.url,'POST','/v1/contexts',{contextId:'ctx',workspace:f.root,binding:{generation:'g'}});
  const request={requestKey:'crash',contextId:'ctx',argv:[process.execPath,'-e',"const f=require('node:fs');f.appendFileSync('dispatch-count','x');setInterval(()=>f.appendFileSync('heartbeat','x'),15)"]};
  const c=await wire(live.url,'POST','/v1/commands',request);await wait(()=>existsSync(join(f.root,'heartbeat')));
  const running=await wire(live.url,'GET','/v1/commands/'+c.commandId);const exited=new Promise(r=>live.child.once('exit',r));live.child.kill('SIGKILL');await exited;
  await wait(()=>{try{process.kill(-running.pgid,0);return false;}catch(e){return e.code==='ESRCH';}});
  const size=readFileSync(join(f.root,'heartbeat')).length;await sleep(100);assert.equal(readFileSync(join(f.root,'heartbeat')).length,size);
  live=await start(f.state);assert.equal(live.storeId,store);assert.notEqual(live.bootId,boot);
  assert.equal((await wire(live.url,'GET','/v1/requests/crash')).status,'UNKNOWN');assert.equal((await wire(live.url,'POST','/v1/commands',request)).status,'UNKNOWN');assert.equal(readFileSync(join(f.root,'dispatch-count'),'utf8'),'x');
  await wire(live.url,'POST','/v1/contexts',{contextId:'ctx2',workspace:f.root,binding:{generation:'g2'}});
  const lease=await wire(live.url,'POST','/v1/commands',{requestKey:'lease',contextId:'ctx2',argv:[process.execPath,'-e','setInterval(()=>{},1000)']});await wait(async()=> (await wire(live.url,'GET','/v1/commands/'+lease.commandId)).status==='RUNNING');
  const ended=new Promise(r=>live.child.once('exit',r));live.child.stdin.end();await ended;live=null;
 }finally{if(live&&live.child.exitCode===null){const ended=new Promise(r=>live.child.once('exit',r));live.child.stdin.end();await ended;}rmSync(f.root,{recursive:true,force:true});}
});
