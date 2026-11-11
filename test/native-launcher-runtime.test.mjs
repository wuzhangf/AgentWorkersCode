import {test} from 'node:test';
import assert from 'node:assert/strict';
import {spawn,execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const launcher=fileURLToPath(new URL('../Run-NativeWorker.sh',import.meta.url));
for(const engine of ['pi','codex'])test(`actual Mac launcher initializes and serves the ${engine} Runner profile`,{skip:process.platform!=='darwin'||process.env.NWF_TEST_NATIVE_LAUNCHER!=='1'},async()=>{
 const root=mkdtempSync(join(tmpdir(),'native-launcher-runtime-')),project=join(root,'项目 source'),state=join(root,'state'),token=randomBytes(32).toString('hex');let child;
 try{
  mkdirSync(project);writeFileSync(join(project,'README.md'),'launcher fixture\n');
  for(const args of [['init'],['add','.'],['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','fixture']])execFileSync('git',['-C',project,...args],{stdio:'ignore'});
  const args=[launcher,'--engine',engine,'--state',state,'--initialize','--project',project,'--project-id','fixture'];
  if(engine==='codex'){assert(process.env.NWF_TEST_CODEX_HOST&&process.env.NWF_CODEX_EXECUTABLE);args.push('--codex-host-executable',process.env.NWF_TEST_CODEX_HOST,'--codex-executable',process.env.NWF_CODEX_EXECUTABLE);}
  child=spawn('/bin/bash',args,{env:{PATH:process.env.PATH,HOME:root,WORKER_LOCAL_TOKEN:token,MODEL_API_KEY:'NWF_NON_SECRET_NO_NETWORK_SENTINEL',MODEL_DEFAULT_MODEL:'deepseek-v4.1-flash',MODEL_BASE_URL:'http://127.0.0.1:1/v1'},stdio:['ignore','pipe','pipe']});
  let output='',error='';child.stdout.on('data',b=>output+=b);child.stderr.on('data',b=>error+=b);let url;
  for(let n=0;n<1500;n++){
   assert.equal(child.exitCode,null,error);
   const line=output.split('\n').find(l=>l.startsWith('LOCAL_TASK_READY '));
   if(line){const raw=line.slice('LOCAL_TASK_READY '.length);try{url=JSON.parse(raw).url;}catch{url=raw.match(/^http:\/\/127\.0\.0\.1:\d+/)?.[0];}break;}
   await new Promise(r=>setTimeout(r,20));
  }
  assert.match(url??'',/^http:\/\/127\.0\.0\.1:\d+$/);
  const r=await fetch(url+'/local/v1/identity',{headers:{authorization:'Bearer '+token}});assert.equal(r.status,200);const identity=await r.json();
  const manifest=await (await fetch(url+'/worker-host/v1/manifest',{headers:{authorization:'Bearer '+token}})).json();
  assert.equal(manifest.storeId,identity.storeId);
  assert.equal(manifest.executionProfile??identity.nativeToolProfile,'macos-runner-v1');
  assert(!output.includes(token)&&!error.includes(token));
 }finally{
  if(child&&child.exitCode===null&&child.signalCode===null){const end=new Promise(r=>child.once('exit',r));child.kill();await Promise.race([end,new Promise(r=>setTimeout(r,5000).unref())]);if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await end;}}
  rmSync(root,{recursive:true,force:true});
 }
});
