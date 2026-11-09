import {DatabaseSync} from 'node:sqlite';
import {createServer} from 'node:http';
import {fork} from 'node:child_process';
import {randomUUID, timingSafeEqual, createHash} from 'node:crypto';
import {mkdirSync, realpathSync, statSync} from 'node:fs';
import {join, resolve, relative, isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {StringDecoder} from 'node:string_decoder';

const TERMINAL = new Set(['SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','UNKNOWN']);
const OUTPUT_LIMIT = 1024 * 1024;
const sleep = ms => new Promise(r => setTimeout(r, ms));
function fail(code) { throw new Error(code); }
const validId = value => typeof value === 'string' && /^[A-Za-z0-9._:-]{1,240}$/.test(value);
function canonical(v) {
  if (Array.isArray(v)) return '[' + v.map(canonical).join(',') + ']';
  if (v && typeof v === 'object') return '{' + Object.keys(v).sort().map(k => JSON.stringify(k)+':'+canonical(v[k])).join(',') + '}';
  return JSON.stringify(v);
}
const inside = (root, path) => { const r = relative(root, path); return r === '' || (!isAbsolute(r) && r !== '..' && !r.startsWith('../')); };
const secretName = k => /KEY|TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTH/i.test(k) || /^(MODEL_|LLM_|WORKER_|PAC_|ACS_|DYLD_|LD_|GIT_CONFIG|NODE_OPTIONS$|BASH_ENV$|ENV$)/i.test(k);
const internalKey=(...parts)=>createHash('sha256').update(JSON.stringify(parts)).digest('hex');
const neverDispatchedProof=()=>({scope:'REGISTERED_PROCESS_GROUP',groupExited:true,neverDispatched:true,arbitraryEscapedProcesses:'NOT_PROVEN'});
export class CommandRunner {
  live = new Map(); shuttingDown = false; bootId = randomUUID();
  constructor({state, token}) {
    if (typeof token !== 'string' || token.length < 24) fail('RUNNER_TOKEN_REQUIRED');
    this.token = token; this.state = resolve(state); mkdirSync(this.state,{recursive:true,mode:0o700});
    this.owner = new DatabaseSync(join(this.state,'owner.sqlite'));
    try { this.owner.exec('PRAGMA busy_timeout=0; BEGIN EXCLUSIVE; CREATE TABLE IF NOT EXISTS owner(x)'); }
    catch { this.owner.close(); fail('RUNNER_STATE_ALREADY_OWNED'); }
    this.db = new DatabaseSync(join(this.state,'commands.sqlite'));
    if(this.db.prepare('PRAGMA user_version').get().user_version>1){this.db.close();this.owner.close();fail('RUNNER_STATE_VERSION_UNSUPPORTED');}
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS contexts(id TEXT PRIMARY KEY,body TEXT NOT NULL,data TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS commands(id TEXT PRIMARY KEY,data TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS requests(scope TEXT NOT NULL,k TEXT NOT NULL,body TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(scope,k));
      CREATE TABLE IF NOT EXISTS output(command TEXT NOT NULL,seq INTEGER NOT NULL,stream TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(command,seq));`);
    this.db.exec('PRAGMA user_version=1');
    let store = this.db.prepare('SELECT v FROM meta WHERE k=?').get('store');
    if (!store) { this.db.prepare('INSERT INTO meta VALUES(?,?)').run('store',randomUUID()); store=this.db.prepare('SELECT v FROM meta WHERE k=?').get('store'); }
    this.storeId = store.v;
    for (const {id,data} of this.db.prepare('SELECT id,data FROM commands').all()) {
      const c=JSON.parse(data); if(!TERMINAL.has(c.status)) {
        if(c.status==='ACCEPTED')c.stopProof=neverDispatchedProof();
        c.status=c.status==='ACCEPTED'?'CANCELLED':'UNKNOWN';c.reasonCode='RUNNER_RESTART_UNCONFIRMED';c.finishedAt=new Date().toISOString();this.save(c);
      }
    }
    for (const {id,data} of this.db.prepare('SELECT id,data FROM contexts').all()) {
      const c=JSON.parse(data);c.closed=true;this.db.prepare('UPDATE contexts SET data=? WHERE id=?').run(JSON.stringify(c),id);
    }
    this.server=createServer((req,res)=>void this.http(req,res));
  }
  identity() { return {schemaVersion:'command-runner/1',storeId:this.storeId,bootId:this.bootId}; }
  transaction(fn) { this.db.exec('BEGIN IMMEDIATE');try{const r=fn();this.db.exec('COMMIT');return r;}catch(e){this.db.exec('ROLLBACK');throw e;} }
  save(c) { this.db.prepare('INSERT OR REPLACE INTO commands VALUES(?,?)').run(c.commandId,JSON.stringify(c));return c; }
  command(id) { const r=this.db.prepare('SELECT data FROM commands WHERE id=?').get(id);if(!r)fail('COMMAND_NOT_FOUND');return JSON.parse(r.data); }
  rawContext(id) { const r=this.db.prepare('SELECT data FROM contexts WHERE id=?').get(id);if(!r)fail('CONTEXT_NOT_FOUND');return JSON.parse(r.data); }
  context(id) {
    const c=this.rawContext(id),commands=this.db.prepare('SELECT data FROM commands').all().map(r=>JSON.parse(r.data)).filter(r=>r.contextId===id);
    const active=commands.filter(r=>!TERMINAL.has(r.status)),unknown=commands.filter(r=>r.status==='UNKNOWN');
    return {...c,activeCommands:active.map(r=>r.commandId),unknownCommands:unknown.map(r=>r.commandId),quiescent:c.closed&&active.length===0&&unknown.length===0,
      stopProof:{scope:'REGISTERED_PROCESS_GROUP',groupExited:active.length===0&&commands.every(r=>!r.stopProof||r.stopProof.groupExited),arbitraryEscapedProcesses:'NOT_PROVEN'}};
  }
  register(b) {
    if(this.shuttingDown)fail('RUNNER_CLOSED');if(!validId(b.contextId)||typeof b.workspace!=='string'||!isAbsolute(b.workspace)||!b.binding||Array.isArray(b.binding)||typeof b.binding!=='object')fail('INVALID_CONTEXT');
    const workspace=realpathSync(b.workspace);if(!statSync(workspace).isDirectory())fail('INVALID_WORKSPACE');
    const names=b.envAllowlist??[];if(!Array.isArray(names)||names.some(k=>typeof k!=='string'||!/^[A-Z][A-Z0-9_]{0,63}$/.test(k)||secretName(k)))fail('ENV_ALLOWLIST_DENIED');
    const normalized={contextId:b.contextId,workspace,binding:b.binding,envAllowlist:[...new Set(names)].sort()},body=canonical(normalized);
    const old=this.db.prepare('SELECT body FROM contexts WHERE id=?').get(b.contextId);
    if(old){if(old.body!==body)fail('CONTEXT_BINDING_CHANGED');return this.context(b.contextId);}
    const dirs=join(this.state,'contexts',b.contextId);for(const d of ['home','tmp','cache'])mkdirSync(join(dirs,d),{recursive:true,mode:0o700});
    const c={...normalized,closed:false,createdAt:new Date().toISOString()};this.db.prepare('INSERT INTO contexts VALUES(?,?,?)').run(c.contextId,body,JSON.stringify(c));return this.context(c.contextId);
  }
  replay(scope,key,body) {
    if(!validId(key))fail('REQUEST_KEY_REQUIRED');const row=this.db.prepare('SELECT body,target FROM requests WHERE scope=? AND k=?').get(scope,key);
    if(row&&row.body!==canonical(body))fail('IDEMPOTENCY_CONFLICT');return row;
  }
  lookup(key) { const r=this.db.prepare('SELECT target FROM requests WHERE scope=? AND k=?').get('create',key);if(!r)fail('REQUEST_NOT_FOUND');return this.command(r.target); }
  create(b) {
    const prior=this.replay('create',b.requestKey,b);if(prior)return this.command(prior.target);
    if(this.shuttingDown)fail('RUNNER_CLOSED');const context=this.rawContext(b.contextId);if(context.closed)fail('CONTEXT_CLOSED');
    const kind=b.kind??'command';if(!['command','build','test'].includes(kind))fail('INVALID_COMMAND_KIND');
    let argv;if(b.argv!==undefined&&b.shell!==undefined)fail('COMMAND_SHAPE_CONFLICT');
    if(b.argv!==undefined){if(!Array.isArray(b.argv)||!b.argv.length||b.argv.length>256||b.argv.some(s=>typeof s!=='string'||s.includes('\0')||s.length>32768)||!b.argv[0])fail('INVALID_ARGV');argv=b.argv;}
    else {const s=b.shell;if(!s||typeof s.script!=='string'||!s.script.trim()||s.script.length>32768||s.script.includes('\0')||!['/bin/bash','/bin/zsh','/bin/sh'].includes(s.executable??'/bin/bash'))fail('INVALID_SHELL');argv=[s.executable??'/bin/bash','-c',s.script];}
    const cwd=realpathSync(resolve(context.workspace,b.cwd??'.'));if(!inside(context.workspace,cwd)||!statSync(cwd).isDirectory())fail('CWD_OUTSIDE_WORKSPACE');
    const timeoutMs=b.timeoutMs??120000;if(!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>600000)fail('INVALID_TIMEOUT');
    const commandId='command-'+randomUUID(),c={commandId,requestKey:b.requestKey,contextId:b.contextId,kind,status:'ACCEPTED',argv,cwd,timeoutMs,binding:context.binding,
      exitCode:null,signal:null,stdout:'',stderr:'',outputTruncated:false,createdAt:new Date().toISOString(),bootId:this.bootId};
    this.transaction(()=>{this.save(c);this.db.prepare('INSERT INTO requests VALUES(?,?,?,?)').run('create',b.requestKey,canonical(b),commandId);});
    setImmediate(()=>void this.dispatch(commandId));return c;
  }
  environment(context) {
    const env={};for(const k of ['PATH','LANG','LC_ALL','DEVELOPER_DIR','JAVA_HOME','RUSTUP_HOME','CARGO_HOME',...context.envAllowlist])if(!secretName(k)&&process.env[k])env[k]=process.env[k];
    const root=join(this.state,'contexts',context.contextId);Object.assign(env,{HOME:join(root,'home'),TMPDIR:join(root,'tmp'),TMP:join(root,'tmp'),TEMP:join(root,'tmp'),XDG_CACHE_HOME:join(root,'cache'),GIT_CONFIG_NOSYSTEM:'1',GIT_CONFIG_GLOBAL:'/dev/null'});
    return env;
  }
  async dispatch(id) {
    let c=this.command(id);if(c.status!=='ACCEPTED')return;
    if(this.shuttingDown||this.rawContext(c.contextId).closed){c.status='CANCELLED';c.reasonCode='CLOSED_BEFORE_DISPATCH';c.stopProof=neverDispatchedProof();this.save(c);return;}
    c.status='DISPATCHING';this.save(c);
    let guardian;
    try{guardian=fork(fileURLToPath(new URL('./guardian.mjs',import.meta.url)),[],{detached:true,execArgv:[],env:{},stdio:['ignore','ignore','ignore','ipc']});}
    catch{c.status='UNKNOWN';c.reasonCode='GUARDIAN_START_UNCONFIRMED';this.save(c);return;}
    let done;const completion=new Promise(r=>done=r);
    const live={guardian,done,completion,finishing:false,result:null,cancel:null,seq:0,decoders:{stdout:new StringDecoder('utf8'),stderr:new StringDecoder('utf8')}};this.live.set(id,live);
    const finish=()=>void this.finish(id);
    guardian.once('error',finish);guardian.once('exit',finish);
    guardian.on('message',m=>{
      if(live.finishing&&m.type!=='observation')return;
      if(m.type==='ready'){
        c=this.command(id);c.pgid=guardian.pid;
        if(this.shuttingDown||this.rawContext(c.contextId).closed||live.cancel){live.cancel??='CANCELLED';this.save(c);return finish();}
        c.status='RUNNING';c.startedAt=new Date().toISOString();this.save(c);
        guardian.send({type:'run',argv:c.argv,cwd:c.cwd,environment:this.environment(this.rawContext(c.contextId))},error=>{if(error)finish();});
        live.timer=setTimeout(()=>{live.cancel='TIMED_OUT';finish();},c.timeoutMs);
      }else if(m.type==='observation'){live.observedDetached = m.escaped===true;live.observationFailed ||= m.observationFailed===true;
      }else if(m.type==='output'&&['stdout','stderr'].includes(m.stream)&&!live.finishing){
        const c=this.command(id),remaining=OUTPUT_LIMIT-Buffer.byteLength(c[m.stream]),bytes=Buffer.from(m.bytes,'base64');
        const data=live.decoders[m.stream].write(bytes.subarray(0,Math.max(0,remaining)));if(bytes.length>remaining)c.outputTruncated=true;
        if(data){c[m.stream]+=data;this.db.prepare('INSERT INTO output VALUES(?,?,?,?)').run(id,++live.seq,m.stream,data);}this.save(c);
      }else if(m.type==='result'){live.result=m;finish();}
    });
  }
  async finish(id) {
    const live=this.live.get(id);if(!live||live.finishing)return live?.completion;
    live.finishing=true;clearTimeout(live.timer);
    const child=live.guardian;
    if(child.connected)try{child.send({type:'stop'},()=>{});}catch{}
    let zero=false;
    for(let n=0;n<500;n++){
      if(child.pid){try{process.kill(-child.pid,0);}catch(e){if(e.code==='ESRCH'){zero=true;break;}}}
      if(n===100&&child.pid)try{process.kill(-child.pid,'SIGKILL');}catch{}
      await sleep(10);
    }
    const c=this.command(id),result=live.result;c.exitCode=result?.exitCode??null;c.signal=result?.signal??null;
    const detached=live.observedDetached??result?.escaped??false;
    const observationFailed=live.observationFailed||result?.observationFailed||false;
    c.stopProof={scope:'REGISTERED_PROCESS_GROUP',groupExited:zero,observedDetached:detached,observationFailed,arbitraryEscapedProcesses:'NOT_PROVEN'};
    if(!zero||detached||observationFailed||(!result&&!live.cancel)){c.status='UNKNOWN';c.reasonCode=detached?'DETACHED_PROCESS_OBSERVED':observationFailed?'PROCESS_OBSERVATION_FAILED':'COMMAND_OUTCOME_UNCONFIRMED';}
    else if(live.cancel)c.status=live.cancel;
    else c.status=result.launchFailed?'FAILED':result.exitCode===0?'SUCCEEDED':'FAILED';
    c.finishedAt=new Date().toISOString();this.save(c);this.live.delete(id);live.done(c);return c;
  }
  async stopCommand(id,key,reason='CANCELLED') {
    const body={commandId:id,requestKey:key},old=this.replay('stop',key,body);
    if(!old)this.db.prepare('INSERT INTO requests VALUES(?,?,?,?)').run('stop',key,canonical(body),id);
    const c=this.command(id);if(TERMINAL.has(c.status))return c;
    const live=this.live.get(id);if(live){live.cancel=reason;await this.finish(id);return this.command(id);}
    c.status=reason;c.reasonCode='CLOSED_BEFORE_DISPATCH';c.stopProof=neverDispatchedProof();return this.save(c);
  }
  async stopContext(id,key,reason='CANCELLED') {
    const body={contextId:id,requestKey:key};this.replay('context-stop',key,body);const c=this.rawContext(id);
    this.transaction(()=>{c.closed=true;this.db.prepare('UPDATE contexts SET data=? WHERE id=?').run(JSON.stringify(c),id);this.db.prepare('INSERT OR IGNORE INTO requests VALUES(?,?,?,?)').run('context-stop',key,canonical(body),id);});
    const commands=this.db.prepare('SELECT data FROM commands').all().map(r=>JSON.parse(r.data)).filter(r=>r.contextId===id&&!TERMINAL.has(r.status));
    await Promise.all(commands.map(r=>this.stopCommand(r.commandId,internalKey('ctxstop',key,r.commandId),reason)));return this.context(id);
  }
  async listen(port=0){await new Promise(r=>this.server.listen(port,'127.0.0.1',r));return 'http://127.0.0.1:'+this.server.address().port;}
  async close(reason='UNKNOWN') {
    if(this.shuttingDown)return;this.shuttingDown=true;
    for(const {id} of this.db.prepare('SELECT id FROM contexts').all())await this.stopContext(id,internalKey('ownerclose',this.bootId,id),reason);
    if(this.server.listening){this.server.closeAllConnections();await new Promise(r=>this.server.close(r));}
    this.db.close();this.owner.close();
  }
  async http(req,res) {
    const send=(status,body)=>{const bytes=Buffer.from(JSON.stringify(body));res.writeHead(status,{'content-type':'application/json','content-length':bytes.length,'cache-control':'no-store','connection':'close'});res.end(bytes);};
    try{
      const actual=Buffer.from(req.headers.authorization??''),expected=Buffer.from('Bearer '+this.token);
      if(actual.length!==expected.length||!timingSafeEqual(actual,expected))return send(401,{code:'AUTH_REQUIRED'});
      if(req.headers.origin)return send(403,{code:'ORIGIN_DENIED'});
      let body={};if(req.method==='POST'){let n=0,b=[];for await(const data of req){n+=data.length;if(n>65536)fail('REQUEST_TOO_LARGE');b.push(data);}try{body=JSON.parse(Buffer.concat(b).toString()||'{}');}catch{fail('INVALID_JSON');}}
      const url=new URL(req.url,'http://127.0.0.1'),path=url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
      if(req.method==='POST'&&path.join('/')!=='v1/contexts'&&req.headers['idempotency-key']!==body.requestKey)fail('IDEMPOTENCY_KEY_REQUIRED');
      if(path[0]!=='v1')fail('ROUTE_NOT_FOUND');
      if(req.method==='GET'&&path[1]==='identity')return send(200,this.identity());
      if(path[1]==='contexts'){
        if(req.method==='POST'&&path.length===2)return send(200,this.register(body));
        if(req.method==='GET'&&path.length===3)return send(200,this.context(path[2]));
        if(req.method==='POST'&&path.length===4&&path[3]==='stop')return send(200,await this.stopContext(path[2],body.requestKey));
      }
      if(req.method==='GET'&&path[1]==='requests'&&path.length===3)return send(200,this.lookup(path[2]));
      if(path[1]==='commands'){
        if(req.method==='POST'&&path.length===2)return send(202,this.create(body));
        if(req.method==='GET'&&path.length===3)return send(200,this.command(path[2]));
        if(req.method==='POST'&&path[3]==='stop')return send(200,await this.stopCommand(path[2],body.requestKey));
        if(req.method==='GET'&&path[3]==='output'){
          this.command(path[2]);const cursor=url.searchParams.get('cursor')??'0';if(!/^\d+$/.test(cursor))fail('INVALID_CURSOR');
          const rows=this.db.prepare('SELECT seq,stream,data FROM output WHERE command=? AND seq>? ORDER BY seq LIMIT 100').all(path[2],Number(cursor));
          return send(200,{items:rows.map(r=>({sequence:String(r.seq),stream:r.stream,data:r.data})),cursor:rows.length?String(rows.at(-1).seq):cursor});
        }
      }
      fail('ROUTE_NOT_FOUND');
    }catch(e){const code=/^[A-Z0-9_]{1,100}$/.test(e.message)?e.message:'RUNNER_REQUEST_FAILED';send(code.endsWith('NOT_FOUND')?404:code.includes('CONFLICT')||code.includes('CLOSED')||code.includes('CHANGED')?409:400,{code});}
  }
}
