#!/usr/bin/env node
import {CommandRunner} from './runner.mjs';
const args=process.argv.slice(2),mode=args.shift(),options={};
for(let n=0;n<args.length;n+=2)options[args[n]]=args[n+1];
if(mode!=='serve'||!options['--state']){console.error('Usage: main.mjs serve --state DIR [--port 0]');process.exit(2);}
const port=Number(options['--port']??0);if(!Number.isInteger(port)||port<0||port>65535)throw Error('INVALID_PORT');
const runner=new CommandRunner({state:options['--state'],token:process.env.WORKER_COMMAND_RUNNER_TOKEN});
let stopping=false;
const close=async()=>{if(stopping)return;stopping=true;await runner.close('UNKNOWN');};
process.stdin.resume();process.stdin.once('end',()=>void close());
process.once('SIGTERM',()=>void close());process.once('SIGINT',()=>void close());
console.log('COMMAND_RUNNER_READY '+JSON.stringify({url:await runner.listen(port),...runner.identity()}));
