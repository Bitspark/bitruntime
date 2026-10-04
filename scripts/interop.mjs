import assert from 'node:assert/strict';
import {spawn, spawnSync} from 'node:child_process';
import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createInterface} from 'node:readline';
import {atom,tuple,pathEqual} from '@bitspark/bitwire';
import {connectWebSocket,listenWebSocket} from '../dist/websocket/ts/src/index.js';
const directory=mkdtempSync(join(tmpdir(),'bitruntime-interop-'));
const binary=join(directory,process.platform==='win32'?'peer.exe':'peer');
const built=spawnSync('go',['build','-o',binary,'./conformance/interop/go'],{stdio:'inherit'});
assert.equal(built.status,0,'Go peer build failed');
function peer(args){
 const child=spawn(binary,args,{stdio:['pipe','pipe','inherit']});
 const lines=[],readers=[];
 createInterface({input:child.stdout}).on('line',line=>{const reader=readers.shift();if(reader)reader(line);else lines.push(line);});
 const timeout=setTimeout(()=>child.kill(),30000);
 const ended=new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',code=>{clearTimeout(timeout);resolve(code);while(readers.length)readers.shift()(undefined);});});
 return {child,ended,line:()=>lines.length?Promise.resolve(lines.shift()):new Promise(r=>readers.push(r))};
}
const message={source:[atom([0,255])],destination:[atom([]),atom([97,47,98])],id:atom([]),payload:tuple([atom(new TextEncoder().encode('unknown.embedding')),atom([255,0,128]),tuple([])])};
const server=peer([]);
let wire;
try{
 const url=await server.line();assert.match(url??'',/^ws:/);wire=await connectWebSocket(url);
 let reply,greeting;const received=new Promise(r=>{reply=r;}),incoming=new Promise(r=>{greeting=r;});
 wire.receive(e=>{if(e.correlation!==undefined)reply(e);else greeting(e);});
 const announced=await incoming;assert.ok(announced.payload.equals(message.payload));assert.ok(pathEqual(announced.destination,message.destination));
 await wire.send({...announced,source:announced.destination,destination:announced.source,correlation:announced.id});
 await wire.send(message);const response=await received;assert.ok(response.payload.equals(message.payload));assert.ok(pathEqual(response.source,message.destination));assert.ok(response.correlation.equals(message.id));
 await wire.close();server.child.stdin.end();assert.equal(await server.ended,0);
}finally{await wire?.close();server.child.kill();}
const listener=await listenWebSocket({},wire=>wire.receive(e=>{
 void wire.send({source:e.destination,destination:e.source,id:atom([255]),correlation:e.id,payload:e.payload});
}));
try{const client=peer(['client',listener.url]);assert.equal(await client.line(),'ok');assert.equal(await client.ended,0);}finally{await listener.close();}
console.log('Go and TypeScript exchange exact byte paths and opaque Ontos envelopes in both connection roles.');
