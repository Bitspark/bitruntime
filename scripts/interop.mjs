import assert from 'node:assert/strict';
import {spawn, spawnSync} from 'node:child_process';
import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createInterface} from 'node:readline';
import {atom,tuple,pathEqual} from '@bitspark/bitwire';
import {addressed} from '../dist/core/ts/src/index.js';
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
const message=tuple([atom(new TextEncoder().encode('unknown.embedding')),atom([255,0,128]),tuple([])]);
const path=[atom([]),atom([97,47,98]),atom([255])], greeting=atom([255]);
for(const mode of ['raw','addressed']){
 const server=peer([mode]);let endpoint;
 try{
  const url=await server.line();assert.match(url??'',/^ws:/);endpoint=await connectWebSocket(url);
  const seen=[];let finish;const received=new Promise(r=>{finish=r;});
  const observe=(p,v)=>{seen.push({path:p,message:v});if(seen.length===2)finish();};
  if(mode==='addressed'){const wire=addressed(endpoint);wire.receive(observe);await wire.send(path,message);}
  else{endpoint.receive(v=>observe([],v));await endpoint.send(message);}
  await received;assert.ok(seen[0].message.equals(greeting));assert.ok(pathEqual(seen[0].path,[]));
  assert.ok(seen[1].message.equals(message));assert.ok(pathEqual(seen[1].path,mode==='addressed'?path:[]));
  await endpoint.close();server.child.stdin.end();assert.equal(await server.ended,0);
 }finally{await endpoint?.close();server.child.kill();}
 const listener=await listenWebSocket({},endpoint=>{
  if(mode==='addressed'){const wire=addressed(endpoint);wire.receive((p,v)=>{void wire.send(p,v);});void wire.send([],greeting);}
  else{endpoint.receive(v=>{void endpoint.send(v);});void endpoint.send(greeting);}
 });
 try{const client=peer(['client',listener.url,mode]);assert.equal(await client.line(),'ok');assert.equal(await client.ended,0);}finally{await listener.close();}
}
console.log('Go and TypeScript exchange raw messages and addressed byte paths in both connection roles.');
