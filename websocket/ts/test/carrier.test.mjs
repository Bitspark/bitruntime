import {test} from 'node:test';
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {createServer} from 'node:http';
import {createServer as createHttpsServer} from 'node:https';
import {readFileSync} from 'node:fs';
import WebSocket from 'ws';
import {atom,tuple,encodeMessage,WEBSOCKET_PROTOCOL} from '@bitspark/bitwire';
import {checkAddressedEndpoints} from '@bitspark/bitwire/conformance';
import {addressed} from '../../../dist/core/ts/src/index.js';
import {connectWebSocket,listenWebSocket,bindWebSocketServer} from '../../../dist/websocket/ts/src/index.js';
const next=(wire)=>new Promise(resolve=>{const detach=wire.receive(e=>{detach();resolve(e);});});
const e=()=>tuple([atom([255]),tuple([])]);

test('the same independent addressed observations over WebSocket',async t=>{
 let accept;const incoming=new Promise(r=>{accept=r;});
 const server=await listenWebSocket({},wire=>accept(wire));t.after(()=>server.close());
 const a=await connectWebSocket(server.url);t.after(()=>a.close());const b=await incoming;
 await checkAddressedEndpoints(a,b,addressed);
});
test('real binary duplex endpoints preserve unknown values and repeated admissions',async t=>{
 let accepted;const serverWire=new Promise(r=>{accepted=r;});const server=await listenWebSocket({},wire=>accepted(wire));t.after(()=>server.close());
 const a=await connectWebSocket(server.url);t.after(()=>a.close());const b=await serverWire;
 const got=[];let complete;const delivered=new Promise(r=>{complete=r;});b.receive(x=>{got.push(x);if(got.length===2)complete();});await a.send(e());await a.send(e());await delivered;
 assert.equal(got.length,2);assert.ok(got[0].equals(e()));
 const reply=next(a);await b.send(e());assert.ok((await reply).equals(e()));await a.close();await b.closed;
});
test('native binary fragmented messages are reassembled; text/malformed traffic fails',async t=>{
 for(const bad of [Buffer.from([255]),'text']){
  let accepted;const connected=new Promise(r=>{accepted=r;});const server=await listenWebSocket({},wire=>accepted(wire));
  const raw=new WebSocket(server.url,WEBSOCKET_PROTOCOL);await once(raw,'open');const wire=await connected;const got=next(wire);
  const bytes=encodeMessage(e());raw.send(bytes.slice(0,5),{binary:true,fin:false});raw.send(bytes.slice(5),{binary:true,fin:true});assert.ok((await got).equals(e()));
  raw.send(bad);assert.equal((await wire.closed).kind,'failed');raw.terminate();await server.close();
 }
});
test('protocol negotiation and establishment abort are explicit',async t=>{
 const server=await listenWebSocket({},()=>{});t.after(()=>server.close());
 const wrong=new WebSocket(server.url,'other.protocol');wrong.on('error',()=>{});await new Promise(r=>wrong.once('close',r));
 const controller=new AbortController();controller.abort();await assert.rejects(connectWebSocket(server.url,{signal:controller.signal}),/aborted/);
 await assert.rejects(connectWebSocket('http://localhost/'),/ws:/);
});
test('receiver throws terminate; a bound listener preserves caller-owned HTTP',async t=>{
 const http=createServer((_req,res)=>{res.end('http remains');});await new Promise(r=>http.listen(0,'127.0.0.1',r));t.after(()=>new Promise(r=>http.close(r)));
 const binding=bindWebSocketServer(http,wire=>wire.receive(()=>{throw new Error('handler');}));
 const url=`ws://127.0.0.1:${http.address().port}/wire`;const a=await connectWebSocket(url);await a.send(e());assert.equal((await a.closed).kind,'failed');await binding.close();
 assert.equal(await (await fetch(url.replace('ws:','http:'))).text(),'http remains');
});
test('detached receive budgets fail the connection and close releases listener',async()=>{
 let accepted;const connected=new Promise(r=>{accepted=r;});const server=await listenWebSocket({maxQueuedMessages:1},wire=>accepted(wire));
 const a=await connectWebSocket(server.url);const b=await connected;await a.send(e());await a.send(e());assert.equal((await b.closed).kind,'failed');await a.closed;await server.close();await server.closed;
 await assert.rejects(connectWebSocket(server.url),/failed/);
});

test('TLS verification and explicit origin/authorization policy', async t => {
 const cert=readFileSync(new URL('./fixtures/localhost-cert.pem',import.meta.url));
 const key=readFileSync(new URL('./fixtures/localhost-key.pem',import.meta.url));
 const server=createHttpsServer({cert,key});await new Promise(r=>server.listen(0,'127.0.0.1',r));
 const bound=bindWebSocketServer(server,()=>{}, {allowedOrigins:['https://allowed.example']});
 t.after(async()=>{await bound.close();await new Promise(r=>server.close(r));});
 const url=`wss://127.0.0.1:${server.address().port}/wire`;
 await assert.rejects(connectWebSocket(url),/failed/);
 await assert.rejects(connectWebSocket(url,{ca:cert,headers:{origin:'https://wrong.example'}}),/failed/);
 const wire=await connectWebSocket(url,{ca:cert,headers:{origin:'https://allowed.example'}});await wire.close();
 const refused=await listenWebSocket({authorize:()=>{throw new Error('private');}},()=>assert.fail('unauthorized connection'));
 await assert.rejects(connectWebSocket(refused.url),/failed/);await refused.close();
});

test('outgoing byte/count limits refuse admission; later write failure terminates admitted work', async t => {
 const server=await listenWebSocket({},()=>{});t.after(()=>server.close());
 const bytes=encodeMessage(e()).length;
 const wire=await connectWebSocket(server.url,{maxQueuedBytes:bytes,maxQueuedMessages:1});t.after(()=>wire.close());
 const callbacks=[];
 t.mock.method(WebSocket.prototype,'send',function(_bytes,_options,callback){callbacks.push(callback);});
 await wire.send(e());await assert.rejects(wire.send(e()),/queue/);assert.equal(callbacks.length,1);
 callbacks[0](new Error('write failed after admission'));
 assert.equal((await wire.closed).kind,'failed');
});

test('independently encoded bare atom and malformed bounds use only current protocol', async t => {
 let accept;const incoming=new Promise(r=>{accept=r;});const server=await listenWebSocket({maxMessageBytes:80},wire=>accept(wire));t.after(()=>server.close());
 const raw=new WebSocket(server.url,WEBSOCKET_PROTOCOL);await once(raw,'open');t.after(()=>raw.terminate());const wire=await incoming;
 const delivered=next(wire);raw.send(Buffer.from('0000','hex'));
 const received=await delivered;assert.ok(received.equals(atom([])));
 raw.send(Buffer.alloc(81));assert.equal((await wire.closed).kind,'failed');
});
