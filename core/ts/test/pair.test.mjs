import {test} from 'node:test';
import assert from 'node:assert/strict';
import {atom,tuple} from '@bitspark/bitwire';
import {checkWirePair} from '@bitspark/bitwire/conformance';
import {pair,compose,select,route} from '../../../dist/core/ts/src/index.js';
const e=(id=1)=>tuple([atom([id]),atom([0,255])]);
test('independent bitwire admission/ownership/delivery observations',async()=>{ const results=await checkWirePair(pair);assert.equal(results.length,4); });
test('oversized sends reject without closing or reaching the peer',async()=>{
 const [a,b]=pair({maxMessageBytes:80});const got=[];b.receive(x=>got.push(x));
 await assert.rejects(a.send(atom(new Uint8Array(100))),/limit/);
 await a.send(e());await new Promise(r=>setTimeout(r,0));assert.equal(got.length,1);await a.close();
});
test('close discards undelivered work and does not claim execution cancellation',async()=>{
 const [a,b]=pair();await a.send(e());await a.close();await b.closed;assert.throws(()=>b.receive(()=>{}),/closed/);
 const [c,d]=pair();let finish;const work=new Promise(r=>{finish=r;});let result=false;
 d.receive(()=>{void work.then(()=>{result=true;});});await c.send(e());await new Promise(r=>setTimeout(r,0));
 await c.close();assert.equal(result,false);finish();await work;await new Promise(r=>setTimeout(r,0));assert.equal(result,true);
});
test('queue byte budgets and options are finite and explicit',async()=>{
 assert.throws(()=>pair({maxQueuedMessages:0}),/positive/);
 const [a,b]=pair({maxQueuedBytes:1});await assert.rejects(a.send(e()),/limit/);assert.equal((await b.closed).kind,'failed');await a.closed;
});
test('complete structures preserve values and exact byte paths; route has no fallback',()=>{
 const seen=[];const leaf=compose(x=>seen.push(x)); const own=x=>seen.push('root');
 const keys=[[atom([97,47,98]),leaf],[atom([]),leaf],[atom([0,255]),leaf]];
 const tree=compose(own,keys);keys.length=0;
 assert.equal(select(tree,[]),tree);assert.equal(tree.own(),own);assert.equal(tree.children().length,3);
 assert.equal(select(tree,[atom([])]),leaf);assert.equal(select(tree,[atom([97]),atom([98])]),undefined);
 assert.equal(select(tree,[atom([0,255])]),leaf);const rebuilt=compose(tree.decompose().own,tree.decompose().children);assert.equal(rebuilt.own(),own);
 assert.equal(route(tree,[atom([97,47,98])],e()),true);assert.equal(seen.length,1);
 assert.equal(route(tree,[atom([97])],e()),false);assert.equal(seen.length,1);
 assert.throws(()=>compose(own,[[atom([]),leaf],[atom([]),leaf]]),/duplicate/);
 const cyclic={children:()=>[[atom([]),cyclic]]};assert.throws(()=>compose(own,[[atom([]),cyclic]]),/cyclic/);
 const malformed={children:()=>[[atom([]),leaf],[atom([]),leaf]]};assert.throws(()=>compose(own,[[atom([]),malformed]]),/duplicate/);
 const nil={children:()=>[[atom([]),null]]};assert.throws(()=>compose(own,[[atom([]),nil]]),/invalid/);
});
