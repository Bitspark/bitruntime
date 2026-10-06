import {test} from 'node:test';
import assert from 'node:assert/strict';
import {atom,tuple,pathEqual} from '@bitspark/bitwire';
import {checkAddressedEndpoints} from '@bitspark/bitwire/conformance';
import {pair,addressed,bind,under,asAddressed,compose,MissingPathError} from '../../../dist/core/ts/src/index.js';

test('independent addressed observations over local endpoints',async()=>{
 const [a,b]=pair(); await checkAddressedEndpoints(a,b,addressed);
});
test('prefix cuts capture scope; binding grants sending only',async()=>{
 const seen=[]; const target={send:async(path,message)=>{seen.push({path,message});}};
 const p=[atom([255])],q=[atom([])], message=tuple([]);
 const prefix=under(target,p), selected=bind(prefix,q);p.length=0;q.length=0;
 assert.deepEqual(Object.keys(selected),['send']);assert.equal(seen.length,0);
 await selected.send(message);
 await under(under(target,[atom([255])]),[atom([])]).send([],message);
 await target.send([atom([255]),atom([])],message);
 assert.equal(seen.length,3);
 for(const sent of seen){assert.ok(pathEqual(sent.path,[atom([255]),atom([])]));assert.equal(sent.message,message);}
});
test('tree lifting preserves own capabilities, absence and operation refusal',async()=>{
 const calls=[], refused=new Error('sender refused');
 const own={send:async value=>{calls.push(['root',value]);}};
 const child={send:async value=>{calls.push(['child',value]);}};
 const refusing={send:async()=>{throw refused;}};
 const leaf=compose(child), node=compose(refusing,[[atom([255]),leaf]]);
 const tree=compose(own,[[atom([]),node]]),parts=tree.decompose(),rebuilt=compose(parts.own,parts.children);
 assert.equal(rebuilt.own(),own);assert.equal(rebuilt.at([atom([])]),node);assert.equal(calls.length,0);
 const message=atom([7]);
 await asAddressed(rebuilt).send([],message);
 await asAddressed(tree.at([atom([])])).send([atom([255])],message);
 await asAddressed(tree).send([atom([]),atom([255])],message);
 assert.deepEqual(calls.map(([who])=>who),['root','child','child']);
 assert.ok(calls.every(([,value])=>value===message));
 await assert.rejects(asAddressed(tree).send([atom([1])],message),MissingPathError);
 await assert.rejects(asAddressed(tree).send([atom([])],message),error=>error===refused);
 assert.equal(calls.length,3);
});
test('raw data stays opaque; malformed addressed data fails only its interpretation',async()=>{
 const [a,b]=pair();const invalid=tuple([]), got=new Promise(r=>b.receive(r));
 await a.send(invalid);assert.ok((await got).equals(invalid));await a.close();
 const [c,d]=pair();addressed(d);await c.send(invalid); // still just queued data
 addressed(d).receive(()=>assert.fail('malformed address dispatched'));
 assert.equal((await d.closed).kind,'failed');await c.closed;
});
