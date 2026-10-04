import { captureEnvelope, encodeEnvelope, type Envelope, type Wire, type Termination } from '@bitspark/bitwire';
import { pairOptions, type PairOptions } from './limits.ts';
class PairEndpoint implements Wire {
  readonly closed: Promise<Termination>;
  readonly #options: ReturnType<typeof pairOptions>;
  readonly #peer: () => PairEndpoint;
  #resolve!: (end: Termination) => void;
  #end: Termination | undefined;
  #handler: ((envelope: Envelope) => void) | undefined;
  #queue: { envelope: Envelope; size: number }[] = [];
  #bytes=0;
  #scheduled=false;
  constructor(peer: () => PairEndpoint, options: PairOptions) {
    this.#peer=peer; this.#options=pairOptions(options);
    this.closed=new Promise(resolve=>{this.#resolve=resolve;});
  }
  async send(envelope: Envelope): Promise<void> {
    if(this.#end) throw new Error('wire is closed');
    const e=captureEnvelope(envelope);const size=encodeEnvelope(e,this.#options.maxEnvelopeBytes).byteLength;
    const peer=this.#peer();
    if(peer.#end) throw new Error('peer is closed');
    if(size>peer.#options.maxEnvelopeBytes || peer.#queue.length>=peer.#options.maxQueuedEnvelopes || peer.#bytes+size>peer.#options.maxQueuedBytes) {
      peer.#terminate({kind:'failed',message:'wire input limit exceeded'});throw new Error('wire input limit exceeded');
    }
    peer.#queue.push({envelope:e,size});peer.#bytes+=size;peer.#schedule();
  }
  receive(handler: (envelope: Envelope)=>void): ()=>void {
    if(this.#end) throw new Error('wire is closed');
    if(this.#handler) throw new Error('wire already has a receive handler');
    if(typeof handler!=='function') throw new TypeError('wire handler must be a function');
    this.#handler=handler;this.#schedule();let detached=false;
    return ()=>{if(!detached){detached=true;if(this.#handler===handler)this.#handler=undefined;}};
  }
  async close(): Promise<void> {this.#terminate({kind:'closed'});await this.closed;}
  #finish(end: Termination): void {
    if(this.#end)return;this.#end=Object.freeze(end);this.#handler=undefined;
    this.#queue.length=0;this.#bytes=0;this.#resolve(this.#end);
  }
  #terminate(end: Termination): void {this.#finish(end);this.#peer().#finish(end);}
  #schedule(): void {
    if(this.#scheduled||!this.#handler||this.#end)return;this.#scheduled=true;
    queueMicrotask(()=>{this.#scheduled=false;
      while(this.#handler&&this.#queue.length&&!this.#end){
        const {envelope,size}=this.#queue.shift()!;this.#bytes-=size;
        try{this.#handler(envelope);}catch{this.#terminate({kind:'failed',message:'wire receive handler failed'});}
      }
    });
  }
}
export function pair(options: PairOptions = {}): readonly [Wire,Wire] {
  pairOptions(options); let a!:PairEndpoint,b!:PairEndpoint;
  a=new PairEndpoint(()=>b,options);b=new PairEndpoint(()=>a,options);return Object.freeze([a,b]);
}
