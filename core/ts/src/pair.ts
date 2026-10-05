import { encodeMessage, type Value, type Endpoint, type Termination } from '@bitspark/bitwire';
import { pairOptions, type PairOptions } from './limits.ts';
class PairEndpoint implements Endpoint {
  readonly closed: Promise<Termination>;
  readonly #options: ReturnType<typeof pairOptions>;
  readonly #peer: () => PairEndpoint;
  #resolve!: (end: Termination) => void;
  #end: Termination | undefined;
  #handler: ((message: Value) => void) | undefined;
  #queue: { message: Value; size: number }[] = [];
  #bytes=0;
  #scheduled=false;
  constructor(peer: () => PairEndpoint, options: PairOptions) {
    this.#peer=peer; this.#options=pairOptions(options);
    this.closed=new Promise(resolve=>{this.#resolve=resolve;});
  }
  async send(message: Value): Promise<void> {
    if(this.#end) throw new Error('wire is closed');
    const e=message;const size=encodeMessage(e,this.#options.maxMessageBytes).byteLength;
    const peer=this.#peer();
    if(peer.#end) throw new Error('peer is closed');
    if(size>peer.#options.maxMessageBytes || peer.#queue.length>=peer.#options.maxQueuedMessages || peer.#bytes+size>peer.#options.maxQueuedBytes) {
      peer.#terminate({kind:'failed',message:'wire input limit exceeded'});throw new Error('wire input limit exceeded');
    }
    peer.#queue.push({message:e,size});peer.#bytes+=size;peer.#schedule();
  }
  receive(handler: (message: Value)=>void): ()=>void {
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
        const {message,size}=this.#queue.shift()!;this.#bytes-=size;
        try{this.#handler(message);}catch{this.#terminate({kind:'failed',message:'wire receive handler failed'});}
      }
    });
  }
}
export function pair(options: PairOptions = {}): readonly [Endpoint,Endpoint] {
  pairOptions(options); let a!:PairEndpoint,b!:PairEndpoint;
  a=new PairEndpoint(()=>b,options);b=new PairEndpoint(()=>a,options);return Object.freeze([a,b]);
}
