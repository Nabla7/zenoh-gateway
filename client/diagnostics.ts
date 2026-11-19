/** Shared opt-in diagnostics; no payloads, no background network traffic, bounded retention. */
export const traceSchema = 'dimos.trace/v1';
export interface TraceContext {
    schema: string; run_id: string; publisher: string; stream: string; sequence: number;
    origin_ns: number; clock_domain: string; parents: string[]; lineage: 'complete' | 'partial' | 'unknown';
}
export type TraceEvent = Record<string, unknown> & {schema: string; event: string; mono_ns: number; clock_domain: string; process: string; context: TraceContext | null};
const prefix = new TextEncoder().encode('DIMOS_TRACE_V1\0');
export function traceContext(attachment?: Uint8Array): TraceContext | undefined {
    if (!attachment || attachment.length > 16384 + prefix.length || !prefix.every((v,i) => attachment[i] === v)) return;
    try {
        const c = JSON.parse(new TextDecoder().decode(attachment.subarray(prefix.length)));
        if (c.schema !== traceSchema || !['run_id','publisher','stream','clock_domain'].every(k => typeof c[k] === 'string' && c[k].length > 0)
            || !Number.isSafeInteger(c.sequence) || c.sequence < 0 || !Number.isSafeInteger(c.origin_ns) || c.origin_ns < 0
            || !Array.isArray(c.parents) || c.parents.length > 32 || !c.parents.every((p: unknown) => typeof p === 'string')
            || !['complete','partial','unknown'].includes(c.lineage)) return;
        return c;
    } catch { return; }
}
export function tracePayload(payload: Uint8Array, context: TraceContext): Uint8Array {
    const json = new TextEncoder().encode(JSON.stringify(context));
    if (json.length > 16384) throw new Error('trace context too large');
    const data = new Uint8Array(4 + prefix.length + json.length + payload.length);
    new DataView(data.buffer).setUint32(0, prefix.length + json.length, true);
    data.set(prefix,4); data.set(json,4+prefix.length); data.set(payload,4+prefix.length+json.length);
    return data;
}

export class TraceRecorder {
    readonly process = crypto.randomUUID();
    readonly clockDomain = `browser/${this.process}/performance`;
    private sequences = new Map<string, number>();
    private events: TraceEvent[] = [];
    private bytes = 0;
    drops = 0;
    private spanId = 0;
    private watches = new Map<string, {at:number,context?:TraceContext}>();
    private lastTick = 0;
    constructor(readonly runId: string, readonly maxBytes = 4 * 1024 * 1024, readonly maxEvents = 4096) {
        if (!/^[a-zA-Z0-9_-]{1,128}$/.test(runId) || maxBytes < 1024 || maxBytes > 16*1024*1024 || maxEvents < 1 || maxEvents > 16384) throw new Error('invalid trace configuration');
    }
    now(): number { return Math.round(performance.now() * 1e6); }
    sample(stream: string, originNs = this.now()): TraceContext {
        if (!this.sequences.has(stream) && this.sequences.size >= 1024) throw new Error('trace stream limit');
        const sequence = (this.sequences.get(stream) ?? 0) + 1; this.sequences.set(stream,sequence);
        return {schema:traceSchema,run_id:this.runId,publisher:this.process,stream,sequence,origin_ns:originNs,clock_domain:this.clockDomain,parents:[],lineage:'complete'};
    }
    event(event: string, context?: TraceContext | null, labels: Record<string, unknown> = {}): void {
        const row: TraceEvent = {...labels,schema:traceSchema,event,mono_ns:this.now(),clock_domain:this.clockDomain,process:this.process,context:context??null};
        let encoded: string;
        try { encoded = JSON.stringify(row); } catch { this.drops++; return; }
        const size = encoded.length * 2;
        if (size > 32768) { this.drops++; return; }
        // Retain an immutable metadata copy; a caller mutating its stats object
        // later must not rewrite the evidence or bypass the memory bound.
        this.events.push(JSON.parse(encoded)); this.bytes += size;
        while (this.events.length > this.maxEvents || this.bytes > this.maxBytes) {
            this.bytes -= JSON.stringify(this.events.shift()).length * 2; this.drops++;
        }
    }
    span(stage: string, context?: TraceContext, labels: Record<string, unknown> = {}): () => void {
        const fields = {...labels,span_id:String(++this.spanId)}; this.event(`${stage}.start`,context,fields);
        return () => this.event(`${stage}.end`,context,fields);
    }
    /** Drain only after a successful durable upload; caller retains an unacknowledged batch. */
    drain(maxBytes = 48000, maxEvents = 512): TraceEvent[] {
        const batch: TraceEvent[] = []; let bytes = 0;
        while (this.events.length && batch.length < maxEvents) {
            const size = JSON.stringify(this.events[0]).length * 2;
            if (size > maxBytes) { this.events.shift(); this.bytes -= size; this.drops++; continue; }
            if (bytes + size > maxBytes) break;
            bytes += size; this.bytes -= size; batch.push(this.events.shift()!);
        }
        return batch;
    }
    observe(watch: string, context?: TraceContext): void {
        if (this.watches.has(watch) || this.watches.size < 128) {
            if (!this.watches.has(watch)) this.event('freshness.observe',context,{watch,active:true});
            this.watches.set(watch,{at:this.now(),context});
        }
    }
    unwatch(watch: string): void {
        if (this.watches.delete(watch)) this.event('freshness.observe',null,{watch,active:false});
    }
    tick(): void {
        const at = this.now(); if (at-this.lastTick < 100_000_000) return; this.lastTick = at;
        for (const [watch,last] of this.watches) this.event('freshness.age',last.context,{watch,age_ns:at-last.at});
    }
    manifest(): Record<string, unknown> {
        return {kind:'manifest',schema:'dimos.incident/v1',run_id:this.runId,process:this.process,clock_domain:this.clockDomain,
            wall_ns:Date.now()*1e6,mono_ns:this.now(),health:{observer_drops:this.drops,buffer_bytes:this.bytes,buffer_events:this.events.length},coverage:'software presentation and XR submission; physical display unmeasured'};
    }
}
