// One acknowledged batch at a time. Compression and network IO run outside XR callbacks.
import {TraceRecorder, type TraceEvent} from './diagnostics.ts';

interface Recorder { drain(maxBytes: number, maxEvents: number): TraceEvent[]; manifest(): Record<string, unknown> }
export class TraceUpload {
  send: (body: Uint8Array<ArrayBuffer>, gzip: boolean, batchId: string) => Promise<unknown>;
  now: () => number;
  credit: number; last: number; pending: {body: Uint8Array<ArrayBuffer>, gzip: boolean, batchId: string} | null;
  bytesSent: number; failures: number; busy: boolean;
  constructor(send: (body: Uint8Array<ArrayBuffer>, gzip: boolean, batchId: string) => Promise<unknown>, now = () => performance.now()) {
    this.send = send; this.now = now; this.credit = 0; this.last = now(); this.pending = null;
    this.bytesSent = 0; this.failures = 0; this.busy = false;
  }
  async flush(trace: Recorder | null | undefined): Promise<void> {
    if (!trace || this.busy) return;
    this.busy = true;
    try {
      if (!this.pending) {
        // Leave room for the manifest and framing below the token bucket's
        // maximum credit, including browsers without streaming compression.
        const events = trace.drain(128 * 1024, 512);
        if (!events.length) return;
        const batch = {batchId: crypto.randomUUID(), manifest: trace.manifest(), events};
        const bytes = new TextEncoder().encode(JSON.stringify(batch));
        // Retain the drained batch before any asynchronous operation. A
        // compression failure must not erase evidence already removed from
        // the recorder. The uncompressed batch is a valid fallback.
        this.pending = {body: bytes, gzip: false, batchId: batch.batchId};
        if (typeof CompressionStream !== 'undefined') {
          try {
            const body = new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
            if (body.length < bytes.length) this.pending = {body, gzip: true, batchId: batch.batchId};
          } catch { /* Send the retained uncompressed batch. */ }
        }
      }
      const at = this.now();
      // Reserve 20 KiB/s for existing peer/RTC metrics and HTTP headers.
      this.credit = Math.min(256 * 1024, this.credit + Math.max(0, at - this.last) * 80 * 1024 / 1000);
      this.last = at;
      if (this.pending.body.length > this.credit) return;
      this.credit -= this.pending.body.length;
      this.bytesSent += this.pending.body.length;
      await this.send(this.pending.body, this.pending.gzip, this.pending.batchId);
      this.pending = null;
    } catch (error) { this.failures++; throw error; }
    finally { this.busy = false; }
  }
}


/** Shared lifecycle and durable upload for any Desktop app. No application payloads. */
export class DiagnosticSession {
  readonly upload: TraceUpload;
  error: string | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closed = false;
  private checked = 0;

  constructor(readonly base: URL, readonly trace: TraceRecorder, private fetcher: typeof fetch = fetch) {
    this.upload = new TraceUpload(async (body, gzip, batchId) => {
      const response = await this.fetcher.call(globalThis, new URL('browser', this.base), {method:'POST', body,
        headers:{'content-type':'application/json', ...(gzip?{'content-encoding':'gzip'}:{})},
        signal:AbortSignal.timeout(3000)});
      if (!response.ok) throw new Error(`diagnostic upload HTTP ${response.status}`);
      const receipt = await response.json();
      if (receipt.stored !== true || receipt.batchId !== batchId) throw new Error('diagnostic batch not acknowledged');
    });
    trace.event('browser.connected');
  }

  start(): void { if (!this.closed && this.timer === undefined) this.timer = setTimeout(()=>void this.tick(),250); }

  async tick(): Promise<void> {
    if (this.closed) return;
    this.timer = undefined;
    try {
      if (performance.now()-this.checked > 2000) {
        const response = await this.fetcher.call(globalThis, new URL('session',this.base), {signal:AbortSignal.timeout(3000)});
        if (!response.ok) throw new Error(`diagnostic session HTTP ${response.status}`);
        const {session} = await response.json();
        if (session?.run_id !== this.trace.runId || session?.mode !== 'trace') {this.close();return;}
        this.checked = performance.now();
      }
      this.trace.tick();
      this.trace.event('browser.heartbeat');
      await this.upload.flush(this.trace);
      this.error = null;
    } catch(error) { this.error = error instanceof Error ? error.message : String(error); }
    finally {this.start();}
  }

  close(): void {this.closed=true;clearTimeout(this.timer);this.timer=undefined;this.trace.close();}
}

/** Discover the explicitly started host session, with no app-owned storage or query parameter. */
export async function openDiagnostics(base: URL, fetcher: typeof fetch = fetch): Promise<DiagnosticSession | null> {
  if (!base.pathname.endsWith('/')) base = new URL(base.href+'/');
  const response = await fetcher(new URL('session',base), {signal:AbortSignal.timeout(3000)});
  // A normal app can run on an older runtime. Session readiness will report absent browser evidence.
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`diagnostic session HTTP ${response.status}`);
  const value = await response.json();
  if (value.schema !== 'dimos.diagnostics.session/v1') throw new Error('incompatible diagnostic session');
  if (!value.session || value.session.mode !== 'trace') return null;
  return new DiagnosticSession(base, new TraceRecorder(value.session.run_id), fetcher);
}
