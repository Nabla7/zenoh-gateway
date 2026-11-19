import {TraceRecorder, traceContext, tracePayload} from '../diagnostics.ts';

Deno.test('negotiated payload retains identity and every application byte', () => {
    const recorder = new TraceRecorder('run'); const context = recorder.sample('hand/左');
    const bytes = new Uint8Array([0,255,1,0]); const frame = tracePayload(bytes, context);
    const size = new DataView(frame.buffer).getUint32(0,true);
    if (JSON.stringify(traceContext(frame.subarray(4,4+size))) !== JSON.stringify(context)) throw new Error('lost context');
    if (String(frame.subarray(4+size)) !== String(bytes)) throw new Error('changed payload');
});

Deno.test('an oversized event cannot permanently block diagnostic draining', () => {
    const recorder = new TraceRecorder('run');
    recorder.event('large',null,{label:'x'.repeat(5000)}); recorder.event('small');
    const events = recorder.drain(1024);
    if (events.length !== 1 || events[0].event !== 'small' || recorder.drops !== 1) throw new Error('drain blocked');
});

Deno.test('bounded observer loss is visible while sample identity keeps advancing', () => {
    const recorder = new TraceRecorder('run',1024,2);
    for (let i=0;i<10;i++) recorder.event('frame',recorder.sample('camera'));
    const rows = recorder.drain();
    if (!recorder.drops || rows.length > 2 || rows.at(-1)?.context?.sequence !== 10) throw new Error('bad bounded capture');
});
