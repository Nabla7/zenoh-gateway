import {TraceRecorder} from '../diagnostics.ts';
import {TraceUpload, openDiagnostics} from '../diagnostics_session.ts';

function equal(actual:unknown, expected:unknown) {if(JSON.stringify(actual)!==JSON.stringify(expected))throw new Error(`${JSON.stringify(actual)} != ${JSON.stringify(expected)}`);}

Deno.test('failed upload retries identical retained batch', async()=>{
  let now=0;const sent:Uint8Array[]=[];
  const recorder=new TraceRecorder('lap');recorder.event('video.presented');
  const upload=new TraceUpload(async bytes=>{sent.push(bytes);if(sent.length===1)throw new Error('offline');},()=>now);
  await upload.flush(recorder);equal(sent.length,0);
  now=1000;try{await upload.flush(recorder);}catch{/* injected connection loss */}
  now=2000;await upload.flush(recorder);
  equal(sent.length,2);equal(Array.from(sent[0]),Array.from(sent[1]));equal(upload.pending,null);
});

Deno.test('diagnostic egress stays bounded under continuous production', async()=>{
  let now=0,bytes=0;const recorder=new TraceRecorder('lap');
  const upload=new TraceUpload(async data=>{bytes+=data.length;},()=>now);
  for(let i=0;i<40;i++){for(let k=0;k<500;k++)recorder.event('frame',null,{value:crypto.randomUUID()});now+=250;await upload.flush(recorder);}
  if(bytes/10>80*1024)throw new Error('telemetry exceeded allocation');
});

Deno.test('shared discovery selects the active run and stop halts uploads', async()=>{
  let active=true,uploads=0;
  const fetcher:typeof fetch=async input=>{
    const path=new URL(String(input)).pathname;
    if(path.endsWith('/session'))return Response.json({schema:'dimos.diagnostics.session/v1',session:active?{run_id:'lap',mode:'trace'}:null});
    uploads++;return Response.json({stored:true});
  };
  const session=await openDiagnostics(new URL('http://localhost/diagnostics/'),fetcher);
  if(!session)throw new Error('missing session');
  try{equal(session.trace.runId,'lap');active=false;await session.tick();}
  finally{session.close();}
  equal(uploads,0);
});

Deno.test('missing or incompatible services do not fabricate an active collector',async()=>{
  equal(await openDiagnostics(new URL('http://localhost/'),async()=>new Response(null,{status:404})),null);
  let rejected=false;
  try{await openDiagnostics(new URL('http://localhost/'),async()=>Response.json({schema:'wrong'}));}catch{rejected=true;}
  equal(rejected,true);
});
