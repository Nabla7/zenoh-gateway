import {Publisher, Priority} from '../zenoh_gateway.ts'

Deno.test('latest control lifetime follows its 100 ms freshness limit; reliable delivery stays reliable', async () => {
    for (const delivery of ['latest', 'reliable'] as const) {
        let options: RTCDataChannelInit | undefined
        const channel = {readyState: 'open', bufferedAmount: 0, close() {}} as unknown as RTCDataChannel
        const peer = {createDataChannel(_label: string, init: RTCDataChannelInit) { options = init; return channel }} as unknown as RTCPeerConnection
        const owner = {options: {}, now: () => 1000, _forget() {}} as unknown as ConstructorParameters<typeof Publisher>[0]
        const publisher = new Publisher(owner, 1, 'test/control', {delivery, latencyLimit: 100, priority: Priority.INTERACTIVE_HIGH})
        try {
            publisher.attach(peer); publisher._accepted(); await publisher.ready()
            if (delivery === 'latest' && (options?.maxPacketLifeTime !== 100 || options.ordered !== false)) throw new Error(JSON.stringify(options))
            if (delivery === 'reliable' && options?.maxPacketLifeTime !== undefined) throw new Error('reliable channel received a packet expiry')
        } finally { publisher.close() }
    }
})
