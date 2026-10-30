import * as Assert from '@std/assert'
import L4RPC from '@neabyte/l4rpc'
import Client from '@app/client.ts'
import Server from '@app/server.ts'
import Features from '@tests/features.ts'

Deno.test('L4RPC - exposes Server as the class and Client as a connector', () => {
  Assert.assertEquals(L4RPC.Server, Server)
  Assert.assertEquals(typeof L4RPC.Client, 'function')
})

Deno.test('L4RPC.Client - rejects an empty hostname before dialing', async () => {
  await Assert.assertRejects(
    () => L4RPC.Client({ hostname: '   ', port: 1 }),
    TypeError,
    'hostname must be a non-empty string'
  )
})

Deno.test('L4RPC.Client - returns a Client instance', async () => {
  const server = new Server().listen()
  const client = await L4RPC.Client({
    hostname: '127.0.0.1',
    port: server.addr.port
  })
  try {
    Assert.assertEquals(client instanceof Client, true)
  } finally {
    await Features.stop(client, server)
  }
})
