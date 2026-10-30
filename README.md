<div align='center'>

<img src="./assets/preview.webp" alt="L4RPC Preview" width="100%">

</div>

## What is L4RPC?

L4RPC is a TCP procedure call for daemon services. Each call is one length-prefixed JSON frame, the server keeps no session, and a dropped call is retried only when reconnect is above 0.

## Installation

**Deno:**

```bash
deno add jsr:@neabyte/l4rpc
```

## Usage

### Server

```ts
import L4RPC from '@neabyte/l4rpc'

// Bind, register, then accept
const server = new L4RPC.Server(
  {
    hostname: '127.0.0.1',
    port: 9000
  },
  (route) => {
    const requireId = (params: { id: number }) => {
      if (!Number.isSafeInteger(params.id) || params.id <= 0) {
        throw new TypeError('User id must be a positive integer')
      }
      return params
    }
    route('getUser', [requireId], ({ id }: { id: number }) => ({
      id,
      name: 'Nea'
    }))
  }
).listen()
```

The server binds, registers `getUser`, then starts accepting. `requireId` runs before the handler, so a bad id never reaches it. `server.close()` stops accepting and closes open sockets.

### Client

```ts
import L4RPC from '@neabyte/l4rpc'

// Dial the listening server
const client = await L4RPC.Client({
  hostname: '127.0.0.1',
  port: 9000,
  timeoutMs: 5000
})

// True when the peer answers
const alive = await client.ping()

// Returns the registered user
const user = await client.call<{
  id: number
  name: string
}>('getUser', { id: 1 })

// Close the connection
client.close()
```

The client dials the server above. `ping()` returns `true` when the peer answers, and `call()` returns `{ id: 1, name: 'Nea' }`. `client.close()` rejects any call that is still waiting.

`timeoutMs` defaults to `5000`, `reconnect` defaults to `0`, and `frameLimit` defaults to 16 MiB. A reconnect count above `0` retries unfinished calls and pings after a new socket connects, with a fixed 500 ms pause between attempts.

## API

### `L4RPC.Client(options)`

| Parameter | Type               | Description                                      |
| --------- | ------------------ | ------------------------------------------------ |
| `options` | `RpcClientOptions` | Remote address, timeout, frame limit, reconnect. |

**Returns:** `Promise<Client>`.

```ts
interface RpcClientOptions {
  hostname: string
  port: number
  frameLimit?: number
  reconnect?: number
  timeoutMs?: number
}
```

`hostname` must not be blank and `port` must be an integer from `1` to `65535`.

### `client.call(method, params?, timeoutMs?)`

| Parameter   | Type     | Description                                         |
| ----------- | -------- | --------------------------------------------------- |
| `method`    | `string` | Registered method name.                             |
| `params`    | `T`      | Optional params, omitted when `undefined`.          |
| `timeoutMs` | `number` | Optional timeout that overrides the client timeout. |

**Returns:** `Promise<TResult>`.

A handler error arrives as `TypeError` whose message starts with the failure code. `close()` rejects calls that are still waiting.

### `client.ping(timeoutMs?)`

| Parameter   | Type     | Description             |
| ----------- | -------- | ----------------------- |
| `timeoutMs` | `number` | Optional wait for pong. |

**Returns:** `Promise<boolean>`, `true` when pong arrives.

### `client.close()`

Closes the socket and rejects every pending call.

### `new L4RPC.Server(options?, setup?)`

| Parameter | Type               | Description                                   |
| --------- | ------------------ | --------------------------------------------- |
| `options` | `RpcServerOptions` | Optional bind address, port, and frame limit. |
| `setup`   | `RpcSetup`         | Optional callback that registers methods.     |

The constructor binds the port, and `listen()` starts accepting. `hostname` defaults to `127.0.0.1` and `port` defaults to `0`. A thrown `setup` closes that listener before the error leaves the constructor.

```ts
interface RpcServerOptions {
  frameLimit?: number
  hostname?: string
  port?: number
}
```

### `server.listen()`

Starts the accept loop once and returns the same server.

### `server.register(method, handler)`

### `server.register(method, guards, handler)`

| Parameter | Type         | Description                                          |
| --------- | ------------ | ---------------------------------------------------- |
| `method`  | `string`     | Non-empty method name, replaced by a later register. |
| `guards`  | `RpcGuard[]` | Optional guards, each receiving the previous params. |
| `handler` | `RpcHandler` | Procedure that receives params after the guards.     |

**Returns:** the same server.

### `server.addr`

Bound `Deno.NetAddr`.

### `server.done`

`Promise<void>` that settles when the accept loop ends. It is already settled before `listen()`.

### `server.close()`

Stops accepting and closes open sockets. A second call does nothing.

## Testing

```bash
deno task check
```

```bash
deno task test
```

## License

This repository is licensed under [MIT](./LICENSE).
