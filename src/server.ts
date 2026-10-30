import type * as Types from '@app/types.ts'
import Protocol from '@app/protocol.ts'
import Queue from '@app/queue.ts'
import Reader from '@app/reader.ts'

/**
 * TCP JSON-RPC server.
 * @description Accepts connections and dispatches methods.
 */
export default class Server {
  /** Live accepted sockets */
  readonly #connections = new Set<Deno.Conn>()
  /** Maximum encoded payload bytes */
  readonly #frameLimit: number
  /** Registered method handlers */
  readonly #handlers = new Map<string, Types.RpcHandler<unknown, unknown>>()
  /** Bound TCP listener */
  readonly #listener: Deno.TcpListener
  /** True after close */
  #closed = false
  /** Accept loop promise */
  #serving: Promise<void> | null = null

  /**
   * Bind and optionally register routes.
   * @description Listens, then runs setup if given.
   * @param options - Server listen options
   * @param setup - Optional route registration callback
   * @throws {TypeError} When hostname is empty
   * @throws {RangeError} When port or frame limit is invalid
   */
  constructor(options: Types.RpcServerOptions = {}, setup?: Types.RpcSetup) {
    const hostname = options.hostname ?? '127.0.0.1'
    const port = options.port ?? 0
    if (typeof hostname !== 'string' || hostname.trim().length === 0) {
      throw new TypeError('hostname must be a non-empty string')
    }
    if (!Number.isSafeInteger(port) || port < 0 || port > 65535) {
      throw new RangeError('port must be an integer between 0 and 65535')
    }
    Protocol.validateLimit(options.frameLimit ?? Protocol.limit)
    this.#frameLimit = options.frameLimit ?? Protocol.limit
    this.#listener = Deno.listen({ hostname, port, transport: 'tcp' })
    if (setup !== undefined) {
      try {
        setup(
          ((
            method: string,
            guards: readonly Types.RpcGuard[] | Types.RpcHandler,
            handler?: Types.RpcHandler
          ): void => {
            this.register(method, guards, handler)
          }) as Types.RpcRoute
        )
      } catch (caught) {
        try {
          this.#listener.close()
        } catch {
          void 0
        }
        throw caught
      }
    }
  }

  /** Bound network address */
  get addr(): Deno.NetAddr {
    return this.#listener.addr as Deno.NetAddr
  }

  /** Accept loop completion promise */
  get done(): Promise<void> {
    return this.#serving ?? Promise.resolve()
  }

  /** Stop accepting and close sockets */
  close(): void {
    if (this.#closed) {
      return
    }
    this.#closed = true
    try {
      this.#listener.close()
    } catch {
      void 0
    }
    for (const socket of this.#connections) {
      try {
        socket.close()
      } catch {
        void 0
      }
    }
    this.#connections.clear()
  }

  /**
   * Start accepting connections.
   * @description Begins the accept loop once.
   * @returns This server instance
   */
  listen(): this {
    if (this.#serving === null && !this.#closed) {
      this.#serving = (async (): Promise<void> => {
        try {
          for await (const socket of this.#listener) {
            if (this.#closed) {
              try {
                socket.close()
              } catch {
                void 0
              }
              break
            }
            Protocol.configure(socket)
            this.#connections.add(socket)
            this.#serve(socket)
          }
        } catch {
          void 0
        }
      })()
    }
    return this
  }

  /**
   * Register a method handler.
   * @description Stores the handler, wrapping guards when present.
   * @template TParams - Call params type
   * @template TResult - Call result type
   * @param method - Registered method name
   * @param guards - Guard list or the handler
   * @param handler - Procedure when guards are given
   * @returns This server instance
   * @throws {TypeError} When method, guards, or handler is invalid
   */
  register<TParams = unknown, TResult = unknown>(
    method: string,
    guards: readonly Types.RpcGuard[] | Types.RpcHandler<TParams, TResult>,
    handler?: Types.RpcHandler<TParams, TResult>
  ): this {
    if (typeof method !== 'string' || method.length === 0) {
      throw new TypeError('Method name must be a non-empty string')
    }
    if (typeof guards === 'function') {
      this.#handlers.set(method, guards as Types.RpcHandler<unknown, unknown>)
      return this
    }
    if (!Array.isArray(guards)) {
      throw new TypeError('Middleware must be a function')
    }
    if (typeof handler !== 'function') {
      throw new TypeError('Handler must be a function')
    }
    for (const guard of guards) {
      if (typeof guard !== 'function') {
        throw new TypeError('Middleware must be a function')
      }
    }
    if (guards.length === 0) {
      this.#handlers.set(method, handler as Types.RpcHandler<unknown, unknown>)
      return this
    }
    this.#handlers.set(
      method,
      (async (params: TParams): Promise<TResult> => {
        let current: unknown = params
        for (const guard of guards) {
          current = await (guard as Types.RpcGuard<unknown>)(current)
        }
        return await handler(current as TParams)
      }) as Types.RpcHandler<unknown, unknown>
    )
    return this
  }

  /**
   * Run one request frame.
   * @description Decodes, invokes, and writes the response.
   * @param socket - Calling connection
   * @param writer - Connection write queue
   * @param ident - Correlation identifier
   * @param payload - Raw request bytes
   */
  async #dispatch(
    socket: Deno.Conn,
    writer: Queue,
    ident: number,
    payload: Uint8Array
  ): Promise<void> {
    let response: Uint8Array | null = null
    let request: Types.RpcRequest | null = null
    if (payload.byteLength > 0) {
      try {
        const decoded = Protocol.decode<Types.RpcRequest>(payload)
        if (
          decoded !== null &&
          typeof decoded === 'object' &&
          !Array.isArray(decoded) &&
          typeof decoded.m === 'string' &&
          decoded.m.length > 0
        ) {
          request = decoded
        }
      } catch {
        request = null
      }
    }
    if (request === null) {
      response = this.#failure(ident, 'INVALID_REQUEST', 'Invalid RPC request envelope')
    } else {
      const handler = this.#handlers.get(request.m)
      if (handler === undefined) {
        response = this.#failure(
          ident,
          'METHOD_NOT_FOUND',
          `Procedure ${request.m} is not registered`
        )
      } else {
        try {
          const result = await handler(request.p)
          if (this.#closed || !this.#connections.has(socket)) {
            return
          }
          response = Protocol.encode(Protocol.opcode.success, ident, result, this.#frameLimit)
        } catch (caught) {
          if (this.#closed || !this.#connections.has(socket)) {
            return
          }
          response = this.#failure(
            ident,
            'EXECUTION_ERROR',
            caught instanceof Error ? caught.message : String(caught)
          )
        }
      }
    }
    if (response === null) {
      try {
        socket.close()
      } catch {
        void 0
      }
      return
    }
    await writer.enqueue(socket, response).catch(() => {})
  }

  /**
   * Encode a failure response.
   * @description Uses the code if encoding fails.
   * @param ident - Correlation identifier
   * @param code - Machine-readable error code
   * @param message - Human-readable error message
   * @returns Encoded frame or null
   */
  #failure(ident: number, code: string, message: string): Uint8Array | null {
    try {
      return Protocol.encode(Protocol.opcode.failure, ident, { code, message }, this.#frameLimit)
    } catch {
      try {
        return Protocol.encode(
          Protocol.opcode.failure,
          ident,
          { code, message: code },
          this.#frameLimit
        )
      } catch {
        return null
      }
    }
  }

  /**
   * Serve one accepted socket.
   * @description Reads frames until close or a bad frame.
   * @param socket - Accepted connection
   */
  async #serve(socket: Deno.Conn): Promise<void> {
    const reader = new Reader(socket, this.#frameLimit)
    const writer = new Queue()
    try {
      while (!this.#closed) {
        const frame = await reader.readFrame()
        if (frame === null || frame.ident === 0) {
          break
        }
        if (frame.opcode === Protocol.opcode.ping) {
          if (frame.payload.byteLength > 0) {
            break
          }
          await writer.enqueue(socket, Protocol.encode(Protocol.opcode.pong, frame.ident))
          continue
        }
        if (frame.opcode !== Protocol.opcode.request) {
          break
        }
        this.#dispatch(socket, writer, frame.ident, frame.payload)
      }
    } catch {
      void 0
    } finally {
      this.#connections.delete(socket)
      try {
        socket.close()
      } catch {
        void 0
      }
    }
  }
}
