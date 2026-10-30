import type * as Types from '@app/types.ts'
import Protocol from '@app/protocol.ts'
import Queue from '@app/queue.ts'
import Reader from '@app/reader.ts'

/**
 * TCP JSON-RPC client.
 * @description Calls methods and reconnects after drops.
 */
export default class Client {
  /** Delay between reconnect attempts */
  static readonly #delay = 500
  /** Largest accepted timeout milliseconds */
  static readonly #maxTimeout = 0x7fffffff
  /** Maximum encoded payload bytes */
  readonly #frameLimit: number
  /** Remote host name or address */
  readonly #hostname: string
  /** Calls waiting for a response */
  readonly #pending = new Map<number, Types.RpcTicket>()
  /** Remote TCP port number */
  readonly #port: number
  /** Reconnect attempts after disconnect */
  readonly #reconnect: number
  /** Default call timeout milliseconds */
  readonly #timeout: number
  /** Ordered outbound frame writer */
  readonly #writer = new Queue()
  /** Cancels the reconnect wait */
  #cancelDelay: (() => void) | null = null
  /** True after close or fatal failure */
  #closed = false
  /** In-flight reconnect attempt */
  #restoring: Promise<boolean> | null = null
  /** Last issued correlation identifier */
  #sequence = 0
  /** Active TCP connection */
  #socket: Deno.TcpConn

  /**
   * Bind a connected socket.
   * @description Stores options and starts the read loop.
   * @param socket - Connected TCP socket
   * @param hostname - Remote host name or address
   * @param port - Remote TCP port number
   * @param timeout - Default call timeout milliseconds
   * @param frameLimit - Maximum payload byte count
   * @param reconnect - Reconnect attempts after disconnect
   */
  private constructor(
    socket: Deno.TcpConn,
    hostname: string,
    port: number,
    timeout: number,
    frameLimit: number,
    reconnect: number
  ) {
    this.#socket = socket
    this.#hostname = hostname
    this.#port = port
    this.#timeout = timeout
    this.#frameLimit = frameLimit
    this.#reconnect = reconnect
    Protocol.configure(this.#socket)
    this.#receive()
  }

  /**
   * Invoke a registered method.
   * @description Sends a request and waits for the reply.
   * @template TResult - Expected result type
   * @template TParams - Call params type
   * @param method - Registered method name
   * @param params - Optional call params
   * @param timeoutMs - Call timeout in milliseconds
   * @returns Promise resolving to the result
   * @throws {TypeError} When closed, method, or the reply is invalid
   * @throws {RangeError} When timeout or payload is invalid
   */
  call<TResult = unknown, TParams = unknown>(
    method: string,
    params?: TParams,
    timeoutMs: number = this.#timeout
  ): Promise<TResult> {
    if (this.#closed) {
      return Promise.reject(new TypeError('RPC client connection is closed'))
    }
    if (typeof method !== 'string' || method.length === 0) {
      return Promise.reject(new TypeError('Method name must be a non-empty string'))
    }
    try {
      Client.#checkTimeout(timeoutMs)
    } catch (caught) {
      return Promise.reject(caught as RangeError)
    }
    const ident = this.#nextIdent()
    let frame: Uint8Array
    try {
      frame = Protocol.encode(
        Protocol.opcode.request,
        ident,
        params === undefined ? { m: method } : { m: method, p: params },
        this.#frameLimit
      )
    } catch (caught) {
      return Promise.reject(caught instanceof Error ? caught : new TypeError(String(caught)))
    }
    return new Promise<TResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(ident)
        reject(new RangeError(`RPC timeout after ${timeoutMs}ms calling ${method}`))
      }, timeoutMs)
      this.#pending.set(ident, {
        frame,
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
        isPing: false
      })
      if (this.#restoring === null) {
        this.#write(ident, frame, reject, timer)
      }
    })
  }

  /**
   * Open a TCP client.
   * @description Validates options then connects the socket.
   * @param options - Client connection options
   * @returns Connected client instance
   * @throws {TypeError} When hostname is empty
   * @throws {RangeError} When port, timeout, or limits are invalid
   */
  static async connect(options: Types.RpcClientOptions): Promise<Client> {
    if (typeof options.hostname !== 'string' || options.hostname.trim().length === 0) {
      throw new TypeError('hostname must be a non-empty string')
    }
    if (!Number.isSafeInteger(options.port) || options.port < 1 || options.port > 65535) {
      throw new RangeError('port must be an integer between 1 and 65535')
    }
    const timeoutMs = options.timeoutMs ?? 5000
    const frameLimit = options.frameLimit ?? Protocol.limit
    const reconnect = options.reconnect ?? 0
    Client.#checkTimeout(timeoutMs)
    Protocol.validateLimit(frameLimit)
    if (!Number.isSafeInteger(reconnect) || reconnect < 0) {
      throw new RangeError('reconnect must be a non-negative integer')
    }
    return new Client(
      await Deno.connect({ hostname: options.hostname, port: options.port, transport: 'tcp' }),
      options.hostname,
      options.port,
      timeoutMs,
      frameLimit,
      reconnect
    )
  }

  /** Close socket and reject pending calls */
  close(): void {
    this.#shutdown(new TypeError('RPC client closed'))
  }

  /**
   * Probe the remote peer.
   * @description Sends ping and waits for pong.
   * @param timeoutMs - Ping timeout in milliseconds
   * @returns True when pong arrives
   * @throws {TypeError} When the connection is closed
   * @throws {RangeError} When timeout is invalid or elapses
   */
  ping(timeoutMs: number = this.#timeout): Promise<boolean> {
    if (this.#closed) {
      return Promise.reject(new TypeError('RPC client connection is closed'))
    }
    try {
      Client.#checkTimeout(timeoutMs)
    } catch (caught) {
      return Promise.reject(caught as RangeError)
    }
    const ident = this.#nextIdent()
    const frame = Protocol.encode(Protocol.opcode.ping, ident, undefined, this.#frameLimit)
    return new Promise<boolean>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(ident)
        reject(new RangeError(`RPC ping timeout after ${timeoutMs}ms`))
      }, timeoutMs)
      this.#pending.set(ident, {
        frame,
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
        isPing: true
      })
      if (this.#restoring === null) {
        this.#write(ident, frame, reject, timer)
      }
    })
  }

  /**
   * Check a call timeout.
   * @description Rejects non-positive or oversized values.
   * @param timeoutMs - Timeout in milliseconds
   * @throws {RangeError} When timeout is out of range
   */
  static #checkTimeout(timeoutMs: number): void {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > Client.#maxTimeout) {
      throw new RangeError('timeoutMs must be a positive finite number')
    }
  }

  /**
   * Allocate a free identifier.
   * @description Skips zero and identifiers still pending.
   * @returns Unused correlation identifier
   */
  #nextIdent(): number {
    let ident = (this.#sequence = (this.#sequence + 1) >>> 0)
    while (ident === 0 || this.#pending.has(ident)) {
      ident = this.#sequence = (this.#sequence + 1) >>> 0
    }
    return ident
  }

  /**
   * Read frames until closed.
   * @description Settles tickets and reconnects after drops.
   */
  async #receive(): Promise<void> {
    while (!this.#closed) {
      const reader = new Reader(this.#socket, this.#frameLimit)
      let failure: Error = new TypeError('RPC TCP stream closed by remote peer')
      try {
        while (!this.#closed) {
          const frame = await reader.readFrame()
          if (frame === null) {
            break
          }
          if (
            frame.ident === 0 ||
            frame.opcode === Protocol.opcode.request ||
            frame.opcode === Protocol.opcode.ping ||
            (frame.opcode === Protocol.opcode.pong && frame.payload.byteLength > 0) ||
            (frame.opcode === Protocol.opcode.failure && frame.payload.byteLength === 0)
          ) {
            throw new TypeError(`Invalid frame received by RPC client with opcode ${frame.opcode}`)
          }
          const pending = this.#pending.get(frame.ident)
          if (pending === undefined) {
            continue
          }
          clearTimeout(pending.timer)
          this.#pending.delete(frame.ident)
          try {
            if (pending.isPing) {
              if (frame.opcode !== Protocol.opcode.pong) {
                throw new TypeError(`Unexpected frame opcode ${frame.opcode} for ping request`)
              }
              pending.resolve(true)
              continue
            }
            if (frame.opcode === Protocol.opcode.success) {
              pending.resolve(
                frame.payload.byteLength > 0 ? Protocol.decode<unknown>(frame.payload) : undefined
              )
              continue
            }
            if (frame.opcode !== Protocol.opcode.failure) {
              throw new TypeError(`Unexpected frame opcode ${frame.opcode} for RPC response`)
            }
            const decoded = Protocol.decode<Types.RpcFailure>(frame.payload)
            if (
              decoded === null ||
              typeof decoded !== 'object' ||
              Array.isArray(decoded) ||
              typeof decoded.code !== 'string' ||
              decoded.code.length === 0 ||
              typeof decoded.message !== 'string'
            ) {
              throw new TypeError('Invalid RPC failure envelope')
            }
            pending.reject(
              new TypeError(
                decoded.message.length > 0 ? `${decoded.code} ${decoded.message}` : decoded.code
              )
            )
          } catch (caught) {
            pending.reject(caught as Error)
            throw caught
          }
        }
      } catch (caught) {
        failure = caught instanceof Error ? caught : new TypeError(String(caught))
      }
      if (this.#closed) {
        return
      }
      if (this.#reconnect === 0) {
        this.#shutdown(failure)
        return
      }
      try {
        this.#socket.close()
      } catch {
        void 0
      }
      this.#writer.reset()
      this.#restoring = (async (): Promise<boolean> => {
        for (let attempt = 1; attempt <= this.#reconnect && !this.#closed; attempt += 1) {
          await new Promise<void>((resolve) => {
            const timer = setTimeout(() => {
              this.#cancelDelay = null
              resolve()
            }, Client.#delay)
            this.#cancelDelay = () => {
              clearTimeout(timer)
              this.#cancelDelay = null
              resolve()
            }
          })
          if (this.#closed) {
            return false
          }
          try {
            const socket = await Deno.connect({
              hostname: this.#hostname,
              port: this.#port,
              transport: 'tcp'
            })
            if (this.#closed) {
              try {
                socket.close()
              } catch {
                void 0
              }
              return false
            }
            this.#socket = socket
            Protocol.configure(this.#socket)
            return true
          } catch {
            void 0
          }
        }
        return false
      })()
      const restored = await this.#restoring
      this.#restoring = null
      if (!restored) {
        this.#shutdown(failure)
        return
      }
      for (const [ident, pending] of this.#pending) {
        this.#write(ident, pending.frame, pending.reject, pending.timer)
      }
    }
  }

  /**
   * Stop the client permanently.
   * @description Closes the socket and rejects pending calls.
   * @param reason - Rejection reason
   */
  #shutdown(reason: Error): void {
    if (this.#closed) {
      return
    }
    this.#closed = true
    this.#cancelDelay?.()
    this.#writer.reset()
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(reason)
    }
    this.#pending.clear()
    try {
      this.#socket.close()
    } catch {
      void 0
    }
  }

  /**
   * Queue a frame on the socket.
   * @description Rejects the ticket if the write fails.
   * @param ident - Correlation identifier
   * @param frame - Encoded frame bytes
   * @param reject - Call rejection callback
   * @param timer - Call timeout handle
   */
  #write(
    ident: number,
    frame: Uint8Array,
    reject: (reason: Error) => void,
    timer: ReturnType<typeof setTimeout>
  ): void {
    const socket = this.#socket
    this.#writer.enqueue(socket, frame).catch((caught) => {
      if (this.#reconnect > 0 && !this.#closed) {
        try {
          socket.close()
        } catch {
          void 0
        }
        return
      }
      clearTimeout(timer)
      if (!this.#pending.delete(ident)) {
        return
      }
      reject(caught instanceof Error ? caught : new TypeError(String(caught)))
    })
  }
}
