/**
 * Client connection options.
 * @description Hostname, port, timeout, and reconnect.
 */
export interface RpcClientOptions {
  /** Maximum encoded payload bytes */
  frameLimit?: number
  /** Remote host name or address */
  hostname: string
  /** Remote TCP port number */
  port: number
  /** Reconnect attempts after disconnect */
  reconnect?: number
  /** Call timeout in milliseconds */
  timeoutMs?: number
}

/**
 * Failed call error body.
 * @description Machine code and human message.
 */
export interface RpcFailure {
  /** Machine-readable error code */
  code: string
  /** Human-readable error message */
  message: string
}

/**
 * Decoded wire frame.
 * @description Opcode, identifier, and raw payload.
 */
export interface RpcFrame {
  /** Correlation identifier */
  ident: number
  /** Frame kind byte */
  opcode: RpcOpcode
  /** Raw JSON payload bytes */
  payload: Uint8Array
}

/**
 * Encoded request envelope.
 * @description Method name and optional params.
 * @template T - Params value type
 */
export interface RpcRequest<T = unknown> {
  /** Registered method name */
  m: string
  /** Optional method params */
  p?: T
}

/**
 * Server listen options.
 * @description Bind address, port, and frame limit.
 */
export interface RpcServerOptions {
  /** Maximum encoded payload bytes */
  frameLimit?: number
  /** Bind host name or address */
  hostname?: string
  /** Bind TCP port number */
  port?: number
}

/**
 * Pending client call slot.
 * @description Frame, resolve, reject, timer, and ping flag.
 */
export interface RpcTicket {
  /** Outbound encoded frame bytes */
  frame: Uint8Array
  /** True when ticket is a ping */
  isPing?: boolean
  /** Reject the pending call */
  reject: (reason: Error) => void
  /** Resolve the pending call */
  resolve: (value: unknown) => void
  /** Timeout timer handle */
  timer: ReturnType<typeof setTimeout>
}

/**
 * Guard transforming call params.
 * @description Runs before the registered handler.
 * @template TInput - Incoming params type
 * @template TOutput - Outgoing params type
 * @param params - Incoming params value
 * @returns Next params value
 */
export type RpcGuard<TInput = never, TOutput = unknown> = (
  params: TInput
) => Promise<TOutput> | TOutput

/**
 * Registered procedure function.
 * @description Accepts params and returns a result.
 * @template TParams - Call params type
 * @template TResult - Call result type
 * @param params - Decoded call params
 * @returns Handler result value
 */
export type RpcHandler<TParams = unknown, TResult = unknown> = (
  params: TParams
) => Promise<TResult> | TResult

/** Frame opcode byte values */
export type RpcOpcode = 0x01 | 0x02 | 0x03 | 0x04 | 0x05

/**
 * Setup registration function.
 * @description Registers a handler with optional guards.
 * @template TParams - Call params type
 * @template TResult - Call result type
 * @param method - Registered method name
 * @param guards - Guard list or the handler
 * @param handler - Procedure when guards are given
 */
export type RpcRoute = {
  <TParams, TResult>(method: string, handler: RpcHandler<TParams, TResult>): void
  <TParams, TResult>(
    method: string,
    guards: readonly RpcGuard[],
    handler: RpcHandler<TParams, TResult>
  ): void
}

/**
 * Server setup callback.
 * @description Receives route and registers handlers.
 * @param route - Registration function
 */
export type RpcSetup = (route: RpcRoute) => void
