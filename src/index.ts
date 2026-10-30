import type * as Types from '@app/types.ts'
import Client from '@app/client.ts'
import Server from '@app/server.ts'

/**
 * Public client and server entry.
 * @description Connects a client or constructs a server.
 */
export default {
  /**
   * Open a TCP client.
   * @description Connects using the given options.
   * @param options - Client connection options
   * @returns Connected client instance
   * @throws {TypeError} When hostname is empty
   * @throws {RangeError} When port or limits are invalid
   */
  Client: (options: Types.RpcClientOptions): Promise<Client> => Client.connect(options),
  Server
} as const

/** Re-exported RPC protocol and option types */
export type * from '@app/types.ts'
