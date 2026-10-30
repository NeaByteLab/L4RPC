import type * as Types from '@app/types.ts'

/**
 * Length-prefixed JSON frame codec.
 * @description Encodes, decodes, and checks frames.
 */
export default class Protocol {
  /** Shared UTF-8 decoder */
  static readonly #decoder = new TextDecoder()
  /** Shared UTF-8 encoder */
  static readonly #encoder = new TextEncoder()
  /** Fixed header byte count */
  static readonly #header = 9
  /** Maximum 32-bit payload length */
  static readonly #maximum = 0xffffffff
  /** Reused small-frame scratch buffer */
  static readonly #scratch = new Uint8Array(9 + 4096 * 3)
  /** Scratch region after the header */
  static readonly #scratchBody = this.#scratch.subarray(9)
  /** Default maximum payload bytes */
  static readonly limit = 16 * 1024 * 1024
  /** Opcode byte constants */
  static readonly opcode = {
    /** Failure response opcode */
    failure: 0x03,
    /** Ping request opcode */
    ping: 0x04,
    /** Pong response opcode */
    pong: 0x05,
    /** Call request opcode */
    request: 0x01,
    /** Success response opcode */
    success: 0x02
  } as const satisfies Record<string, Types.RpcOpcode>

  /**
   * Tune a TCP socket.
   * @description Enables no-delay and keep-alive.
   * @param socket - Open connection to tune
   */
  static configure(socket: Deno.Conn): void {
    const transport = socket as Partial<Deno.TcpConn>
    if (typeof transport.setNoDelay === 'function') {
      try {
        transport.setNoDelay(true)
      } catch {
        void 0
      }
    }
    if (typeof transport.setKeepAlive === 'function') {
      try {
        transport.setKeepAlive(true)
      } catch {
        void 0
      }
    }
  }

  /**
   * Parse JSON payload bytes.
   * @description Decodes UTF-8 then parses JSON.
   * @template T - Parsed value type
   * @param bytes - UTF-8 JSON bytes
   * @returns Parsed JSON value
   * @throws {SyntaxError} When JSON is invalid
   */
  static decode<T = unknown>(bytes: Uint8Array): T {
    return JSON.parse(this.#decoder.decode(bytes)) as T
  }

  /**
   * Check a frame size limit.
   * @description Rejects non-positive or oversized limits.
   * @param frameLimit - Maximum payload byte count
   * @throws {RangeError} When the limit is out of range
   */
  static validateLimit(frameLimit: number): void {
    if (!Number.isSafeInteger(frameLimit) || frameLimit <= 0 || frameLimit > this.#maximum) {
      throw new RangeError(`frameLimit must be an integer between 1 and ${this.#maximum}`)
    }
  }

  /**
   * Build one wire frame.
   * @description Writes header and optional JSON body.
   * @param opcode - Frame kind byte
   * @param ident - Correlation identifier
   * @param payload - JSON value to encode
   * @param frameLimit - Maximum payload byte count
   * @returns Encoded frame bytes
   * @throws {RangeError} When the payload exceeds the limit
   */
  static encode(
    opcode: Types.RpcOpcode,
    ident: number,
    payload?: unknown,
    frameLimit: number = this.limit
  ): Uint8Array {
    const source = payload === undefined ? undefined : JSON.stringify(payload)
    if (source === undefined) {
      const frame = new Uint8Array(this.#header)
      frame[0] = opcode
      frame[1] = ident >>> 24
      frame[2] = ident >>> 16
      frame[3] = ident >>> 8
      frame[4] = ident
      return frame
    }
    if (source.length <= 4096) {
      const { written } = this.#encoder.encodeInto(source, this.#scratchBody)
      if (written > frameLimit) {
        throw new RangeError(`Frame payload length ${written} exceeds maximum ${frameLimit}`)
      }
      this.#scratch[0] = opcode
      this.#scratch[1] = ident >>> 24
      this.#scratch[2] = ident >>> 16
      this.#scratch[3] = ident >>> 8
      this.#scratch[4] = ident
      this.#scratch[5] = written >>> 24
      this.#scratch[6] = written >>> 16
      this.#scratch[7] = written >>> 8
      this.#scratch[8] = written
      return this.#scratch.slice(0, this.#header + written)
    }
    const encoded = this.#encoder.encode(source)
    const length = encoded.byteLength
    if (length > frameLimit || length > this.#maximum) {
      throw new RangeError(`Frame payload length ${length} exceeds maximum ${frameLimit}`)
    }
    const frame = new Uint8Array(this.#header + length)
    frame[0] = opcode
    frame[1] = ident >>> 24
    frame[2] = ident >>> 16
    frame[3] = ident >>> 8
    frame[4] = ident
    frame[5] = length >>> 24
    frame[6] = length >>> 16
    frame[7] = length >>> 8
    frame[8] = length
    frame.set(encoded, this.#header)
    return frame
  }
}
