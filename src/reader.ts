import type * as Types from '@app/types.ts'
import Protocol from '@app/protocol.ts'

/**
 * Incremental TCP frame reader.
 * @description Buffers bytes and yields complete frames.
 */
export default class Reader {
  /** Initial buffer byte capacity */
  static readonly #capacity = 64 * 1024
  /** Shared empty payload buffer */
  static readonly #empty = new Uint8Array(0)
  /** Spare bytes kept before compacting */
  static readonly #minimum = 4096
  /** Maximum accepted payload bytes */
  readonly #frameLimit: number
  /** Source TCP connection */
  readonly #socket: Deno.Conn
  /** Sliding read buffer */
  #buffer: Uint8Array
  /** First unread byte index */
  #readIndex = 0
  /** Buffer view for header reads */
  #view: DataView
  /** First free byte index */
  #writeIndex = 0

  /**
   * Bind a reader to a socket.
   * @description Allocates the buffer and checks the limit.
   * @param socket - Open TCP connection
   * @param frameLimit - Maximum payload byte count
   * @throws {RangeError} When the limit is out of range
   */
  constructor(socket: Deno.Conn, frameLimit: number = Protocol.limit) {
    Protocol.validateLimit(frameLimit)
    this.#socket = socket
    this.#frameLimit = frameLimit
    this.#buffer = new Uint8Array(Reader.#capacity)
    this.#view = new DataView(this.#buffer.buffer, this.#buffer.byteOffset, this.#buffer.byteLength)
  }

  /**
   * Read the next complete frame.
   * @description Returns null when the stream ends cleanly.
   * @returns Decoded frame or null
   * @throws {RangeError} When opcode, length, or stream is invalid
   */
  async readFrame(): Promise<Types.RpcFrame | null> {
    if (!(await this.#fill(9))) {
      return null
    }
    const opcode = this.#view.getUint8(this.#readIndex) as Types.RpcOpcode
    if (opcode < Protocol.opcode.request || opcode > Protocol.opcode.pong) {
      throw new RangeError(`Invalid frame opcode ${opcode}`)
    }
    const ident = this.#view.getUint32(this.#readIndex + 1, false)
    const length = this.#view.getUint32(this.#readIndex + 5, false)
    if (length > this.#frameLimit) {
      throw new RangeError(`Frame payload length ${length} exceeds maximum ${this.#frameLimit}`)
    }
    this.#readIndex += 9
    if (length === 0) {
      this.#rewind()
      return { opcode, ident, payload: Reader.#empty }
    }
    if (!(await this.#fill(length))) {
      throw new RangeError('Frame payload ended before the declared length was read')
    }
    const payload = this.#buffer.slice(this.#readIndex, this.#readIndex + length)
    this.#readIndex += length
    this.#rewind()
    return { opcode, ident, payload }
  }

  /**
   * Buffer until enough bytes exist.
   * @description Reads the socket until the need is met.
   * @param needed - Unread bytes required
   * @returns False when the stream ends empty
   * @throws {RangeError} When the stream ends mid-frame
   */
  async #fill(needed: number): Promise<boolean> {
    while (this.#writeIndex - this.#readIndex < needed) {
      const unread = this.#writeIndex - this.#readIndex
      const missing = needed - unread
      if (this.#buffer.byteLength - this.#writeIndex < missing) {
        if (
          this.#readIndex > 0 &&
          this.#buffer.byteLength > Reader.#capacity &&
          needed <= Reader.#capacity - Reader.#minimum &&
          unread <= Reader.#capacity - Reader.#minimum
        ) {
          const shrunk = new Uint8Array(Reader.#capacity)
          if (unread > 0) {
            shrunk.set(this.#buffer.subarray(this.#readIndex, this.#writeIndex), 0)
          }
          this.#buffer = shrunk
          this.#view = new DataView(
            this.#buffer.buffer,
            this.#buffer.byteOffset,
            this.#buffer.byteLength
          )
          this.#writeIndex = unread
          this.#readIndex = 0
        } else if (
          this.#readIndex > 0 &&
          (
            this.#buffer.byteLength - unread >= missing ||
            this.#buffer.byteLength - unread >= Reader.#minimum
          )
        ) {
          if (unread > 0) {
            this.#buffer.copyWithin(0, this.#readIndex, this.#writeIndex)
          }
          this.#writeIndex = unread
          this.#readIndex = 0
        } else if (
          this.#readIndex > 0 || this.#buffer.byteLength - this.#writeIndex < Reader.#minimum
        ) {
          const expanded = new Uint8Array(this.#buffer.byteLength * 2)
          expanded.set(this.#buffer.subarray(this.#readIndex, this.#writeIndex), 0)
          this.#buffer = expanded
          this.#view = new DataView(
            this.#buffer.buffer,
            this.#buffer.byteOffset,
            this.#buffer.byteLength
          )
          this.#writeIndex = unread
          this.#readIndex = 0
        }
      }
      const received = await this.#socket.read(this.#buffer.subarray(this.#writeIndex))
      if (received === null) {
        if (this.#writeIndex - this.#readIndex === 0) {
          return false
        }
        throw new RangeError('TCP frame ended before the declared bytes were read')
      }
      if (received === 0) {
        throw new RangeError('Zero-byte read from TCP stream')
      }
      this.#writeIndex += received
    }
    return true
  }

  /** Reset indexes and shrink idle buffer */
  #rewind(): void {
    if (this.#readIndex === this.#writeIndex) {
      this.#readIndex = 0
      this.#writeIndex = 0
      if (this.#buffer.byteLength > Reader.#capacity) {
        this.#buffer = new Uint8Array(Reader.#capacity)
        this.#view = new DataView(
          this.#buffer.buffer,
          this.#buffer.byteOffset,
          this.#buffer.byteLength
        )
      }
    }
  }
}
