/**
 * Ordered socket write queue.
 * @description Serializes frame writes on one socket.
 */
export default class Queue {
  /** In-flight write chain */
  #chain: Promise<void> | null = null

  /**
   * Queue one frame write.
   * @description Appends a write after prior frames.
   * @param socket - Open TCP connection
   * @param frame - Encoded frame bytes
   * @returns Promise resolving after the write
   * @throws {RangeError} When the socket writes zero bytes
   */
  enqueue(socket: Deno.Conn, frame: Uint8Array): Promise<void> {
    const pending = this.#chain === null
      ? Queue.#write(socket, frame)
      : this.#chain.then(() => Queue.#write(socket, frame))
    const finish = (): void => {
      if (this.#chain === tracked) {
        this.#chain = null
      }
    }
    const tracked = pending.then(finish, finish)
    this.#chain = tracked
    return pending
  }

  /** Drop the pending write chain */
  reset(): void {
    this.#chain = null
  }

  /**
   * Write frame bytes fully.
   * @description Loops until every byte is sent.
   * @param socket - Open TCP connection
   * @param frame - Encoded frame bytes
   * @throws {RangeError} When the socket writes zero bytes
   */
  static async #write(socket: Deno.Conn, frame: Uint8Array): Promise<void> {
    let written = 0
    while (written < frame.byteLength) {
      const count = await socket.write(written === 0 ? frame : frame.subarray(written))
      if (count <= 0) {
        throw new RangeError('Failed to write frame bytes to TCP stream')
      }
      written += count
    }
  }
}
