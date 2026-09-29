// SPDX-License-Identifier: MIT

import { EventEmitter } from "node:events";
import { closeSync } from "node:fs";
import net from "node:net";

import { PrivateBridge } from "./bridge.mjs";

function inheritedFD(value, name) {
  if (!Number.isSafeInteger(value) || value < 3) {
    throw new TypeError(`Pi-family inherited ${name} descriptor is invalid`);
  }
  return value;
}

export function openPiInheritedStream(fd) {
  return new net.Socket({ fd: inheritedFD(fd, "Pi"), readable: true, writable: true });
}

class BunPipeStream extends EventEmitter {
  constructor(readFD, writeFD) {
    super();
    if (typeof globalThis.Bun?.file !== "function") {
      throw new Error("OMP inherited bridge requires Bun.file");
    }
    this.readFD = inheritedFD(readFD, "OMP read");
    this.writeFD = inheritedFD(writeFD, "OMP write");
    if (this.readFD === this.writeFD) throw new TypeError("OMP inherited bridge descriptors must differ");
    this.reader = Bun.file(this.readFD).stream().getReader();
    this.writer = Bun.file(this.writeFD).writer();
    this.writeTail = Promise.resolve();
    this.destroyed = false;
    this.closed = false;
    queueMicrotask(() => this.#read());
  }

  write(body, callback = () => {}) {
    if (this.destroyed) {
      queueMicrotask(() => callback(new Error("OMP inherited bridge is closed")));
      return false;
    }
    const payload = Buffer.from(body);
    this.writeTail = this.writeTail.then(async () => {
      if (this.destroyed) throw new Error("OMP inherited bridge is closed");
      await this.writer.write(payload);
      await this.writer.flush();
    });
    this.writeTail.then(
      () => callback(),
      (error) => {
        callback(error);
        this.destroy(error);
      },
    );
    return false;
  }

  destroy(error = undefined) {
    if (this.destroyed) return this;
    this.destroyed = true;
    // Bun 1.4 treats numeric Bun.file descriptors as borrowed: cancel/end
    // finalize their JS objects but do not close the OS descriptors. This
    // stream is their sole OS-FD owner and closes each exactly once here.
    void this.reader.cancel().catch(() => {});
    try {
      this.writer.end();
    } catch {}
    try {
      closeSync(this.readFD);
    } catch {}
    try {
      closeSync(this.writeFD);
    } catch {}
    if (error) this.emit("error", error);
    this.closed = true;
    this.emit("close");
    return this;
  }

  async #read() {
    try {
      while (!this.destroyed) {
        const part = await this.reader.read();
        if (part.done) {
          this.emit("end");
          this.destroy();
          return;
        }
        this.emit("data", Buffer.from(part.value));
      }
    } catch (error) {
      if (!this.destroyed) this.destroy(error);
    }
  }
}

export function openOMPInheritedStream(readFD, writeFD) {
  return new BunPipeStream(readFD, writeFD);
}

async function connectInherited(stream, options) {
  let bridge;
  try {
    bridge = new PrivateBridge(stream, options);
    await bridge.ready(options?.signal);
    return bridge;
  } catch (error) {
    if (bridge) await bridge.close().catch(() => {});
    else stream.destroy();
    throw error;
  }
}

export function connectPiInheritedBridge(fd, options = {}) {
  return connectInherited(openPiInheritedStream(fd), options);
}

export function connectOMPInheritedBridge(readFD, writeFD, options = {}) {
  return connectInherited(openOMPInheritedStream(readFD, writeFD), options);
}
