import * as net from "net";
import * as tls from "tls";
import { DatabaseError } from "./errors";

/** One cancellable TCP/TLS handshake. No commands are retried by this helper. */
export function dialSocket(
  host: string,
  port: number,
  options: tls.ConnectionOptions | undefined,
  timeoutMs: number,
  signal: AbortSignal,
): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DatabaseError("connection cancelled", "CONNECTION_CLOSED"));
      return;
    }
    const ready = options ? "secureConnect" : "connect";
    const socket = options
      ? tls.connect({ ...options, host, port })
      : net.createConnection({ host, port });
    let settled = false;
    const cleanup = (): void => {
      if (timer) clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      socket.off(ready, connected);
      socket.off("error", failed);
      socket.off("close", closed);
    };
    const failed = (error: Error): void => {
      if (settled) return;
      settled = true;
      cleanup();
      socket.destroy();
      reject(error);
    };
    const abort = (): void =>
      failed(new DatabaseError("connection cancelled", "CONNECTION_CLOSED"));
    const closed = (): void =>
      failed(
        new DatabaseError(
          "connection closed during handshake",
          "CONNECTION_CLOSED",
        ),
      );
    const connected = (): void => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(socket);
    };
    socket.on("error", () => undefined); // Covers the handoff before caller handlers attach.
    socket.once("error", failed);
    socket.once("close", closed);
    socket.once(ready, connected);
    signal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(
      () =>
        failed(new DatabaseError("connection handshake timed out", "TIMEOUT")),
      timeoutMs,
    );
  });
}
