// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: the host a Node source's worker runs -- the same on Node, Bun and Deno. The protocol is
// every code source's (provisa/codesource/worker.py), served by host-core.mjs. In a built image the
// front end (provisa/codehost/front.py, REQ-2100) runs it, with a socket of its own.
//
//   <runtime> host.mjs <socket path | fd:N[,M...]> <bundle entry module>
//
// It imports the bundle (the bundle imports nothing of Provisa's), connects to the socket
// Provisa listens on PROVISA_HOST_CONNECTIONS times (or takes the connections it inherited), says
// on each which exports it has, and serves every connection's calls until one closes, when it
// exits: each of Provisa's concurrent calls has a connection of its own, and a connection's rows
// wait for its reader, so one call's slow reader holds back no other. A frame is a 4-byte
// big-endian length and that many bytes: a kind byte and its body. "J" is a control message,
// UTF-8 JSON, where a Date is written {"$date": iso}, a bigint {"$bigint": digits}, undefined as null, and a {"$decimal": digits} argument stays as it is,
// since JavaScript has no decimal. "A" is rows (REQ-2098): the call's id (8 bytes) and an Arrow
// IPC stream of them, built against the schema the call gives (provisa/codesource/arrow_rows.py);
// a numeric or json field is text, named so in its metadata. Arrow is the only form rows take.
// The Arrow library is apache-arrow's self-contained build, from PROVISA_HOST_LIB.
//
//   Provisa -> host  {op: "call", id, export, kind: "table"|"command"|"stream"|"columns", args, names,
//                     schema}  -- the rows' schema, base64 IPC; null for a single value
//                    {op: "cancel", id}      {op: "ping", id}
//   host -> Provisa  {op: "hello", protocol, code_hash, exports}
//                    rows (an Arrow frame)   {op: "value", id, value}
//                    {op: "end", id}         {op: "error", id, message, stack}  {op: "pong", id}
//   Provisa -> host  {op: "refused", message}, when it refuses this host's protocol or code
//
// REQ-2098: the hello names the protocol this host speaks and the content hash of its code
// (PROVISA_CODE_HASH in its environment), which Provisa checks.

import fs from "node:fs";
import net from "node:net";
import process from "node:process";
import { once } from "node:events";
import { Connection, loadBundle } from "./host-core.mjs";

const [socketPath, entry] = process.argv.slice(2);
// A path is connected to; fd:N,M,... are sockets Provisa already connected and handed down (the
// Linux sandbox refuses the host any new socket but TCP). An inherited one is read and written as
// a file descriptor: Bun's net.Socket does not take one, and node:fs does the same on every
// runtime. Each of its reads and writes holds one of libuv's pool threads while it waits, so the
// pool has two per connection beside its own four; set before the pool's first use.
const inherited = socketPath.startsWith("fd:");
const fds = inherited ? socketPath.slice(3).split(",").map(Number) : [];
const count = inherited ? fds.length : Number(process.env.PROVISA_HOST_CONNECTIONS);
process.env.UV_THREADPOOL_SIZE = String(4 + 2 * count);
const loaded = await loadBundle(entry);

function refused(message) {
  process.stderr.write(`provisa refused this host: ${message}\n`);
  process.exit(2);
}

/** A frame written to an inherited descriptor, after the ones before it: a promise of its end. */
function fdWriter(fd) {
  let tail = Promise.resolve();
  const writeAll = (frame) =>
    new Promise((resolve, reject) => {
      const step = (at) =>
        fs.write(fd, frame, at, frame.length - at, null, (error, written) => {
          if (error) reject(error);
          else if (at + written < frame.length) step(at + written);
          else resolve();
        });
      step(0);
    });
  return (frame) => (tail = tail.then(() => writeAll(frame)));
}

for (let i = 0; i < count; i++) {
  if (inherited) {
    const fd = fds[i];
    const connection = new Connection(loaded, fdWriter(fd), refused);
    const stream = fs.createReadStream(null, { fd, autoClose: false });
    stream.on("data", (chunk) => connection.data(chunk));
    stream.on("end", () => process.exit(0));
    stream.on("error", () => process.exit(1));
    connection.hello();
  } else {
    const socket = net.createConnection(socketPath);
    // A frame the socket buffers past its mark is a promise of its drain: the call's next waits.
    const write = (frame) => (socket.write(frame) ? undefined : once(socket, "drain"));
    const connection = new Connection(loaded, write, refused);
    socket.on("data", (chunk) => connection.data(chunk));
    socket.on("connect", () => connection.hello());
    socket.on("close", () => process.exit(0));
    socket.on("error", () => process.exit(1));
  }
}
