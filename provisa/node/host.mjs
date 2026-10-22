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
//   <runtime> host.mjs <socket path | fd:N> <bundle entry module>
//
// It imports the bundle (the bundle imports nothing of Provisa's), connects to the socket
// Provisa listens on, says which exports it has, and serves calls until the socket closes, when
// it exits. A frame is a 4-byte big-endian length and that many bytes: a kind byte and its body.
// "J" is a control message, UTF-8 JSON, where a Date is written {"$date": iso}, a bigint
// {"$bigint": digits}, undefined as null, and a {"$decimal": digits} argument stays as it is,
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
import { Connection, loadBundle } from "./host-core.mjs";

const [socketPath, entry] = process.argv.slice(2);
const loaded = await loadBundle(entry);

// A path is connected to; fd:N is a socket Provisa already connected and handed down (the Linux
// sandbox refuses the host any new socket but TCP). An inherited one is read and written as a
// file descriptor: Bun's net.Socket does not take one, and node:fs does the same on every runtime.
const inherited = socketPath.startsWith("fd:");
const fd = inherited ? Number(socketPath.slice(3)) : null;
const socket = inherited
  ? fs.createReadStream(null, { fd, autoClose: false })
  : net.createConnection(socketPath);

function write(frame) {
  if (!inherited) {
    socket.write(frame);
    return;
  }
  for (let at = 0; at < frame.length; ) at += fs.writeSync(fd, frame, at);
}

const connection = new Connection(loaded, write, (message) => {
  process.stderr.write(`provisa refused this host: ${message}\n`);
  process.exit(2);
});
socket.on("data", (chunk) => connection.data(chunk));
if (inherited) connection.hello();
else socket.on("connect", () => connection.hello());
socket.on(inherited ? "end" : "close", () => process.exit(0));
socket.on("error", () => process.exit(1));
