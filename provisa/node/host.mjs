// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: the host a Node source's worker runs -- the same on Node, Bun and Deno. The protocol is
// every code source's (provisa/codesource/worker.py).
//
//   <runtime> host.mjs <socket path | fd:N> <bundle entry module>
//
// It imports the bundle (the bundle imports nothing of Provisa's), connects to the socket
// Provisa listens on, says which exports it has, and serves calls until the socket closes, when
// it exits. Frames are a 4-byte big-endian length and that many bytes of UTF-8 JSON; a Date is
// written {"$date": iso}, a bigint {"$bigint": digits}, undefined as null; a {"$decimal": digits}
// argument stays as it is, since JavaScript has no decimal.
//
//   Provisa -> host  {op: "call", id, export, kind: "table"|"command"|"stream", args, names}
//                    {op: "cancel", id}      {op: "ping", id}
//   host -> Provisa  {op: "hello", protocol, code_hash, exports}
//                    {op: "rows", id, rows}  {op: "value", id, value}
//                    {op: "end", id}         {op: "error", id, message, stack}  {op: "pong", id}
//   Provisa -> host  {op: "refused", message}, when it refuses this host's protocol or code
//
// REQ-2098: the hello names the protocol this host speaks and the content hash of its code
// (PROVISA_CODE_HASH in its environment), which Provisa checks.

import fs from "node:fs";
import net from "node:net";
import process from "node:process";
import { pathToFileURL } from "node:url";

const BATCH = 1000;
// The protocol this host speaks (provisa/codesource/worker.py PROTOCOL).
const PROTOCOL = 1;
const [socketPath, entry] = process.argv.slice(2);
const bundle = await import(pathToFileURL(entry).href);

function encode(value) {
  // A replacer sees a Date after its toJSON has run; the holder's own property is the Date.
  return JSON.stringify(value, function (key, v) {
    const raw = this[key];
    if (raw instanceof Date) return { $date: raw.toISOString() };
    if (typeof v === "bigint") return { $bigint: v.toString() };
    if (v === undefined) return null;
    return v;
  });
}

function revive(value) {
  if (Array.isArray(value)) return value.map(revive);
  if (value && typeof value === "object") {
    if (typeof value.$date === "string") return new Date(value.$date);
    if (typeof value.$day === "string") return new Date(`${value.$day}T00:00:00.000Z`);
    if (value.$absent === true) return undefined; // the function's own default applies
    if (typeof value.$bigint === "string") return BigInt(value.$bigint);
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = revive(v);
    return out;
  }
  return value;
}

// A path is connected to; fd:N is a socket Provisa already connected and handed down (the Linux
// sandbox refuses the host any new socket but TCP). An inherited one is read and written as a
// file descriptor: Bun's net.Socket does not take one, and node:fs does the same on every runtime.
const inherited = socketPath.startsWith("fd:");
const fd = inherited ? Number(socketPath.slice(3)) : null;
const socket = inherited
  ? fs.createReadStream(null, { fd, autoClose: false })
  : net.createConnection(socketPath);
function send(message) {
  const body = Buffer.from(encode(message), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  const frame = Buffer.concat([head, body]);
  if (!inherited) {
    socket.write(frame);
    return;
  }
  for (let at = 0; at < frame.length; ) at += fs.writeSync(fd, frame, at);
}

const running = new Map();

function iterable(result) {
  return result != null && (typeof result[Symbol.asyncIterator] === "function" || typeof result[Symbol.iterator] === "function");
}

async function serve({ id, export: name, kind, args }) {
  const fn = bundle[name];
  if (typeof fn !== "function") throw new Error(`the bundle exports no function ${name}`);
  const state = { cancelled: false, iterator: null };
  running.set(id, state);
  try {
    let result = fn(...revive(args));
    if (kind !== "stream") result = await result;
    if (kind === "command" && !iterable(result)) {
      send({ op: "value", id, value: result ?? null });
      return;
    }
    if (Array.isArray(result)) {
      for (let i = 0; i < result.length && !state.cancelled; i += BATCH) {
        send({ op: "rows", id, rows: result.slice(i, i + BATCH) });
      }
      return;
    }
    const iterator = result[Symbol.asyncIterator] ? result[Symbol.asyncIterator]() : result[Symbol.iterator]();
    state.iterator = iterator;
    let batch = [];
    for (;;) {
      const step = await iterator.next();
      if (step.done || state.cancelled) break;
      if (kind === "stream") {
        send({ op: "rows", id, rows: [step.value] });
      } else {
        batch.push(step.value);
        if (batch.length >= BATCH) {
          send({ op: "rows", id, rows: batch });
          batch = [];
        }
      }
    }
    if (batch.length && !state.cancelled) send({ op: "rows", id, rows: batch });
  } finally {
    running.delete(id);
  }
}

function dispatch(message) {
  if (message.op === "refused") {
    process.stderr.write(`provisa refused this host: ${message.message}\n`);
    process.exit(2);
  }
  if (message.op === "ping") return send({ op: "pong", id: message.id });
  if (message.op === "cancel") {
    const state = running.get(message.id);
    if (state) {
      state.cancelled = true;
      state.iterator?.return?.();
    }
    return;
  }
  if (message.op === "call") {
    serve(message).then(
      () => send({ op: "end", id: message.id }),
      (error) => send({ op: "error", id: message.id, message: String(error?.message ?? error), stack: String(error?.stack ?? "") }),
    );
  }
}

let pending = Buffer.alloc(0);
socket.on("data", (chunk) => {
  pending = Buffer.concat([pending, chunk]);
  while (pending.length >= 4) {
    const size = pending.readUInt32BE(0);
    if (pending.length < 4 + size) break;
    const body = pending.subarray(4, 4 + size).toString("utf8");
    pending = pending.subarray(4 + size);
    dispatch(JSON.parse(body));
  }
});
const hello = () =>
  send({ op: "hello", protocol: PROTOCOL, code_hash: process.env.PROVISA_CODE_HASH, exports: Object.keys(bundle) });
if (inherited) hello();
else socket.on("connect", hello);
socket.on(inherited ? "end" : "close", () => process.exit(0));
socket.on("error", () => process.exit(1));
