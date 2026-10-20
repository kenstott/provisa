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
// it exits. A frame is a 4-byte big-endian length and that many bytes: a kind byte and its body.
// "J" is a control message, UTF-8 JSON, where a Date is written {"$date": iso}, a bigint
// {"$bigint": digits}, undefined as null, and a {"$decimal": digits} argument stays as it is,
// since JavaScript has no decimal. "A" is rows (REQ-2098): the call's id (8 bytes) and an Arrow
// IPC stream of them, built against the schema the call gives (provisa/codesource/arrow_rows.py);
// a numeric or json field is text, named so in its metadata. Arrow is the only form rows take.
// The Arrow library is apache-arrow's self-contained build, from PROVISA_HOST_LIB.
//
//   Provisa -> host  {op: "call", id, export, kind: "table"|"command"|"stream", args, names,
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
import { createRequire } from "node:module";
import net from "node:net";
import process from "node:process";
import { pathToFileURL } from "node:url";

const BATCH = 1000;
// The protocol this host speaks (provisa/codesource/worker.py PROTOCOL).
const PROTOCOL = 2;
const IR_KEY = "provisa.ir";
const WHOLE_ROW = "$row";
let Arrow = null;
function arrow() {
  Arrow ??= createRequire(import.meta.url)(`${process.env.PROVISA_HOST_LIB}/package/Arrow.es2015.min.js`);
  return Arrow;
}
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
  write(Buffer.concat([Buffer.from("J"), Buffer.from(encode(message), "utf8")]));
}

// A value in the form an Arrow field takes it.
function forField(value, field) {
  const A = arrow();
  if (value === null || value === undefined) return null;
  const ir = field.metadata.get(IR_KEY);
  if (ir === "json") return encode(value);
  if (ir === "numeric") return String(value);
  const type = field.type;
  if (A.DataType.isStruct(type)) {
    const out = {};
    for (const child of type.children) out[child.name] = forField(value[child.name], child);
    return out;
  }
  if (A.DataType.isList(type)) return Array.from(value, (v) => forField(v, type.children[0]));
  if (A.DataType.isInt(type) && type.bitWidth === 64 && typeof value === "number") return BigInt(value);
  return value;
}

function sendRows(id, rows, schema) {
  const A = arrow();
  const fields = schema.fields;
  const values = fields.length === 1 && fields[0].name === WHOLE_ROW ? rows.map((r) => ({ [WHOLE_ROW]: r })) : rows;
  const children = fields.map((f) => {
    const builder = A.makeBuilder({ type: f.type, nullValues: [null, undefined] });
    for (const row of values) builder.append(forField(row?.[f.name], f));
    return builder.finish().toVector().data[0];
  });
  const batch = new A.RecordBatch(
    schema,
    A.makeData({ type: new A.Struct(fields), length: values.length, nullCount: 0, children }),
  );
  const ipc = A.tableToIPC(new A.Table(batch), "stream");
  const head = Buffer.alloc(9);
  head.write("A", 0);
  head.writeBigUInt64BE(BigInt(id), 1);
  write(Buffer.concat([head, Buffer.from(ipc)]));
}

function write(body) {
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

async function serve({ id, export: name, kind, args, schema: schemaText }) {
  const fn = bundle[name];
  if (typeof fn !== "function") throw new Error(`the bundle exports no function ${name}`);
  const state = { cancelled: false, iterator: null };
  running.set(id, state);
  try {
    let result = fn(...revive(args));
    if (kind !== "stream") result = await result;
    if (schemaText === null || schemaText === undefined) {
      // A single value, or a command whose result is not rows: its JSON value message.
      send({ op: "value", id, value: result ?? null });
      return;
    }
    const schema = arrow().tableFromIPC(Buffer.from(schemaText, "base64")).schema;
    if (!iterable(result)) throw new Error(`a ${typeof result} is not a collection of rows`);
    if (Array.isArray(result)) {
      for (let i = 0; i < result.length && !state.cancelled; i += BATCH) {
        sendRows(id, result.slice(i, i + BATCH), schema);
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
        sendRows(id, [step.value], schema); // a stream's row goes as it comes
      } else {
        batch.push(step.value);
        if (batch.length >= BATCH) {
          sendRows(id, batch, schema);
          batch = [];
        }
      }
    }
    if (batch.length && !state.cancelled) sendRows(id, batch, schema);
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
    const body = pending.subarray(5, 4 + size).toString("utf8"); // after the "J" kind byte
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
