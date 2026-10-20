// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-2098: the protocol a Node source's host speaks (provisa/codesource/worker.py), over
// any connection -- the local worker's socket (host.mjs) or a remote host's TLS connections
// (remote_host.mjs), each served by its own Connection with its own calls. See host.mjs for the
// frames.
//
// REQ-2106: "@provisa/services" resolves to services.mjs beside this host on every runtime --
// Node by a resolve hook, Bun by a runtime plugin, Deno by the import map its command names
// (provisa/node/worker.py) -- and each call runs in its call's context (services.mjs __call), and
// in the request's trace when the code traces itself with @opentelemetry/api. A call's end names
// the CPU it took in this process.

import module, { createRequire } from "node:module";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

const BATCH = 1000;
// The protocol this host speaks (provisa/codesource/worker.py PROTOCOL).
export const PROTOCOL = 2;
const IR_KEY = "provisa.ir";
const WHOLE_ROW = "$row";
let Arrow = null;
function arrow() {
  Arrow ??= createRequire(import.meta.url)(`${process.env.PROVISA_HOST_LIB}/package/Arrow.es2015.min.js`);
  return Arrow;
}

// REQ-2106: "@provisa/services" is this host's services.mjs, the one instance calls are bound in.
const SERVICES = new URL("./services.mjs", import.meta.url).href;

function resolveServices() {
  if (globalThis.Bun !== undefined) {
    globalThis.Bun.plugin({
      name: "provisa-services",
      setup(build) {
        build.onResolve({ filter: /^@provisa\/services$/ }, () => ({ path: fileURLToPath(SERVICES) }));
      },
    });
  } else if (globalThis.Deno === undefined) {
    module.registerHooks({
      resolve(specifier, context, next) {
        if (specifier === "@provisa/services") return { url: SERVICES, shortCircuit: true };
        return next(specifier, context);
      },
    });
  }
}

/** The code's bundle, imported once per process, with Provisa's services resolvable from it and
 * the code's own OpenTelemetry (null when it does not trace itself). */
export async function loadBundle(entry) {
  resolveServices();
  const services = await import(SERVICES);
  const bundle = await import(pathToFileURL(entry).href);
  let otel = null;
  try {
    otel = createRequire(pathToFileURL(entry).href)("@opentelemetry/api");
  } catch {
    otel = null; // the code does not trace itself: there is no trace of its own to join
  }
  return { bundle, services, otel };
}

export function encode(value) {
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

function iterable(result) {
  return result != null && (typeof result[Symbol.asyncIterator] === "function" || typeof result[Symbol.iterator] === "function");
}

/** A frame: a 4-byte big-endian length, then the body. */
export function frame(body) {
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

/** One connection's serving: its calls, the frames it reads and the ones it writes (`write`
 * takes a whole frame). `onRefused` is told when Provisa refuses this host. */
export class Connection {
  constructor(loaded, write, onRefused) {
    this.bundle = loaded.bundle;
    this.services = loaded.services;
    this.otel = loaded.otel;
    this.write = write;
    this.onRefused = onRefused;
    this.running = new Map();
    this.pending = Buffer.alloc(0);
  }

  send(message) {
    this.write(frame(Buffer.concat([Buffer.from("J"), Buffer.from(encode(message), "utf8")])));
  }

  hello() {
    this.send({ op: "hello", protocol: PROTOCOL, code_hash: process.env.PROVISA_CODE_HASH, exports: Object.keys(this.bundle) });
  }

  sendRows(id, rows, schema) {
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
    this.write(frame(Buffer.concat([head, Buffer.from(ipc)])));
  }

  async serve({ id, export: name, kind, args, schema: schemaText }) {
    const fn = this.bundle[name];
    if (typeof fn !== "function") throw new Error(`the bundle exports no function ${name}`);
    const state = { cancelled: false, iterator: null };
    this.running.set(id, state);
    try {
      let result = fn(...revive(args));
      if (kind !== "stream") result = await result;
      if (schemaText === null || schemaText === undefined) {
        // A single value, or a command whose result is not rows: its JSON value message.
        this.send({ op: "value", id, value: result ?? null });
        return;
      }
      const schema = arrow().tableFromIPC(Buffer.from(schemaText, "base64")).schema;
      if (!iterable(result)) throw new Error(`a ${typeof result} is not a collection of rows`);
      if (Array.isArray(result)) {
        for (let i = 0; i < result.length && !state.cancelled; i += BATCH) {
          this.sendRows(id, result.slice(i, i + BATCH), schema);
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
          this.sendRows(id, [step.value], schema); // a stream's row goes as it comes
        } else {
          batch.push(step.value);
          if (batch.length >= BATCH) {
            this.sendRows(id, batch, schema);
            batch = [];
          }
        }
      }
      if (batch.length && !state.cancelled) this.sendRows(id, batch, schema);
    } finally {
      this.running.delete(id);
    }
  }

  dispatch(message) {
    if (message.op === "refused") return this.onRefused(message.message);
    if (message.op === "ping") return this.send({ op: "pong", id: message.id });
    if (message.op === "cancel") {
      const state = this.running.get(message.id);
      if (state) {
        state.cancelled = true;
        state.iterator?.return?.();
      }
      return;
    }
    if (message.op === "call") {
      const started = process.cpuUsage();
      const call = { id: message.id, traceparent: message.traceparent ?? null };
      this.services.__call.run(call, () => this.inTrace(call.traceparent, () => this.serve(message))).then(
        () => {
          const used = process.cpuUsage(started);
          this.send({ op: "end", id: message.id, cpu_ms: (used.user + used.system) / 1000 });
        },
        (error) =>
          this.send({ op: "error", id: message.id, message: String(error?.message ?? error), stack: String(error?.stack ?? "") }),
      );
    }
  }

  /** ``fn`` in the request's trace, when the code traces itself. */
  inTrace(traceparent, fn) {
    if (!this.otel || !traceparent) return fn();
    const ctx = this.otel.propagation.extract(this.otel.context.active(), { traceparent });
    return this.otel.context.with(ctx, fn);
  }

  /** Bytes read from the connection: every whole frame in them dispatched. */
  data(chunk) {
    this.pending = Buffer.concat([this.pending, chunk]);
    while (this.pending.length >= 4) {
      const size = this.pending.readUInt32BE(0);
      if (this.pending.length < 4 + size) break;
      const body = this.pending.subarray(5, 4 + size).toString("utf8"); // after the "J" kind byte
      this.pending = this.pending.subarray(4 + size);
      this.dispatch(JSON.parse(body));
    }
  }

  /** The connection closed: its calls are stopped. */
  close() {
    for (const state of this.running.values()) {
      state.cancelled = true;
      state.iterator?.return?.();
    }
  }
}
