// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2104, REQ-2105, REQ-2106: Provisa's services to a code source's code, as "@provisa/services".
// Always present in a Node, Bun or Deno source's worker: the host resolves the name to this file
// (host.mjs). Outside Provisa it is not there, and code that imports it is tested by mocking it.
//
//   import { secrets, settings, cache, vectors, http, llm, log, metric } from "@provisa/services";
//
// The same calls, by the same names, as Python's provisa_services (provisa/python/lib): each is
// made to $PROVISA_SERVICES through the worker's egress proxy, naming the call it serves, so what
// it does is recorded against the request that called the code. A refusal is thrown as a
// ServiceError naming the service; with no $PROVISA_SERVICES every call throws it.

import { AsyncLocalStorage } from "node:async_hooks";
import process from "node:process";

// The call this code is serving: its id and the request's traceparent (the host runs each call in it).
export const __call = new AsyncLocalStorage();

export class ServiceError extends Error {
  constructor(service, status, message) {
    super(`${service}: ${message}`);
    this.name = "ServiceError";
    this.service = service;
    this.status = status;
  }
}

function base(service) {
  const value = process.env.PROVISA_SERVICES;
  if (!value) {
    throw new ServiceError(
      service,
      0,
      "Provisa's services are reached only from a Provisa code source's worker ($PROVISA_SERVICES is not set)",
    );
  }
  return value;
}

async function send(service, method, path, payload) {
  const headers = {};
  if (payload !== undefined) headers["Content-Type"] = "application/json";
  const held = __call.getStore();
  if (held) {
    headers["X-Provisa-Call"] = String(held.id);
    if (held.traceparent) headers.traceparent = held.traceparent;
  }
  const answered = await fetch(`${base(service)}/${path}`, {
    method,
    headers,
    body: payload === undefined ? undefined : JSON.stringify(payload),
  });
  return { status: answered.status, text: await answered.text() };
}

function reason(text) {
  try {
    return JSON.parse(text).error.message;
  } catch {
    return text;
  }
}

async function json(service, method, path, payload) {
  const { status, text } = await send(service, method, path, payload);
  if (status !== 200) throw new ServiceError(service, status, reason(text));
  return JSON.parse(text);
}

const q = encodeURIComponent;

export const secrets = {
  /** The declared secret `name`, read from the org's vault now. */
  async get(name) {
    const { status, text } = await send("secrets", "GET", `secrets/${q(name)}`);
    if (status !== 200) throw new ServiceError("secrets", status, text);
    return text;
  },
};

export const settings = {
  /** The source's setting `name`; `fallback` when it declares none (else thrown). */
  async get(name, ...fallback) {
    const { status, text } = await send("settings", "GET", `settings/${q(name)}`);
    if (status === 404 && fallback.length) return fallback[0];
    if (status !== 200) throw new ServiceError("settings", status, reason(text));
    return JSON.parse(text).value;
  },
  async all() {
    return json("settings", "GET", "settings");
  },
};

export const cache = {
  /** The cached value of `key`, or `fallback` when none is cached. */
  async get(key, fallback = null) {
    const { status, text } = await send("cache", "GET", `cache/${q(key)}`);
    if (status === 404) return fallback;
    if (status !== 200) throw new ServiceError("cache", status, reason(text));
    return JSON.parse(text).value;
  },
  /** Cache `value` (JSON) under `key` for every worker of this source, for `ttl` seconds (null: until deleted). */
  async set(key, value, ttl = null) {
    await json("cache", "PUT", `cache/${q(key)}`, { value, ttl });
  },
  async delete(key) {
    await json("cache", "DELETE", `cache/${q(key)}`);
  },
};

export const dq = {
  /** A dataset's checker tables (description, contract, latest run, assigned product) and its
   *  profiles, as the meta domain shows them to the caller (REQ-2133). `name`: `domain.table` or a table id. */
  async dataset(name) {
    return json("dq", "GET", `dq/dataset/${q(String(name))}`);
  },
  /** A data product's promises, expectations, partner DQ and orphans (REQ-1443). */
  async product(productId) {
    return json("dq", "GET", `dq/product/${q(productId)}`);
  },
};

export const vectors = {
  async embed(texts, model) {
    return (await json("vectors", "POST", "vectors/embed", { texts, model })).vectors;
  },
  /** Add or replace `items` -- each {id, text | vector, metadata}; a text is embedded by `model`. */
  async upsert(items, model = null) {
    return (await json("vectors", "POST", "vectors/upsert", { items, model })).upserted;
  },
  /** The `k` items nearest `query` (a text, embedded by `model`, or a vector), nearest first. */
  async search(query, k = 5, model = null) {
    return (await json("vectors", "POST", "vectors/search", { query, k, model })).matches;
  },
  async delete(ids) {
    await json("vectors", "POST", "vectors/delete", { ids });
  },
};

class Response {
  constructor(answered) {
    this.status = answered.status;
    this.headers = answered.headers;
    this.body =
      answered.body_base64 !== undefined
        ? Uint8Array.from(atob(answered.body_base64), (c) => c.charCodeAt(0))
        : new TextEncoder().encode(answered.body);
  }
  text() {
    return new TextDecoder().decode(this.body);
  }
  json() {
    return JSON.parse(this.text());
  }
}

export const http = {
  /** A fetch Provisa makes for the code: to a host on the egress allow-list, recorded. */
  async get(url, { headers = {}, timeout = null } = {}) {
    return new Response(await json("http", "POST", "http", { method: "GET", url, headers, timeout }));
  },
  async post(url, { body = null, json: value, headers = {}, timeout = null } = {}) {
    const sent = { ...headers };
    const request = { method: "POST", url, timeout };
    if (value !== undefined) {
      sent["Content-Type"] ??= "application/json";
      request.body = JSON.stringify(value);
    } else if (body instanceof Uint8Array) {
      request.body_base64 = btoa(String.fromCharCode(...body));
    } else if (body !== null) {
      request.body = body;
    }
    request.headers = sent;
    return new Response(await json("http", "POST", "http", request));
  },
};

export const llm = {
  /** Provisa's OpenAI-compatible LLM service: any OpenAI SDK also works at this base URL. */
  baseUrl() {
    return `${base("llm")}/llm/v1`;
  },
  /** A chat completion: `model` is provisa:<role> or a model the org assigns. */
  async chat(model, messages, params = {}) {
    return json("llm", "POST", "llm/v1/chat/completions", { model, messages, ...params });
  },
  async embeddings(model, input) {
    return json("llm", "POST", "llm/v1/embeddings", { model, input });
  },
};

/** A log line, recorded against the request this call serves. */
export async function log(message, level = "info", fields = {}) {
  await json("log", "POST", "log", { level, message, fields });
}

/** A metric point (counter, gauge or histogram), named code_source.<name>. */
export async function metric(name, value, kind = "counter", attributes = {}) {
  await json("metric", "POST", "metric", { name, value, kind, attributes });
}

export default { ServiceError, secrets, settings, cache, vectors, http, llm, log, metric };
