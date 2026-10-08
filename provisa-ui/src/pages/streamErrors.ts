// Copyright (c) 2026 Kenneth Stott
// Canary: ceb75f3d-f64d-4735-b82b-5a8337b864d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A subscription stream that ends on a failure says why in one last frame
// (provisa/api/data/stream_end.py): the GraphQL `errors` shape, with the refusal's stable code
// and params under `extensions`. The query page shows that frame, with the message taken from
// the server-error catalog when the catalog has the code, so the subscriber reads the refusal
// in their own language instead of a stream that just stops.

import { serverMessage, type ServerMessageShape } from "../i18n/serverMessage";

type Message = (body: ServerMessageShape, fallback: string) => string;

interface StreamError {
  message?: unknown;
  extensions?: { code?: unknown; params?: unknown } | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** `frame` with each coded error's message rendered from the catalog; anything else unchanged. */
export function localizedStreamFrame<T>(frame: T, msg: Message = serverMessage): T {
  if (!isRecord(frame) || !Array.isArray(frame.errors)) return frame;
  const errors = frame.errors.map((error: StreamError) => {
    const code = error?.extensions?.code;
    if (typeof code !== "string" || typeof error.message !== "string") return error;
    const params = isRecord(error.extensions?.params) ? error.extensions.params : {};
    return { ...error, message: msg({ code, params, message: error.message }, error.message) };
  });
  return { ...frame, errors } as T;
}
