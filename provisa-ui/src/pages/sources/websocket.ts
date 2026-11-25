// Copyright (c) 2026 Kenneth Stott
// Canary: 494bf6c4-d418-4cf2-a76e-c861a63b57e6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1739: a WebSocket feed's connection settings that host, port and path cannot carry travel
// in its federation hints.

import { hintKey } from "./descriptor";

export const WEBSOCKET = "websocket";
// REQ-2114: the hint keys are the descriptor's (provisa/api/admin/source_types.py).
const USE_SSL = hintKey(WEBSOCKET, "use_ssl");
const SUBSCRIBE_PAYLOAD = hintKey(WEBSOCKET, "subscribe_payload");
const EVENT_PATH = hintKey(WEBSOCKET, "event_path");
const RECONNECT_INTERVAL = hintKey(WEBSOCKET, "reconnect_interval");

/** The form's WebSocket settings. */
export interface WebsocketSeed {
  useSsl: boolean;
  subscribePayload: string;
  eventPath: string;
  reconnectInterval: string;
}

/** The settings of a new WebSocket source. */
export const EMPTY_WEBSOCKET: WebsocketSeed = {
  useSsl: false,
  subscribePayload: "",
  eventPath: "",
  reconnectInterval: "",
};

/** The hints a form saves: each setting given. */
export function websocketHints(ws: WebsocketSeed): Record<string, string> {
  return Object.fromEntries(
    (
      [
        [USE_SSL, ws.useSsl ? "true" : ""],
        [SUBSCRIBE_PAYLOAD, ws.subscribePayload.trim()],
        [EVENT_PATH, ws.eventPath.trim()],
        [RECONNECT_INTERVAL, ws.reconnectInterval.trim()],
      ] as const
    ).filter(([, v]) => v),
  );
}

/** A saved source's hints into the form; a setting it does not hold is a new source's. */
export function websocketFromHints(hints: Record<string, string> | null): WebsocketSeed {
  return {
    useSsl: hints?.[USE_SSL]?.toLowerCase() === "true",
    subscribePayload: hints?.[SUBSCRIBE_PAYLOAD] ?? EMPTY_WEBSOCKET.subscribePayload,
    eventPath: hints?.[EVENT_PATH] ?? EMPTY_WEBSOCKET.eventPath,
    reconnectInterval: hints?.[RECONNECT_INTERVAL] ?? EMPTY_WEBSOCKET.reconnectInterval,
  };
}
