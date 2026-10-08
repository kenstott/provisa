// Copyright (c) 2026 Kenneth Stott
// Canary: 3334264f-0813-41b5-b22f-4a5cea01ddeb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A subscription's last frame names why the stream ended; the query page shows it from the
// server-error catalog.

import { describe, it, expect } from "vitest";
import type { ServerMessageShape } from "../../i18n/serverMessage";
import { localizedStreamFrame } from "../streamErrors";

// As serverMessage: the catalog's text for a code it has, else the server's English text.
const catalog: Record<string, string> = {
  "subscribe.avro_topic_without_registry": "Thema {{topic}} ist Avro",
};
const msg = (body: ServerMessageShape, fallback: string) =>
  body.code && catalog[body.code]
    ? `${catalog[body.code]} ${JSON.stringify(body.params ?? {})}`
    : (body.message ?? fallback);

describe("localizedStreamFrame", () => {
  it("renders a coded refusal from the catalog and keeps its code and params", () => {
    const frame = {
      errors: [
        {
          message: "Topic 'p.app.orders' carries Avro messages",
          extensions: {
            code: "subscribe.avro_topic_without_registry",
            params: { topic: "p.app.orders" },
            subscription_id: "abc",
          },
        },
      ],
    };
    expect(localizedStreamFrame(frame, msg)).toEqual({
      errors: [
        {
          message: 'Thema {{topic}} ist Avro {"topic":"p.app.orders"}',
          extensions: frame.errors[0].extensions,
        },
      ],
    });
  });

  it("keeps the server's text for a code the catalog does not have", () => {
    const frame = { errors: [{ message: "new refusal", extensions: { code: "x.unknown" } }] };
    expect(localizedStreamFrame(frame, msg)).toEqual(frame);
  });

  it("leaves an error with no code as the server sent it", () => {
    const frame = {
      errors: [{ message: "Internal server error", extensions: { type: "RuntimeError" } }],
    };
    expect(localizedStreamFrame(frame, msg)).toEqual(frame);
  });

  it("leaves a data frame, and anything that is not a GraphQL result, untouched", () => {
    const data = { data: { orders: [{ id: 1 }] } };
    expect(localizedStreamFrame(data, msg)).toBe(data);
    expect(localizedStreamFrame(null, msg)).toBeNull();
    expect(localizedStreamFrame([1, 2], msg)).toEqual([1, 2]);
  });

  it("keeps the data of a frame that carries both data and errors", () => {
    const frame = { data: { a: 1 }, errors: [{ message: "m" }] };
    expect(localizedStreamFrame(frame, msg)).toEqual(frame);
  });
});
