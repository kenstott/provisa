// Copyright (c) 2026 Kenneth Stott
// Canary: 475f9314-9f22-4159-8315-9d9af520ead0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Response } from "@playwright/test";

/**
 * A response with its body, read the moment the response arrives. Chromium evicts a response body
 * from its inspector cache under load, so a body read after the fact can fail with "Request
 * content was evicted from inspector cache" (run 45698524787). Wrap a `page.waitForResponse`
 * promise in this so the read happens in the same step as the arrival, not later in the test.
 */
export interface ResponseWithBody {
  response: Response;
  text: string;
  json<T>(): T;
}

export function withBody(arrival: Promise<Response>): Promise<ResponseWithBody> {
  return arrival.then(async (response) => {
    const text = await response.text();
    return { response, text, json: <T>() => JSON.parse(text) as T };
  });
}
