// Copyright (c) 2026 Kenneth Stott
// Canary: cddbd0a2-ca63-4734-8da2-0a6eddbbf1cf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Plugin } from "vite";
import path from "path";
import fs from "fs";

// The splash's binary streams (index.html) come from splash/binary-streams.js, the one renderer
// the SaaS front door's waking page also runs. The splash has to paint before any bundle loads,
// so the file is inlined into the page at the marker below, in dev and in the build alike.
export const BINARY_STREAMS_MARKER = "<!-- provisa:binary-streams -->";

export const BINARY_STREAMS_PATH = path.resolve(__dirname, "../../splash/binary-streams.js");

export function inlineBinaryStreams(html: string, renderer: string): string {
  if (!html.includes(BINARY_STREAMS_MARKER)) {
    throw new Error(`index.html has no ${BINARY_STREAMS_MARKER} for the splash's renderer`);
  }
  return html.replace(BINARY_STREAMS_MARKER, () => `<script>\n${renderer}</script>`);
}

// Only the app's index.html shows the splash; the other HTML entries (source-sign-in.html,
// auth-relay.html) are built through the same hook and pass through unchanged.
export function binaryStreamsFor(html: string, page: string, renderer: () => string): string {
  return path.basename(page) === "index.html" ? inlineBinaryStreams(html, renderer()) : html;
}

export const binaryStreams = (): Plugin => ({
  name: "provisa-binary-streams",
  transformIndexHtml: {
    order: "pre",
    handler: (html: string, ctx: { path: string }) =>
      binaryStreamsFor(html, ctx.path, () => fs.readFileSync(BINARY_STREAMS_PATH, "utf-8")),
  },
});
