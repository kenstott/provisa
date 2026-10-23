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

export const binaryStreams = (): Plugin => ({
  name: "provisa-binary-streams",
  transformIndexHtml: {
    order: "pre",
    handler: (html: string) =>
      inlineBinaryStreams(html, fs.readFileSync(BINARY_STREAMS_PATH, "utf-8")),
  },
});
