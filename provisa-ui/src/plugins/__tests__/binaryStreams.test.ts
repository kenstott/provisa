import fs from "fs";
import path from "path";
import { describe, expect, it } from "vitest";
import { BINARY_STREAMS_MARKER, BINARY_STREAMS_PATH, inlineBinaryStreams } from "../binaryStreams";

const INDEX_HTML = path.resolve(__dirname, "../../../index.html");

describe("the splash's binary streams", () => {
  const html = fs.readFileSync(INDEX_HTML, "utf-8");
  const renderer = fs.readFileSync(BINARY_STREAMS_PATH, "utf-8");

  it("inlines the shared renderer ahead of the splash script that starts it", () => {
    const page = inlineBinaryStreams(html, renderer);
    expect(page).not.toContain(BINARY_STREAMS_MARKER);
    const at = page.indexOf(renderer);
    expect(at).toBeGreaterThan(0);
    expect(at).toBeLessThan(page.indexOf("window.provisaBinaryStreams(canvas"));
  });

  it("draws glyphs from one sprite and gives reduced motion one static frame", () => {
    expect(renderer.match(/fillText\(/g)).toHaveLength(1);
    expect(renderer).toContain("ctx.drawImage(");
    const start = renderer.indexOf("if (options.reduce) {");
    const branch = renderer.slice(start, renderer.indexOf("return", start));
    expect(branch).toContain("render(1);");
    expect(branch).not.toContain("requestAnimationFrame");
    expect(html).toContain('matchMedia("(prefers-reduced-motion: reduce)")');
  });

  it("keeps the halo on the P's loop and fades the glyphs over the mark and its text", () => {
    expect(html).toContain("y: r.top + r.height * 0.35");
    expect(html).toContain('document.querySelector(".provisa-splash-mark")');
  });

  it("refuses a page without the marker rather than shipping a splash with no renderer", () => {
    expect(() => inlineBinaryStreams("<html></html>", renderer)).toThrow(BINARY_STREAMS_MARKER);
  });
});
