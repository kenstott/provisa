// Copyright (c) 2026 Kenneth Stott
// Canary: 90434e51-451e-4549-b3ee-9e7de4e55cad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every 3D view's Export control, on its stage: the image formats the browser encodes, SVG only for
// a scene that draws flat, the video containers it records; a choice saves a file named after the
// view, and a recording shows its progress and can be stopped.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../../test-utils/render";
import type { ExportableScene } from "../exportImage";

const saved = vi.hoisted(() => ({ files: [] as string[], recordings: [] as AbortSignal[] }));
vi.mock("../exportImage", async (actual) => {
  const real = await actual<typeof import("../exportImage")>();
  return {
    ...real,
    rasterFormats: () => ["png", "jpeg"],
    videoFormats: () => [{ ext: "webm", mime: "video/webm;codecs=vp9" }],
    exportImage: vi.fn(async () => new Blob(["x"])),
    recordOrbit: vi.fn(
      (_s: unknown, _st: unknown, o: { onProgress: (f: number) => void; signal: AbortSignal }) => {
        saved.recordings.push(o.signal);
        o.onProgress(0.25);
        return new Promise((_resolve, reject) =>
          o.signal.addEventListener("abort", () =>
            reject(new DOMException("cancelled", "AbortError")),
          ),
        );
      },
    ),
    saveFile: (_b: Blob, name: string) => saved.files.push(name),
  };
});

import { Stage3D } from "../Stage3D";

const scene = (canVector: boolean) =>
  ({
    background: "#fff",
    snapshot: vi.fn(),
    orbit: vi.fn(),
    vector: () => null,
    canVector,
    placedLabels: () => [],
  }) as unknown as ExportableScene;

function stage(exportable: ExportableScene) {
  render(
    <Stage3D
      wrapRef={{ current: null }}
      canvasRef={{ current: null }}
      labelsRef={{ current: null }}
      ready
      error={null}
      canvasLabel="graph"
      tip={null}
      legend={null}
      exportable={exportable}
      exportName="ontology"
      testId="x"
    />,
  );
}

afterEach(() => {
  saved.files.length = 0;
  saved.recordings.length = 0;
});

describe("a 3D view's Export control", () => {
  it("offers SVG only for a scene that draws flat", async () => {
    stage(scene(false));
    fireEvent.click(screen.getByTestId("x-export"));
    expect(await screen.findByTestId("x-export-png")).toBeInTheDocument();
    expect(screen.getByTestId("x-export-jpeg")).toBeInTheDocument();
    expect(screen.queryByTestId("x-export-svg")).toBeNull();
    expect(screen.getByTestId("x-record-webm")).toBeInTheDocument();
  });

  it("saves the chosen image under the view's name and the day", async () => {
    stage(scene(true));
    fireEvent.click(screen.getByTestId("x-export"));
    fireEvent.click(await screen.findByTestId("x-export-svg"));
    await waitFor(() => expect(saved.files).toHaveLength(1));
    expect(saved.files[0]).toMatch(/^ontology-\d{4}-\d{2}-\d{2}\.svg$/);
  });

  it("shows a recording's progress, and cancels it", async () => {
    stage(scene(true));
    fireEvent.click(screen.getByTestId("x-export"));
    fireEvent.click(await screen.findByTestId("x-record-webm"));
    expect(await screen.findByTestId("x-recording")).toHaveTextContent("25%");
    fireEvent.click(screen.getByTestId("x-recording-cancel"));
    await waitFor(() => expect(screen.queryByTestId("x-recording")).toBeNull());
    expect(saved.recordings[0].aborted).toBe(true);
    expect(saved.files).toHaveLength(0);
  });
});
