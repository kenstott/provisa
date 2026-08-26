// Copyright (c) 2026 Kenneth Stott
// Canary: 3b5f27ac-98d1-4a6e-b0c2-7e14d9f6a058
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1592: the workbook download — the request the button issues and the file the browser gets.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { downloadModelReport } from "../api/modelReport";

const XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

// jsdom's Blob and the fetch Response implementation come from different realms, so a real
// `new Response(blob)` throws here. The download path only reads ok/headers/blob().
function xlsxResponse(
  disposition: string | null = 'attachment; filename="provisa-model-acme.xlsx"',
) {
  return {
    ok: true,
    status: 200,
    headers: new Headers(disposition ? { "content-disposition": disposition } : {}),
    blob: async () => new Blob(["PK"], { type: XLSX }),
  } as unknown as Response;
}

let clicked: HTMLAnchorElement[] = [];
let createdUrls: string[] = [];
let revokedUrls: string[] = [];

beforeEach(() => {
  clicked = [];
  createdUrls = [];
  revokedUrls = [];
  let seq = 0;
  URL.createObjectURL = vi.fn(() => {
    const url = `blob:model-report-${(seq += 1)}`;
    createdUrls.push(url);
    return url;
  });
  URL.revokeObjectURL = vi.fn((url: string) => {
    revokedUrls.push(url);
  });
  // jsdom's anchor click would try to navigate; record the element instead.
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
    this: HTMLAnchorElement,
  ) {
    clicked.push(this);
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("downloadModelReport", () => {
  it("downloads under the filename the server chose", async () => {
    const fetchMock = vi.fn(async () => xlsxResponse());
    vi.stubGlobal("fetch", fetchMock);

    await downloadModelReport();

    expect(fetchMock).toHaveBeenCalledWith("/admin/report.xlsx");
    expect(clicked).toHaveLength(1);
    expect(clicked[0].download).toBe("provisa-model-acme.xlsx");
    expect(clicked[0].href).toBe(createdUrls[0]);
    expect(revokedUrls).toEqual(createdUrls);
  });

  it("sends one repeated domains parameter per selected domain", async () => {
    const fetchMock = vi.fn(async () => xlsxResponse());
    vi.stubGlobal("fetch", fetchMock);

    // The no-domain domain's id IS the empty string, so it must survive the query build.
    await downloadModelReport(["sales", "", "pet store"]);

    expect(fetchMock).toHaveBeenCalledWith(
      "/admin/report.xlsx?domains=sales&domains=&domains=pet%20store",
    );
  });

  it("raises rather than downloading when the server refuses", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: false, status: 403 }) as unknown as Response),
    );

    await expect(downloadModelReport()).rejects.toThrow(/403/);
    expect(clicked).toHaveLength(0);
  });

  it("raises when the response carries no filename", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => xlsxResponse(null)),
    );

    await expect(downloadModelReport()).rejects.toThrow(/content-disposition/);
    expect(clicked).toHaveLength(0);
  });
});
