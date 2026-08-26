// Copyright (c) 2026 Kenneth Stott
// Canary: 6d8c1a37-24bf-4e0a-9c85-1f7b3e0a5d92
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The model as a downloadable workbook (REQ-1592). Download only: the endpoint has no POST twin,
// so this module has no upload counterpart either.

import { requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// The server names the file (org id and all). It is the same server that produced the bytes, so a
// client-invented name could disagree with the workbook's own Report sheet.
function filenameFrom(disposition: string): string {
  const match = /filename="([^"]+)"/.exec(disposition);
  if (!match) throw new Error(`Model report: unreadable content-disposition "${disposition}"`);
  return match[1];
}

/**
 * Fetch `GET /admin/report.xlsx` and hand the workbook to the browser's downloader.
 *
 * `domains` narrows exactly as the page's domain filter does — repeated `domains=` parameters
 * rather than one comma-joined string, because the no-domain domain's id IS the empty string.
 * Omit it to take the caller's full authority.
 */
export async function downloadModelReport(domains?: string[]): Promise<void> {
  const query = domains
    ? `?${domains.map((d) => `domains=${encodeURIComponent(d)}`).join("&")}`
    : "";
  const resp = await fetch(`${API_BASE}/admin/report.xlsx${query}`);
  if (!resp.ok) throw new Error(requestFailed("Model report download", resp.status));
  const disposition = resp.headers.get("content-disposition");
  if (!disposition) throw new Error("Model report: response carried no content-disposition");
  const blob = await resp.blob();
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filenameFrom(disposition);
  anchor.click();
  URL.revokeObjectURL(url);
}
