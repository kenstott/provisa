// Copyright (c) 2026 Kenneth Stott
// Canary: 3d7c087b-41f9-4ab2-9a6f-87faeb3f4efd
//
// This source code is licensed under the Business Source License 1.1

// REQ-1997: a file-based source's file, given by upload. The file is sent as the request's body
// (streamed by the browser, never read into memory here) and kept in the org's folder under
// files/<source id>/; the answer is the path the source then names.

import { requestFailed, serverMessage } from "../../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** The source types whose path names one file, which an upload can be. */
export const UPLOAD_TYPES = new Set(["sqlite", "duckdb", "csv", "parquet"]);

/** REQ-2113: the source types whose one file may be downloaded into the organization's folder. */
export const DOWNLOAD_TYPES = new Set(["csv", "parquet"]);

/** What a new source's form asks of its table (REQ-2113): set up with it as a draft -- on by
 * default, a single-file source having one table -- in the domain picked, else the only or
 * first one. Nothing for a source whose file is not one CSV or Parquet file. */
export function draftTable(
  form: { type: string; registerTableDraft?: boolean; tableDomainId?: string | null },
  domains: { id: string }[],
): { registerTableDraft: boolean; tableDomainId: string | null } {
  if (!DOWNLOAD_TYPES.has(form.type)) return { registerTableDraft: false, tableDomainId: null };
  return {
    registerTableDraft: form.registerTableDraft ?? true,
    tableDomainId: form.tableDomainId ?? domains[0]?.id ?? null,
  };
}

/** The file databases, which a location elsewhere attaches read-only. */
export const FILE_DATABASES = new Set(["sqlite", "duckdb"]);

/** Where the form keeps a file source's location: a DuckDB file in `database`, the rest in
 * `path` (SourcesPage sends the DuckDB one as the path). */
export function fileLocationOf(form: { type: string; path: string; database: string }): string {
  return form.type === "duckdb" ? form.database : form.path;
}

const escaped = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** Whether `path` is the file `sourceId` was given by upload: `{files}/<source>/<name>`, as an
 * upload's path is kept (the org's token, never the file's physical location), or the absolute
 * form an upload saved before was kept in. */
export function isUploadPath(path: string | null | undefined, sourceId: string): boolean {
  if (!path || !sourceId) return false;
  const id = escaped(sourceId);
  return (
    new RegExp(`^\\{files\\}/${id}/[^/]+$`).test(path) ||
    new RegExp(`/orgs/[^/]+/files/${id}/[^/]+$`).test(path)
  );
}

/** A file name the server keeps: letters, digits, '.', '_' and '-' (others become '_'). */
export function uploadName(name: string): string {
  const safe = name.replace(/[^A-Za-z0-9._-]/g, "_").replace(/^[^A-Za-z0-9]+/, "");
  return safe || "file";
}

/** Send `file` as `sourceId`'s upload; `onProgress` is told the share sent, 0 to 1. */
export function uploadSourceFile(
  sourceId: string,
  type: string,
  file: File,
  onProgress: (share: number) => void,
): Promise<{ path: string; bytes: number }> {
  const query = new URLSearchParams({ name: uploadName(file.name), type });
  const url = `${API_BASE}/admin/sources/${encodeURIComponent(sourceId)}/file?${query}`;
  return new Promise((resolve, reject) => {
    // XMLHttpRequest, not fetch: only it reports how much of a body has been sent.
    const request = new XMLHttpRequest();
    request.open("PUT", url);
    request.setRequestHeader("Content-Type", "application/octet-stream");
    request.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(event.loaded / event.total);
    };
    request.onload = () => {
      const said = request.responseText ? JSON.parse(request.responseText) : {};
      if (request.status >= 200 && request.status < 300) {
        resolve(said as { path: string; bytes: number });
      } else {
        reject(new Error(serverMessage(said, requestFailed("uploadSourceFile", request.status))));
      }
    };
    request.onerror = () => reject(new Error(requestFailed("uploadSourceFile", "network")));
    request.send(file);
  });
}
