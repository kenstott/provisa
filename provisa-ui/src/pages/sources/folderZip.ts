// Copyright (c) 2026 Kenneth Stott
// Canary: d761a2f4-c924-430a-a994-a09a4a694508
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2121: a code source's local folder, zipped in the browser as its archive, named after the
// folder, without what the developer's tools make. A browser that can keep the folder's handle
// (the File System Access API: Chrome, Edge) has it read again at Save; another is asked for the
// folder again. The zip stores its files as they are (no compression): the server unpacks it as
// it unpacks any uploaded archive.

/** The folders left out of a folder's zip, wherever they are. */
export const FOLDER_SKIPPED = new Set(["node_modules", ".venv", "__pycache__", ".git"]);

export interface FolderFile {
  /** Its path under the folder, with "/" between the parts. */
  path: string;
  data: Uint8Array;
}

/** A folder handle as the File System Access API gives it (typed here: no lib carries it yet). */
export interface FolderHandle {
  name: string;
  entries(): AsyncIterable<[string, FolderHandle | { kind: "file"; getFile(): Promise<File> }]>;
  kind: "directory";
}

const skipped = (path: string) => path.split("/").some((part) => FOLDER_SKIPPED.has(part));

/** The folder's files through its kept handle. */
export async function filesOfHandle(handle: FolderHandle): Promise<FolderFile[]> {
  const out: FolderFile[] = [];
  const walk = async (dir: FolderHandle, prefix: string) => {
    for await (const [name, entry] of dir.entries()) {
      if (FOLDER_SKIPPED.has(name)) continue;
      const path = prefix ? `${prefix}/${name}` : name;
      if (entry.kind === "directory") await walk(entry as FolderHandle, path);
      else
        out.push({
          path,
          data: new Uint8Array(
            await (await (entry as { getFile(): Promise<File> }).getFile()).arrayBuffer(),
          ),
        });
    }
  };
  await walk(handle, "");
  return out;
}

/** The folder's files as a folder input gives them (each named by its webkitRelativePath, the
 * folder's own name first); and the folder's name. */
export async function filesOfInput(files: File[]): Promise<{ name: string; files: FolderFile[] }> {
  const name = (files[0]?.webkitRelativePath ?? "").split("/")[0] ?? "";
  const out: FolderFile[] = [];
  for (const file of files) {
    const path = file.webkitRelativePath.split("/").slice(1).join("/");
    if (!path || skipped(path)) continue;
    out.push({ path, data: new Uint8Array(await file.arrayBuffer()) });
  }
  return { name, files: out };
}

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32(data: Uint8Array): number {
  let c = 0xffffffff;
  for (let i = 0; i < data.length; i++) c = CRC_TABLE[(c ^ data[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

/** The folder as a zip file named after it, its files under one top-level directory of its name
 * (the server takes a zip's single top-level directory as the code's root). */
export function zipFolder(name: string, files: FolderFile[]): File {
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];
  const central: Uint8Array[] = [];
  let offset = 0;
  for (const file of [...files].sort((a, b) => (a.path < b.path ? -1 : 1))) {
    const path = encoder.encode(`${name}/${file.path}`);
    const crc = crc32(file.data);
    const local = new DataView(new ArrayBuffer(30));
    local.setUint32(0, 0x04034b50, true);
    local.setUint16(4, 20, true);
    local.setUint16(6, 0x0800, true); // names are UTF-8
    local.setUint32(14, crc, true);
    local.setUint32(18, file.data.length, true);
    local.setUint32(22, file.data.length, true);
    local.setUint16(26, path.length, true);
    parts.push(new Uint8Array(local.buffer), path, file.data);
    const entry = new DataView(new ArrayBuffer(46));
    entry.setUint32(0, 0x02014b50, true);
    entry.setUint16(4, 20, true);
    entry.setUint16(6, 20, true);
    entry.setUint16(8, 0x0800, true);
    entry.setUint32(16, crc, true);
    entry.setUint32(20, file.data.length, true);
    entry.setUint32(24, file.data.length, true);
    entry.setUint16(28, path.length, true);
    entry.setUint32(42, offset, true);
    central.push(new Uint8Array(entry.buffer), path);
    offset += 30 + path.length + file.data.length;
  }
  const size = central.reduce((n, p) => n + p.length, 0);
  const end = new DataView(new ArrayBuffer(22));
  end.setUint32(0, 0x06054b50, true);
  end.setUint16(8, files.length, true);
  end.setUint16(10, files.length, true);
  end.setUint32(12, size, true);
  end.setUint32(16, offset, true);
  return new File([...parts, ...central, new Uint8Array(end.buffer)] as BlobPart[], `${name}.zip`, {
    type: "application/zip",
  });
}

/** The folders kept this session, by the source they are the code of (NEW_SOURCE for one being
 * created). */
export const NEW_SOURCE = "";
const held = new Map<string, FolderHandle>();

export const canKeepFolders = () =>
  typeof (window as unknown as { showDirectoryPicker?: unknown }).showDirectoryPicker ===
  "function";

export async function pickFolder(key: string): Promise<FolderHandle> {
  const picker = (window as unknown as { showDirectoryPicker: () => Promise<FolderHandle> })
    .showDirectoryPicker;
  const handle = await picker();
  held.set(key, handle);
  return handle;
}

export const heldFolder = (key: string) => held.get(key);

/** Save's part (REQ-2121): the folder kept for ``key`` zipped and uploaded again, its hash and
 * name in ``fields``; ``fields`` as they are when no folder is kept. */
export async function resendHeldFolder(
  key: string,
  fields: Record<string, string>,
  upload: (file: File) => Promise<string>,
): Promise<Record<string, string>> {
  const handle = held.get(key);
  if (!handle || fields.code_origin !== "upload") return fields;
  const file = zipFolder(handle.name, await filesOfHandle(handle));
  return { ...fields, code_upload: await upload(file), code_upload_filename: file.name };
}

/** A source just created keeps, under its id, the folder chosen while it was being created. */
export function keepFolderAs(id: string): void {
  const handle = held.get(NEW_SOURCE);
  if (!handle) return;
  held.delete(NEW_SOURCE);
  held.set(id, handle);
}
