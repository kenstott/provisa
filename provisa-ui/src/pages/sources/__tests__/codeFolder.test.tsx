// REQ-2121: a code source's code from a folder on the developer's machine (Choose folder zips it
// in the browser, without node_modules, .venv, __pycache__ and .git, named after the folder; Save
// sends a folder the browser keeps again), from a directory on the server on a single-tenant
// install, and Open in VS Code.

import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, renderInForm, screen, waitFor } from "../../../test-utils/render";

const auth = vi.hoisted(() => ({ activeOrgId: "acme" as string | null, multitenancy: false }));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
const uploaded = vi.hoisted(() => [] as File[]);
vi.mock("../../../api/codeUploads", () => ({
  uploadCodeArchive: vi.fn(async (_org: string, file: File) => {
    uploaded.push(file);
    return `hash-${uploaded.length}`;
  }),
  uploadLockfile: vi.fn(),
}));
vi.mock("../../../api/secrets", () => ({
  fetchSecrets: vi.fn().mockResolvedValue({ provider: {}, secrets: [] }),
}));

import { FormRead } from "../../../components/form/FormLayout";
import { CodeSourceFields } from "../CodeSourceFields";
import {
  codeFieldsFromMapping,
  codeMappingJson,
  missingCodeFields,
  vscodeLink,
} from "../codeSource";
import {
  NEW_SOURCE,
  crc32,
  filesOfInput,
  resendHeldFolder,
  zipFolder,
  type FolderHandle,
} from "../folderZip";

afterEach(() => {
  uploaded.length = 0;
  auth.multitenancy = false;
  delete (window as unknown as { showDirectoryPicker?: unknown }).showDirectoryPicker;
});

/** The names a zip's central directory lists, and whether each entry's CRC is its bytes'. */
async function zipEntries(file: File): Promise<string[]> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  const view = new DataView(bytes.buffer);
  const end = bytes.length - 22;
  expect(view.getUint32(end, true)).toBe(0x06054b50);
  const count = view.getUint16(end + 10, true);
  let at = view.getUint32(end + 16, true);
  const names: string[] = [];
  for (let i = 0; i < count; i++) {
    expect(view.getUint32(at, true)).toBe(0x02014b50);
    const length = view.getUint16(at + 28, true);
    const local = view.getUint32(at + 42, true);
    const size = view.getUint32(at + 20, true);
    const nameLength = view.getUint16(local + 26, true);
    const data = bytes.slice(local + 30 + nameLength, local + 30 + nameLength + size);
    expect(crc32(data)).toBe(view.getUint32(at + 16, true));
    names.push(new TextDecoder().decode(bytes.slice(at + 46, at + 46 + length)));
    at += 46 + length;
  }
  return names;
}

const text = (s: string) => new TextEncoder().encode(s);

/** A folder as the File System Access API hands it over. */
function folder(name: string, files: Record<string, string>): FolderHandle {
  const tree: Record<string, unknown> = {};
  for (const [path, body] of Object.entries(files)) {
    const parts = path.split("/");
    let at = tree;
    for (const part of parts.slice(0, -1)) at = (at[part] ??= {}) as Record<string, unknown>;
    at[parts.at(-1)!] = body;
  }
  const handle = (dirName: string, node: Record<string, unknown>): FolderHandle => ({
    name: dirName,
    kind: "directory",
    async *entries() {
      for (const [n, v] of Object.entries(node)) {
        yield [
          n,
          typeof v === "string"
            ? { kind: "file" as const, getFile: async () => new File([v], n) }
            : handle(n, v as Record<string, unknown>),
        ];
      }
    },
  });
  return handle(name, tree);
}

describe("a folder zipped in the browser", () => {
  it("is named after the folder, its files under it, without what the tools make", async () => {
    const zip = zipFolder("rates", [
      { path: "pyproject.toml", data: text("[project]") },
      { path: "rates/__init__.py", data: text("def rate(): return 1") },
    ]);
    expect(zip.name).toBe("rates.zip");
    expect(await zipEntries(zip)).toEqual(["rates/pyproject.toml", "rates/rates/__init__.py"]);
    expect(crc32(text("123456789"))).toBe(0xcbf43926);
  });

  it("leaves out node_modules, .venv, __pycache__ and .git from a folder input's files", async () => {
    const file = (path: string) => {
      const f = new File(["x"], path.split("/").at(-1)!);
      Object.defineProperty(f, "webkitRelativePath", { value: path });
      return f;
    };
    const picked = await filesOfInput([
      file("rates/rates.py"),
      file("rates/.venv/bin/python"),
      file("rates/node_modules/a/index.js"),
      file("rates/pkg/__pycache__/x.pyc"),
      file("rates/.git/HEAD"),
    ]);
    expect(picked.name).toBe("rates");
    expect(picked.files.map((f) => f.path)).toEqual(["rates.py"]);
  });
});

function Harness({
  onFields,
  sourceId,
}: {
  onFields?: (f: Record<string, string>) => void;
  sourceId?: string;
}) {
  const [fields, setFields] = useState<Record<string, string>>({ code_origin: "upload" });
  return (
    <CodeSourceFields
      type="python"
      fields={fields}
      sourceId={sourceId}
      setFields={(f) => {
        onFields?.(f);
        setFields(f);
      }}
    />
  );
}

describe("Choose folder", () => {
  it("uploads the folder kept by the browser, and Save zips and sends it again", async () => {
    const handle = folder("rates", {
      "rates.py": "def rate(): return 1",
      ".venv/bin/python": "elf",
      ".git/HEAD": "ref",
    });
    (
      window as unknown as { showDirectoryPicker: () => Promise<FolderHandle> }
    ).showDirectoryPicker = async () => handle;
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness onFields={(f) => seen.push(f)} />);
    fireEvent.click(screen.getByTestId("code-choose-folder"));
    await waitFor(() => expect(seen.at(-1)?.code_upload).toBe("hash-1"));
    expect(seen.at(-1)?.code_upload_filename).toBe("rates.zip");
    expect(await zipEntries(uploaded[0])).toEqual(["rates/rates.py"]);
    expect(screen.getByTestId("code-folder-note").textContent).toBe(
      "Save zips this folder and sends it again.",
    );
    const saved = await resendHeldFolder(NEW_SOURCE, seen.at(-1)!, async (file) => {
      uploaded.push(file);
      return "hash-again";
    });
    expect(saved.code_upload).toBe("hash-again");
    expect(saved.code_upload_filename).toBe("rates.zip");
  });

  it("in a browser that keeps no folder, says to choose it again", async () => {
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness onFields={(f) => seen.push(f)} sourceId="other" />);
    const input = screen.getByTestId("code-folder-input") as HTMLInputElement;
    const f = new File(["x"], "rates.py");
    Object.defineProperty(f, "webkitRelativePath", { value: "rates/rates.py" });
    fireEvent.change(input, { target: { files: [f] } });
    await waitFor(() => expect(seen.at(-1)?.code_upload_filename).toBe("rates.zip"));
    expect(screen.getByTestId("code-folder-note").textContent).toBe(
      "This browser cannot keep the folder: choose it again to send a newer copy.",
    );
    // Nothing is kept for it, so Save sends nothing again.
    expect(await resendHeldFolder("other", seen.at(-1)!, async () => "never")).toBe(seen.at(-1));
  });
});

describe("a directory on the server", () => {
  it("is a fourth origin on a single-tenant install, and none on a multi-tenant one", async () => {
    renderInForm(<Harness />);
    fireEvent.click(screen.getByTestId("code-origin"));
    expect(await screen.findByRole("option", { name: "Directory on the server" })).toBeTruthy();
  });

  it("is not offered on a multi-tenant deployment", async () => {
    auth.multitenancy = true;
    renderInForm(<Harness />);
    fireEvent.click(screen.getByTestId("code-origin"));
    await screen.findByRole("option", { name: "Upload" });
    expect(screen.queryByRole("option", { name: "Directory on the server" })).toBeNull();
  });

  it("is kept in the mapping as its path, and needs one", () => {
    const fields = { code_origin: "directory", code_directory: " /srv/code/rates " };
    const mapping = JSON.parse(codeMappingJson("python", fields));
    expect(mapping.code).toEqual({ kind: "directory", path: "/srv/code/rates" });
    expect(codeFieldsFromMapping(JSON.stringify(mapping))).toMatchObject({
      code_origin: "directory",
      code_directory: "/srv/code/rates",
    });
    expect(missingCodeFields({ code_origin: "directory" })).toEqual(["code_directory"]);
  });
});

describe("Open in VS Code", () => {
  it("opens a server directory in the installed VS Code and a Git origin on vscode.dev", () => {
    expect(vscodeLink({ code_origin: "directory", code_directory: "/srv/code/rates" })).toBe(
      "vscode://file/srv/code/rates",
    );
    expect(
      vscodeLink({
        code_origin: "git",
        code_repository: "https://github.com/acme/rates.git",
        code_commit: "4f2a9c1e",
        code_subdirectory: "packages/rates",
      }),
    ).toBe("https://vscode.dev/github/acme/rates/tree/4f2a9c1e/packages/rates");
    expect(vscodeLink({ code_origin: "package", code_package: "acme-rates" })).toBeNull();
    expect(vscodeLink({ code_origin: "upload", code_upload: "h" })).toBeNull();
  });

  it("is offered in the read view as in the form", () => {
    render(
      <FormRead testId="read">
        <CodeSourceFields
          type="python"
          fields={codeFieldsFromMapping(
            JSON.stringify({
              runtime: "python",
              code: { kind: "directory", path: "/srv/code/rates" },
            }),
          )}
          setFields={() => {}}
        />
      </FormRead>,
    );
    const link = screen.getByTestId("code-open-vscode") as HTMLAnchorElement;
    expect(link.getAttribute("href")).toBe("vscode://file/srv/code/rates");
    expect(link.textContent).toBe("Open in VS Code");
  });
});
