// Copyright (c) 2026 Kenneth Stott
// Canary: 73d0a708-e7d4-4d24-8c91-674c7034f2a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114: Polly's changes through the admin API, sent from the user's own session only after
// they confirm the change shown to them; a REST route the UI calls reached the same way.

import { afterEach, describe, expect, it, vi } from "vitest";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";

function context(confirmed: boolean, file: File | null = null): ClientToolContext {
  return {
    navigate: vi.fn(),
    confirm: vi.fn(async () => confirmed),
    runMutation: vi.fn(),
    presentChoice: vi.fn(),
    runAdminMutation: vi.fn(async () => ({ success: true, message: '{"createSource":{}}' })),
    chooseFile: vi.fn(async () => file),
    changed: vi.fn(),
  };
}

describe("admin_mutation", () => {
  it("shows the mutation and its summary, then sends it once confirmed", async () => {
    const ctx = context(true);
    const mutation = 'mutation { createSource(input: {id: "x"}) { success } }';
    const result = await executeClientTool(
      "admin_mutation",
      { mutation, variables: { a: 1 }, summary: "Create the source x." },
      ctx,
    );
    expect(ctx.confirm).toHaveBeenCalledWith({
      title: "Make this change?",
      message: "Create the source x.",
      detail: `${mutation}\n\n{\n  "a": 1\n}`,
    });
    expect(ctx.runAdminMutation).toHaveBeenCalledWith(mutation, { a: 1 });
    expect(result.success).toBe(true);
  });

  it("sends nothing when the user cancels", async () => {
    const ctx = context(false);
    const result = await executeClientTool(
      "admin_mutation",
      { mutation: "mutation { x }", summary: "Do x." },
      ctx,
    );
    expect(ctx.runAdminMutation).not.toHaveBeenCalled();
    expect(result).toEqual({ success: false, message: "Cancelled by the user." });
  });
});

describe("admin_request", () => {
  afterEach(() => vi.restoreAllMocks());

  it("runs a GET at once, from the user's session", async () => {
    const ctx = context(false);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response('[{"id": 7}]', { status: 200, statusText: "OK" }));
    const result = await executeClientTool(
      "admin_request",
      { method: "GET", path: "/admin/discover/candidates", summary: "List candidates." },
      ctx,
    );
    expect(ctx.confirm).not.toHaveBeenCalled();
    expect(fetchSpy).toHaveBeenCalledWith("/admin/discover/candidates", { method: "GET" });
    // A read changes nothing: no screen reads again.
    expect(ctx.changed).not.toHaveBeenCalled();
    expect(result).toEqual({ success: true, message: '200 OK\n[{"id": 7}]' });
  });

  it("confirms a change and sends its JSON body", async () => {
    const ctx = context(true);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("{}", { status: 200, statusText: "OK" }));
    await executeClientTool(
      "admin_request",
      {
        method: "POST",
        path: "/admin/discover/candidates/7/accept",
        body: { name: "orders_customer" },
        summary: "Accept the orders to customers relationship.",
      },
      ctx,
    );
    expect(ctx.confirm).toHaveBeenCalledWith(
      expect.objectContaining({
        message: "Accept the orders to customers relationship.",
        detail: 'POST /admin/discover/candidates/7/accept\n{\n  "name": "orders_customer"\n}',
      }),
    );
    expect(fetchSpy).toHaveBeenCalledWith("/admin/discover/candidates/7/accept", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: '{"name":"orders_customer"}',
    });
    // The change sent, the screens showing what it changed read again (pollyWrites.ts).
    expect(ctx.changed).toHaveBeenCalledWith("/admin/discover/candidates/7/accept");
  });

  it("reads the screens again after a change the route refused", async () => {
    const ctx = context(true);
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("no", { status: 409, statusText: "Conflict" }),
    );
    const result = await executeClientTool(
      "admin_request",
      {
        method: "PATCH",
        path: "/admin/glossary/terms/41",
        body: { definition: "x" },
        summary: "x",
      },
      ctx,
    );
    expect(result.success).toBe(false);
    expect(ctx.changed).toHaveBeenCalledWith("/admin/glossary/terms/41");
  });

  it("sends nothing when the user cancels a change", async () => {
    const ctx = context(false);
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const result = await executeClientTool(
      "admin_request",
      { method: "DELETE", path: "/admin/discover/candidates/rejected", summary: "Clear them." },
      ctx,
    );
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(result.message).toBe("Cancelled by the user.");
    expect(ctx.changed).not.toHaveBeenCalled();
  });

  it("asks the user for the file a route uploads and sends its bytes, named in the path", async () => {
    const file = new File(["data"], "my code.tar.gz");
    const ctx = context(true, file);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("{}", { status: 200, statusText: "OK" }));
    await executeClientTool(
      "admin_request",
      {
        method: "POST",
        path: "/admin/orgs/acme/code-uploads/archive?filename={filename}",
        upload: true,
        summary: "Upload the code archive.",
      },
      ctx,
    );
    expect(ctx.chooseFile).toHaveBeenCalledWith("Upload the code archive.");
    expect(fetchSpy).toHaveBeenCalledWith(
      "/admin/orgs/acme/code-uploads/archive?filename=my%20code.tar.gz",
      {
        method: "POST",
        headers: { "Content-Type": "application/octet-stream" },
        body: file,
      },
    );
  });

  it("sends nothing when the user chooses no file", async () => {
    const ctx = context(true, null);
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const result = await executeClientTool(
      "admin_request",
      { method: "POST", path: "/admin/x?filename={filename}", upload: true, summary: "Up." },
      ctx,
    );
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(result.message).toBe("Cancelled by the user: no file was chosen.");
  });

  // REQ-1984: code Polly wrote (upload_code), handed as its files and their zip archive.
  const handedCode = {
    method: "POST",
    path: "/admin/orgs/acme/code-uploads/archive?filename=code.zip",
    summary: "Upload the orders code source.",
    files: { "orders_source.py": "def orders(region):\n    return []\n" },
    archive: btoa("PK\u0003\u0004zip"),
  };

  it("shows the code it was handed, then sends its archive once confirmed", async () => {
    const ctx = context(true);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response('{"content_hash": "h"}', { status: 200, statusText: "OK" }));
    const result = await executeClientTool("admin_request", handedCode, ctx);
    expect(ctx.confirm).toHaveBeenCalledWith({
      title: "Make this change?",
      message: "Upload the orders code source.",
      detail:
        "POST /admin/orgs/acme/code-uploads/archive?filename=code.zip\n" +
        "--- orders_source.py ---\ndef orders(region):\n    return []\n",
    });
    expect(ctx.chooseFile).not.toHaveBeenCalled();
    const [path, init] = fetchSpy.mock.calls[0];
    expect(path).toBe("/admin/orgs/acme/code-uploads/archive?filename=code.zip");
    expect(init?.headers).toEqual({ "Content-Type": "application/zip" });
    expect(Array.from(init?.body as Uint8Array)).toEqual(
      Array.from("PK\u0003\u0004zip", (c) => c.charCodeAt(0)),
    );
    expect(result).toEqual({ success: true, message: '200 OK\n{"content_hash": "h"}' });
  });

  it("sends no handed code when the user cancels", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const result = await executeClientTool("admin_request", handedCode, context(false));
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(result.message).toBe("Cancelled by the user.");
  });

  it.each(["https://elsewhere.example/x", "//elsewhere.example/x", "admin/x"])(
    "refuses %s, which is not one of the app's own routes",
    async (path) => {
      const fetchSpy = vi.spyOn(globalThis, "fetch");
      const result = await executeClientTool(
        "admin_request",
        { method: "GET", path, summary: "x" },
        context(true),
      );
      expect(result.success).toBe(false);
      expect(fetchSpy).not.toHaveBeenCalled();
    },
  );
});
