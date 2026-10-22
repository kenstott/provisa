// Copyright (c) 2026 Kenneth Stott
// Canary: 98dc78cc-97b2-4d55-a9b8-8343bffc267f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Page, Request } from "@playwright/test";
import { test, expect, BACKEND_URL } from "./coverage";

/**
 * REQ-2114: Polly makes a change through the admin API the UI uses, as the chatting user.
 *
 * The chat stream is mocked -- it plays the turn in which Polly proposes the change -- and
 * everything after it is real: the confirm card the panel shows, the request the user's own
 * browser session sends once they confirm (the UI's GraphQL client for a mutation, the page's
 * fetch for a REST route), the real server's answer, and the open admin page refetching. A
 * cancel sends nothing.
 */

const ADMIN_GQL = `${BACKEND_URL}/admin/graphql`;

const CREATE = `mutation CreateRelationship($input: RelationshipInput!) {
  createRelationship(input: $input) { success message }
}`;

// A 1x1 PNG: the smallest image the logo route accepts.
const PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
  "base64",
);

function sse(events: object[]): string {
  return events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
}

/** The chat answers its first turn with ``pending`` -- the change Polly proposes -- and the
 * turn that resumes after the browser ran it with a closing line. */
async function polly(page: Page, pending: { name: string; input: object }) {
  await page.route("**/admin/mcp/chat/status", (route) =>
    route.fulfill({ json: { configured: true, reason: "" } }),
  );
  let turn = 0;
  await page.route("**/admin/mcp/chat", (route) => {
    turn += 1;
    const call = { id: "c1", ...pending };
    const events =
      turn === 1
        ? [
            {
              type: "awaiting_client_tools",
              assistant_content: [{ type: "tool_use", ...call }],
              server_tool_results: [],
              pending: [call],
            },
            { type: "done" },
          ]
        : [{ type: "text", text: "All done." }, { type: "done" }];
    return route.fulfill({ status: 200, contentType: "text/event-stream", body: sse(events) });
  });
}

async function ask(page: Page, question: string) {
  await page.getByTestId("chat-panel-toggle").click();
  const box = page
    .getByTestId("chat-panel")
    .getByPlaceholder("Ask me a question, or tell me what to do.");
  await box.fill(question);
  await page.getByTestId("chat-panel-send").click();
}

async function relationshipExists(
  request: { post: (url: string, opts: object) => Promise<{ json: () => Promise<unknown> }> },
  id: string,
): Promise<boolean> {
  const answer = (await (
    await request.post(ADMIN_GQL, { data: { query: "{ relationships { id } }" } })
  ).json()) as { data: { relationships: { id: string }[] } };
  return answer.data.relationships.some((r) => r.id === id);
}

test.describe("Polly changes things through the admin API, as the user", () => {
  test("a mutation is shown, sent by the user's session once confirmed, and the page refetches", async ({
    page,
    request,
  }) => {
    const suffix = Date.now().toString(36);
    const id = `polly-e2e-${suffix}`;
    const alias = `POLLY_E2E_${suffix.toUpperCase()}`;
    const input = {
      id,
      sourceTableId: "fact_pet_inquiries",
      targetTableId: "pets",
      sourceColumn: "pet_id",
      targetColumn: "id",
      cardinality: "many-to-one",
      alias,
    };
    await polly(page, {
      name: "admin_mutation",
      input: {
        mutation: CREATE,
        variables: { input },
        summary: "Create the relationship from fact_pet_inquiries.pet_id to pets.id.",
      },
    });
    try {
      await page.goto(`/relationships?search=${alias}`);
      // The Relationships page's own row for it, which the chat's transcript cannot match.
      const row = page.locator(`[data-tour-relationship="${alias}"]`);
      await expect(row).toHaveCount(0);

      await ask(page, "Create any relationships to the pets table");
      const detail = page.getByTestId("chat-confirm-detail");
      await expect(detail).toBeVisible({ timeout: 30000 });
      await expect(detail).toContainText("createRelationship");
      await expect(detail).toContainText(alias);

      // Confirm: the user's own browser session sends the mutation the UI's GraphQL client sends.
      const sent = page.waitForRequest(
        (r: Request) =>
          r.url().endsWith("/admin/graphql") && (r.postData() ?? "").includes("CreateRelationship"),
      );
      await page.getByRole("button", { name: "Confirm" }).click();
      const response = await (await sent).response();
      expect(response?.ok()).toBe(true);
      expect(await response?.json()).toMatchObject({
        data: { createRelationship: { success: true } },
      });

      // The open page refetches: the new relationship appears without a reload.
      await expect(row).toBeVisible({ timeout: 30000 });
      const chat = page.getByTestId("chat-panel");
      await expect(chat.getByText("Confirmed — go ahead.")).toBeVisible();
      await expect(chat.getByText("All done.")).toBeVisible({ timeout: 30000 });
      expect(await relationshipExists(request, id)).toBe(true);
    } finally {
      await request.post(ADMIN_GQL, {
        data: { query: `mutation { deleteRelationship(id: "${id}") { success } }` },
      });
    }
  });

  test("a cancelled mutation sends nothing", async ({ page, request }) => {
    const id = `polly-e2e-cancel-${Date.now().toString(36)}`;
    await polly(page, {
      name: "admin_mutation",
      input: {
        mutation: CREATE,
        variables: {
          input: {
            id,
            sourceTableId: "fact_pet_inquiries",
            targetTableId: "pets",
            sourceColumn: "pet_id",
            targetColumn: "id",
            cardinality: "many-to-one",
          },
        },
        summary: "Create the relationship from fact_pet_inquiries.pet_id to pets.id.",
      },
    });
    const mutations: string[] = [];
    page.on("request", (r) => {
      if (
        r.url().endsWith("/admin/graphql") &&
        (r.postData() ?? "").includes("CreateRelationship")
      ) {
        mutations.push(r.url());
      }
    });
    await page.goto("/relationships");
    await ask(page, "Create any relationships to the pets table");
    await expect(page.getByTestId("chat-confirm-detail")).toBeVisible({ timeout: 30000 });
    await page.getByRole("button", { name: "Cancel" }).click();

    const chat = page.getByTestId("chat-panel");
    await expect(chat.getByText("Cancelled.")).toBeVisible();
    await expect(chat.getByText("All done.")).toBeVisible({ timeout: 30000 });
    expect(mutations).toEqual([]);
    expect(await relationshipExists(request, id)).toBe(false);
  });

  test("an upload is shown, its file chosen in the chat, and sent as its bytes by the user's session", async ({
    page,
    request,
  }) => {
    await page.goto("/sources");
    const me = (await page.evaluate(() => fetch("/auth/me").then((r) => r.json()))) as {
      active_org_id: string | null;
    };
    expect(me.active_org_id, "the session names the org it acts in").toBeTruthy();
    const org = me.active_org_id as string;
    await polly(page, {
      name: "admin_request",
      input: {
        method: "PUT",
        path: `/admin/orgs/${org}/branding/logo`,
        upload: true,
        summary: "Set the organization's logo to the image you choose.",
      },
    });
    try {
      await ask(page, "Set our logo");
      const detail = page.getByTestId("chat-confirm-detail");
      await expect(detail).toBeVisible({ timeout: 30000 });
      await expect(detail).toContainText(`PUT /admin/orgs/${org}/branding/logo`);
      await page.getByRole("button", { name: "Confirm" }).click();

      // The file is chosen in the chat, then sent as its own bytes, typed as it is.
      await page.locator('input[type="file"]').setInputFiles({
        name: "logo.png",
        mimeType: "image/png",
        buffer: PNG,
      });
      const sent = page.waitForRequest(
        (r: Request) =>
          r.method() === "PUT" && r.url().endsWith(`/admin/orgs/${org}/branding/logo`),
      );
      await page.getByTestId("chat-choose-file-send").click();
      const upload = await sent;
      expect(upload.headers()["content-type"]).toBe("image/png");
      expect(upload.postDataBuffer()?.equals(PNG)).toBe(true);
      const response = await upload.response();
      expect(response?.ok()).toBe(true);
      expect(await response?.json()).toMatchObject({
        logo_media_type: "image/png",
        bytes: PNG.length,
      });
      await expect(page.getByTestId("chat-panel").getByText("All done.")).toBeVisible({
        timeout: 30000,
      });
    } finally {
      await request.delete(`${BACKEND_URL}/admin/orgs/${org}/branding/logo`);
    }
  });

  test("a cancelled upload sends nothing", async ({ page }) => {
    await page.goto("/sources");
    const uploads: string[] = [];
    page.on("request", (r) => {
      if (r.url().includes("/branding/logo") && r.method() === "PUT") uploads.push(r.url());
    });
    await polly(page, {
      name: "admin_request",
      input: {
        method: "PUT",
        path: "/admin/orgs/any/branding/logo",
        upload: true,
        summary: "Set the organization's logo.",
      },
    });
    await ask(page, "Set our logo");
    await expect(page.getByTestId("chat-confirm-detail")).toBeVisible({ timeout: 30000 });
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.getByTestId("chat-panel").getByText("All done.")).toBeVisible({
      timeout: 30000,
    });
    expect(uploads).toEqual([]);
  });
});
