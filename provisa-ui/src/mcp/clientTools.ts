// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8f1a52-9c6d-4b70-8a1f-5d2e7c9b0a34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { NavigateFunction } from "react-router-dom";
import { awaitRun } from "../dataScience/runReports";
import { readScreen } from "./screenState";
import { parseUiActions, runUiActions } from "./uiActions";

/** REQ-2115: how long each of Polly's steps on a screen lets itself be seen, and how long a step
 * waits for its element while the screen mounts. */
const UI_STEP_PACE_MS = 700;
const UI_ELEMENT_WAIT_MS = 10_000;

/** REQ-2071: Explore's Python and Notebook tabs, whose runs report back to the model. */
const RUN_ROUTES: Record<string, string> = {
  "/python": "Ran the code in the Python tab.",
  "/notebook": "Ran the cells in the Notebook tab.",
};
/** How long a Python run Polly asked for may take before the tool answers without it. */
const PYTHON_RUN_TIMEOUT_MS = 180_000;
let pollyRuns = 0;

/**
 * Client-executed MCP chat tools (REQ-1795) — the browser-only half of the tool set
 * provisa/api/mcp/chat.py declares. The server never runs these; when the model calls one, the
 * chat loop pauses (`awaiting_client_tools`) and the frontend (useMcpChat.ts) calls
 * `executeClientTool` here, then resumes the loop with the result.
 *
 * This module never imports the Apollo client directly (repo lint rule: only App.tsx may — every
 * other caller goes through a hook). `runMutation` is supplied by ChatPanel.tsx, which owns the
 * actual `useMutation` calls, so every mutation dispatched here rides the user's own Apollo
 * session — the SAME authorization (capability checks, domain gates, RLS, the REQ-434
 * creation-request queue for under-privileged callers) a manual click would hit applies
 * unchanged, no new privilege surface. `confirm` similarly defers the actual dialog to ChatPanel.
 */

/** REQ-1810: what present_choice asks for and what it resolves with. */
export type ChoiceMode = "single" | "multi" | "yes_no";
export type ChoiceAnswer = string | string[] | boolean;

export interface PresentChoiceRequest {
  question: string;
  mode: ChoiceMode;
  options?: string[];
}

/** What a confirmation shows: a question, and optionally the change itself (REQ-2114). */
export interface ConfirmRequest {
  title: string;
  message: string;
  detail?: string;
}

export interface ClientToolContext {
  navigate: NavigateFunction;
  confirm: (opts: ConfirmRequest) => Promise<boolean>;
  runMutation: (name: string, variables: Record<string, unknown>) => Promise<ClientToolResult>;
  // REQ-2114: any admin mutation, sent by the user's own Apollo session; and a file the user
  // chooses for a route that uploads one (null when they cancel).
  runAdminMutation: (
    mutation: string,
    variables: Record<string, unknown>,
  ) => Promise<ClientToolResult>;
  chooseFile: (prompt: string) => Promise<File | null>;
  // REQ-1810: renders a multiple-choice / checklist / yes-no widget and resolves with the user's
  // answer — the same pause-the-loop-and-wait pattern `confirm` already uses for refresh_mv.
  presentChoice: (req: PresentChoiceRequest) => Promise<ChoiceAnswer>;
}

export interface ClientToolResult {
  success: boolean;
  message: string;
  // REQ-1810: present_choice's actual answer, read by the model from the tool_result content —
  // every other client tool leaves this unset.
  selected?: ChoiceAnswer;
}

export const CLIENT_TOOL_NAMES = new Set([
  "navigate",
  "refresh_mv",
  "present_choice",
  "admin_mutation",
  "admin_request",
  "read_screen",
]);

/** How much of a REST route's answer, or of the screen, goes back to the model. */
const ANSWER_LIMIT = 4000;

/** REQ-2114: one of the UI's admin REST routes, called from the user's session -- the global
 * fetch interceptor attaches their sign-in and acting roles, as for any page's call. */
async function adminRequest(
  input: Record<string, unknown>,
  ctx: ClientToolContext,
): Promise<ClientToolResult> {
  const method = String(input.method ?? "GET").toUpperCase();
  const path = String(input.path ?? "");
  const summary = String(input.summary ?? "");
  // The app's own routes only: a path from the root, never another origin ("//host/...").
  if (!path.startsWith("/") || path.startsWith("//"))
    return { success: false, message: "admin_request: 'path' must start with /" };
  const body =
    input.body && typeof input.body === "object"
      ? (input.body as Record<string, unknown>)
      : undefined;
  const upload = input.upload === true;
  const init: RequestInit = { method };
  if (method !== "GET") {
    const ok = await ctx.confirm({
      title: "Make this change?",
      message: summary,
      detail: [`${method} ${path}`, body ? JSON.stringify(body, null, 2) : ""]
        .filter(Boolean)
        .join("\n"),
    });
    if (!ok) return { success: false, message: "Cancelled by the user." };
    if (upload) {
      // The app's upload routes take the file's own bytes as the body, as the UI's uploads send
      // them; a route that names the file takes it in the path, where {filename} stands for it.
      const file = await ctx.chooseFile(summary);
      if (!file) return { success: false, message: "Cancelled by the user: no file was chosen." };
      // Typed as the file is, as a page's own upload sends it (an image route reads its kind
      // from Content-Type); a file the browser cannot type goes as plain bytes.
      init.headers = { "Content-Type": file.type || "application/octet-stream" };
      init.body = file;
      return send(path.replaceAll("{filename}", encodeURIComponent(file.name)), init);
    } else if (body) {
      init.headers = { "Content-Type": "application/json" };
      init.body = JSON.stringify(body);
    }
  }
  return send(path, init);
}

async function send(path: string, init: RequestInit): Promise<ClientToolResult> {
  const res = await fetch(path, init);
  const text = await res.text();
  return {
    success: res.ok,
    message: `${res.status} ${res.statusText}\n${text.slice(0, ANSWER_LIMIT)}`,
  };
}

export async function executeClientTool(
  name: string,
  input: Record<string, unknown>,
  ctx: ClientToolContext,
): Promise<ClientToolResult> {
  if (name === "navigate") {
    const route = String(input.route ?? "");
    if (!route) return { success: false, message: "navigate: 'route' is required" };
    // REQ-1846: an optional react-router location.state payload — the SAME mechanism in-app
    // hyperlinks already use to deep-link a prepared value into a destination page (e.g.
    // QueryPage.tsx's AutoRunFromNav reads location.state.query/autoRun to populate and run a
    // GraphQL query the instant the page mounts). Generic passthrough, not hardcoded to one
    // page, so any route that already reads its own location.state keys keeps working here too.
    const state =
      input.state && typeof input.state === "object"
        ? (input.state as Record<string, unknown>)
        : undefined;
    // REQ-2071: Python the model hands the Data Science area to run comes back as the run's
    // output, so the model can read what its code printed, returned or raised.
    const handed = typeof state?.code === "string" || Array.isArray(state?.cells);
    if (route in RUN_ROUTES && state?.autoRun === true && handed) {
      const runId = `polly-${++pollyRuns}`;
      const report = awaitRun(runId, PYTHON_RUN_TIMEOUT_MS);
      ctx.navigate(route, { state: { ...state, runId } });
      const { stdout, value, error } = await report;
      const parts = [
        RUN_ROUTES[route],
        stdout && `Printed:\n${stdout}`,
        value && `Value:\n${value}`,
        error && `Error:\n${error}`,
      ].filter(Boolean);
      return { success: error === null, message: parts.join("\n\n") };
    }
    // REQ-2115: steps on the screen the route opens, each shown as it is taken.
    const actions = input.actions === undefined ? [] : parseUiActions(input.actions);
    if (typeof actions === "string") return { success: false, message: `navigate: ${actions}` };
    ctx.navigate(route, state ? { state } : undefined);
    if (actions.length === 0) return { success: true, message: `Navigated to ${route}` };
    const { success, steps } = await runUiActions(actions, {
      confirm: async (question) => (await ctx.presentChoice({ question, mode: "yes_no" })) === true,
      paceMs: UI_STEP_PACE_MS,
      waitMs: UI_ELEMENT_WAIT_MS,
    });
    return { success, message: [`Navigated to ${route}.`, ...steps].join("\n") };
  }

  if (name === "refresh_mv") {
    const mvId = String(input.mv_id ?? "");
    if (!mvId) return { success: false, message: "refresh_mv: 'mv_id' is required" };
    const ok = await ctx.confirm({
      title: "Refresh materialized view?",
      message: `Refresh "${mvId}" now?`,
    });
    if (!ok) return { success: false, message: "Cancelled by the user." };
    return ctx.runMutation("refresh_mv", { mvId });
  }

  if (name === "admin_mutation") {
    const mutation = String(input.mutation ?? "");
    const summary = String(input.summary ?? "");
    if (!mutation) return { success: false, message: "admin_mutation: 'mutation' is required" };
    const variables =
      input.variables && typeof input.variables === "object"
        ? (input.variables as Record<string, unknown>)
        : {};
    const ok = await ctx.confirm({
      title: "Make this change?",
      message: summary,
      detail: Object.keys(variables).length
        ? `${mutation}\n\n${JSON.stringify(variables, null, 2)}`
        : mutation,
    });
    if (!ok) return { success: false, message: "Cancelled by the user." };
    return ctx.runAdminMutation(mutation, variables);
  }

  if (name === "admin_request") return adminRequest(input, ctx);

  // REQ-2115: what the user is looking at, read from their own session.
  if (name === "read_screen") {
    const screen = await readScreen(UI_ELEMENT_WAIT_MS);
    if (typeof screen === "string") return { success: false, message: `read_screen: ${screen}` };
    return { success: true, message: JSON.stringify(screen).slice(0, ANSWER_LIMIT) };
  }

  if (name === "present_choice") {
    const question = String(input.question ?? "");
    const mode = input.mode as ChoiceMode;
    if (!question) return { success: false, message: "present_choice: 'question' is required" };
    if (mode !== "single" && mode !== "multi" && mode !== "yes_no") {
      return { success: false, message: `present_choice: unknown mode ${JSON.stringify(mode)}` };
    }
    const options = Array.isArray(input.options) ? input.options.map(String) : undefined;
    if (mode !== "yes_no" && (!options || options.length === 0)) {
      return { success: false, message: `present_choice: 'options' is required for mode ${mode}` };
    }
    const selected = await ctx.presentChoice({ question, mode, options });
    return { success: true, message: "Answered.", selected };
  }

  throw new Error(`unknown client tool ${JSON.stringify(name)}`);
}
