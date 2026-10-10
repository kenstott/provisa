// Copyright (c) 2026 Kenneth Stott
// Canary: 60140755-22e5-4945-a894-d484f362b781
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: signing a source in to its issuer, as the browser sees it.
//
// The operator presses Connect in the Sources form. The form (the OPENER) asks the server to
// start a sign-in and opens the issuer's page in a popup. When the operator approves, the issuer
// sends the popup to this deployment's sign-in page (the RETURN PAGE, source-sign-in.html) with
// a one-time code in its address.
//
// The return page runs on the one host of the deployment that an issuer can be told about
// (lib/authHost.ts, REQ-1348), which is not always the host the form is on, and the server reads
// the organisation from the host it is called on. So the return page calls nothing. It takes the
// code out of its address and hands it to its opener, which is on the organisation's host and
// holds the operator's session, and the opener completes the sign-in.
//
// Two rules keep that hand-over closed:
// - the return page answers only its own opener, only from this deployment (the same origin or a
//   sibling under the same base domain), and addresses the answer to that exact origin;
// - the opener accepts an answer only from the popup it opened, at the sign-in origin.

import { isSiblingOrigin } from "./authHost";

/** Opener → return page: "hand me what the issuer sent you". */
export const HELLO = "provisa-source-sign-in-hello";
/** Return page → opener: the issuer's answer. */
export const ANSWER = "provisa-source-sign-in-answer";

export interface IssuerAnswer {
  state: string | null;
  code: string | null;
  error: string | null;
}

/**
 * What the issuer put in the return page's address, removed from the address before it is
 * returned: nothing the page does afterwards can send it anywhere by accident.
 */
export function takeIssuerAnswer(
  location: Pick<Location, "search" | "pathname">,
  history: Pick<History, "replaceState">,
): IssuerAnswer {
  const given = new URLSearchParams(location.search);
  history.replaceState(null, "", location.pathname);
  return { state: given.get("state"), code: given.get("code"), error: given.get("error") };
}

/** Whether a window at `origin` may be handed the answer by a return page at `self`. */
export function mayReceiveAnswer(origin: string, self: Location = window.location): boolean {
  return origin === self.origin || isSiblingOrigin(origin, self);
}

/**
 * The return page's whole behaviour: answer the opener's hello with the issuer's answer, once.
 * Returns the listener's remover.
 */
export function answerOpener(answer: IssuerAnswer, win: Window = window): () => void {
  let answered = false;
  const onMessage = (event: MessageEvent) => {
    if (answered || event.data?.type !== HELLO) return;
    if (!win.opener || event.source !== win.opener) return;
    if (!mayReceiveAnswer(event.origin, win.location)) return;
    answered = true;
    (event.source as WindowProxy).postMessage({ type: ANSWER, ...answer }, event.origin);
    win.close();
  };
  win.addEventListener("message", onMessage);
  return () => win.removeEventListener("message", onMessage);
}

export interface SignedIn {
  source_id: string;
  account: string;
  refresh_token: string; // a reference into the organisation's vault, never the credential
}

/** What the person adding a source gives. The client is the organisation's (Admin › Email) and
 * is read by the server; the form neither holds nor sends one. */
export interface SignInRequest {
  source_id: string;
  kind: string;
  account: string;
  scopes: string[];
}

/** Whether the organisation has connected the platform, and whether the caller may do so. */
export interface SignInStatus {
  configured: boolean;
  may_configure: boolean;
  /** Whether an administrator allows sources that read the organisation's mailboxes. */
  organisation_mailboxes?: boolean;
}

/** A refusal by the server, with the code its message is localised by. */
export class SignInRefused extends Error {
  code: string | null;
  params: Record<string, unknown>;
  constructor(body: { code?: string; params?: Record<string, unknown>; detail?: string }) {
    super(body.detail ?? "");
    this.code = body.code ?? null;
    this.params = body.params ?? {};
  }
}

async function call<T>(path: string, body?: unknown): Promise<T> {
  const answer = await fetch(`/admin/source-sign-in/${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const said = await answer.json();
  if (!answer.ok) throw new SignInRefused(said);
  return said as T;
}

export async function signInStatus(kind: string): Promise<SignInStatus> {
  return call<SignInStatus>(`status?kind=${encodeURIComponent(kind)}`);
}

const HELLO_EVERY_MS = 500;

/**
 * Wait for the return page in `popup` to hand over the issuer's answer. `signInOrigin` is the
 * origin the issuer returns the browser to, as the server states it: hellos are addressed to it
 * alone, so none is delivered while the popup is still at the issuer.
 */
export function awaitIssuerAnswer(
  popup: Window,
  signInOrigin: string,
  timeoutMs: number,
  win: Window = window,
): Promise<IssuerAnswer> {
  return new Promise<IssuerAnswer>((resolve, reject) => {
    const finish = (settle: () => void) => {
      win.clearInterval(hello);
      win.clearTimeout(timer);
      win.removeEventListener("message", onMessage);
      settle();
    };
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== signInOrigin || event.source !== popup) return;
      if (event.data?.type !== ANSWER) return;
      const { state, code, error } = event.data as IssuerAnswer;
      finish(() => resolve({ state, code, error }));
    };
    const hello = win.setInterval(() => {
      if (popup.closed) {
        finish(() => reject(new SignInRefused({ code: "source_sign_in.window_closed" })));
        return;
      }
      popup.postMessage({ type: HELLO }, signInOrigin);
    }, HELLO_EVERY_MS);
    const timer = win.setTimeout(
      () => finish(() => reject(new SignInRefused({ code: "source_sign_in.state_expired" }))),
      timeoutMs,
    );
    win.addEventListener("message", onMessage);
  });
}

/** Sign a source in: start, send the operator to the issuer in a popup, complete. */
export async function signInSource(
  request: SignInRequest,
  win: Window = window,
): Promise<SignedIn> {
  // Opened before the start call answers, while the press that asked for it still counts as the
  // operator's, or the browser blocks it.
  const popup = win.open("about:blank", "provisa-source-sign-in", "popup,width=520,height=680");
  if (!popup) throw new SignInRefused({ code: "source_sign_in.popup_blocked" });
  try {
    const started = await call<{
      authorization_url: string;
      expires_in: number;
      return_origin: string;
    }>("start", request);
    popup.location.href = started.authorization_url;
    const answer = await awaitIssuerAnswer(
      popup,
      started.return_origin,
      started.expires_in * 1000,
      win,
    );
    return await call<SignedIn>("complete", answer);
  } finally {
    if (!popup.closed) popup.close();
  }
}
