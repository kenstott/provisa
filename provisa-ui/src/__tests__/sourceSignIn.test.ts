// Copyright (c) 2026 Kenneth Stott
// Canary: e05556ff-6e1d-47ce-95e9-37f987de7cd2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: the browser's half of signing a source in. The page an issuer returns to takes the
// one-time code out of its address before anything else, asks no server for anything, and hands
// the code only to its own opener at an origin of this deployment; the form accepts it only from
// the popup it opened, at the sign-in origin.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ANSWER,
  HELLO,
  answerOpener,
  awaitIssuerAnswer,
  mayReceiveAnswer,
  takeIssuerAnswer,
} from "../lib/sourceSignIn";

const SIGN_IN = "https://cloud.provisa.test";
const ORG = "https://acme.provisa.test";
const RETURNED = `${SIGN_IN}/source-sign-in.html?state=made-up-state&code=made-up-code`;

function loc(href: string): Location {
  const u = new URL(href);
  return {
    protocol: u.protocol,
    hostname: u.hostname,
    port: u.port,
    origin: u.origin,
    href: u.href,
    search: u.search,
    pathname: u.pathname,
  } as unknown as Location;
}

/** A window as the return page uses one: its opener, its address, its message listeners. */
function returnPage(href: string = RETURNED) {
  const listeners: ((event: MessageEvent) => void)[] = [];
  const opener = { postMessage: vi.fn() };
  const win = {
    opener,
    location: loc(href),
    close: vi.fn(),
    addEventListener: (_: string, fn: (event: MessageEvent) => void) => listeners.push(fn),
    removeEventListener: vi.fn(),
  };
  const say = (origin: string, data: unknown, source: unknown = opener) =>
    listeners.forEach((fn) => fn({ origin, data, source } as unknown as MessageEvent));
  return { win: win as unknown as Window, opener, say, close: win.close };
}

describe("the page an issuer returns to", () => {
  it("takes the issuer's answer out of its address", () => {
    const history = { replaceState: vi.fn() };
    const answer = takeIssuerAnswer(loc(RETURNED), history);
    expect(answer).toEqual({ state: "made-up-state", code: "made-up-code", error: null });
    expect(history.replaceState).toHaveBeenCalledWith(null, "", "/source-sign-in.html");
  });

  it("reads a refusal the same way", () => {
    const answer = takeIssuerAnswer(
      loc(`${SIGN_IN}/source-sign-in.html?state=s&error=access_denied`),
      { replaceState: vi.fn() },
    );
    expect(answer).toEqual({ state: "s", code: null, error: "access_denied" });
  });

  it("strips its address before anything else and asks no server for anything", async () => {
    const fetched = vi.spyOn(globalThis, "fetch");
    const replaced = vi.spyOn(window.history, "replaceState");
    window.history.pushState(
      null,
      "",
      "/source-sign-in.html?state=made-up-state&code=made-up-code",
    );
    await import("../sourceSignInPage");
    expect(replaced).toHaveBeenCalledWith(null, "", "/source-sign-in.html");
    expect(window.location.search).toBe("");
    expect(fetched).not.toHaveBeenCalled();
    fetched.mockRestore();
    replaced.mockRestore();
  });

  it("is a page that names no other site and sends no referrer", () => {
    const html = readFileSync(resolve(__dirname, "../../source-sign-in.html"), "utf8");
    expect(html).toContain('<meta name="referrer" content="no-referrer" />');
    expect(html).not.toMatch(/https?:\/\//);
    expect(html.match(/<script[^>]*>/g)).toEqual([
      '<script type="module" src="/src/sourceSignInPage.ts">',
    ]);
    expect(html).not.toMatch(/<link|<img|<iframe|@import|url\(/);
  });
});

describe("who the return page hands the answer to", () => {
  const answer = { state: "made-up-state", code: "made-up-code", error: null };

  it("answers its opener on an organisation's host, at exactly that origin", () => {
    const page = returnPage();
    answerOpener(answer, page.win);
    page.say(ORG, { type: HELLO });
    expect(page.opener.postMessage).toHaveBeenCalledWith({ type: ANSWER, ...answer }, ORG);
    expect(page.close).toHaveBeenCalled();
  });

  it("answers an opener on its own origin (a deployment with one host)", () => {
    const page = returnPage("http://localhost:3000/source-sign-in.html?state=s&code=c");
    answerOpener(answer, page.win);
    page.say("http://localhost:3000", { type: HELLO });
    expect(page.opener.postMessage).toHaveBeenCalledWith(
      { type: ANSWER, ...answer },
      "http://localhost:3000",
    );
  });

  it.each([
    "https://acme.example.test", // another site
    "https://evilprovisa.test", // not under the base domain
    "http://acme.provisa.test", // another scheme
    "https://acme.provisa.test:8443", // another port
    "null",
  ])("does not answer %s", (origin) => {
    const page = returnPage();
    answerOpener(answer, page.win);
    page.say(origin, { type: HELLO });
    expect(page.opener.postMessage).not.toHaveBeenCalled();
    expect(page.close).not.toHaveBeenCalled();
  });

  it("never addresses an answer to every origin", () => {
    expect(mayReceiveAnswer("*", loc(RETURNED))).toBe(false);
  });

  it("does not answer a window that is not its opener, even from this deployment", () => {
    const page = returnPage();
    const stranger = { postMessage: vi.fn() };
    answerOpener(answer, page.win);
    page.say(ORG, { type: HELLO }, stranger);
    expect(stranger.postMessage).not.toHaveBeenCalled();
    expect(page.opener.postMessage).not.toHaveBeenCalled();
  });

  it("does not answer anything but the hello", () => {
    const page = returnPage();
    answerOpener(answer, page.win);
    page.say(ORG, { type: "something-else" });
    page.say(ORG, null);
    expect(page.opener.postMessage).not.toHaveBeenCalled();
  });

  it("answers once", () => {
    const page = returnPage();
    answerOpener(answer, page.win);
    page.say(ORG, { type: HELLO });
    page.say(ORG, { type: HELLO });
    expect(page.opener.postMessage).toHaveBeenCalledTimes(1);
  });
});

describe("what the form accepts as the issuer's answer", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  function form() {
    const popup = { closed: false, postMessage: vi.fn() };
    const listeners: ((event: MessageEvent) => void)[] = [];
    const win = {
      setInterval: (fn: () => void, ms: number) => setInterval(fn, ms),
      clearInterval: (id: number) => clearInterval(id),
      setTimeout: (fn: () => void, ms: number) => setTimeout(fn, ms),
      clearTimeout: (id: number) => clearTimeout(id),
      addEventListener: (_: string, fn: (event: MessageEvent) => void) => listeners.push(fn),
      removeEventListener: vi.fn(),
    };
    const hear = (origin: string, data: unknown, source: unknown = popup) =>
      listeners.forEach((fn) => fn({ origin, data, source } as unknown as MessageEvent));
    const waiting = awaitIssuerAnswer(
      popup as unknown as Window,
      SIGN_IN,
      600_000,
      win as unknown as Window,
    );
    return { popup, hear, waiting };
  }

  const answer = { type: ANSWER, state: "made-up-state", code: "made-up-code", error: null };

  it("says hello to the sign-in origin only, so none reaches the issuer's page", () => {
    const { popup } = form();
    vi.advanceTimersByTime(1600);
    expect(popup.postMessage).toHaveBeenCalledTimes(3);
    for (const [message, target] of popup.postMessage.mock.calls) {
      expect(message).toEqual({ type: HELLO });
      expect(target).toBe(SIGN_IN);
    }
  });

  it("takes the answer from its popup at the sign-in origin", async () => {
    const { hear, waiting } = form();
    hear(SIGN_IN, answer);
    await expect(waiting).resolves.toEqual({
      state: "made-up-state",
      code: "made-up-code",
      error: null,
    });
  });

  it("ignores an answer from any other origin or any other window", async () => {
    const { hear, waiting } = form();
    hear("https://accounts.example.test", answer);
    hear(ORG, answer);
    hear(SIGN_IN, answer, { postMessage: vi.fn() });
    hear(SIGN_IN, { ...answer, type: "something-else" });
    let settled = false;
    void waiting.then(() => (settled = true));
    await vi.advanceTimersByTimeAsync(1000);
    expect(settled).toBe(false);
    hear(SIGN_IN, answer);
    await expect(waiting).resolves.toMatchObject({ code: "made-up-code" });
  });

  it("stops by name when the operator closes the window", async () => {
    const { popup, waiting } = form();
    popup.closed = true;
    const refused = expect(waiting).rejects.toMatchObject({ code: "source_sign_in.window_closed" });
    await vi.advanceTimersByTimeAsync(600);
    await refused;
  });

  it("stops by name when the sign-in runs out of time", async () => {
    const { waiting } = form();
    const refused = expect(waiting).rejects.toMatchObject({ code: "source_sign_in.state_expired" });
    await vi.advanceTimersByTimeAsync(600_001);
    await refused;
  });
});
