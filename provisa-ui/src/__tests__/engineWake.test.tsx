// Copyright (c) 2026 Kenneth Stott
// Canary: f1fe3c57-4f60-4f08-b344-311eac354469
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1516: a shard that idled to zero makes the first query take ~2-4min, which is indistinguishable
// from a hang unless something says so. What these tests pin is that the notice is derived from a
// slow request plus the server's own answer — never asserted by a call site, never raised on a wait
// the engine is not responsible for, and never left up once the request comes back.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, act } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { installAuthFetch } from "../lib/authFetch";
import { engineWaking, prewarmEngine, resetEngineWake } from "../lib/engineWake";
import { EngineWakingBanner } from "../components/EngineWakingBanner";
import { resetServerReachability } from "../lib/serverReachability";

vi.mock("../lib/firebase", () => ({ currentFirebaseToken: async () => null }));

const BANNER = "Starting the query engine";

/** A request whose completion the test controls, so "still in flight" is not a timing race. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function renderBanner() {
  return render(
    <MantineProvider theme={{ components: {} }}>
      <EngineWakingBanner />
    </MantineProvider>,
  );
}

/** Advance past the slow-request threshold and let the poll's promises settle. */
async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

describe("REQ-1516 engine wake visibility", () => {
  let nativeFetch: typeof globalThis.fetch;

  beforeEach(() => {
    vi.useFakeTimers();
    resetEngineWake();
    resetServerReachability();
    localStorage.clear();
    nativeFetch = window.fetch;
  });

  afterEach(() => {
    window.fetch = nativeFetch;
    resetEngineWake();
    resetServerReachability();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  /** Wire a fetch where /data/engine/state answers `state` and every other path hangs. */
  function slowRequestAgainst(state: string) {
    const pending = deferred<Response>();
    window.fetch = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/engine/state")) {
        return Promise.resolve(new Response(JSON.stringify({ state }), { status: 200 }));
      }
      return pending.promise;
    }) as unknown as typeof fetch;
    installAuthFetch();
    return pending;
  }

  it("a request that returns promptly never asks about the engine", async () => {
    const probes = vi.fn();
    window.fetch = vi.fn((input: RequestInfo | URL) => {
      if (String(input).includes("/data/engine/state")) probes();
      return Promise.resolve(new Response("{}", { status: 200 }));
    }) as unknown as typeof fetch;
    installAuthFetch();

    await act(async () => {
      await window.fetch("/data/graphql");
    });
    await advance(20_000);

    expect(probes).not.toHaveBeenCalled();
    expect(engineWaking()).toBe(false);
  });

  it("a request outstanding past the threshold raises the notice when the engine is starting", async () => {
    slowRequestAgainst("starting");
    renderBanner();

    void window.fetch("/data/graphql");
    expect(screen.queryByText(BANNER)).toBeNull();

    await advance(6_000);

    expect(engineWaking()).toBe(true);
    expect(screen.getByTestId("engine-waking-banner")).toBeInTheDocument();
  });

  it("a shard resting at zero is the same notice as one coming up", async () => {
    slowRequestAgainst("stopped");

    void window.fetch("/data/graphql");
    await advance(6_000);

    expect(engineWaking()).toBe(true);
  });

  it("a slow request the engine is not responsible for shows nothing", async () => {
    slowRequestAgainst("ready");
    renderBanner();

    void window.fetch("/data/graphql");
    await advance(20_000);

    expect(engineWaking()).toBe(false);
    expect(screen.queryByText(BANNER)).toBeNull();
  });

  it("an engine that is simply there is never a wake", async () => {
    slowRequestAgainst("always-on");

    void window.fetch("/data/graphql");
    await advance(20_000);

    expect(engineWaking()).toBe(false);
  });

  it("the notice clears when the request it was explaining comes back", async () => {
    const pending = slowRequestAgainst("starting");
    renderBanner();

    void window.fetch("/data/graphql");
    await advance(6_000);
    expect(screen.getByTestId("engine-waking-banner")).toBeInTheDocument();

    await act(async () => {
      pending.resolve(new Response("{}", { status: 200 }));
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(engineWaking()).toBe(false);
    expect(screen.queryByText(BANNER)).toBeNull();
  });

  // The probe is a same-origin request like any other. Were it counted as outstanding it would hold
  // the count above zero by itself, so polling would continue after the query it was explaining
  // returned and the banner would never come down.
  it("the poll does not count itself as the slow request", async () => {
    const pending = slowRequestAgainst("starting");

    void window.fetch("/data/graphql");
    await advance(9_000);
    expect(engineWaking()).toBe(true);

    await act(async () => {
      pending.resolve(new Response("{}", { status: 200 }));
      await vi.advanceTimersByTimeAsync(0);
    });
    await advance(9_000);

    expect(engineWaking()).toBe(false);
  });

  it("a prewarm asks the server to start the engine and does not wait for it", async () => {
    const calls: Array<{ url: string; method: string | undefined }> = [];
    window.fetch = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ url: String(input), method: init?.method });
      return Promise.resolve(new Response("{}", { status: 202 }));
    }) as unknown as typeof fetch;
    installAuthFetch();

    prewarmEngine();
    await advance(0);

    expect(calls).toEqual([{ url: "/data/engine/prewarm", method: "POST" }]);
  });
});
