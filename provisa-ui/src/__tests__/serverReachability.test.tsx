// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1514: a same-origin request that fails at the transport is an outage the whole app has to
// name, and the recovery has to clear itself. What these tests pin is the classification — which
// failures count, which do not — and that the notice is derived from it rather than asserted.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { installAuthFetch } from "../lib/authFetch";
import {
  markServerUnreachable,
  resetServerReachability,
  serverUnreachable,
} from "../lib/serverReachability";
import { ServerUnavailableModal } from "../components/ServerUnavailableModal";

vi.mock("../lib/firebase", () => ({ currentFirebaseToken: async () => null }));

const ORIGIN = "http://localhost:3000";

function renderModal() {
  return render(
    <MantineProvider theme={{ components: {} }}>
      <ServerUnavailableModal />
    </MantineProvider>,
  );
}

describe("REQ-1514 server reachability", () => {
  let nativeFetch: typeof globalThis.fetch;

  beforeEach(() => {
    resetServerReachability();
    localStorage.clear();
    nativeFetch = window.fetch;
  });

  afterEach(() => {
    window.fetch = nativeFetch;
    resetServerReachability();
    vi.restoreAllMocks();
  });

  it("a same-origin transport failure marks the deployment unreachable and re-throws", async () => {
    window.fetch = vi.fn().mockRejectedValue(new TypeError("Failed to fetch")) as typeof fetch;
    installAuthFetch();

    await expect(window.fetch(`${ORIGIN}/data/graphql`)).rejects.toThrow("Failed to fetch");
    expect(serverUnreachable()).toBe(true);
  });

  it("a response of any status is a reachable server", async () => {
    let answering = false;
    window.fetch = vi.fn(() =>
      answering
        ? Promise.resolve(new Response("", { status: 500 }))
        : Promise.reject(new TypeError("Failed to fetch")),
    ) as unknown as typeof fetch;
    installAuthFetch();

    await expect(window.fetch("/data/graphql")).rejects.toThrow();
    expect(serverUnreachable()).toBe(true);

    answering = true;
    await window.fetch("/data/graphql");
    expect(serverUnreachable()).toBe(false);
  });

  it("a caller's abort is not an outage", async () => {
    window.fetch = vi
      .fn()
      .mockRejectedValue(new DOMException("aborted", "AbortError")) as typeof fetch;
    installAuthFetch();

    await expect(window.fetch("/data/graphql")).rejects.toThrow("aborted");
    expect(serverUnreachable()).toBe(false);
  });

  it("a failure against a foreign origin is not this deployment's outage", async () => {
    window.fetch = vi.fn().mockRejectedValue(new TypeError("Failed to fetch")) as typeof fetch;
    installAuthFetch();

    await expect(window.fetch("https://securetoken.googleapis.com/v1/token")).rejects.toThrow();
    expect(serverUnreachable()).toBe(false);
  });

  it("the modal appears while the server is down and clears when it answers", async () => {
    let resolveProbe: (r: Response) => void = () => {};
    window.fetch = vi.fn(
      () =>
        new Promise<Response>((res) => {
          resolveProbe = res;
        }),
    ) as unknown as typeof fetch;
    installAuthFetch();
    renderModal();

    expect(screen.queryByText("Server unavailable")).toBeNull();

    markServerUnreachable();
    expect(await screen.findByText("Server unavailable")).toBeInTheDocument();

    resolveProbe(new Response("", { status: 200 }));
    await waitFor(() => expect(screen.queryByText("Server unavailable")).toBeNull());
  });
});
