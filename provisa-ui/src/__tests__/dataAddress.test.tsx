// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e9f17-4a8c-4d03-a5f1-3c7e0b9d2a64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1916 (amended 2026-11-02): a coordinator-served UI sends every data call (every request under
// /data) to the data node the browser chose, from the coordinator's list or typed, carrying the
// sign-in token; an every node's UI shows no setting and keeps its data calls on its own origin.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import userEvent from "@testing-library/user-event";

vi.mock("../lib/firebase", () => ({ currentFirebaseToken: vi.fn() }));
vi.mock("../lib/sessionToken", () => ({ storedToken: vi.fn() }));

import { storedToken } from "../lib/sessionToken";
import { currentFirebaseToken } from "../lib/firebase";
import { installAuthFetch } from "../lib/authFetch";
import {
  atDataAddress,
  chooseDataAddress,
  DATA_ADDRESS_KEY,
  dataAddressFor,
  isDataAddress,
  pageAtDataAddress,
  requestDataAddresses,
  resetDataAddresses,
  storedDataAddress,
} from "../lib/dataAddress";
import { removeStored } from "../lib/browserStorage";
import { render, screen, waitFor } from "../test-utils/render";
import { DataAddressSwitcher } from "../components/DataAddressSwitcher";

const ORIGIN = window.location.origin;
const original = window.fetch;
let offer: unknown = { choose: false, addresses: [] };
let offerStatus = 200;
const sent: { url: string; headers: Headers }[] = [];

beforeEach(() => {
  vi.clearAllMocks();
  resetDataAddresses();
  removeStored(DATA_ADDRESS_KEY);
  sent.length = 0;
  offer = { choose: false, addresses: [] };
  offerStatus = 200;
  vi.mocked(currentFirebaseToken).mockResolvedValue(null);
  vi.mocked(storedToken).mockReturnValue("tok");
  window.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    sent.push({ url, headers: new Headers(init?.headers) });
    if (url.endsWith("/admin/data-addresses"))
      return new Response(JSON.stringify(offer), { status: offerStatus });
    return new Response("{}", { status: 200 });
  }) as typeof window.fetch;
  installAuthFetch();
});

afterEach(() => {
  window.fetch = original;
});

const coordinator = {
  choose: true,
  addresses: ["https://q1.example.com", "https://lb.example.com"],
};

describe("the data address of a data call", () => {
  it("stays on this origin until the session asks where data calls go", async () => {
    await window.fetch("/data/graphql");
    expect(sent.map((s) => s.url)).toEqual(["/data/graphql"]);
  });

  it("stays on this origin on an every node, which offers no choice", async () => {
    requestDataAddresses();
    await window.fetch("/data/graphql");
    expect(sent.map((s) => s.url)).toEqual(["/admin/data-addresses", "/data/graphql"]);
  });

  it("goes to the first address a coordinator offers, with the sign-in token", async () => {
    offer = coordinator;
    requestDataAddresses();
    await window.fetch("/data/sparql?x=1", { method: "POST" });
    const call = sent[sent.length - 1];
    expect(call.url).toBe("https://q1.example.com/data/sparql?x=1");
    expect(call.headers.get("authorization")).toBe("Bearer tok");
  });

  it("goes to the browser's choice, a typed one included", async () => {
    offer = coordinator;
    chooseDataAddress("https://typed.example.com:8443/");
    requestDataAddresses();
    await window.fetch(`${ORIGIN}/data/odata/sales/orders`);
    expect(sent[sent.length - 1].url).toBe(
      "https://typed.example.com:8443/data/odata/sales/orders",
    );
    expect(storedDataAddress()).toBe("https://typed.example.com:8443/");
  });

  it("moves a Request too, keeping its method", async () => {
    offer = coordinator;
    requestDataAddresses();
    await window.fetch(new Request(`${ORIGIN}/data/cypher`, { method: "POST", body: "{}" }));
    expect(sent[sent.length - 1].url).toBe("https://q1.example.com/data/cypher");
  });

  it("leaves the UI's backend on the coordinator, and other origins alone", async () => {
    offer = coordinator;
    requestDataAddresses();
    await window.fetch("/admin/graphql");
    await window.fetch("https://accounts.google.com/data/x");
    const urls = sent.map((s) => s.url);
    expect(urls).toContain("/admin/graphql");
    expect(urls).toContain("https://accounts.google.com/data/x");
    expect(
      sent.find((s) => s.url.startsWith("https://accounts"))?.headers.get("authorization"),
    ).toBeNull();
  });

  it("fails a data call when the coordinator could not say where to send it", async () => {
    offerStatus = 503;
    requestDataAddresses();
    await expect(window.fetch("/data/graphql")).rejects.toThrow("/admin/data-addresses 503");
    expect(sent.some((s) => s.url.includes("/data/graphql"))).toBe(false);
  });
});

describe("data address helpers", () => {
  it("accepts only http(s) URLs with a host and no query", () => {
    expect(isDataAddress("https://d.example.com")).toBe(true);
    expect(isDataAddress("http://10.0.0.7:8001")).toBe(true);
    expect(isDataAddress("d.example.com")).toBe(false);
    expect(isDataAddress("ftp://d.example.com")).toBe(false);
    expect(isDataAddress("https://d.example.com/?a=1")).toBe(false);
  });

  it("moves only data paths", () => {
    expect(atDataAddress("/data", "https://q")).toBe("https://q/data");
    expect(atDataAddress("/database", "https://q")).toBe("/database");
    expect(atDataAddress("/data/x", null)).toBe("/data/x");
  });

  it("chooses nothing for an every node", () => {
    chooseDataAddress("https://typed.example.com");
    expect(dataAddressFor({ choose: false, addresses: ["https://q"] })).toBeNull();
    expect(dataAddressFor(null)).toBeNull();
  });

  it("points a docs page's root-relative URLs at the data address", () => {
    expect(pageAtDataAddress("<html><head><title>x</title>", "https://q.example.com/")).toBe(
      '<html><head><base href="https://q.example.com/"><title>x</title>',
    );
    expect(pageAtDataAddress("<head>", null)).toBe("<head>");
  });
});

describe("DataAddressSwitcher", () => {
  it("shows nothing on an every node", async () => {
    requestDataAddresses();
    render(<DataAddressSwitcher />);
    await waitFor(() => expect(sent.length).toBeGreaterThan(0));
    expect(screen.queryByTestId("data-address-trigger")).toBeNull();
  });

  it("shows nothing before the session asks", () => {
    render(<DataAddressSwitcher />);
    expect(screen.queryByTestId("data-address-trigger")).toBeNull();
  });

  it("offers the coordinator's list and a typed address", async () => {
    offer = coordinator;
    requestDataAddresses();
    const user = userEvent.setup();
    render(<DataAddressSwitcher />);
    const trigger = await screen.findByTestId("data-address-trigger");
    expect(trigger).toHaveTextContent("q1.example.com");
    await user.click(trigger);
    await user.click(await screen.findByText("https://lb.example.com"));
    expect(storedDataAddress()).toBe("https://lb.example.com");
    await waitFor(() => expect(trigger).toHaveTextContent("lb.example.com"));

    await user.click(trigger);
    const typed = await screen.findByTestId("data-address-typed");
    await user.type(typed, "not a url");
    expect(screen.getByTestId("data-address-use")).toBeDisabled();
    await user.clear(typed);
    await user.type(typed, "https://q9.example.com");
    await user.click(screen.getByTestId("data-address-use"));
    expect(storedDataAddress()).toBe("https://q9.example.com");
  });

  it("says when a coordinator lists none", async () => {
    offer = { choose: true, addresses: [] };
    requestDataAddresses();
    const user = userEvent.setup();
    render(<DataAddressSwitcher />);
    await user.click(await screen.findByTestId("data-address-trigger"));
    expect(await screen.findByTestId("data-address-none-offered")).toBeInTheDocument();
  });

  it("shows a failed load as an error, not as an every node", async () => {
    offerStatus = 500;
    requestDataAddresses();
    render(<DataAddressSwitcher />);
    expect(await screen.findByTestId("data-address-load-error")).toBeInTheDocument();
  });
});
