// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2c6f41-0a7e-4b93-9c15-e4b7a2d9f068
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The "Load Management and Timeliness" panel: collapsed by default, the source's load and recency
// settings inside it, and an (i) icon whose explanation appears on hover or keyboard focus.

import { useState } from "react";
import { afterEach, describe, it, expect, vi } from "vitest";
import { renderInForm, screen, fireEvent, within } from "../../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { SourceLoadManagementPanel } from "../SourceLoadManagementPanel";
import type { SourceFormState } from "../SourceFormFields";

afterEach(() => vi.restoreAllMocks());

const FORM: SourceFormState = {
  id: "sales-pg",
  type: "postgresql",
  host: "localhost",
  port: 5432,
  database: "sales",
  username: "u",
  password: "",
  gqlNamingConvention: "",
  cacheTtl: "60",
  cacheEnabled: true,
  replicate: null,
  region: null,
  loadProtected: false,
  offPeakWindow: "",
  offPeakTz: "UTC",
  changeSignal: "ttl",
  sentinelPath: "",
  freshnessGate: false,
  maxLiveConcurrency: "",
  path: "",
  allowedDomains: "",
  description: "",
};

const INFO =
  "Balances how current each reader's data is against load on the platform and on upstream sources.";

function Harness({ onForm }: { onForm?: (f: SourceFormState) => void }) {
  const [form, setForm] = useState<SourceFormState>(FORM);
  return (
    <SourceLoadManagementPanel
      form={form}
      setForm={(f) => {
        setForm(f);
        onForm?.(f);
      }}
    />
  );
}

describe("SourceLoadManagementPanel", () => {
  it("is titled and open, with no toggle, showing its help text and settings", async () => {
    renderInForm(<Harness />);
    expect(screen.getByTestId("source-load-management-panel")).toHaveTextContent(
      "Load Management and Timeliness",
    );
    expect(screen.queryByTestId("source-load-management-panel-toggle")).toBeNull();
    expect(screen.getByTestId("source-load-management-help")).toHaveTextContent(
      "How current the data must be for each reader, balanced against load on the platform and upstream sources.",
    );
    expect(await screen.findByTestId("cache-ttl-input")).toHaveValue("60");
    // Load protection is a choice of the Replicate drop-down, not a tick of its own.
    expect(screen.queryByTestId("load-protected-checkbox")).toBeNull();
  });

  const SCHEDULED = "Always, source read only on schedule";
  async function openReplicate() {
    const select = await screen.findByTestId("source-replicate-select");
    fireEvent.click(select);
    const listbox = document.getElementById(select.getAttribute("aria-controls") as string);
    return { select, listbox: listbox as HTMLElement };
  }

  it("offers reading the source only on schedule as the last Replicate choice", async () => {
    renderInForm(<Harness />);
    const { listbox } = await openReplicate();
    const options = within(listbox).getAllByRole("option", { hidden: true });
    expect(options.map((o) => o.textContent)).toEqual([
      "Default",
      "Live if possible",
      "Hot-50",
      "Hot-100",
      "Hot-500",
      "Hot-1000",
      "Hot-10000",
      "Always",
      SCHEDULED,
    ]);
  });

  it("saves load protection with that choice and shows the optional window and time zone", async () => {
    const onForm = vi.fn();
    renderInForm(<Harness onForm={onForm} />);
    const { select, listbox } = await openReplicate();
    expect(screen.queryByTestId("off-peak-window")).toBeNull();
    fireEvent.click(within(listbox).getByText(SCHEDULED));
    // The server takes load protection with Always: reads never go to the source.
    expect(onForm).toHaveBeenLastCalledWith(
      expect.objectContaining({ loadProtected: true, replicate: 0 }),
    );
    expect(select).toHaveValue(SCHEDULED);
    // The same control as the table form's: two time boxes and the zone picker, all optional.
    expect(await screen.findByTestId("off-peak-opens")).not.toBeRequired();
    expect(screen.getByTestId("off-peak-closes")).not.toBeRequired();
    expect(screen.getByTestId("off-peak-tz")).not.toBeRequired();
    fireEvent.change(screen.getByTestId("off-peak-opens"), { target: { value: "01:00" } });
    fireEvent.change(screen.getByTestId("off-peak-closes"), { target: { value: "05:00" } });
    expect(onForm).toHaveBeenLastCalledWith(
      expect.objectContaining({ offPeakWindow: "01:00-05:00" }),
    );
    expect(
      screen.getByText("Without a window, the refresh follows the Cache TTL or the change probe."),
    ).toBeInTheDocument();
  });

  it("drops load protection and its two fields when another choice is made", async () => {
    const onForm = vi.fn();
    function Protected() {
      const [form, setForm] = useState<SourceFormState>({ ...FORM, loadProtected: true });
      return (
        <SourceLoadManagementPanel
          form={form}
          setForm={(f) => {
            setForm(f);
            onForm(f);
          }}
        />
      );
    }
    renderInForm(<Protected />);
    const { select, listbox } = await openReplicate();
    // A source stored with load protection opens on the choice, whatever its Replicate value.
    expect(select).toHaveValue(SCHEDULED);
    expect(screen.getByTestId("off-peak-window")).toBeInTheDocument();
    fireEvent.click(within(listbox).getByText("Always"));
    expect(onForm).toHaveBeenLastCalledWith(
      expect.objectContaining({ loadProtected: false, replicate: 0 }),
    );
    expect(screen.queryByTestId("off-peak-window")).toBeNull();
    expect(screen.queryByTestId("off-peak-tz")).toBeNull();
  });

  it("says in its help what refreshes a source read only on schedule", async () => {
    renderInForm(<Harness />);
    const help = (await screen.findByTestId("source-replicate-select")).getAttribute(
      "aria-description",
    );
    expect(help).toContain("a query never causes the source to be read");
    expect(help).toContain("the off-peak window, the Cache TTL or the change probe");
    expect(help).toContain("at least one of the three is required");
  });

  it("renders an accessible (i) icon whose explanation shows on keyboard focus", async () => {
    renderInForm(<Harness />);
    const icon = screen.getByRole("button", { name: "About load management and timeliness" });
    expect(icon).toHaveAttribute("tabindex", "0");
    // The hint opens at once for a focus the keyboard made (a click focuses too, and shows none).
    // jsdom does not know which focus that was, so it is told: the focus Tab gives is visible.
    const matches = Element.prototype.matches;
    vi.spyOn(Element.prototype, "matches").mockImplementation(function (this: Element, sel) {
      return sel === ":focus-visible" ? this === document.activeElement : matches.call(this, sel);
    });
    for (let hop = 0; hop < 50 && document.activeElement !== icon; hop++) await userEvent.tab();
    expect(icon).toHaveFocus();
    expect(
      await screen.findByText(new RegExp(INFO.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))),
    ).toBeInTheDocument();
  });

  it("shows the explanation on hover, and its settings stay in view", async () => {
    renderInForm(<Harness />);
    const icon = screen.getByTestId("source-load-management-panel-info");
    await userEvent.hover(icon);
    expect(
      await screen.findByText(/A table the engine reads live ignores the TTL settings/),
    ).toBeInTheDocument();
    await userEvent.click(icon);
    expect(screen.getByTestId("source-load-management-help")).toBeVisible();
  });

  it("edits the sentinel path, freshness gate and live-concurrency cap", async () => {
    const onForm = vi.fn();
    renderInForm(<Harness onForm={onForm} />);
    await userEvent.type(await screen.findByTestId("sentinel-path-input"), "https://x/_SUCCESS");
    expect(onForm).toHaveBeenLastCalledWith(
      expect.objectContaining({ sentinelPath: "https://x/_SUCCESS" }),
    );
    await userEvent.click(screen.getByTestId("freshness-gate-checkbox"));
    expect(onForm).toHaveBeenLastCalledWith(expect.objectContaining({ freshnessGate: true }));
    await userEvent.type(screen.getByTestId("max-live-concurrency-input"), "4");
    expect(onForm).toHaveBeenLastCalledWith(expect.objectContaining({ maxLiveConcurrency: "4" }));
  });

  it("sets Replicate through the shared drop-down, in place of the old checkbox", async () => {
    const onForm = vi.fn();
    renderInForm(<Harness onForm={onForm} />);
    const select = await screen.findByTestId("source-replicate-select");
    expect(select).toHaveValue("Default");
    expect(screen.queryByTestId("prefer-materialized-checkbox")).toBeNull();
    fireEvent.click(select);
    const listbox = document.getElementById(select.getAttribute("aria-controls") as string);
    fireEvent.click(within(listbox as HTMLElement).getByText("Always"));
    expect(onForm).toHaveBeenLastCalledWith(expect.objectContaining({ replicate: 0 }));
  });

  it("cannot set load protection against Live if possible: the two are one choice", async () => {
    // What used to be flagged as a contradiction cannot be entered any more.
    renderInForm(<Harness />);
    const { select, listbox } = await openReplicate();
    fireEvent.click(within(listbox).getByText(SCHEDULED));
    fireEvent.click(select);
    fireEvent.click(within(listbox).getByText("Live if possible"));
    expect(select).toHaveValue("Live if possible");
    expect(
      screen.queryByText(
        "Live if possible cannot be combined with load protection: a load-protected table is never read live.",
      ),
    ).toBeNull();
  });

  it("flags a sentinel path with an unsupported scheme", async () => {
    renderInForm(<Harness />);
    await userEvent.type(await screen.findByTestId("sentinel-path-input"), "s3://b/_SUCCESS");
    expect(
      await screen.findByText("Use a file:, ftp:, sftp:, http: or https: URL, or leave empty."),
    ).toBeInTheDocument();
  });

  it("errors on Cache TTL for a ttl signal with none set, clearing on a TTL or another signal", async () => {
    function NoTtl() {
      const [form, setForm] = useState<SourceFormState>({
        ...FORM,
        cacheTtl: "",
        replicate: 0,
      });
      return <SourceLoadManagementPanel form={form} setForm={setForm} />;
    }
    renderInForm(<NoTtl />);
    const msg = "The ttl change signal judges staleness by the Cache TTL, so set a Cache TTL.";
    expect(await screen.findByText(msg)).toBeInTheDocument();
    await userEvent.type(screen.getByTestId("cache-ttl-input"), "30");
    expect(screen.queryByText(msg)).toBeNull();
    await userEvent.clear(screen.getByTestId("cache-ttl-input"));
    expect(await screen.findByText(msg)).toBeInTheDocument();
  });

  it("does not error without a Cache TTL for a signal that has its own clock", async () => {
    renderInForm(
      <SourceLoadManagementPanel
        form={{ ...FORM, cacheTtl: "", changeSignal: "probe", freshnessGate: true }}
        setForm={vi.fn()}
      />,
    );
    await screen.findByTestId("cache-ttl-input");
    expect(screen.queryByText(/judges staleness by the Cache TTL/)).toBeNull();
  });

  it("does not error for a live-read ttl source with no Cache TTL", async () => {
    renderInForm(<SourceLoadManagementPanel form={{ ...FORM, cacheTtl: "" }} setForm={vi.fn()} />);
    await screen.findByTestId("cache-ttl-input");
    expect(screen.queryByText(/judges staleness by the Cache TTL/)).toBeNull();
  });

  // #208: the server says every table of a mail source lands, so the source states the clock.
  describe("for a kind the server says must state its Cache TTL", () => {
    function kinds(mail: Record<string, boolean>) {
      return vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
        const kind = decodeURIComponent(String(url).split("/admin/source-kinds/")[1]);
        const isMail = mail[kind] ?? false;
        return new Response(JSON.stringify({ cache_ttl_required: isMail, sentinel_path: !isMail }));
      });
    }

    it("marks the Cache TTL required and errors until one is given", async () => {
      const asked = kinds({ microsoft_365: true });
      function Mail() {
        const [form, setForm] = useState<SourceFormState>({
          ...FORM,
          type: "microsoft_365",
          cacheTtl: "",
        });
        return <SourceLoadManagementPanel form={form} setForm={setForm} />;
      }
      renderInForm(<Mail />);
      // The field that must be filled is in view: the panel is always open. It is required once
      // the server has said what the kind needs.
      await vi.waitFor(() => expect(screen.getByTestId("cache-ttl-input")).toBeRequired());
      const msg = "The ttl change signal judges staleness by the Cache TTL, so set a Cache TTL.";
      expect(await screen.findByText(msg)).toBeInTheDocument();
      await userEvent.type(screen.getByTestId("cache-ttl-input"), "300");
      expect(screen.queryByText(msg)).toBeNull();
      expect(String(asked.mock.calls[0][0])).toBe("/admin/source-kinds/microsoft_365");
      asked.mockRestore();
    });

    // REQ-1148: nothing of the operator's produces a mailbox's data, so there is no marker.
    it("offers no sentinel path, and drops one typed for another kind", async () => {
      const asked = kinds({ google_workspace: true });
      const seen: SourceFormState[] = [];
      function Mail() {
        const [form, setForm] = useState<SourceFormState>({
          ...FORM,
          type: "google_workspace",
          sentinelPath: "https://example.com/_SUCCESS",
        });
        return (
          <SourceLoadManagementPanel
            form={form}
            setForm={(f) => {
              seen.push(f);
              setForm(f);
            }}
          />
        );
      }
      renderInForm(<Mail />);
      await vi.waitFor(() => expect(screen.queryByTestId("sentinel-path-input")).toBeNull());
      await vi.waitFor(() => expect(seen.at(-1)?.sentinelPath).toBe(""));
      expect(screen.getByTestId("change-signal-select")).toBeInTheDocument();
      asked.mockRestore();
    });

    it("leaves another kind as it was: collapsed, the Cache TTL optional", async () => {
      const asked = kinds({ microsoft_365: true });
      renderInForm(
        <SourceLoadManagementPanel
          form={{ ...FORM, type: "mysql", cacheTtl: "" }}
          setForm={vi.fn()}
        />,
      );
      await vi.waitFor(() => expect(asked).toHaveBeenCalled());
      expect(await screen.findByTestId("cache-ttl-input")).not.toBeRequired();
      expect(screen.getByTestId("sentinel-path-input")).toBeInTheDocument();
      expect(screen.queryByText(/judges staleness by the Cache TTL/)).toBeNull();
      asked.mockRestore();
    });
  });

  // REQ-2000: a live-only kind's tables are read live, never replicated.
  it("offers no Replicate choice for a kind the server says is live-only", async () => {
    const asked = vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
      const kind = decodeURIComponent(String(url).split("/admin/source-kinds/")[1]);
      return new Response(
        JSON.stringify({
          cache_ttl_required: false,
          sentinel_path: false,
          live_only: kind === "qdrant",
        }),
      );
    });
    renderInForm(
      <SourceLoadManagementPanel form={{ ...FORM, type: "qdrant" }} setForm={vi.fn()} />,
    );
    await vi.waitFor(() => expect(screen.queryByTestId("source-replicate-select")).toBeNull());
    expect(screen.getByTestId("cache-ttl-input")).toBeInTheDocument();
    asked.mockRestore();
  });
});

describe("the source's Cache TTL editor (REQ-2031)", () => {
  it("takes -1 and says the source's tables never expire", () => {
    const seen: SourceFormState[] = [];
    renderInForm(<Harness onForm={(f) => seen.push(f)} />);
    const input = screen.getByTestId("cache-ttl-input");
    expect(screen.getByText("-1: never expires")).toBeTruthy();
    fireEvent.change(input, { target: { value: "-1" } });
    expect(seen.at(-1)?.cacheTtl).toBe("-1");
    expect(screen.getByText("Never expires: lands once.")).toBeTruthy();
  });

  it("goes no lower than -1", () => {
    const seen: SourceFormState[] = [];
    renderInForm(<Harness onForm={(f) => seen.push(f)} />);
    const input = screen.getByTestId("cache-ttl-input");
    fireEvent.change(input, { target: { value: "-5" } });
    fireEvent.blur(input);
    expect(seen.at(-1)?.cacheTtl).toBe("-1");
  });
});
