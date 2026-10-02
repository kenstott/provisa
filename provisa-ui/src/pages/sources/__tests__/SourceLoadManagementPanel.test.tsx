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
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { SourceLoadManagementPanel } from "../SourceLoadManagementPanel";
import type { SourceFormState } from "../SourceFormFields";

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
  it("is titled, collapsed by default, and opens to its help text and settings", async () => {
    render(<Harness />);
    const toggle = screen.getByTestId("source-load-management-panel-toggle");
    expect(toggle).toHaveTextContent("Load Management and Timeliness");
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    await userEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("source-load-management-help")).toHaveTextContent(
      "How current the data must be for each reader, balanced against load on the platform and upstream sources.",
    );
    expect(await screen.findByTestId("cache-ttl-input")).toHaveValue("60");
    expect(screen.getByTestId("load-protected-checkbox")).not.toBeChecked();
  });

  it("keeps a moved field's behavior: load protection reveals the off-peak inputs", async () => {
    const onForm = vi.fn();
    render(<Harness onForm={onForm} />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
    await userEvent.click(await screen.findByTestId("load-protected-checkbox"));
    expect(onForm).toHaveBeenLastCalledWith(expect.objectContaining({ loadProtected: true }));
    expect(await screen.findByTestId("off-peak-window-input")).toBeInTheDocument();
  });

  it("renders an accessible (i) icon whose explanation shows on keyboard focus", async () => {
    render(<Harness />);
    const icon = screen.getByRole("button", { name: "About load management and timeliness" });
    expect(icon).toHaveAttribute("tabindex", "0");
    fireEvent.focus(icon);
    expect(
      await screen.findByText(new RegExp(INFO.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))),
    ).toBeInTheDocument();
  });

  it("shows the explanation on hover without toggling the section", async () => {
    render(<Harness />);
    const toggle = screen.getByTestId("source-load-management-panel-toggle");
    const icon = screen.getByTestId("source-load-management-panel-info");
    await userEvent.hover(icon);
    expect(
      await screen.findByText(/A table the engine reads live ignores the TTL settings/),
    ).toBeInTheDocument();
    await userEvent.click(icon);
    expect(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("edits the sentinel path, freshness gate and live-concurrency cap", async () => {
    const onForm = vi.fn();
    render(<Harness onForm={onForm} />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
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
    render(<Harness onForm={onForm} />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
    const select = await screen.findByTestId("source-replicate-select");
    expect(select).toHaveValue("Default");
    expect(screen.queryByTestId("prefer-materialized-checkbox")).toBeNull();
    fireEvent.click(select);
    const listbox = document.getElementById(select.getAttribute("aria-controls") as string);
    fireEvent.click(within(listbox as HTMLElement).getByText("Always"));
    expect(onForm).toHaveBeenLastCalledWith(expect.objectContaining({ replicate: 0 }));
  });

  it("flags Never on a load-protected source", async () => {
    function Contradictory() {
      const [form, setForm] = useState<SourceFormState>({
        ...FORM,
        replicate: -1,
        loadProtected: true,
      });
      return <SourceLoadManagementPanel form={form} setForm={setForm} />;
    }
    render(<Contradictory />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
    expect(
      await screen.findByText(
        "Never cannot be combined with load protection: a load-protected table is never read live.",
      ),
    ).toBeInTheDocument();
  });

  it("flags a sentinel path with an unsupported scheme", async () => {
    render(<Harness />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
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
    render(<NoTtl />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
    const msg = "The ttl change signal judges staleness by the Cache TTL, so set a Cache TTL.";
    expect(await screen.findByText(msg)).toBeInTheDocument();
    await userEvent.type(screen.getByTestId("cache-ttl-input"), "30");
    expect(screen.queryByText(msg)).toBeNull();
    await userEvent.clear(screen.getByTestId("cache-ttl-input"));
    expect(await screen.findByText(msg)).toBeInTheDocument();
  });

  it("does not error without a Cache TTL for a signal that has its own clock", async () => {
    render(
      <SourceLoadManagementPanel
        form={{ ...FORM, cacheTtl: "", changeSignal: "probe", freshnessGate: true }}
        setForm={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
    await screen.findByTestId("cache-ttl-input");
    expect(screen.queryByText(/judges staleness by the Cache TTL/)).toBeNull();
  });

  it("does not error for a live-read ttl source with no Cache TTL", async () => {
    render(<SourceLoadManagementPanel form={{ ...FORM, cacheTtl: "" }} setForm={vi.fn()} />);
    await userEvent.click(screen.getByTestId("source-load-management-panel-toggle"));
    await screen.findByTestId("cache-ttl-input");
    expect(screen.queryByText(/judges staleness by the Cache TTL/)).toBeNull();
  });
});
