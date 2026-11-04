// Copyright (c) 2026 Kenneth Stott
// Canary: 8a41d6e2-5b7f-4c09-93e1-2f6d0c4b7a58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A page's own (?) help: opened by hovering or focusing it (and by a click), named for a screen
// reader, kept open while the pointer is over the icon or the card, closed by Escape.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { PageHelp, PAGE_HELP_CLOSE_DELAY_MS } from "../components/PageHelp";

function renderHelp() {
  return render(
    <PageHelp
      title="About this page"
      paragraphs={["First.", "Second."]}
      ariaLabel="About this page"
      testId="page-help"
    />,
  );
}

describe("PageHelp", () => {
  it("is a button named by its label, closed until clicked", () => {
    renderHelp();
    expect(screen.getByRole("button", { name: "About this page" })).toBe(
      screen.getByTestId("page-help"),
    );
    expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument();
  });

  it("opens on a click with its title and paragraphs", async () => {
    renderHelp();
    fireEvent.click(screen.getByTestId("page-help"));
    const card = await screen.findByTestId("page-help-card");
    expect(card).toHaveTextContent("About this page");
    expect(card).toHaveTextContent("First.");
    expect(card).toHaveTextContent("Second.");
    expect(screen.getByTestId("page-help")).toHaveAttribute("aria-expanded", "true");
  });

  it("closes on Escape", async () => {
    renderHelp();
    fireEvent.click(screen.getByTestId("page-help"));
    const card = await screen.findByTestId("page-help-card");
    fireEvent.keyDown(card, { key: "Escape" });
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
    expect(screen.getByTestId("page-help")).toHaveAttribute("aria-expanded", "false");
  });

  it("closes on a second click of its button", async () => {
    renderHelp();
    fireEvent.click(screen.getByTestId("page-help"));
    await screen.findByTestId("page-help-card");
    fireEvent.click(screen.getByTestId("page-help"));
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
  });

  it("opens when the pointer enters the icon", async () => {
    renderHelp();
    fireEvent.mouseEnter(screen.getByTestId("page-help"));
    const card = await screen.findByTestId("page-help-card");
    expect(card).toHaveTextContent("First.");
  });

  it("stays open while the pointer moves from the icon onto the card", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    const card = await screen.findByTestId("page-help-card");
    fireEvent.mouseLeave(icon);
    fireEvent.mouseEnter(card);
    await new Promise((r) => setTimeout(r, 400));
    expect(screen.getByTestId("page-help-card")).toBeInTheDocument();
  });

  it("closes after a short delay once the pointer leaves both", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    const card = await screen.findByTestId("page-help-card");
    fireEvent.mouseLeave(icon);
    fireEvent.mouseEnter(card);
    fireEvent.mouseLeave(card);
    // Not at once: the pointer may be crossing the gap between icon and card.
    expect(screen.getByTestId("page-help-card")).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
    expect(icon).toHaveAttribute("aria-expanded", "false");
  });

  it("opens on keyboard focus, and closes on blur", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    const matches = vi.spyOn(icon, "matches").mockImplementation((s) => s === ":focus-visible");
    fireEvent.focus(icon);
    await screen.findByTestId("page-help-card");
    fireEvent.blur(icon);
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
    matches.mockRestore();
  });

  it("does not open on the focus a mouse press gives, so the click still toggles it", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.focus(icon);
    expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument();
    fireEvent.click(icon);
    await screen.findByTestId("page-help-card");
  });

  it("closes on Escape after it opened on hover", async () => {
    renderHelp();
    fireEvent.mouseEnter(screen.getByTestId("page-help"));
    const card = await screen.findByTestId("page-help-card");
    fireEvent.keyDown(card, { key: "Escape" });
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
  });

  it("stays open after the pointer leaves once a click pinned the card the hover opened", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    await screen.findByTestId("page-help-card");
    fireEvent.click(icon);
    fireEvent.mouseLeave(icon);
    await new Promise((r) => setTimeout(r, PAGE_HELP_CLOSE_DELAY_MS + 250));
    expect(screen.getByTestId("page-help-card")).toBeInTheDocument();
    expect(icon).toHaveAttribute("aria-expanded", "true");
  });

  it("stays open after the pointer leaves the card of a card pinned by a click", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    const card = await screen.findByTestId("page-help-card");
    fireEvent.click(icon);
    fireEvent.mouseLeave(icon);
    fireEvent.mouseEnter(card);
    fireEvent.mouseLeave(card);
    await new Promise((r) => setTimeout(r, PAGE_HELP_CLOSE_DELAY_MS + 250));
    expect(screen.getByTestId("page-help-card")).toBeInTheDocument();
  });

  it("closes on a second click of the icon when the card is pinned", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    await screen.findByTestId("page-help-card");
    fireEvent.click(icon);
    fireEvent.mouseLeave(icon);
    fireEvent.mouseEnter(icon);
    fireEvent.click(icon);
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
    // Unpinned: the next hover opens it and leaving closes it again.
    fireEvent.mouseEnter(icon);
    await screen.findByTestId("page-help-card");
    fireEvent.mouseLeave(icon);
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
  });

  it("closes on Escape when the card is pinned", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    const card = await screen.findByTestId("page-help-card");
    fireEvent.click(icon);
    fireEvent.mouseLeave(icon);
    fireEvent.keyDown(card, { key: "Escape" });
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
  });

  it("closes on a click outside when the card is pinned", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.mouseEnter(icon);
    await screen.findByTestId("page-help-card");
    fireEvent.click(icon);
    fireEvent.mouseLeave(icon);
    fireEvent.mouseDown(document.body);
    fireEvent.mouseUp(document.body);
    await waitFor(() => expect(screen.queryByTestId("page-help-card")).not.toBeInTheDocument());
  });

  it("a click while closed opens it pinned, so it stays open after the pointer leaves", async () => {
    renderHelp();
    const icon = screen.getByTestId("page-help");
    fireEvent.click(icon);
    await screen.findByTestId("page-help-card");
    fireEvent.mouseLeave(icon);
    await new Promise((r) => setTimeout(r, PAGE_HELP_CLOSE_DELAY_MS + 250));
    expect(screen.getByTestId("page-help-card")).toBeInTheDocument();
  });
});
