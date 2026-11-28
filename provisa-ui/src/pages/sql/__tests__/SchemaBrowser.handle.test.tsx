// Copyright (c) 2026 Kenneth Stott
// Canary: 2b8f4d61-7a3e-4c95-b0d2-9e6a1f5c3b78
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the SQL schema tree's right edge is the side panel's splitter. The collapse tab beside
// it stands outside that edge only (13px wide, no Mantine ActionIcon minimum of 28px pulling it
// back over the edge), so a press at the splitter's middle reaches the splitter and a drag resizes
// the tree.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { SchemaBrowser } from "../SchemaBrowser";

function renderBrowser() {
  render(
    <SchemaBrowser
      sidebarOpen
      setSidebarOpen={vi.fn()}
      domainGroups={{}}
      domainMap={{}}
      expandedDomains={new Set()}
      expandedTables={new Set()}
      domainPages={{}}
      topTab="sql"
      insertAtCursor={vi.fn()}
      toggleDomain={vi.fn()}
      toggleTable={vi.fn()}
      setDomainPages={vi.fn()}
      metrics={[]}
      tables={[]}
    />,
  );
}

describe("SchemaBrowser side splitter", () => {
  beforeEach(() => localStorage.clear());

  it("keeps the collapse tab to its 13px outside the edge, clear of the splitter", () => {
    renderBrowser();
    const tab = screen.getByTestId("schema-browser-toggle");
    expect(tab.style.insetInlineEnd).toBe("-13px");
    expect(tab.style.width).toBe("13px");
    // Mantine's ActionIcon has min-width/min-height 28px; without these the tab grows back
    // 15px over the tree and covers the splitter's middle.
    expect(tab.style.minWidth).toBe("0px");
    expect(tab.style.minHeight).toBe("0px");
  });

  it("follows the pointer when the splitter is dragged", () => {
    renderBrowser();
    const handle = screen.getByTestId("sql-schema-side-handle");
    const pane = handle.previousElementSibling as HTMLElement;
    pane.getBoundingClientRect = () =>
      ({ left: 0, right: 240, top: 0, bottom: 600, width: 240, height: 600 }) as DOMRect;
    fireEvent.pointerDown(handle, { clientX: 240, clientY: 300 });
    fireEvent.pointerMove(window, { clientX: 300, clientY: 300 });
    fireEvent.pointerUp(window, { clientX: 300, clientY: 300 });
    expect(pane.style.width).toBe("300px");
  });
});
