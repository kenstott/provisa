// Copyright (c) 2026 Kenneth Stott
// Canary: 50a9daab-5e7a-4ec2-8113-70a015da6c6e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A collapsible panel opens as this viewer left it: its state is kept per panel id and item, and
// restored when the panel is drawn again. A panel never opened or closed takes its default; where
// the browser keeps nothing, every panel takes its default and still opens and closes.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { CollapseAllIcons, CollapsibleItem, CollapsibleScope, Collapsibles } from "../Collapsible";
import { storageKey as scopedKey } from "../../../lib/browserStorage";

function panels(reveal?: string) {
  return render(
    <Collapsibles id="test.page" reveal={reveal}>
      <CollapsibleItem value="a" title="A">
        <span>content a</span>
      </CollapsibleItem>
      <CollapsibleItem value="b" title="B" defaultOpen={false}>
        <span>content b</span>
      </CollapsibleItem>
      <CollapsibleItem value="c" title="C" defaultOpen={false} forceOpen>
        <span>content c</span>
      </CollapsibleItem>
    </Collapsibles>,
  );
}

const expanded = (value: string) =>
  screen.getByTestId(`${value}-toggle`).getAttribute("aria-expanded");

afterEach(() => {
  window.localStorage.clear();
  vi.restoreAllMocks();
});

describe("Collapsibles", () => {
  it("takes each item's default the first time", () => {
    panels();
    expect([expanded("a"), expanded("b"), expanded("c")]).toEqual(["true", "false", "true"]);
  });

  it("opens again as it was left", () => {
    const first = panels();
    fireEvent.click(screen.getByTestId("a-toggle"));
    fireEvent.click(screen.getByTestId("b-toggle"));
    expect([expanded("a"), expanded("b")]).toEqual(["false", "true"]);
    first.unmount();
    panels();
    expect([expanded("a"), expanded("b")]).toEqual(["false", "true"]);
    expect(
      JSON.parse(window.localStorage.getItem(scopedKey("provisa.collapsibles")) ?? "{}"),
    ).toEqual({
      "test.page": { a: false, b: true, c: true },
    });
  });

  it("holds an item that needs attention open whatever was left", () => {
    window.localStorage.setItem(
      scopedKey("provisa.collapsibles"),
      JSON.stringify({ "test.page": { c: false } }),
    );
    panels();
    expect(expanded("c")).toBe("true");
  });

  it("opens the item a link asks for", () => {
    panels("b");
    expect(expanded("b")).toBe("true");
  });

  it("takes the defaults, and still opens and closes, where the browser keeps nothing", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    panels();
    expect([expanded("a"), expanded("b")]).toEqual(["true", "false"]);
    fireEvent.click(screen.getByTestId("b-toggle"));
    expect(expanded("b")).toBe("true");
  });
});

describe("expand all and collapse all", () => {
  it("a group of three or more has both icon buttons, which open and close every item", () => {
    panels();
    fireEvent.click(screen.getByTestId("test.page-collapse-all"));
    expect([expanded("a"), expanded("b"), expanded("c")]).toEqual(["false", "false", "true"]);
    fireEvent.click(screen.getByTestId("test.page-expand-all"));
    expect([expanded("a"), expanded("b"), expanded("c")]).toEqual(["true", "true", "true"]);
    expect(
      JSON.parse(window.localStorage.getItem(scopedKey("provisa.collapsibles")) ?? "{}")[
        "test.page"
      ],
    ).toEqual({ a: true, b: true, c: true });
  });

  it("a group of two has neither", () => {
    render(
      <Collapsibles id="two">
        <CollapsibleItem value="a" title="A">
          x
        </CollapsibleItem>
        <CollapsibleItem value="b" title="B">
          y
        </CollapsibleItem>
      </Collapsibles>,
    );
    expect(screen.queryByTestId("two-expand-all")).toBeNull();
    expect(screen.queryByTestId("two-collapse-all")).toBeNull();
  });

  it("a scope's icons open and close the groups inside it", () => {
    render(
      <CollapsibleScope id="scope">
        <CollapseAllIcons />
        {["a", "b"].map((v) => (
          <Collapsibles key={v} id={`scope.${v}`}>
            <CollapsibleItem value={v} title={v}>
              x
            </CollapsibleItem>
          </Collapsibles>
        ))}
      </CollapsibleScope>,
    );
    fireEvent.click(screen.getByTestId("scope-collapse-all"));
    expect([expanded("a"), expanded("b")]).toEqual(["false", "false"]);
    fireEvent.click(screen.getByTestId("scope-expand-all"));
    expect([expanded("a"), expanded("b")]).toEqual(["true", "true"]);
  });
});
