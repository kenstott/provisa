// Copyright (c) 2026 Kenneth Stott
// Canary: 35feeb06-2828-43a4-aae3-6fb6ea58a9da
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list's search box remembers what the reader typed, in the one place every list's box goes
// through: after a remount it reads as it was left. A deep link decides its own visit and is not
// saved over; a list that keeps its search itself opts out.

import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { getStored, setStored } from "../../../lib/browserStorage";
import { FilterInput } from "../FilterInput";

function Page({
  initial = "",
  testId,
  remember,
}: {
  initial?: string;
  testId?: string;
  remember?: string | false;
}) {
  const [value, setValue] = useState(initial);
  return (
    <>
      <FilterInput value={value} onChange={setValue} testId={testId} remember={remember} />
      <output data-testid="page-value">{value}</output>
    </>
  );
}

const box = (id = "filter-input") => screen.getByTestId(id) as HTMLInputElement;
const type = (text: string, id?: string) => fireEvent.change(box(id), { target: { value: text } });

afterEach(() => {
  vi.restoreAllMocks();
  window.history.replaceState(null, "", "/");
});

describe("a list's search box", () => {
  it("is restored after a remount, and the page has it too", () => {
    const view = render(<Page />);
    type("orders");
    view.unmount();
    render(<Page />);
    expect(box().value).toBe("orders");
    expect(screen.getByTestId("page-value")).toHaveTextContent("orders");
  });

  it("stays empty once cleared", () => {
    const view = render(<Page />);
    type("orders");
    fireEvent.click(screen.getByTestId("filter-input-clear"));
    view.unmount();
    render(<Page />);
    expect(box().value).toBe("");
  });

  it("is per page and per list", () => {
    setStored("filter./tables.filter-input", "orders");
    setStored("filter./other.filter-input", "not this page");
    window.history.replaceState(null, "", "/tables");
    render(<Page />);
    expect(box().value).toBe("orders");
    render(<Page testId="second" />);
    expect(box("second").value).toBe("");
  });

  it("takes a name where two lists on a page share a test id", () => {
    setStored("filter./.invites", "bob");
    render(<Page remember="invites" />);
    expect(box().value).toBe("bob");
  });

  it("yields to a deep link, and does not save over what was remembered", () => {
    setStored("filter./tables.filter-input", "orders");
    window.history.replaceState(null, "", "/tables?table=7");
    render(<Page />);
    expect(box().value).toBe("");
    expect(getStored("filter./tables.filter-input")).toBe("orders");
  });

  it("yields to a value the page opened with, and does not save over what was remembered", () => {
    setStored("filter./.filter-input", "orders");
    render(<Page initial="from a link" />);
    expect(box().value).toBe("from a link");
    expect(getStored("filter./.filter-input")).toBe("orders");
  });

  it("neither reads nor saves where the list keeps its own", () => {
    setStored("filter./.filter-input", "orders");
    render(<Page remember={false} />);
    expect(box().value).toBe("");
    type("mine");
    expect(getStored("filter./.filter-input")).toBe("orders");
  });

  it("still filters where storage is blocked", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    render(<Page />);
    type("orders");
    expect(screen.getByTestId("page-value")).toHaveTextContent("orders");
  });
});
