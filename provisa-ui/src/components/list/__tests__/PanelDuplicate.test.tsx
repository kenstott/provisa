// Copyright (c) 2026 Kenneth Stott
// Canary: 7d846e07-ab58-4505-b805-7a30661f934e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A detail panel's Duplicate: an icon in the panel's head, beside Edit and Delete, named
// "Duplicate", its tip naming the object; pressing it asks the page to open its create form
// prefilled from the object.

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../../test-utils/render";
import { PanelHeadStyle } from "../PanelHeadStyle";
import { PanelDuplicate } from "../PanelDuplicate";

describe("PanelDuplicate", () => {
  it("is an icon named Duplicate, its tip naming the object", async () => {
    render(
      <PanelHeadStyle>
        <PanelDuplicate name="sales-pg" onDuplicate={() => {}} testId="thing-duplicate" />
      </PanelHeadStyle>,
    );
    const action = screen.getByTestId("thing-duplicate");
    expect(action).toHaveAccessibleName("Duplicate");
    expect(action).toHaveTextContent("");
    expect(action.querySelector("svg.lucide-copy")).not.toBeNull();
    // The head's one style: borderless, no variant of its own.
    expect(action).toHaveAttribute("data-variant", "subtle");
    await userEvent.hover(action);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Duplicate sales-pg");
  });

  it("asks the page to duplicate, and nothing behind the head hears the press", async () => {
    const onDuplicate = vi.fn();
    const behind = vi.fn();
    render(
      <div onClick={behind}>
        <PanelDuplicate name="sales-pg" onDuplicate={onDuplicate} testId="thing-duplicate" />
      </div>,
    );
    await userEvent.click(screen.getByTestId("thing-duplicate"));
    expect(onDuplicate).toHaveBeenCalledTimes(1);
    expect(behind).not.toHaveBeenCalled();
  });
});
