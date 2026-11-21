// Copyright (c) 2026 Kenneth Stott
// Canary: 3f9c2b6e-8d41-4a7f-b5e3-1c7a9d2e6f08
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Tables field of Register Table, as the tests of the form reach it: the standard
// multi-select's open list, its options, and the reason an option that cannot be picked gives.

import { expect } from "vitest";
import userEvent from "@testing-library/user-event";
import { screen, waitFor } from "../../../test-utils/render";

export const tablesField = () => screen.getByRole("textbox", { name: "Tables" });
export const submitTables = () => screen.getByTestId("register-many-tables-submit");
export const tableOption = (name: string) =>
  screen.getByRole("option", { name: new RegExp(`^${name}`), hidden: true });
/** Whether the option is offered but cannot be picked. */
export const held = (name: string) => tableOption(name).hasAttribute("data-combobox-disabled");
export const registerAction = (name: string) => screen.queryByTestId(`option-action-${name}`);

/** Opens the Tables field's list once its tables are listed. */
export async function openTables() {
  const user = userEvent.setup();
  await waitFor(() => expect(tablesField()).not.toBeDisabled());
  await user.click(tablesField());
  return user;
}

/** The reason an option that cannot be picked shows on hover. */
export async function expectReason(
  user: ReturnType<typeof userEvent.setup>,
  name: string,
  reason: string,
) {
  await user.hover(screen.getByTestId(`option-not-held-${name}`));
  await waitFor(() =>
    expect(
      screen.getAllByRole("tooltip", { hidden: true }).map((tip) => tip.textContent),
    ).toContain(reason),
  );
}
