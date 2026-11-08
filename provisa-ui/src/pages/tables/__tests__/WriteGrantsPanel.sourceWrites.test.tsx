// Copyright (c) 2026 Kenneth Stott
// Canary: 3f0b6a52-91de-4c53-8a47-0d2f6c1b7e18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Write grants tab refers to the writes the source accepts (the API tab): a source that
// accepts none gets a note in place of an editor, and one that accepts some offers only those.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import "../../../i18n";
import { MantineProvider } from "@mantine/core";
import { WriteGrantsPanel } from "../WriteGrantsPanel";

function panel(
  sourceWriteOps: string[],
  onSave?: () => Promise<{ success: boolean; message: string }>,
) {
  render(
    <MantineProvider>
      <WriteGrantsPanel
        grants={[]}
        columns={["id"]}
        sourceWriteOps={sourceWriteOps}
        roles={[{ id: "clerk", label: "clerk" }]}
        onSave={onSave}
        kept={false}
      />
    </MantineProvider>,
  );
}

describe("WriteGrantsPanel — the writes the source accepts", () => {
  it("says the source accepts no writes instead of offering grants", () => {
    panel([], vi.fn());
    expect(screen.getByTestId("write-grants-source-none")).toHaveTextContent(
      "The source accepts no writes",
    );
    expect(screen.queryByTestId("write-grant-add")).toBeNull();
  });

  it("says so when read only, too", () => {
    panel([]);
    expect(screen.getByTestId("write-grants-source-none")).toBeInTheDocument();
  });

  it("offers the editor when the source accepts writes", () => {
    panel(["insert"], vi.fn());
    fireEvent.click(screen.getByTestId("write-grant-add"));
    expect(screen.queryByTestId("write-grants-source-none")).toBeNull();
    expect(screen.getByTestId("write-grant-0")).toBeInTheDocument();
  });
});
