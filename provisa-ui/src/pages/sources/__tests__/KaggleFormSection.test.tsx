// Copyright (c) 2026 Kenneth Stott
// Canary: 167f6c6b-6dd5-428e-9826-7affba1dcc53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1783, REQ-1989: a search replaced by a newer one answers nothing, so what the picker lists
// stays as it was until the newer search answers.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";

const search = vi.fn();

vi.mock("../../../hooks/useAdminQueries", () => ({
  useCreateSource: () => ({ createSource: vi.fn() }),
  useKaggleDatasetsLazy: () => search,
  useKaggleTokenValidLazy: () => async () => true,
  useStageKaggleDataset: () => ({ stageKaggleDataset: vi.fn() }),
}));

import { FormLayout } from "../../../components/form/FormLayout";
import { KaggleFormSection } from "../KaggleFormSection";

const IRIS = { ref: "arshid/iris-flower-dataset", title: "Iris", subtitle: "" };

describe("KaggleFormSection, a search replaced by a newer one", () => {
  it("keeps the listed datasets when a search answers null", async () => {
    search.mockResolvedValueOnce([IRIS]).mockResolvedValueOnce(null);
    render(
      <MantineProvider>
        <MemoryRouter>
          <FormLayout>
            <KaggleFormSection onSourcesRegistered={vi.fn()} />
          </FormLayout>
        </MemoryRouter>
      </MantineProvider>,
    );

    fireEvent.change(screen.getByTestId("kaggle-token-input"), { target: { value: "t" } });
    fireEvent.click(screen.getByTestId("kaggle-validate-token-button"));
    const input = await screen.findByTestId("kaggle-dataset-search-input");
    fireEvent.focus(input);
    expect(await screen.findByTestId(`kaggle-dataset-${IRIS.ref}`)).toBeTruthy();

    fireEvent.change(input, { target: { value: "iris flower" } });
    await waitFor(() => expect(search).toHaveBeenCalledTimes(2));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(screen.getByTestId(`kaggle-dataset-${IRIS.ref}`)).toBeTruthy();
  });
});
