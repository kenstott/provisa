// Copyright (c) 2026 Kenneth Stott
// Canary: c69c8463-a919-4d3d-9deb-355db06d6e31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReactElement, ReactNode } from "react";
import { render, type RenderOptions } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { I18nextProvider } from "react-i18next";
import { MockedProvider } from "@apollo/client/testing/react";
import type { MockedResponse } from "@apollo/client/testing";
import { theme } from "../theme/theme";
import i18n from "../i18n";

// Wraps components in the same MantineProvider + i18n runtime the app uses so
// component tests exercise real theming and translated strings (REQ-1016). The
// MockedProvider satisfies Apollo hooks (e.g. the REQ-1143 refresh-policy
// preview) that need a client in context; unmocked operations simply never
// resolve. Callers that assert on a hook's data pass `mocks` so the real hook
// resolves through Apollo rather than needing a leak-prone module mock.
function makeWrapper(mocks: readonly MockedResponse[]) {
  return function AllProviders({ children }: { children: ReactNode }) {
    return (
      <MockedProvider mocks={mocks}>
        <MantineProvider theme={theme} defaultColorScheme="dark">
          <I18nextProvider i18n={i18n}>{children}</I18nextProvider>
        </MantineProvider>
      </MockedProvider>
    );
  };
}

export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, "wrapper"> & { mocks?: readonly MockedResponse[] },
) {
  const { mocks = [], ...renderOptions } = options ?? {};
  return render(ui, { wrapper: makeWrapper(mocks), ...renderOptions });
}

export * from "@testing-library/react";
export { renderWithProviders as render };
