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
import { MemoryRouter, type MemoryRouterProps } from "react-router-dom";
import { MockedProvider } from "@apollo/client/testing/react";
import type { MockedResponse } from "@apollo/client/testing";
import { theme } from "../theme/theme";
import i18n from "../i18n";
import { PollyProvider } from "../context/PollyContext";
import { FormLayout } from "../components/form/FormLayout";
import { SuggestColumnAliases } from "../hooks/admin.graphql";
import { toSnakeCase } from "../naming";

// REQ-2361: the admin's column alias suggestion (naming.suggest_column_alias: a column's snake-case
// name, or null where it is already so named), answered here as the server answers it so every
// form that previews columns gets its suggestions. namingVectors.test.ts pins toSnakeCase to the
// server's rule. A test asserting on a suggestion passes its own mock first.
const COLUMN_ALIAS_SUGGESTIONS: MockedResponse<
  { suggestColumnAliases: (string | null)[] },
  { columns: string[] }
> = {
  request: { query: SuggestColumnAliases, variables: () => true },
  maxUsageCount: Number.POSITIVE_INFINITY,
  result: ({ columns }) => ({
    data: {
      suggestColumnAliases: columns.map((c) => {
        const snake = toSnakeCase(c);
        return snake !== c ? snake : null;
      }),
    },
  }),
};

// Wraps components in the same MantineProvider + i18n runtime the app uses so
// component tests exercise real theming and translated strings (REQ-1016). The
// MockedProvider satisfies Apollo hooks (e.g. the REQ-1143 refresh-policy
// preview) that need a client in context; unmocked operations simply never
// resolve. Callers that assert on a hook's data pass `mocks` so the real hook
// resolves through Apollo rather than needing a leak-prone module mock.
// MemoryRouter satisfies REQ-1387's useSearchParams deep-linking. `initialEntries` lets a
// caller seed the route/query-string a test depends on instead of nesting its own Router.
function makeWrapper(
  mocks: readonly MockedResponse[],
  initialEntries?: MemoryRouterProps["initialEntries"],
  inForm = false,
) {
  return function AllProviders({ children }: { children: ReactNode }) {
    return (
      <MockedProvider mocks={[...mocks, COLUMN_ALIAS_SUGGESTIONS]}>
        <MantineProvider theme={theme} defaultColorScheme="dark">
          <I18nextProvider i18n={i18n}>
            <MemoryRouter initialEntries={initialEntries}>
              <PollyProvider>
                {inForm ? <FormLayout>{children}</FormLayout> : children}
              </PollyProvider>
            </MemoryRouter>
          </I18nextProvider>
        </MantineProvider>
      </MockedProvider>
    );
  };
}

export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, "wrapper"> & {
    mocks?: readonly MockedResponse[];
    initialEntries?: MemoryRouterProps["initialEntries"];
  },
) {
  const { mocks = [], initialEntries, ...renderOptions } = options ?? {};
  return render(ui, { wrapper: makeWrapper(mocks, initialEntries), ...renderOptions });
}

/**
 * Renders a section of a form on its own. A form's fields live in the shared form layout
 * (REQ-1968) and refuse to render outside one, so a section tested alone is given one.
 */
export function renderInForm(
  ui: ReactElement,
  options?: Omit<RenderOptions, "wrapper"> & {
    mocks?: readonly MockedResponse[];
    initialEntries?: MemoryRouterProps["initialEntries"];
  },
) {
  const { mocks = [], initialEntries, ...renderOptions } = options ?? {};
  return render(ui, { wrapper: makeWrapper(mocks, initialEntries, true), ...renderOptions });
}

export * from "@testing-library/react";
export { renderWithProviders as render };
