// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A page that throws while rendering says so in its own place, with the error, instead of
// taking the whole app down with it: the nav, the rail and the chat stay, and going to another
// page renders that page afresh.

import { Component, type ErrorInfo, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useLocation } from "react-router-dom";
import { Alert, Code, Stack, Text } from "@mantine/core";

function PageFailed({ error }: { error: Error }) {
  const { t } = useTranslation();
  return (
    <Alert color="red" title={t("pageError.title")} m="md" data-testid="page-error">
      <Stack gap="xs">
        <Text size="sm">{t("pageError.body")}</Text>
        <Code block data-testid="page-error-message">
          {error.message}
        </Code>
      </Stack>
    </Alert>
  );
}

class Boundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state: { error: Error | null } = { error: null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Kept in the console with where it was thrown, as an uncaught error would have been.
    console.error(error, info.componentStack);
  }

  render() {
    return this.state.error ? <PageFailed error={this.state.error} /> : this.props.children;
  }
}

/** Each page under its own boundary: a new path is a new boundary, so leaving a page that failed
 * renders the next one. */
export function PageErrorBoundary({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  return <Boundary key={pathname}>{children}</Boundary>;
}
