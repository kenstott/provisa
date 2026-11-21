// Copyright (c) 2026 Kenneth Stott
// Canary: 34cb076d-2b98-407e-9e5c-efbd9bf6707a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1949, REQ-2193

// The rows of an OData read in the explorer's results grid, a page at a time, with what the answer
// says about itself above them: a row limit that cut it first among those.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Loader, Text } from "@mantine/core";
import { AlertTriangle, ChevronLeft, ChevronRight } from "lucide-react";
import type { StatementWarning } from "../../api/admin";
import { serverMessage } from "../../i18n/serverMessage";
import { ResultsGrid } from "../sql/ResultsGrid";
import { useResultsGrid } from "../sql/useResultsGrid";
import type { PagedRead } from "./usePagedRead";
import { gridRow, rowsCut } from "./answer";

function Warnings({ warnings }: { warnings: StatementWarning[] }) {
  const { t } = useTranslation();
  if (warnings.length === 0) return null;
  return (
    <Alert
      color="yellow"
      icon={<AlertTriangle size={16} />}
      title={rowsCut(warnings) ? t("odataPage.rowsCutTitle") : t("odataPage.warningsTitle")}
      data-testid="odata-warnings"
    >
      {warnings.map((w, i) => (
        <Text key={i} size="sm" data-testid={`odata-warning-${w.code}`}>
          {serverMessage(w, w.message)}
        </Text>
      ))}
    </Alert>
  );
}

export function ODataResults({ read, setName }: { read: PagedRead; setName: string }) {
  const { t } = useTranslation();
  const { page, offset, loading, error, urls } = read;
  const rows = useMemo(() => (page ? page.rows.map(gridRow) : []), [page]);
  const grid = useResultsGrid(rows, page?.columns ?? []);

  if (error) {
    return (
      <Alert color="red" data-testid="odata-error">
        {error}
      </Alert>
    );
  }
  if (!page) {
    return loading ? <Loader size="sm" data-testid="odata-loading" /> : null;
  }
  const range =
    page.rows.length === 0
      ? t("odataPage.noRows")
      : page.count !== null
        ? t("odataPage.rowRangeOfTotal", {
            start: offset + 1,
            end: offset + page.rows.length,
            total: page.count,
          })
        : t("odataPage.rowRange", { start: offset + 1, end: offset + page.rows.length });
  return (
    <div className="odata-results" data-testid="odata-results">
      <Warnings warnings={page.warnings} />
      <Group gap="xs" wrap="nowrap">
        <Button
          size="xs"
          variant="default"
          leftSection={<ChevronLeft size={14} />}
          disabled={loading || urls.length < 2}
          onClick={read.previous}
          data-testid="odata-previous"
        >
          {t("odataPage.previousPage")}
        </Button>
        <Button
          size="xs"
          variant="default"
          rightSection={<ChevronRight size={14} />}
          disabled={loading || page.nextLink === null}
          onClick={read.next}
          data-testid="odata-next"
        >
          {t("odataPage.nextPage")}
        </Button>
        <Text size="sm" c="dimmed" data-testid="odata-range">
          {range}
        </Text>
        {loading && <Loader size="xs" />}
      </Group>
      {page.rows.length > 0 && (
        <div className="odata-grid">
          <ResultsGrid
            grid={grid}
            totalRowCount={rows.length}
            rowsPartial={rowsCut(page.warnings) || page.nextLink !== null}
            exportName={setName}
            provenance={[{ label: t("odataPage.provenanceUrl"), value: urls[urls.length - 1] }]}
          />
        </div>
      )}
    </div>
  );
}
