// Copyright (c) 2026 Kenneth Stott
// Canary: 30d36a65-ace2-4af0-935b-80e3fc0392d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A detail panel's read view shows every value of its object, under the labels its edit form
// gives them. This guard reads each panel's edit form and read view and fails on a field the form
// labels that the read view does not: a value that can be set but not seen.

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const SRC = resolve(__dirname, "../../..");

interface Panel {
  /** The files that draw the read view. */
  read: string[];
  /** The files that draw the edit form's fields. */
  form: string[];
  /** A form label the read view does not show as a field, with the reason. */
  notAField?: Readonly<Record<string, string>>;
}

/** Every list page's detail panel, by page. */
const PANELS: Readonly<Record<string, Panel>> = {
  Relationships: {
    read: ["components/relationships/RelationshipRead.tsx"],
    form: [
      "components/relationships/RelationshipDetail.tsx",
      "components/relationships/JunctionPanel.tsx",
    ],
  },
  "Security › Roles": {
    read: ["pages/security/RoleRead.tsx"],
    form: ["pages/SecurityPage.tsx", "components/admin/ResidencyGrant.tsx"],
    notAField: {
      "residencyGrant.right": "the data_residency capability's tick: shown in Capabilities",
    },
  },
  "Commands (functions and webhooks)": {
    read: ["pages/commands/CommandRead.tsx"],
    form: [
      "pages/CommandsPage.tsx",
      "pages/commands/CommandFormFields.tsx",
      "pages/commands/ColumnGovernanceFields.tsx",
    ],
  },
  "Data Products": {
    read: ["pages/data-products/DataProductDetailPanel.tsx"],
    form: ["pages/data-products/DataProductFormCard.tsx"],
  },
  // Edited in place: the panel is the form, and the fields it does not edit are listed above it.
  Glossary: {
    read: ["components/admin/GlossaryTab.tsx", "components/admin/GlossaryRelationships.tsx"],
    form: ["components/admin/GlossaryTab.tsx", "components/admin/GlossaryRelationships.tsx"],
  },
  Metrics: {
    read: ["pages/metrics/MetricDetailPanel.tsx"],
    form: ["pages/MetricsPage.tsx"],
    notAField: {
      "metricsPage.builderMeasure": "a step of the builder that writes the expression: shown in it",
      "metricsPage.builderAgg": "a step of the builder that writes the expression: shown in it",
    },
  },
  "Security › RLS Rules": {
    read: ["pages/security/RlsRuleRead.tsx"],
    form: ["pages/security/SecurityRlsPage.tsx"],
  },
  // Read as its edit form drawn read-only (FormRead): the same files draw both.
  Sources: {
    read: [
      "pages/sources/AthenaFields.tsx",
      "pages/sources/AuthUserPass.tsx",
      "pages/sources/CloudopsFields.tsx",
      "pages/sources/CosmosDbFields.tsx",
      "pages/sources/GoogleWorkspaceFields.tsx",
      "pages/sources/IcebergCatalogFields.tsx",
      "pages/sources/KaggleFormSection.tsx",
      "pages/sources/MailSinceField.tsx",
      "pages/sources/Microsoft365Fields.tsx",
      "pages/sources/ObjectStoreCredentialFields.tsx",
      "pages/sources/OpenApiFormSection.tsx",
      "pages/sources/ProfilerFormSection.tsx",
      "pages/sources/PushFeedFormSection.tsx",
      "pages/sources/QdrantFields.tsx",
      "pages/sources/SalesforceFields.tsx",
      "pages/sources/SchemaRegistryAuthFields.tsx",
      "pages/sources/SharepointFields.tsx",
      "pages/sources/SourceDetailPanel.tsx",
      "pages/sources/SourceFormFields.tsx",
      "pages/sources/SourceFormFieldsExtended.tsx",
      "pages/sources/SourceLoadManagementPanel.tsx",
      "pages/sources/SparqlFormSection.tsx",
      "pages/sources/SplunkFields.tsx",
      "pages/sources/WikipediaFields.tsx",
      "pages/SourcesPage.tsx",
    ],
    form: [
      "pages/sources/AthenaFields.tsx",
      "pages/sources/AuthUserPass.tsx",
      "pages/sources/CloudopsFields.tsx",
      "pages/sources/CosmosDbFields.tsx",
      "pages/sources/GoogleWorkspaceFields.tsx",
      "pages/sources/IcebergCatalogFields.tsx",
      "pages/sources/KaggleFormSection.tsx",
      "pages/sources/MailSinceField.tsx",
      "pages/sources/Microsoft365Fields.tsx",
      "pages/sources/ObjectStoreCredentialFields.tsx",
      "pages/sources/OpenApiFormSection.tsx",
      "pages/sources/ProfilerFormSection.tsx",
      "pages/sources/PushFeedFormSection.tsx",
      "pages/sources/QdrantFields.tsx",
      "pages/sources/SalesforceFields.tsx",
      "pages/sources/SchemaRegistryAuthFields.tsx",
      "pages/sources/SharepointFields.tsx",
      "pages/sources/SourceDetailPanel.tsx",
      "pages/sources/SourceFormFields.tsx",
      "pages/sources/SourceFormFieldsExtended.tsx",
      "pages/sources/SourceLoadManagementPanel.tsx",
      "pages/sources/SparqlFormSection.tsx",
      "pages/sources/SplunkFields.tsx",
      "pages/sources/WikipediaFields.tsx",
      "pages/SourcesPage.tsx",
    ],
  },
  "Tables and Views": {
    read: [
      "pages/tables/TableReadView.tsx",
      "pages/tables/TableSectionsRead.tsx",
      "pages/tables/tableSectionFields.tsx",
      "pages/tables/TableAssociations.tsx",
      "pages/tables/loadReadFields.tsx",
      "pages/tables/queryReadFields.tsx",
      "pages/tables/qualityReadFields.tsx",
      "pages/tables/WriteGrantsPanel.tsx",
    ],
    form: [
      "pages/tables/TableEditForm.tsx",
      // REQ-2109: the editor's sections, shared with the create form.
      "pages/tables/TableIdentitySection.tsx",
      "pages/tables/TableAssociationsSection.tsx",
      "pages/tables/TableLoadSection.tsx",
      "pages/tables/TableQuerySection.tsx",
      "pages/tables/TableQualitySection.tsx",
      "pages/tables/TableColumnsSection.tsx",
      "pages/tables/TableLoadManagementFields.tsx",
      "pages/tables/TablePlacementFields.tsx",
      "pages/tables/LiveDeliveryFieldset.tsx",
      "pages/tables/DeltaFieldset.tsx",
      "pages/tables/MaterializedViewPanels.tsx",
      // REQ-2109: the one Time Travel section, and the three columns it maps.
      "pages/tables/TimeTravelSection.tsx",
      "pages/tables/BitemporalFields.tsx",
      "pages/tables/SnapshotSchedulePanel.tsx",
      "pages/tables/PagingField.tsx",
      "pages/tables/RecurrenceBuilder.tsx",
      "pages/tables/ViewDefinitionForm.tsx",
      "pages/tables/RegisterTableForm.tsx",
      "pages/tables/FilesGlobFieldset.tsx",
      "pages/tables/ModelingForm.tsx",
      "pages/tables/ProfilerPanel.tsx",
      "pages/tables/DataQualityPanel.tsx",
      "components/admin/UniquesPanel.tsx",
      // REQ-2074: the Writes section reads and edits its grants in one component.
      "pages/tables/WriteGrantsPanel.tsx",
      "components/admin/OffPeakWindowFields.tsx",
      "components/admin/ReplicateSelect.tsx",
      "components/admin/RegionSelect.tsx",
      "pages/tables/ColumnsTable.tsx",
      "pages/tables/testdata/TestDataColumns.tsx",
      "pages/tables/CalendarCreateModal.tsx",
      "pages/tables/ProfileConstraintsModal.tsx",
      "pages/tables/NlTableSearch.tsx",
      "pages/tables/DescriptionField.tsx",
    ],
    notAField: {
      "tableEditForm.loadManagementHelp": "a hint beside the load fields, not a value",
      "recurrence.dayOfMonth":
        "a step of the builder that writes the recurrence: shown as Recurrence",
      "recurrence.frequency":
        "a step of the builder that writes the recurrence: shown as Recurrence",
      "recurrence.interval":
        "a step of the builder that writes the recurrence: shown as Recurrence",
      "recurrence.month": "a step of the builder that writes the recurrence: shown as Recurrence",
      "recurrence.ordinal": "a step of the builder that writes the recurrence: shown as Recurrence",
      "recurrence.weekday": "a step of the builder that writes the recurrence: shown as Recurrence",
      "viewDefinitionForm.fieldName":
        "the view's name and domain: shown in Identity as Table and Domain",
      "viewDefinitionForm.fieldDomain":
        "the view's name and domain: shown in Identity as Table and Domain",
      "registerTableForm.aliasLabel": "registration's SQL alias: shown in Identity as SQL Alias",
      "registerTableForm.descriptionLabel":
        "registration's description: shown in Identity as Description",
      "registerTableForm.checkReasonLabel":
        "a checker's claim, its registration's description: shown in Identity as Description",
      "registerTableForm.cacheTtlLabel":
        "registration's cache TTL: shown in Load and timeliness as Cache TTL",
      "registerTableForm.neo4jTableNameLabel": "a neo4j table's name: shown in Identity as Table",
      "registerTableForm.discoverLabel":
        "a registration act that reads the source's columns, not a value",
      "modelingForm.attributesLabel":
        "a create step: the model is lowered to a view, read as that view",
      "modelingForm.domainLabel":
        "a create step: the model is lowered to a view, read as that view",
      "modelingForm.grainLabel": "a create step: the model is lowered to a view, read as that view",
      "modelingForm.kindLabel": "a create step: the model is lowered to a view, read as that view",
      "modelingForm.measuresLabel":
        "a create step: the model is lowered to a view, read as that view",
      "modelingForm.nameLabel": "a create step: the model is lowered to a view, read as that view",
      "modelingForm.sourceLabel":
        "a create step: the model is lowered to a view, read as that view",
      "profilerPanel.addLabel":
        "an act that adds the table to a profiler: the profiler is shown as Data Profiler",
      "dataQualityPanel.checkLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "dataQualityPanel.columnLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "dataQualityPanel.comparatorLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "dataQualityPanel.levelLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "dataQualityPanel.metricLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "dataQualityPanel.thresholdLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "dataQualityPanel.unitLabel":
        "a step of the builder that composes a check: shown in the contract's checks",
      "tableEditForm.calBaseSystemLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calDayCloseLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calFiscalAnchorDayLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calFiscalAnchorMonthLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calHolidayPresetFrom":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calHolidayPresetLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calHolidayPresetTo":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calHolidaysLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calNameLabel": "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calRetailAnchorLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calTzLabel": "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calVersionLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calWeekStartLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "tableEditForm.calWeekendLabel":
        "creates a calendar, which the table names: shown as Calendar",
      "profileConstraintsModal.checkerLabel":
        "picks the checker a profile's constraints go to; stores nothing of the table",
      "registerTableForm.nlSearchLabel":
        "a search for a table to register; stores nothing of the table",
    },
  },
};

/** The catalog keys a file gives as a field's label: `label={t("key")}`. */
function formLabels(file: string): string[] {
  const text = readFileSync(resolve(SRC, file), "utf8");
  return [...text.matchAll(/(?<![-\w])label=\{t\("([^"]+)"/g)].map((m) => m[1]);
}

/** Every catalog key a file reads: `t("key")`. */
function keysRead(file: string): Set<string> {
  const text = readFileSync(resolve(SRC, file), "utf8");
  return new Set([...text.matchAll(/\bt\("([^"]+)"/g)].map((m) => m[1]));
}

describe("a detail panel's read view shows every field its edit form has", () => {
  for (const [page, panel] of Object.entries(PANELS)) {
    it(page, () => {
      for (const file of [...panel.read, ...panel.form]) {
        expect(existsSync(resolve(SRC, file)), file).toBe(true);
      }
      const labels = new Set(panel.form.flatMap(formLabels));
      expect(labels.size, "the form's labels were found").toBeGreaterThan(0);
      const shown = new Set(panel.read.flatMap((f) => [...keysRead(f)]));
      const exempt = panel.notAField ?? {};
      const missing = [...labels].filter((key) => !shown.has(key) && !(key in exempt));
      expect(missing, `${page}: set in the form, not shown in the read view`).toEqual([]);
      // An exemption names a label the form still has.
      expect(Object.keys(exempt).filter((key) => !labels.has(key))).toEqual([]);
    });
  }
});
