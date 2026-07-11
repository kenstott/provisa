// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { Pencil, Trash2, ArrowRight } from "lucide-react";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import type { Source } from "../../types/admin";
import { SOURCE_TYPES } from "./constants";

interface SourceDetailPanelProps {
  s: Source;
  domainsEnabled: boolean;
  getEffectiveTtl: (source: Source) => string;
  onEdit: () => void;
  onNavigate: () => void;
  onDelete: () => void;
}

export function SourceDetailPanel({
  s,
  domainsEnabled,
  getEffectiveTtl,
  onEdit,
  onNavigate,
  onDelete,
}: SourceDetailPanelProps) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <dl
        style={{
          display: "grid",
          gridTemplateColumns: "max-content 1fr",
          gap: "0.25rem 1rem",
          margin: 0,
          color: "var(--text)",
        }}
      >
        {(
          [
            ["Description", s.description || "—"],
            [
              "Type",
              SOURCE_TYPES.find((t) => t.value === s.type)?.label ?? s.type,
            ],
            ["Host", s.host || "—"],
            ["Port", s.port || "—"],
            ["Database", s.database || "—"],
            ["Username", s.username || "—"],
            ["Naming", s.gqlNamingConvention || "inherit (global)"],
            ["Cache", s.cacheEnabled ? "enabled" : "disabled"],
            ["Cache TTL", s.cacheTtl != null ? `${s.cacheTtl}s` : "inherit"],
            ["Effective TTL", getEffectiveTtl(s)],
            [
              "Allowed Domains",
              (s.allowedDomains ?? []).length
                ? (s.allowedDomains ?? []).join(", ")
                : "unrestricted",
            ],
          ] as [string, string | number][]
        )
          .filter(([k]) => domainsEnabled || k !== "Allowed Domains")
          .map(([k, v]) => (
          <React.Fragment key={k}>
            <dt
              style={{
                color: "var(--text-muted)",
                fontWeight: 500,
                fontSize: "0.875rem",
              }}
            >
              {k}
            </dt>
            <dd
              style={{
                color: "var(--text)",
                margin: 0,
                fontSize: "0.875rem",
              }}
            >
              {v}
            </dd>
          </React.Fragment>
        ))}
      </dl>
      <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
        <button
          className="btn-icon"
          title="Edit"
          onClick={(e) => {
            e.stopPropagation();
            onEdit();
          }}
        >
          <Pencil size={14} />
        </button>
        {s.id !== "provisa-otel" && (
          <button
            className="btn-icon"
            title="View registered tables"
            onClick={(e) => {
              e.stopPropagation();
              onNavigate();
            }}
          >
            <ArrowRight size={14} />
          </button>
        )}
        <ConfirmDialog
          title={`Delete source "${s.id}"?`}
          consequence={`This will remove the data source "${s.id}" and may break tables that reference it.`}
          onConfirm={onDelete}
        >
          {(open) => (
            <button
              className="btn-icon-danger"
              title="Delete"
              onClick={(e) => {
                e.stopPropagation();
                open();
              }}
            >
              <Trash2 size={14} />
            </button>
          )}
        </ConfirmDialog>
      </div>
    </div>
  );
}
