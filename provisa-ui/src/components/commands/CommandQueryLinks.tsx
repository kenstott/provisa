// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1d4e93-0b58-4a26-9f37-e2a6b8c0d914
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): a command's Query it line, the one tables and relationships draw
// (QuerySurfaceRow), one link per surface the acting role is served the command on. Choosing one
// asks for the command's arguments (CommandArgumentsModal), then opens that Explore tab with the
// call filled in, run there under Explore's role; a command that writes opens without running.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { rootKey } from "../../api/metaGraph";
import { QuerySurfaceRow } from "../relationships/RelationshipQueryLinks";
import { useRelationshipSurfaces } from "../relationships/useRelationshipSurfaces";
import { CommandArgumentsModal } from "./CommandArgumentsModal";
import type { ArgTable } from "./relationArg";
import {
  commandQueryLink,
  commandSurfaces,
  servedCommand,
  type ArgValue,
  type CommandCall,
  type CommandSurface,
} from "./commandQueries";

export function CommandQueryLinks({
  call,
  actingRole,
  tables,
}: {
  call: CommandCall;
  /** The role Explore runs as: the surfaces are the ones it is served the command on. */
  actingRole: string | null;
  /** The registered tables; a relation argument picks one the acting role reads. */
  tables: readonly ArgTable[];
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { reader, graph } = useRelationshipSurfaces(actingRole);
  const [asking, setAsking] = useState<CommandSurface | null>(null);
  const served = graph ? servedCommand(graph.commands, call.name) : null;
  const surfaces = commandSurfaces(served, reader);
  if (served === null || surfaces.length === 0) return null;
  const readable = reader
    ? tables.filter((tb) => reader.roots.has(rootKey(tb.domainId, tb.tableName)))
    : [];
  const open = (surface: CommandSurface, values: Record<string, ArgValue>) => {
    const link = commandQueryLink(surface, call, served, reader, values);
    navigate(link.to, { state: link.state });
  };
  return (
    <>
      <QuerySurfaceRow
        surfaces={surfaces}
        onChoose={(surface) => {
          if (call.arguments.length === 0) open(surface, {});
          else setAsking(surface);
        }}
        labelOf={(surface) =>
          t("commandQuery.linkLabel", {
            name: call.name,
            surface: t(`relationshipRow.queryLinks.surfaces.${surface}`),
          })
        }
        heading={t("relationshipRow.queryLinks.heading")}
        testIdPrefix="command"
      />
      {asking !== null && (
        <CommandArgumentsModal
          call={call}
          surface={asking}
          tables={readable}
          onOpen={(values) => {
            setAsking(null);
            open(asking, values);
          }}
          onClose={() => setAsking(null)}
        />
      )}
    </>
  );
}
