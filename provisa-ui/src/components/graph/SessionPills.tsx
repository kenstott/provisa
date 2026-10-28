// Copyright (c) 2026 Kenneth Stott
// Canary: 7b2d9e41-3c60-4a85-b1f7-0e6a8d4c5b93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2176: the user's graph session in the Cypher page's left panel. The labels and relationship
// types a statement created for the session (no table behind them) are listed beside the model's,
// each with its count, drawn with a dashed outline; a click runs the same MATCH a model pill runs;
// "Clear session" drops them all. The session is the user's, the same in every tab and client, so
// it is read from the server (GET /data/cypher/session) and read again only when a statement
// answers that it changed it.

import { useCallback, useEffect, useState } from "react";
import { Button } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { labelColor } from "./graph-model";
import { Tip } from "../Tip";

/** What GET /data/cypher/session answers, each list sorted by name. */
export interface GraphSession {
  labels: { label: string; nodes: number; tagged_model_nodes: number }[];
  types: { type: string; relationships: number }[];
  empty: boolean;
}

async function failure(res: Response): Promise<string> {
  const text = await res.text();
  try {
    const body = JSON.parse(text) as { error?: string; message?: string; detail?: string };
    return body.message ?? body.error ?? body.detail ?? text;
  } catch {
    // A body that is not JSON (a proxy's page) is said as it came.
    return text;
  }
}

export function SessionPills({
  roleHeader,
  changed,
  onLabelClick,
  onRelClick,
}: {
  roleHeader: string | undefined;
  /** Counts the statements that said they changed the session: each one reads it again. */
  changed: number;
  onLabelClick: (label: string) => void;
  onRelClick: (type: string) => void;
}) {
  const { t } = useTranslation();
  const [session, setSession] = useState<GraphSession | null>(null);
  const [error, setError] = useState<string | null>(null);
  const headers = useCallback((): Record<string, string> => {
    const h: Record<string, string> = {};
    if (roleHeader) h["X-Provisa-Role"] = roleHeader;
    return h;
  }, [roleHeader]);

  const read = useCallback(async () => {
    const res = await fetch("/data/cypher/session", { headers: headers() });
    if (!res.ok) throw new Error(await failure(res));
    const body = (await res.json()) as Partial<GraphSession>;
    // An answer that is not the session's is an error to show, never a session to guess.
    if (
      !Array.isArray(body.labels) ||
      !Array.isArray(body.types) ||
      typeof body.empty !== "boolean"
    )
      throw new Error("the answer is not a graph session");
    return body as GraphSession;
  }, [headers]);

  useEffect(() => {
    let cancelled = false;
    read().then(
      (s) => {
        if (cancelled) return;
        setSession(s);
        setError(null);
      },
      (e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      },
    );
    return () => {
      cancelled = true;
    };
  }, [read, changed]);

  const clear = async () => {
    try {
      const res = await fetch("/data/cypher/session", { method: "DELETE", headers: headers() });
      if (!res.ok) throw new Error(await failure(res));
      setSession(await read());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="graph-schema-section" data-testid="graph-session">
      <div className="graph-schema-heading">{t("graphSidebar.session")}</div>
      {error !== null && (
        <div className="graph-schema-empty" role="alert" data-testid="graph-session-error">
          {t("graphSidebar.sessionFailed", { error })}
        </div>
      )}
      {session === null ? (
        error === null && <div className="graph-schema-empty">{t("graphSidebar.loading")}</div>
      ) : session.empty ? (
        <div className="graph-schema-empty" data-testid="graph-session-empty">
          {t("graphSidebar.sessionEmpty")}
        </div>
      ) : (
        <>
          <div className="graph-label-list">
            {session.labels.map((l) => (
              <div key={`l:${l.label}`} className="graph-label-item">
                <Tip
                  text={t("graphSidebar.sessionLabelTip", {
                    nodes: l.nodes,
                    tagged: l.tagged_model_nodes,
                  })}
                >
                  <button
                    type="button"
                    className="graph-label-pill graph-label-pill--session"
                    style={{ borderColor: labelColor(l.label) }}
                    onClick={() => onLabelClick(l.label)}
                    data-testid={`graph-session-label-${l.label}`}
                  >
                    {l.label}
                    <span className="graph-session-count">({l.nodes + l.tagged_model_nodes})</span>
                  </button>
                </Tip>
              </div>
            ))}
          </div>
          <div className="graph-label-list">
            {session.types.map((r) => (
              <div key={`t:${r.type}`} className="graph-label-item">
                <Tip text={t("graphSidebar.sessionTypeTip", { count: r.relationships })}>
                  <button
                    type="button"
                    className="graph-rel-badge graph-rel-badge--session"
                    onClick={() => onRelClick(r.type)}
                    data-testid={`graph-session-type-${r.type}`}
                  >
                    {r.type}
                    <span className="graph-session-count">({r.relationships})</span>
                  </button>
                </Tip>
              </div>
            ))}
          </div>
          <Button size="xs" variant="default" onClick={clear} data-testid="graph-session-clear">
            {t("graphSidebar.sessionClear")}
          </Button>
        </>
      )}
    </div>
  );
}
