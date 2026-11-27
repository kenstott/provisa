// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e5c21-94d0-4f6a-8a13-5d2c9e07b4f8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27), REQ-1929: a command's call on the JSON:API Explorer, handed over
// by a command's Query it line. A read is GET /data/jsonapi/<domain>/commands/<name> with each
// argument a query parameter (text as written, any other value as its JSON literal); a write is a
// POST with the arguments as the body's meta. The rows answer under meta.rows. A read runs on
// opening; a write only when Run is pressed.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavPayload } from "../../hooks/useNavPayload";
import { serverMessage } from "../../i18n/serverMessage";
import type { JsonApiDocument } from "./document";

/** What a command's Query it line hands over. */
export interface CommandHandoff {
  jsonapiCommand?: string;
  jsonapiDomain?: string;
  jsonapiWrite?: boolean;
  jsonapiArgs?: Record<string, unknown>;
  autoRun?: boolean;
}

/** One argument as the form holds it: its text, and whether that text is a JSON literal. */
export interface CommandField {
  name: string;
  text: string;
  json: boolean;
}

interface Handed {
  domainId: string;
  name: string;
  write: boolean;
  autoRun: boolean;
  seq: number;
}

/** The rows a command answered, under the document's meta. */
export function commandRows(doc: JsonApiDocument): Record<string, unknown>[] {
  const rows = doc.meta?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

const fieldOf = (name: string, value: unknown): CommandField =>
  typeof value === "string"
    ? { name, text: value, json: false }
    : { name, text: JSON.stringify(value), json: true };

export function useCommandCall(roleId: string, notJson: (name: string) => string) {
  const [handed, setHanded] = useState<Handed | null>(null);
  const [fields, setFields] = useState<CommandField[]>([]);
  const [doc, setDoc] = useState<JsonApiDocument | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");

  useNavPayload<CommandHandoff>((payload) => {
    if (!payload.jsonapiCommand || !payload.jsonapiDomain) return;
    const args = payload.jsonapiArgs ?? {};
    setFields(Object.entries(args).map(([n, v]) => fieldOf(n, v)));
    setDoc(null);
    setError("");
    setHanded((prev) => ({
      domainId: payload.jsonapiDomain as string,
      name: payload.jsonapiCommand as string,
      write: payload.jsonapiWrite === true,
      autoRun: payload.autoRun === true,
      seq: (prev?.seq ?? 0) + 1,
    }));
  });

  const path = handed ? `/data/jsonapi/${handed.domainId}/commands/${handed.name}` : "";
  const url = useMemo(() => {
    if (!handed || handed.write) return path;
    const params = new URLSearchParams(fields.map((f) => [f.name, f.text]));
    const qs = params.toString();
    return qs ? `${path}?${qs}` : path;
  }, [handed, path, fields]);

  const run = useCallback(async () => {
    if (!handed) return;
    const headers: Record<string, string> = {
      Accept: "application/vnd.api+json",
      ...(roleId ? { "x-provisa-role": roleId } : {}),
    };
    let init: RequestInit = { headers };
    if (handed.write) {
      const meta: Record<string, unknown> = {};
      for (const f of fields) {
        if (!f.json) {
          meta[f.name] = f.text;
          continue;
        }
        try {
          meta[f.name] = JSON.parse(f.text);
        } catch {
          setError(notJson(f.name));
          return;
        }
      }
      init = {
        method: "POST",
        headers: { ...headers, "Content-Type": "application/vnd.api+json" },
        body: JSON.stringify({ meta }),
      };
    }
    setRunning(true);
    setError("");
    try {
      const res = await fetch(url, init);
      const json = await res.json();
      if (!res.ok) {
        setError(serverMessage(json.errors?.[0], JSON.stringify(json)));
        setDoc(null);
      } else {
        setDoc(json);
      }
    } catch (e) {
      setError(String(e));
      setDoc(null);
    } finally {
      setRunning(false);
    }
  }, [handed, fields, roleId, url, notJson]);

  // A read handed with autoRun runs once, with the arguments it was handed.
  const ranSeq = useRef<number | null>(null);
  useEffect(() => {
    if (!handed || !handed.autoRun || handed.write || ranSeq.current === handed.seq) return;
    ranSeq.current = handed.seq;
    void run();
  }, [handed, run]);

  const setField = (name: string, text: string) =>
    setFields((prev) => prev.map((f) => (f.name === name ? { ...f, text } : f)));
  const close = () => {
    setHanded(null);
    setFields([]);
    setDoc(null);
    setError("");
  };

  return {
    call: handed ? { domainId: handed.domainId, name: handed.name, write: handed.write } : null,
    fields,
    setField,
    url,
    doc,
    running,
    error,
    run,
    close,
  };
}
