// Copyright (c) 2026 Kenneth Stott
// Canary: 2ced660b-d284-4c4c-b7db-4594cd42d1d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef } from "react";
import { useGraphiQL, useGraphiQLActions } from "@graphiql/react";
import { operationNameOf } from "./graphqlOperationName";

/** Opens a new GraphiQL tab holding a query handed to the page, and runs it when asked.
 *  Mounted once per hand-off (keyed by the caller), whether the page was just opened or was
 *  already open. */
export function OpenFromNav({ query, autoRun }: { query: string; autoRun: boolean }) {
  const { addTab, updateActiveTabValues, run } = useGraphiQLActions();
  const queryEditor = useGraphiQL((s) => s.queryEditor);
  // The formatter behind the toolbar's Prettify button: a handed query is machine-written, often
  // on one line, so it opens as Prettify would leave it (comments kept).
  const prettifyQuery = useGraphiQL((s) => s.onPrettifyQuery);
  const done = useRef(false);

  // Wait for queryEditor to become available (it's set asynchronously by GraphiQL).
  useEffect(() => {
    if (!queryEditor || done.current) return;
    done.current = true;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const open = async () => {
      let text = query;
      try {
        text = await prettifyQuery(query);
      } catch {
        // By design, not a fallback: a query that does not parse cannot be formatted, so it opens
        // exactly as written and the editor shows the parse error where it is (as Prettify does).
      }
      if (cancelled) return;
      addTab();
      // The operation name with the query: GraphiQL titles a tab by the operation name it last read
      // from the editor, which is the name of the tab this one was opened from (REQ-2010: a demo's
      // tour hands over one query after another, each its own titled tab).
      updateActiveTabValues({ query: text, operationName: operationNameOf(text) });
      queryEditor.setValue(text);
      if (autoRun) timer = setTimeout(() => run(), 100);
    };
    void open();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once per mount, when queryEditor becomes available (guarded by done); the caller remounts it per hand-off
  }, [queryEditor]);
  return null;
}
