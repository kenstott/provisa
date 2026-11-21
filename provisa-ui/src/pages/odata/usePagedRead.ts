// Copyright (c) 2026 Kenneth Stott
// Canary: 97ecc37d-3a7f-4584-894c-7604b46b84b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1949, REQ-2193

// A read of the OData service, page by page: the first page at the URL the builder wrote, each next
// page at the service's own @odata.nextLink, and back through the pages already read.

import { useCallback, useRef, useState } from "react";
import { runODataQuery, type ODataPage } from "../../api/odata";

export interface PagedRead {
  /** The URL of each page read so far, the one on screen last. */
  urls: string[];
  page: ODataPage | null;
  /** The number of rows before the page on screen. */
  offset: number;
  loading: boolean;
  error: string | null;
  run: (url: string) => void;
  next: () => void;
  previous: () => void;
}

export function usePagedRead(role: string): PagedRead {
  const [urls, setUrls] = useState<string[]>([]);
  // The row count of each page before the one on screen, so the range it shows is the rows' place
  // in the whole answer.
  const [sizes, setSizes] = useState<number[]>([]);
  const [page, setPage] = useState<ODataPage | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Only the last request's answer is shown: one sent before it and answered after is dropped.
  const latest = useRef(0);

  const load = useCallback(
    (url: string, nextUrls: string[], nextSizes: number[]) => {
      const seq = ++latest.current;
      setLoading(true);
      setError(null);
      runODataQuery(url, role).then(
        (answer) => {
          if (seq !== latest.current) return;
          setUrls(nextUrls);
          setSizes(nextSizes);
          setPage(answer);
          setLoading(false);
        },
        (e: unknown) => {
          if (seq !== latest.current) return;
          setError(e instanceof Error ? e.message : String(e));
          setLoading(false);
        },
      );
    },
    [role],
  );

  const run = useCallback(
    (url: string) => {
      setPage(null);
      load(url, [url], []);
    },
    [load],
  );
  const next = useCallback(() => {
    if (!page?.nextLink) return;
    load(page.nextLink, [...urls, page.nextLink], [...sizes, page.rows.length]);
  }, [load, page, urls, sizes]);
  const previous = useCallback(() => {
    if (urls.length < 2) return;
    load(urls[urls.length - 2], urls.slice(0, -1), sizes.slice(0, -1));
  }, [load, urls, sizes]);

  return {
    urls,
    page,
    offset: sizes.reduce((a, b) => a + b, 0),
    loading,
    error,
    run,
    next,
    previous,
  };
}
