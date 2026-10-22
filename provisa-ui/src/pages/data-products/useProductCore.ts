// Copyright (c) 2026 Kenneth Stott
// Canary: 2a6f9e38-7b14-4c50-9d82-e3b5c0a7f164
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2127: the product list and one product's panel core, read from the meta domain as the role
// the app acts as.

import { useCallback, useEffect, useState } from "react";
import { useAuth } from "../../context/AuthContext";
import { metaReader } from "../../components/graph3d/useProductGraph";
import type { DataProduct } from "../../types/admin";
import type { MetaReader } from "../../api/metaGraph";
import { loadProductCore, loadProducts, type ProductCore } from "./productCore";

interface Read<T> {
  data: T | null;
  /** The role cannot read what this is drawn from: what it fills is absent. */
  unreadable: boolean;
  error: string | null;
  loading: boolean;
}

const START = { data: null, unreadable: false, error: null, loading: true };

function useMetaRead<T>(
  load: (role: string) => Promise<T | null>,
  key: unknown,
): Read<T> & { reload: () => void } {
  const { actingRoles } = useAuth();
  const [state, setState] = useState<Read<T>>(START);
  const [seq, setSeq] = useState(0);
  useEffect(() => {
    if (actingRoles === null || key === null) return;
    let cancelled = false;
    load(actingRoles).then(
      (data) =>
        !cancelled && setState({ data, unreadable: data === null, error: null, loading: false }),
      (e: unknown) =>
        !cancelled &&
        setState({
          data: null,
          unreadable: false,
          error: e instanceof Error ? e.message : String(e),
          loading: false,
        }),
    );
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- load is rebuilt each render from key; key and the role decide what is read
  }, [actingRoles, key, seq]);
  const reload = useCallback(() => setSeq((n) => n + 1), []);
  return { ...state, reload };
}

/** Every product the role reads, from data_products_meta. */
export function useMetaProducts() {
  return useMetaRead<DataProduct[]>(
    async (role) => loadProducts(await metaReader(role), role),
    "products",
  );
}

/** One product's panel core; ``product`` null reads nothing. */
export function useProductCore(product: string | null, version: number) {
  return useMetaRead<ProductCore>(
    async (role) => loadProductCore(await metaReader(role), role, product as string),
    product === null ? null : `${product}#${version}`,
  );
}

/** The acting role's meta reader (its schema and root fields), once read. */
export function useMetaReader(): MetaReader | null {
  const read = useMetaRead<MetaReader>((role) => metaReader(role), "reader");
  return read.data;
}
