// Copyright (c) 2026 Kenneth Stott
// Canary: a0cdf3e1-74b7-470b-a124-5ad644ee2ead
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: the credential fields of the object store a CSV/Parquet file is in. Each store shows
// exactly its own fields, and each field writes the hint key it is saved under.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { ObjectStoreCredentialFields } from "../ObjectStoreCredentialFields";
import { OBJECT_STORE_HINT_KEYS, type ObjectStore } from "../objectStoreHints";

describe("ObjectStoreCredentialFields", () => {
  it.each(["S3", "GCS", "AZURE"] as ObjectStore[])(
    "%s shows its own fields, each writing its hint key",
    (store) => {
      const setFields = vi.fn();
      render(<ObjectStoreCredentialFields store={store} fields={{}} setFields={setFields} />);
      for (const key of OBJECT_STORE_HINT_KEYS[store]) {
        const input = screen.getByTestId(`object-store-${key}`);
        fireEvent.change(input, { target: { value: `v-${key}` } });
        expect(setFields).toHaveBeenLastCalledWith({ [key]: `v-${key}` });
      }
      const others = (Object.keys(OBJECT_STORE_HINT_KEYS) as ObjectStore[])
        .filter((s) => s !== store)
        .flatMap((s) => OBJECT_STORE_HINT_KEYS[s]);
      for (const key of others) {
        expect(screen.queryByTestId(`object-store-${key}`)).toBeNull();
      }
    },
  );
});
