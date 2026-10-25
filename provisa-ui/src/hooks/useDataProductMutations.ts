// Copyright (c) 2026 Kenneth Stott
// Canary: 7d2a9f41-3e86-4b15-a0c7-9e5b1d3f6a28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634: the data product saves. A new product is created, an open one updated, and a
// create refuses an id that exists.

import { useMutation } from "@apollo/client/react";
import type { ContractColumn, ContractSlaProperty, ContractTerms, MutationResult } from "../types/admin";
import {
  DataProductsQuery as DATA_PRODUCTS_QUERY,
  CreateDataProduct,
  UpdateDataProduct,
  DeleteDataProduct,
  SaveDataContract,
} from "./admin.graphql";

// REQ-1660: mirrors the DataProductInput GraphQL input type field-for-field.
export interface DataProductInput {
  id: string;
  domainId: string;
  name: string;
  ownerRole: string | null;
  teamRole: string | null;
  purpose: string;
  limitations: string;
  usage: string;
  version: string | null;
  status: string | null;
  support: string | null;
  customProperties: Record<string, unknown>;
}

export function useCreateDataProduct() {
  // REQ-1634
  const [createDataProduct, { loading }] = useMutation<{ createDataProduct: MutationResult }>(
    CreateDataProduct,
    { refetchQueries: [{ query: DATA_PRODUCTS_QUERY }] },
  );
  return {
    createDataProduct: async (input: DataProductInput) => {
      const result = await createDataProduct({ variables: { input } });
      return (result.data?.createDataProduct ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateDataProduct() {
  // REQ-1634: an edit is an update; a create refuses an id that exists.
  const [updateDataProduct, { loading }] = useMutation<{ updateDataProduct: MutationResult }>(
    UpdateDataProduct,
    { refetchQueries: [{ query: DATA_PRODUCTS_QUERY }] },
  );
  return {
    updateDataProduct: async (input: DataProductInput) => {
      const result = await updateDataProduct({ variables: { input } });
      return (result.data?.updateDataProduct ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteDataProduct() {
  // REQ-1634
  const [deleteDataProduct, { loading }] = useMutation<{ deleteDataProduct: MutationResult }>(
    DeleteDataProduct,
    { refetchQueries: [{ query: DATA_PRODUCTS_QUERY }] },
  );
  return {
    deleteDataProduct: async (id: string) => {
      const result = await deleteDataProduct({ variables: { id } });
      return (result.data?.deleteDataProduct ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

// REQ-2158: mirrors the DataContractInput GraphQL input type field-for-field.
export interface DataContractInput {
  version: string;
  status: string;
  description: string | null;
  terms: ContractTerms;
  sla: ContractSlaProperty[];
  columns: ContractColumn[];
}

export function useSaveDataContract() {
  // REQ-2158: a member's contract is saved on its own; a break at the same version is refused.
  const [saveDataContract, { loading }] = useMutation<{ saveDataContract: MutationResult }>(
    SaveDataContract,
    { refetchQueries: [{ query: DATA_PRODUCTS_QUERY }] },
  );
  return {
    saveDataContract: async (tableId: number, contract: DataContractInput) => {
      const result = await saveDataContract({ variables: { tableId, contract } });
      return (result.data?.saveDataContract ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}
