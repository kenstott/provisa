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

import { useMemo } from "react";
import { useMutation, useQuery } from "@apollo/client/react";
import type { ContractColumn, ContractSlaProperty, ContractTerms, MutationResult } from "../types/admin";
import type { Finding } from "../components/lifecycle/ToReview";
import {
  DataProductsQuery as DATA_PRODUCTS_QUERY,
  CreateDataProduct,
  UpdateDataProduct,
  DeleteDataProduct,
  SaveDataContract,
  PublishDataProduct,
  DeprecateDataProduct,
  RetireDataProduct,
  RestoreDataProduct,
  DataProductFindingsQuery,
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
  support: string | null;
  supportContact: string | null;
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

/** A product's status actions (REQ-1634 STATUS). Each answers the server's MutationResult; a
 * refusal names its code (data_product.status.<code>). */
export function useDataProductStatus() {
  const options = { refetchQueries: [{ query: DATA_PRODUCTS_QUERY }] };
  const [publish] = useMutation<{ publishDataProduct: MutationResult }>(
    PublishDataProduct,
    options,
  );
  const [deprecate] = useMutation<{ deprecateDataProduct: MutationResult }>(
    DeprecateDataProduct,
    options,
  );
  const [retire] = useMutation<{ retireDataProduct: MutationResult }>(RetireDataProduct, options);
  const [restore] = useMutation<{ restoreDataProduct: MutationResult }>(
    RestoreDataProduct,
    options,
  );
  const none = { success: false, message: "" };
  const actions: DataProductStatus = {
    publish: async (id: string) =>
      ((await publish({ variables: { id } })).data?.publishDataProduct ?? none) as MutationResult,
    deprecate: async (id: string, reason: string, retireOn: string | null) =>
      ((await deprecate({ variables: { id, reason, retireOn } })).data?.deprecateDataProduct ??
        none) as MutationResult,
    retire: async (id: string, reason: string, successor: string | null) =>
      ((await retire({ variables: { id, reason, successor } })).data?.retireDataProduct ??
        none) as MutationResult,
    restore: async (id: string) =>
      ((await restore({ variables: { id } })).data?.restoreDataProduct ?? none) as MutationResult,
  };
  return actions;
}

export interface DataProductStatus {
  publish: (id: string) => Promise<MutationResult>;
  deprecate: (id: string, reason: string, retireOn: string | null) => Promise<MutationResult>;
  retire: (id: string, reason: string, successor: string | null) => Promise<MutationResult>;
  restore: (id: string) => Promise<MutationResult>;
}

/** A data product's To Review finding (REQ-2146): its kind names the chip, its code and params
 * the message. */
export type ProductFinding = Finding & { params: Record<string, string> };

const NO_FINDINGS: Map<string, ProductFinding[]> = new Map();

/** Every product's findings, by product id; a product with none is absent. */
export function useDataProductFindings() {
  const { data, refetch, error } = useQuery<{
    dataProductFindings: { productId: string; findings: ProductFinding[] }[];
  }>(DataProductFindingsQuery, { fetchPolicy: "network-only" });
  const findings = useMemo(
    () =>
      data ? new Map(data.dataProductFindings.map((f) => [f.productId, f.findings])) : NO_FINDINGS,
    [data],
  );
  return { findings, refetch, error };
}
