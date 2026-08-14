// Copyright (c) 2026 Kenneth Stott
// Canary: b56a055f-a739-4a3e-8a7c-9e722fc74797
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useQuery, useMutation } from "@apollo/client/react";
import type { MutationResult, Tag, TagAssignment } from "../types/admin";
import {
  TagsQuery as TAGS_QUERY,
  TagAssignmentsQuery as TAG_ASSIGNMENTS_QUERY,
  UpsertTag as UPSERT_TAG_MUTATION,
  DeleteTag as DELETE_TAG_MUTATION,
  AssignTag as ASSIGN_TAG_MUTATION,
  UnassignTag as UNASSIGN_TAG_MUTATION,
  UpsertTagParamValue as UPSERT_TAG_PARAM_VALUE_MUTATION,
  DeleteTagParamValue as DELETE_TAG_PARAM_VALUE_MUTATION,
} from "./admin.graphql";

const NO_TAGS: Tag[] = [];
const NO_TAG_ASSIGNMENTS: TagAssignment[] = [];

export function useTags() {
  const { data, loading, error, refetch } = useQuery<{ tags: Tag[] }>(TAGS_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return {
    tags: data?.tags ?? NO_TAGS,
    loading,
    error,
    refetch,
  };
}

export function useTagAssignments() {
  const { data, loading, error, refetch } = useQuery<{ tagAssignments: TagAssignment[] }>(
    TAG_ASSIGNMENTS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return {
    tagAssignments: data?.tagAssignments ?? NO_TAG_ASSIGNMENTS,
    loading,
    error,
    refetch,
  };
}

export function useUpsertTag() {
  const [upsertTag, { loading }] = useMutation<{ upsertTag: MutationResult }>(UPSERT_TAG_MUTATION, {
    refetchQueries: [{ query: TAGS_QUERY }],
  });
  return {
    upsertTag: async (
      id: string,
      description: string,
      appliesTo: string[],
      reasonPolicy: string = "optional",
      expiresPolicy: string = "optional",
      paramPolicy: string = "none", // REQ-1467
    ) => {
      const result = await upsertTag({
        variables: {
          input: { id, description, appliesTo, reasonPolicy, expiresPolicy, paramPolicy },
        },
      });
      return (result.data?.upsertTag ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

// REQ-1467: the value list is data the maintainer owns, editable on a system tag whose
// definition is not — both mutations refetch the tag registry, which carries the list.
export function useUpsertTagParamValue() {
  const [upsertTagParamValue, { loading }] = useMutation<{
    upsertTagParamValue: MutationResult;
  }>(UPSERT_TAG_PARAM_VALUE_MUTATION, { refetchQueries: [{ query: TAGS_QUERY }] });
  return {
    upsertTagParamValue: async (tagId: string, value: string, description: string) => {
      const result = await upsertTagParamValue({
        variables: { input: { tagId, value, description } },
      });
      return (result.data?.upsertTagParamValue ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteTagParamValue() {
  const [deleteTagParamValue, { loading }] = useMutation<{
    deleteTagParamValue: MutationResult;
  }>(DELETE_TAG_PARAM_VALUE_MUTATION, { refetchQueries: [{ query: TAGS_QUERY }] });
  return {
    deleteTagParamValue: async (tagId: string, value: string) => {
      const result = await deleteTagParamValue({ variables: { tagId, value } });
      return (result.data?.deleteTagParamValue ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteTag() {
  const [deleteTag, { loading }] = useMutation<{ deleteTag: MutationResult }>(DELETE_TAG_MUTATION, {
    refetchQueries: [{ query: TAGS_QUERY }, { query: TAG_ASSIGNMENTS_QUERY }],
  });
  return {
    deleteTag: async (id: string) => {
      const result = await deleteTag({ variables: { id } });
      return (result.data?.deleteTag ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useAssignTag() {
  const [assignTag, { loading }] = useMutation<{ assignTag: MutationResult }>(ASSIGN_TAG_MUTATION, {
    refetchQueries: [{ query: TAG_ASSIGNMENTS_QUERY }],
  });
  return {
    assignTag: async (input: TagAssignment) => {
      const result = await assignTag({ variables: { input: toAssignmentInput(input) } });
      return (result.data?.assignTag ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUnassignTag() {
  const [unassignTag, { loading }] = useMutation<{ unassignTag: MutationResult }>(
    UNASSIGN_TAG_MUTATION,
    { refetchQueries: [{ query: TAG_ASSIGNMENTS_QUERY }] },
  );
  return {
    unassignTag: async (input: TagAssignment) => {
      const result = await unassignTag({ variables: { input: toAssignmentInput(input) } });
      return (result.data?.unassignTag ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

// The GraphQL input has no tableRef — it is a server-derived read-only field.
function toAssignmentInput(input: TagAssignment) {
  return {
    tagId: input.tagId,
    objectType: input.objectType,
    sourceId: input.sourceId ?? null,
    tableId: input.tableId ?? null,
    columnName: input.columnName ?? null,
    relationshipId: input.relationshipId ?? null,
    commandName: input.commandName ?? null,
    reason: input.reason ?? null,
    expiresOn: input.expiresOn ?? null,
  };
}
