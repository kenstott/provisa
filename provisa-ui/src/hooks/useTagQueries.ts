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
import type { LibraryCitation, MutationResult, Tag, TagAssignment } from "../types/admin";
import {
  TagsQuery as TAGS_QUERY,
  TagAssignmentsQuery as TAG_ASSIGNMENTS_QUERY,
  CreateTag as CREATE_TAG_MUTATION,
  UpdateTag as UPDATE_TAG_MUTATION,
  DeleteTag as DELETE_TAG_MUTATION,
  AssignTag as ASSIGN_TAG_MUTATION,
  UnassignTag as UNASSIGN_TAG_MUTATION,
  UpsertTagParamValue as UPSERT_TAG_PARAM_VALUE_MUTATION,
  DeleteTagParamValue as DELETE_TAG_PARAM_VALUE_MUTATION,
  CiteTagDefinition as CITE_TAG_DEFINITION_MUTATION,
  UnciteTagDefinition as UNCITE_TAG_DEFINITION_MUTATION,
  CiteTagAssignment as CITE_TAG_ASSIGNMENT_MUTATION,
  UnciteTagAssignment as UNCITE_TAG_ASSIGNMENT_MUTATION,
  DeleteTags,
} from "./admin.graphql";
import { useBulkDelete } from "./useBulkDelete";

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

/** A tag saved: a new one is created (an id that exists is refused), one that exists updated. */
export function useSaveTag() {
  const saved = { refetchQueries: [{ query: TAGS_QUERY }] };
  const [createTag, { loading: creating }] = useMutation<{ createTag: MutationResult }>(
    CREATE_TAG_MUTATION,
    saved,
  );
  const [updateTag, { loading: updating }] = useMutation<{ updateTag: MutationResult }>(
    UPDATE_TAG_MUTATION,
    saved,
  );
  const input = (
    id: string,
    description: string,
    appliesTo: string[],
    reasonPolicy: string,
    expiresPolicy: string,
    paramPolicy: string, // REQ-1467
    requiresCitation: boolean, // REQ-2208
  ) => ({
    input: {
      id,
      description,
      appliesTo,
      reasonPolicy,
      expiresPolicy,
      paramPolicy,
      requiresCitation,
    },
  });
  return {
    createTag: async (...fields: Parameters<typeof input>) => {
      const result = await createTag({ variables: input(...fields) });
      return (result.data?.createTag ?? { success: false, message: "" }) as MutationResult;
    },
    updateTag: async (...fields: Parameters<typeof input>) => {
      const result = await updateTag({ variables: input(...fields) });
      return (result.data?.updateTag ?? { success: false, message: "" }) as MutationResult;
    },
    loading: creating || updating,
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
    /** ``citations``, when given, is the whole set the assignment cites, saved with it in the
     *  same call (REQ-2208); left out, the assignment's citations stay as they are. */
    assignTag: async (input: TagAssignment, citations?: CitationPlace[]) => {
      const result = await assignTag({
        variables: {
          input: toAssignmentInput(input),
          citations: citations === undefined ? null : citations.map(citationInput),
        },
      });
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
    termId: input.termId ?? null,
    productId: input.productId ?? null,
    documentId: input.documentId ?? null,
    sectionHeading: input.sectionHeading ?? null,
    sectionPage: input.sectionPage ?? null,
    rlsRuleId: input.rlsRuleId ?? null,
    roleId: input.roleId ?? null,
    metricName: input.metricName ?? null,
    alsoTerm: input.alsoTerm ?? false,
    reason: input.reason ?? null,
    expiresOn: input.expiresOn ?? null,
  };
}

/** The place a citation names: a document, or a section of it by heading, or by page without one. */
export type CitationPlace = Pick<LibraryCitation, "documentId" | "sectionHeading" | "sectionPage">;

/** One name for the place a citation points to: its document, and its heading, page or whole. */
export const citationKey = (c: CitationPlace) =>
  `${c.documentId}-${
    c.sectionHeading !== null
      ? `h:${c.sectionHeading}`
      : c.sectionPage !== null
        ? `p:${c.sectionPage}`
        : ""
  }`;

function citationInput(place: CitationPlace) {
  return {
    documentId: place.documentId,
    sectionHeading: place.sectionHeading,
    sectionPage: place.sectionPage,
  };
}

/** REQ-2208: the library places a tag's definition, and each assignment of it, cite. Citing a place
 *  again records its text anew: how a reviewer confirms a citation flagged as changed. */
export function useTagCitations() {
  const again = { refetchQueries: [{ query: TAGS_QUERY }, { query: TAG_ASSIGNMENTS_QUERY }] };
  const [cite] = useMutation<{ citeTagDefinition: MutationResult }>(
    CITE_TAG_DEFINITION_MUTATION,
    again,
  );
  const [uncite] = useMutation<{ unciteTagDefinition: MutationResult }>(
    UNCITE_TAG_DEFINITION_MUTATION,
    again,
  );
  const [citeAssignment] = useMutation<{ citeTagAssignment: MutationResult }>(
    CITE_TAG_ASSIGNMENT_MUTATION,
    again,
  );
  const [unciteAssignment] = useMutation<{ unciteTagAssignment: MutationResult }>(
    UNCITE_TAG_ASSIGNMENT_MUTATION,
    again,
  );
  return {
    citeDefinition: async (tagId: string, place: CitationPlace) =>
      (await cite({ variables: { tagId, citation: citationInput(place) } })).data
        ?.citeTagDefinition as MutationResult,
    unciteDefinition: async (tagId: string, place: CitationPlace) =>
      (await uncite({ variables: { tagId, citation: citationInput(place) } })).data
        ?.unciteTagDefinition as MutationResult,
    citeAssignment: async (assignment: TagAssignment, place: CitationPlace) =>
      (
        await citeAssignment({
          variables: { assignment: toAssignmentInput(assignment), citation: citationInput(place) },
        })
      ).data?.citeTagAssignment as MutationResult,
    unciteAssignment: async (assignment: TagAssignment, place: CitationPlace) =>
      (
        await unciteAssignment({
          variables: { assignment: toAssignmentInput(assignment), citation: citationInput(place) },
        })
      ).data?.unciteTagAssignment as MutationResult,
  };
}

/** REQ-2240: several tags deleted in one call, each through its single delete; the same lists
 *  read again. */
export function useDeleteTags() {
  return useBulkDelete(DeleteTags, "deleteTags", "ids", [
    { query: TAGS_QUERY },
    { query: TAG_ASSIGNMENTS_QUERY },
  ]);
}
