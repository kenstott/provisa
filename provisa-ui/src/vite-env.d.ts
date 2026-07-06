// Copyright (c) 2026 Kenneth Stott
// Canary: 39c09afd-d9aa-4c1c-8bd1-122e684f5d3f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/// <reference types="vite/client" />

declare module '*.graphql' {
  import { DocumentNode } from 'graphql';
  const doc: DocumentNode;
  export const RolesQuery: DocumentNode;
  export const SourcesQuery: DocumentNode;
  export const DomainsQuery: DocumentNode;
  export const TablesQuery: DocumentNode;
  export const RelationshipsQuery: DocumentNode;
  export const AllRelationshipsQuery: DocumentNode;
  export const RLSRulesQuery: DocumentNode;
  export const AvailableSchemas: DocumentNode;
  export const AvailableTables: DocumentNode;
  export const AvailableColumns: DocumentNode;
  export const AvailableColumnsMetadata: DocumentNode;
  export const AvailableFunctions: DocumentNode;
  export const GenerateColumnDescription: DocumentNode;
  export const GenerateTableDescription: DocumentNode;
  export const CompileQuery: DocumentNode;
  export const MVList: DocumentNode;
  export const CacheStats: DocumentNode;
  export const SystemHealth: DocumentNode;
  export const ScheduledTasks: DocumentNode;
  export const CreateDomain: DocumentNode;
  export const DeleteDomain: DocumentNode;
  export const RegisterTable: DocumentNode;
  export const UpdateTable: DocumentNode;
  export const DeleteTable: DocumentNode;
  export const UpsertRelationship: DocumentNode;
  export const DeleteRelationship: DocumentNode;
  export const CreateSource: DocumentNode;
  export const UpdateSource: DocumentNode;
  export const DeleteSource: DocumentNode;
  export const RenameSource: DocumentNode;
  export const UpsertRlsRule: DocumentNode;
  export const DeleteRlsRule: DocumentNode;
  export const CreateRole: DocumentNode;
  export const DeleteRole: DocumentNode;
  export const DeployViewToDb: DocumentNode;
  export const RefreshMv: DocumentNode;
  export const ToggleMv: DocumentNode;
  export const ToggleScheduledTask: DocumentNode;
  export const CreateScheduledTask: DocumentNode;
  export const PurgeCacheByTable: DocumentNode;
  export const InvalidateFileSource: DocumentNode;
  export const PurgeCache: DocumentNode;
  export const UpdateSourceCache: DocumentNode;
  export const UpdateTableCache: DocumentNode;
  export const UpdateSourcePreferMaterialized: DocumentNode;
  export const UpdateTablePreferMaterialized: DocumentNode;
  export const UpdateSourceNaming: DocumentNode;
  export const UpdateTableNaming: DocumentNode;
  export const UpdateSourceAllowedDomains: DocumentNode;
  export const SuggestTableAlias: DocumentNode;
  export default doc;
}

declare module '*.gql' {
  import { DocumentNode } from 'graphql';
  const doc: DocumentNode;
  export const RolesQuery: DocumentNode;
  export const SourcesQuery: DocumentNode;
  export const DomainsQuery: DocumentNode;
  export const TablesQuery: DocumentNode;
  export const RelationshipsQuery: DocumentNode;
  export const AllRelationshipsQuery: DocumentNode;
  export const RLSRulesQuery: DocumentNode;
  export const AvailableSchemas: DocumentNode;
  export const AvailableTables: DocumentNode;
  export const AvailableColumns: DocumentNode;
  export const AvailableColumnsMetadata: DocumentNode;
  export const AvailableFunctions: DocumentNode;
  export const GenerateColumnDescription: DocumentNode;
  export const GenerateTableDescription: DocumentNode;
  export const CompileQuery: DocumentNode;
  export const MVList: DocumentNode;
  export const CacheStats: DocumentNode;
  export const SystemHealth: DocumentNode;
  export const ScheduledTasks: DocumentNode;
  export const CreateDomain: DocumentNode;
  export const DeleteDomain: DocumentNode;
  export const RegisterTable: DocumentNode;
  export const UpdateTable: DocumentNode;
  export const DeleteTable: DocumentNode;
  export const UpsertRelationship: DocumentNode;
  export const DeleteRelationship: DocumentNode;
  export const CreateSource: DocumentNode;
  export const UpdateSource: DocumentNode;
  export const DeleteSource: DocumentNode;
  export const RenameSource: DocumentNode;
  export const UpsertRlsRule: DocumentNode;
  export const DeleteRlsRule: DocumentNode;
  export const CreateRole: DocumentNode;
  export const DeleteRole: DocumentNode;
  export const DeployViewToDb: DocumentNode;
  export const RefreshMv: DocumentNode;
  export const ToggleMv: DocumentNode;
  export const ToggleScheduledTask: DocumentNode;
  export const CreateScheduledTask: DocumentNode;
  export const PurgeCacheByTable: DocumentNode;
  export const InvalidateFileSource: DocumentNode;
  export const PurgeCache: DocumentNode;
  export const UpdateSourceCache: DocumentNode;
  export const UpdateTableCache: DocumentNode;
  export const UpdateSourcePreferMaterialized: DocumentNode;
  export const UpdateTablePreferMaterialized: DocumentNode;
  export const UpdateSourceNaming: DocumentNode;
  export const UpdateTableNaming: DocumentNode;
  export const UpdateSourceAllowedDomains: DocumentNode;
  export const SuggestTableAlias: DocumentNode;
  export default doc;
}