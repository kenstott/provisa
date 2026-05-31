/// <reference types="vite/client" />

declare module '*.graphql' {
  import { DocumentNode } from 'graphql';
  const doc: DocumentNode;
  export const SourcesQuery: DocumentNode;
  export const DomainsQuery: DocumentNode;
  export const TablesQuery: DocumentNode;
  export const RelationshipsQuery: DocumentNode;
  export const RLSRulesQuery: DocumentNode;
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
  export const PurgeCache: DocumentNode;
  export const UpdateSourceCache: DocumentNode;
  export const UpdateTableCache: DocumentNode;
  export const UpdateSourceNaming: DocumentNode;
  export const UpdateTableNaming: DocumentNode;
  export const UpdateSourceAllowedDomains: DocumentNode;
  export default doc;
}
