import { gql } from '@apollo/client';

export const MutationResult = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const ColumnFields = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const ColumnPresetFields = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RLSFilterFields = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RolesQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const SourcesQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DomainsQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const TablesQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RelationshipsQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RLSRulesQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const AvailableSchemas = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const AvailableTables = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const AvailableColumns = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const AvailableColumnsMetadata = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const AvailableFunctions = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const GenerateColumnDescription = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const GenerateTableDescription = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const CompileQuery = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const MVList = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const CacheStats = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const SystemHealth = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const ScheduledTasks = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const CreateDomain = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeleteDomain = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RegisterTable = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateTable = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeleteTable = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpsertRelationship = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeleteRelationship = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const CreateSource = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateSource = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeleteSource = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const PurgeCache = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateSourceCache = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateTableCache = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateSourceNaming = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateTableNaming = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpdateSourceAllowedDomains = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RenameSource = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const UpsertRlsRule = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeleteRlsRule = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const CreateRole = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeleteRole = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const DeployViewToDb = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const RefreshMv = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const ToggleMv = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const ToggleScheduledTask = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const PurgeCacheByTable = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
export const InvalidateFileSource = gql`# Reusable fragments

fragment MutationResult on MutationResult {
  success
  message
}

fragment ColumnFields on Column {
  id
  columnName
  visibleTo
  writableBy
  unmaskedTo
  maskType
  maskPattern
  maskReplace
  maskValue
  maskPrecision
  alias
  description
  nativeFilterType
  isPrimaryKey
  isForeignKey
  isAlternateKey
  scope
}

fragment ColumnPresetFields on ColumnPreset {
  column
  source
  name
  value
  dataType
}

fragment RLSFilterFields on RLSFilter {
  id
  role_id
  domain
  table
  filter_type
  filter_sql
  columns
}

# Queries

query RolesQuery {
  roles {
    id
    capabilities
    domainAccess
  }
}

query SourcesQuery {
  sources {
    id
    type
    host
    port
    database
    username
    dialect
    cacheEnabled
    cacheTtl
    namingConvention
    allowedDomains
    description
  }
}

query DomainsQuery {
  domains {
    id
    description
    graphqlAlias
  }
}

query TablesQuery {
  tables {
    id
    sourceId
    domainId
    schemaName
    tableName
    governance
    alias
    description
    cacheTtl
    namingConvention
    watermarkColumn
    apiEndpoint
    viewSql
    dataProduct
    columns {
      ...ColumnFields
    }
    columnPresets {
      ...ColumnPresetFields
    }
  }
}

query RelationshipsQuery {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceTableName
    targetTableName
    sourceColumn
    targetColumn
    cardinality
    materialize
    refreshInterval
    targetFunctionName
    functionArg
    alias
    graphqlAlias
    computedCypherAlias
    autoSuggested
    disableCypher
  }
}

query RLSRulesQuery {
  rlsRules {
    id
    roleId
    domain
    table
    filters {
      ...RLSFilterFields
    }
  }
}

query AvailableSchemas($sourceId: String!) {
  availableSchemas(sourceId: $sourceId)
}

query AvailableTables($sourceId: String!, $schemaName: String!) {
  availableTables(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName)
}

query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) {
  availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
    name
    dataType
    comment
    nativeFilterType
    isPrimaryKey
  }
}

query AvailableFunctions($sourceId: String!, $schemaName: String!) {
  availableFunctions(sourceId: $sourceId, schemaName: $schemaName) {
    name
    comment
  }
}

query GenerateColumnDescription($tableId: String!, $columnName: String!) {
  generateColumnDescription(tableId: $tableId, columnName: $columnName)
}

query GenerateTableDescription($tableId: String!) {
  generateTableDescription(tableId: $tableId)
}

query CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql
    semanticSql
    trinoSql
    directSql
    route
    routeReason
    sources
    rootField
    canonicalField
    compiledCypher
    cypherError
    optimizations
    warnings
    columnAliases {
      fieldName
      column
    }
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      schemaScope
      maskingApplied
      ceilingApplied
      route
    }
  }
}

query MVList {
  mvList {
    id
    sourceTables
    targetTable
    refreshInterval
    enabled
    status
    lastRefreshAt
    rowCount
    lastError
  }
}

query CacheStats {
  cacheStats {
    totalKeys
    hitCount
    missCount
    storeType
  }
}

query SystemHealth {
  systemHealth {
    trinoConnected
    trinoWorkerCount
    trinoActiveWorkers
    pgPoolSize
    pgPoolFree
    cacheConnected
    flightServerRunning
    mvRefreshLoopRunning
  }
}

query ScheduledTasks {
  scheduledTasks {
    id
    name
    cronExpression
    webhookUrl
    enabled
    lastRunAt
    nextRunAt
  }
}

# Mutations

mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) {
  createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) {
    ...MutationResult
  }
}

mutation DeleteDomain($id: String!) {
  deleteDomain(id: $id) {
    ...MutationResult
  }
}

mutation RegisterTable($input: TableInput!) {
  registerTable(input: $input) {
    ...MutationResult
  }
}

mutation UpdateTable($input: TableInput!) {
  updateTable(input: $input) {
    ...MutationResult
  }
}

mutation DeleteTable($id: Int!) {
  deleteTable(id: $id) {
    ...MutationResult
  }
}

mutation UpsertRelationship($input: RelationshipInput!) {
  upsertRelationship(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRelationship($id: String!) {
  deleteRelationship(id: $id) {
    ...MutationResult
  }
}

mutation CreateSource($input: SourceInput!) {
  createSource(input: $input) {
    ...MutationResult
  }
}

mutation UpdateSource($input: SourceInput!) {
  updateSource(input: $input) {
    ...MutationResult
  }
}

mutation DeleteSource($id: String!) {
  deleteSource(id: $id) {
    ...MutationResult
  }
}

mutation PurgeCache {
  purgeCache {
    ...MutationResult
  }
}

mutation UpdateSourceCache($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) {
  updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateTableCache($tableId: Int!, $cacheTtl: Int) {
  updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) {
    ...MutationResult
  }
}

mutation UpdateSourceNaming($sourceId: String!, $namingConvention: String) {
  updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateTableNaming($tableId: Int!, $namingConvention: String) {
  updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) {
    ...MutationResult
  }
}

mutation UpdateSourceAllowedDomains($sourceId: String!, $allowedDomains: [String!]!) {
  updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) {
    ...MutationResult
  }
}

mutation RenameSource($oldId: String!, $newId: String!) {
  renameSource(oldId: $oldId, newId: $newId) {
    ...MutationResult
  }
}

mutation UpsertRlsRule($input: RLSRuleInput!) {
  upsertRlsRule(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRlsRule($roleId: String!, $tableId: Int, $domainId: String) {
  deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) {
    ...MutationResult
  }
}

mutation CreateRole($input: RoleInput!) {
  createRole(input: $input) {
    ...MutationResult
  }
}

mutation DeleteRole($id: String!) {
  deleteRole(id: $id) {
    ...MutationResult
  }
}

mutation DeployViewToDb($tableId: Int!) {
  deployViewToDb(tableId: $tableId) {
    ...MutationResult
  }
}

mutation RefreshMv($mvId: String!) {
  refreshMv(mvId: $mvId) {
    ...MutationResult
  }
}

mutation ToggleMv($mvId: String!, $enabled: Boolean!) {
  toggleMv(mvId: $mvId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) {
  toggleScheduledTask(taskId: $taskId, enabled: $enabled) {
    ...MutationResult
  }
}

mutation PurgeCacheByTable($tableId: Int!) {
  purgeCacheByTable(tableId: $tableId) {
    ...MutationResult
  }
}

mutation InvalidateFileSource($tableId: Int!) {
  invalidateFileSource(tableId: $tableId) {
    ...MutationResult
  }
}
`;
