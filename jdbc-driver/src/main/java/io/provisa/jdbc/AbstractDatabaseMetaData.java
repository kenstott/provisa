package io.provisa.jdbc;

import java.sql.*;

/**
 * Abstract DatabaseMetaData — defaults for the ~150 methods we don't need.
 */
public abstract class AbstractDatabaseMetaData implements DatabaseMetaData {
    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public boolean isReadOnly() { return true; }
    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return true; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return true; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return true; }
    @Override public boolean storesMixedCaseIdentifiers() { return false; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) { return false; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return false; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return false; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public String getSchemaTerm() { return "schema"; }
    @Override public String getProcedureTerm() { return "procedure"; }
    @Override public String getCatalogTerm() { return "catalog"; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public boolean supportsSchemasInDataManipulation() { return false; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return false; }
    @Override public boolean supportsSubqueriesInExists() { return false; }
    @Override public boolean supportsSubqueriesInIns() { return false; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsCorrelatedSubqueries() { return false; }
    @Override public boolean supportsUnion() { return false; }
    @Override public boolean supportsUnionAll() { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return false; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return false; }
    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 256; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 256; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 256; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 256; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 256; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public boolean supportsTransactions() { return false; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return false; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public ResultSet getProcedures(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getProcedureColumns(String a, String b, String c, String d) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getTableTypes() throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getColumnPrivileges(String a, String b, String c, String d) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getTablePrivileges(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getBestRowIdentifier(String a, String b, String c, int d, boolean e) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getVersionColumns(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getPrimaryKeys(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getImportedKeys(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getExportedKeys(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getCrossReference(String a, String b, String c, String d, String e, String f) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getTypeInfo() throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getIndexInfo(String a, String b, String c, boolean d, boolean e) throws SQLException { return emptyResultSet(); }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) { return concurrency == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsBatchUpdates() { return false; }
    @Override public ResultSet getUDTs(String a, String b, String c, int[] d) throws SQLException { return emptyResultSet(); }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }
    @Override public ResultSet getSuperTypes(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getSuperTables(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getAttributes(String a, String b, String c, String d) throws SQLException { return emptyResultSet(); }
    @Override public boolean supportsResultSetHoldability(int holdability) { return false; }
    @Override public int getResultSetHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public int getDatabaseMajorVersion() { return 0; }
    @Override public int getDatabaseMinorVersion() { return 1; }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 3; }
    @Override public int getSQLStateType() { return sqlStateSQL; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getSchemas() throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getCatalogs() throws SQLException { return emptyResultSet(); }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }
    @Override public ResultSet getClientInfoProperties() throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getFunctions(String a, String b, String c) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getFunctionColumns(String a, String b, String c, String d) throws SQLException { return emptyResultSet(); }
    @Override public ResultSet getPseudoColumns(String a, String b, String c, String d) throws SQLException { return emptyResultSet(); }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }
    @Override public <T> T unwrap(Class<T> iface) { return null; }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    private ResultSet emptyResultSet() {
        return new ProvisaResultSet(java.util.Collections.emptyList(), java.util.Collections.emptyList());
    }
}
