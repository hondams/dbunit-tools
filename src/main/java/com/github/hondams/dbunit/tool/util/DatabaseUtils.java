package com.github.hondams.dbunit.tool.util;

import com.github.hondams.dbunit.tool.model.CatalogDefinition;
import com.github.hondams.dbunit.tool.model.ColumnDefinition;
import com.github.hondams.dbunit.tool.model.ColumnKey;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.DatabaseNodeBuilder;
import com.github.hondams.dbunit.tool.model.SchemaDefinition;
import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.model.TableKey;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleTypes;

@UtilityClass
@Slf4j
public class DatabaseUtils {

    private final Map<Integer, String> SQL_TYPE_NAME_MAP = Map.ofEntries(//
        Map.entry(java.sql.Types.ARRAY, "ARRAY"),//
        Map.entry(java.sql.Types.BIGINT, "BIGINT"),//
        Map.entry(java.sql.Types.BINARY, "BINARY"),//
        Map.entry(java.sql.Types.BIT, "BIT"),//
        Map.entry(java.sql.Types.BLOB, "BLOB"),//
        Map.entry(java.sql.Types.BOOLEAN, "BOOLEAN"),//
        Map.entry(java.sql.Types.CHAR, "CHAR"),//
        Map.entry(java.sql.Types.CLOB, "CLOB"),//
        Map.entry(java.sql.Types.DATALINK, "DATALINK"),//
        Map.entry(java.sql.Types.DATE, "DATE"),//
        Map.entry(java.sql.Types.DECIMAL, "DECIMAL"),//
        Map.entry(java.sql.Types.DISTINCT, "DISTINCT"),//
        Map.entry(java.sql.Types.DOUBLE, "DOUBLE"),//
        Map.entry(java.sql.Types.FLOAT, "FLOAT"),//
        Map.entry(java.sql.Types.INTEGER, "INTEGER"),//
        Map.entry(java.sql.Types.JAVA_OBJECT, "JAVA_OBJECT"),//
        Map.entry(java.sql.Types.LONGNVARCHAR, "LONGNVARCHAR"),//
        Map.entry(java.sql.Types.LONGVARBINARY, "LONGVARBINARY"),//
        Map.entry(java.sql.Types.LONGVARCHAR, "LONGVARCHAR"),//
        Map.entry(java.sql.Types.NCHAR, "NCHAR"),//
        Map.entry(java.sql.Types.NCLOB, "NCLOB"),//
        Map.entry(java.sql.Types.NULL, "NULL"),//
        Map.entry(java.sql.Types.NUMERIC, "NUMERIC"),//
        Map.entry(java.sql.Types.NVARCHAR, "NVARCHAR"),//
        Map.entry(java.sql.Types.OTHER, "OTHER"),//
        Map.entry(java.sql.Types.REAL, "REAL"),//
        Map.entry(java.sql.Types.REF, "REF"),//
        Map.entry(java.sql.Types.REF_CURSOR, "REF_CURSOR"),//
        Map.entry(java.sql.Types.ROWID, "ROWID"),//
        Map.entry(java.sql.Types.SMALLINT, "SMALLINT"),//
        Map.entry(java.sql.Types.SQLXML, "SQLXML"),//
        Map.entry(java.sql.Types.STRUCT, "STRUCT"),//
        Map.entry(java.sql.Types.TIME, "TIME"),//
        Map.entry(java.sql.Types.TIME_WITH_TIMEZONE, "TIME_WITH_TIMEZONE"),//
        Map.entry(java.sql.Types.TIMESTAMP, "TIMESTAMP"),//
        Map.entry(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP_WITH_TIMEZONE"),//
        Map.entry(java.sql.Types.TINYINT, "TINYINT"),//
        Map.entry(java.sql.Types.VARBINARY, "VARBINARY"),//
        Map.entry(java.sql.Types.VARCHAR, "VARCHAR")//

    );
    private final Map<Integer, String> ORACLE_SQL_TYPE_NAME_MAP = Map.ofEntries(//
        Map.entry(OracleTypes.ARRAY, "ORACLE_ARRAY"),//
        Map.entry(OracleTypes.BFILE, "ORACLE_BFILE"),//
        Map.entry(OracleTypes.BIGINT, "ORACLE_BIGINT"),//
        Map.entry(OracleTypes.BINARY, "ORACLE_BINARY"),//
        Map.entry(OracleTypes.BINARY_DOUBLE, "ORACLE_BINARY_DOUBLE"),//
        Map.entry(OracleTypes.BINARY_FLOAT, "ORACLE_BINARY_FLOAT"),//
        Map.entry(OracleTypes.BIT, "ORACLE_BIT"),//
        Map.entry(OracleTypes.BLOB, "ORACLE_BLOB"),//
        Map.entry(OracleTypes.BOOLEAN, "ORACLE_BOOLEAN"),//
        Map.entry(OracleTypes.CHAR, "ORACLE_CHAR"),//
        Map.entry(OracleTypes.CLOB, "ORACLE_CLOB"),//
        Map.entry(OracleTypes.CURSOR, "ORACLE_CURSOR"),//
        Map.entry(OracleTypes.DATALINK, "ORACLE_DATALINK"),//
        Map.entry(OracleTypes.DATE, "ORACLE_DATE"),//
        Map.entry(OracleTypes.DECIMAL, "ORACLE_DECIMAL"),//
        Map.entry(OracleTypes.DOUBLE, "ORACLE_DOUBLE"),//
        Map.entry(OracleTypes.FIXED_CHAR, "ORACLE_FIXED_CHAR"),//
        Map.entry(OracleTypes.FLOAT, "ORACLE_FLOAT"),//
        Map.entry(OracleTypes.INTEGER, "ORACLE_INTEGER"),//
        Map.entry(OracleTypes.INTERVALDS, "ORACLE_INTERVALDS"),//
        Map.entry(OracleTypes.INTERVALYM, "ORACLE_INTERVALYM"),//
        Map.entry(OracleTypes.JAVA_OBJECT, "ORACLE_JAVA_OBJECT"),//
        Map.entry(OracleTypes.JAVA_STRUCT, "ORACLE_JAVA_STRUCT"),//
        Map.entry(OracleTypes.JSON, "ORACLE_JSON"),//
        Map.entry(OracleTypes.LONGNVARCHAR, "ORACLE_LONGNVARCHAR"),//
        Map.entry(OracleTypes.LONGVARBINARY, "ORACLE_LONGVARBINARY"),//
        Map.entry(OracleTypes.LONGVARCHAR, "ORACLE_LONGVARCHAR"),//
        Map.entry(OracleTypes.NCHAR, "ORACLE_NCHAR"),//
        Map.entry(OracleTypes.NCLOB, "ORACLE_NCLOB"),//
        Map.entry(OracleTypes.NULL, "ORACLE_NULL"),//
        Map.entry(OracleTypes.NUMBER, "NUMBER"),//
        //Map.entry(OracleTypes.NUMERIC, "ORACLE_NUMERIC"),//OracleTypes.NUMBERと同じ
        Map.entry(OracleTypes.NVARCHAR, "ORACLE_NVARCHAR"),//
        Map.entry(OracleTypes.OPAQUE, "ORACLE_OPAQUE"),//
        Map.entry(OracleTypes.OTHER, "ORACLE_OTHER"),//
        Map.entry(OracleTypes.PLSQL_BOOLEAN, "ORACLE_PLSQL_BOOLEAN"),//
        Map.entry(OracleTypes.PLSQL_INDEX_TABLE, "ORACLE_PLSQL_INDEX_TABLE"),//
        //Map.entry(OracleTypes.RAW, "ORACLE_RAW"),//OracleTypes.BINARYと同じ
        Map.entry(OracleTypes.REAL, "ORACLE_REAL"),//
        Map.entry(OracleTypes.REF, "ORACLE_REF"),//
        Map.entry(OracleTypes.REF_CURSOR, "ORACLE_REF_CURSOR"),//
        Map.entry(OracleTypes.ROWID, "ORACLE_ROWID"),//
        Map.entry(OracleTypes.SMALLINT, "ORACLE_SMALLINT"),//
        Map.entry(OracleTypes.SQLXML, "ORACLE_SQLXML"),//
        Map.entry(OracleTypes.STRUCT, "ORACLE_STRUCT"),//
        Map.entry(OracleTypes.TIME, "ORACLE_TIME"),//
        Map.entry(OracleTypes.TIMESTAMP, "ORACLE_TIMESTAMP"),//
        Map.entry(OracleTypes.TIMESTAMPLTZ, "ORACLE_TIMESTAMPLTZ"),//
        //Map.entry(OracleTypes.TIMESTAMPNS, "ORACLE_TIMESTAMPNS"),//OracleTypes.BINARY_FLOATと同じ
        Map.entry(OracleTypes.TIMESTAMPTZ, "ORACLE_TIMESTAMPTZ"),//
        Map.entry(OracleTypes.TINYINT, "ORACLE_TINYINT"),//
        Map.entry(OracleTypes.VARBINARY, "ORACLE_VARBINARY"),//
        Map.entry(OracleTypes.VARCHAR, "ORACLE_VARCHAR"),//
        Map.entry(OracleTypes.VECTOR, "ORACLE_VECTOR"),//
        Map.entry(OracleTypes.VECTOR_BINARY, "ORACLE_VECTOR_BINARY"),//
        Map.entry(OracleTypes.VECTOR_FLOAT32, "ORACLE_VECTOR_FLOAT32"),//
        Map.entry(OracleTypes.VECTOR_FLOAT64, "ORACLE_VECTOR_FLOAT64"),//
        Map.entry(OracleTypes.VECTOR_INT8, "ORACLE_VECTOR_INT8")//
    );

    public String getSqlTypeName(Connection connection, int sqlType) throws SQLException {
        String sqlTypeName = SQL_TYPE_NAME_MAP.get(sqlType);
        if (sqlTypeName == null) {
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData.getDatabaseProductName();
            switch (productName.toLowerCase()) {
                case "mysql":
                    //TODO
                    break;
                case "mariadb":
                    // MariaDB is compatible with MySQL
                    //TODO
                    break;
                case "postgresql":
                    //TODO
                    break;
                case "oracle":
                    sqlTypeName = ORACLE_SQL_TYPE_NAME_MAP.get(sqlType);
                    break;
                case "microsoft sql server":
                case "sql server":
                    //TODO
                    break;
                case "db2":
                    //TODO
                    break;
                case "h2":
                    //TODO
                    break;
                default:
                    // Unknown database
                    //TODO
                    break;
            }
        }
        return sqlTypeName;
    }

    public List<CatalogDefinition> getAllCatalogs(Connection connection) throws SQLException {
        List<CatalogDefinition> catalogs = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getCatalogs()) {
            while (rs.next()) {
                CatalogDefinition definition = new CatalogDefinition();
                definition.setCatalogName(rs.getString("TABLE_CAT"));
                catalogs.add(definition);
            }
        }
        return catalogs;
    }

    public List<SchemaDefinition> getAllSchemas(Connection connection) throws SQLException {
        List<SchemaDefinition> schemas = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getSchemas()) {
            while (rs.next()) {
                SchemaDefinition definition = new SchemaDefinition();
                definition.setCatalogName(rs.getString("TABLE_CATALOG"));
                definition.setSchemaName(rs.getString("TABLE_SCHEM"));
                schemas.add(definition);
            }
        }
        return schemas;
    }

    public List<TableDefinition> getAllTables(Connection connection) throws SQLException {
        return getTables(connection, new TableKey(null, null, "%"));
    }


    public List<TableDefinition> getTables(Connection connection, List<TableKey> tableKeys)
        throws SQLException {
        List<TableDefinition> allTables = new ArrayList<>();
        for (TableKey tableKey : tableKeys) {
            allTables.addAll(getTables(connection, tableKey));
        }
        return allTables;
    }

    public List<TableDefinition> getTables(Connection connection, TableKey tableKey)
        throws SQLException {
        List<TableDefinition> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(tableKey.getCatalogName(), tableKey.getSchemaName(),
            tableKey.getTableName(), null)) {
            while (rs.next()) {
                TableDefinition definition = new TableDefinition();
                definition.setCatalogName(rs.getString("TABLE_CAT"));
                definition.setSchemaName(rs.getString("TABLE_SCHEM"));
                definition.setTableName(rs.getString("TABLE_NAME"));
                definition.setTableType(rs.getString("TABLE_TYPE"));
                tables.add(definition);
            }
        }

        return tables;
    }

    public List<ColumnDefinition> getAllColumns(Connection connection) throws SQLException {
        return getColumns(connection, new TableKey(null, null, "%"));
    }

    public List<ColumnDefinition> getColumns(Connection connection, TableKey tableKey)
        throws SQLException {
        List<ColumnDefinition> columns = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(tableKey.getCatalogName(), tableKey.getSchemaName(),
            tableKey.getTableName(), "%")) {
            while (rs.next()) {
                ColumnDefinition definition = new ColumnDefinition();
                definition.setCatalogName(rs.getString("TABLE_CAT"));
                definition.setSchemaName(rs.getString("TABLE_SCHEM"));
                definition.setTableName(rs.getString("TABLE_NAME"));
                definition.setColumnName(rs.getString("COLUMN_NAME"));

                int dataType = rs.getInt("DATA_TYPE");
                String sqlTypeName = DatabaseUtils.getSqlTypeName(connection, dataType);
                if (sqlTypeName == null) {
                    log.warn("Unknown SQL type. dataType={}", dataType);
                    sqlTypeName = "UNKNOWN";
                }
                definition.setSqlTypeName(sqlTypeName);

                definition.setTypeName(rs.getString("TYPE_NAME"));
                definition.setColumnSize(rs.getInt("COLUMN_SIZE"));
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                if (!rs.wasNull()) {
                    definition.setDecimalDigits(decimalDigits);
                }
                definition.setNullable(rs.getString("IS_NULLABLE"));

                columns.add(definition);
            }
        }

        fillKeyIndex(connection, columns);

        return columns;
    }

    public DatabaseNode getDatabaseNode(Connection connection, List<TableKey> tableKeys)
        throws SQLException {

        DatabaseNodeBuilder builder = new DatabaseNodeBuilder();
        for (TableKey tableKey : tableKeys) {
            List<ColumnDefinition> columns = getColumns(connection, tableKey);
            builder.append(columns);
        }

        return builder.build();
    }

    public DatabaseNode getDatabaseNode(Connection connection, TableKey tableKey)
        throws SQLException {

        DatabaseNodeBuilder builder = new DatabaseNodeBuilder();
        List<ColumnDefinition> columns = getColumns(connection, tableKey);
        builder.append(columns);

        return builder.build();
    }

    private void fillKeyIndex(Connection connection, List<ColumnDefinition> columns)
        throws SQLException {

        Map<TableKey, List<ColumnDefinition>> tableColumnsMap = new HashMap<>();

        Map<ColumnKey, ColumnDefinition> columnMap = new HashMap<>();
        for (ColumnDefinition column : columns) {
            ColumnKey key = new ColumnKey(column.getCatalogName(), column.getSchemaName(),
                column.getTableName(), column.getColumnName());
            TableKey tableKey = new TableKey(column.getCatalogName(), column.getSchemaName(),
                column.getTableName());
            columnMap.put(key, column);
            List<ColumnDefinition> tableColumns = tableColumnsMap.computeIfAbsent(tableKey,
                k -> new ArrayList<>());
            tableColumns.add(column);
        }

        for (TableKey tableKey : tableColumnsMap.keySet()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getPrimaryKeys(//
                tableKey.getCatalogName() == null ? "" : tableKey.getCatalogName(),//
                tableKey.getSchemaName() == null ? "" : tableKey.getSchemaName(),//
                tableKey.getTableName())) {

                while (rs.next()) {
                    String catalogName = rs.getString("TABLE_CAT");
                    String schemaName = rs.getString("TABLE_SCHEM");
                    String tableName = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    short keySeq = rs.getShort("KEY_SEQ");
                    ColumnKey key = new ColumnKey(catalogName, schemaName, tableName, columnName);
                    ColumnDefinition column = columnMap.get(key);
                    if (column == null) {
                        log.warn(
                            "Column not found for primary key. catalog={}, schema={}, table={}, column={}",
                            catalogName, schemaName, tableName, columnName);
                    } else {
                        column.setKeyIndex((int) keySeq);
                    }
                }
            }
        }
    }
}