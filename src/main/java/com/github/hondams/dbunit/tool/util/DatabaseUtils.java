package com.github.hondams.dbunit.tool.util;

import com.github.hondams.dbunit.tool.model.CatalogDefinition;
import com.github.hondams.dbunit.tool.model.ColumnDefinition;
import com.github.hondams.dbunit.tool.model.ColumnKey;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.DatabaseNodeBuilder;
import com.github.hondams.dbunit.tool.model.SchemaDefinition;
import com.github.hondams.dbunit.tool.model.TableDefinition;
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

    public String getSqlTypeName(int sqlType) {
        return SQL_TYPE_NAME_MAP.get(sqlType);
    }

    public List<CatalogDefinition> getAllCatalogs(Connection connection) throws SQLException {
        List<CatalogDefinition> catalogs = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getCatalogs();
        while (rs.next()) {
            CatalogDefinition definition = new CatalogDefinition();
            definition.setCatalogName(rs.getString("TABLE_CAT"));
            catalogs.add(definition);
        }
        return catalogs;
    }

    public List<SchemaDefinition> getAllSchemas(Connection connection) throws SQLException {
        List<SchemaDefinition> schemas = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getSchemas();
        while (rs.next()) {
            SchemaDefinition definition = new SchemaDefinition();
            definition.setCatalogName(rs.getString("TABLE_CATALOG"));
            definition.setSchemaName(rs.getString("TABLE_SCHEM"));
            schemas.add(definition);
        }
        return schemas;
    }

    public List<TableDefinition> getAllTables(Connection connection) throws SQLException {
        List<TableDefinition> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables(null, null, "%", null);
        while (rs.next()) {
            TableDefinition definition = new TableDefinition();
            definition.setCatalogName(rs.getString("TABLE_CAT"));
            definition.setSchemaName(rs.getString("TABLE_SCHEM"));
            definition.setTableName(rs.getString("TABLE_NAME"));
            definition.setTableType(rs.getString("TABLE_TYPE"));
            tables.add(definition);
        }

        return tables;
    }

    public List<ColumnDefinition> getAllColumns(Connection connection) throws SQLException {
        return getColumns(connection, null, null, "%");
    }

    public List<ColumnDefinition> getColumns(Connection connection, String catalog,
        String schemaPattern, String tableNamePattern) throws SQLException {
        List<ColumnDefinition> columns = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(catalog, schemaPattern, tableNamePattern, "%");
        while (rs.next()) {
            ColumnDefinition definition = new ColumnDefinition();
            definition.setCatalogName(rs.getString("TABLE_CAT"));
            definition.setSchemaName(rs.getString("TABLE_SCHEM"));
            definition.setTableName(rs.getString("TABLE_NAME"));
            definition.setColumnName(rs.getString("COLUMN_NAME"));

            int dataType = rs.getInt("DATA_TYPE");
            String sqlTypeName = DatabaseUtils.getSqlTypeName(dataType);
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

        fillKeyIndex(connection, columns, catalog, schemaPattern, tableNamePattern);

        return columns;
    }

    public DatabaseNode getDatabaseNode(Connection connection, String catalog, String schemaPattern,
        String tableNamePattern) throws SQLException {

        DatabaseNodeBuilder builder = new DatabaseNodeBuilder();
        List<ColumnDefinition> columns = getColumns(connection, catalog, schemaPattern,
            tableNamePattern);
        builder.append(columns);

        return builder.build();
    }

    private void fillKeyIndex(Connection connection, List<ColumnDefinition> columns, String catalog,
        String schemaPattern, String tableNamePattern) throws SQLException {

        Map<ColumnKey, ColumnDefinition> columnMap = new HashMap<>();
        for (ColumnDefinition column : columns) {
            ColumnKey key = new ColumnKey(column.getCatalogName(), column.getSchemaName(),
                column.getTableName(), column.getColumnName());
            columnMap.put(key, column);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
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
