package com.github.hondams.dbunit.tool.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hondams.dbunit.tool.dbunit.DbUnitDataTypeFactory;
import com.github.hondams.dbunit.tool.dbunit.DbUnitDataTypeFactoryProvider;
import com.github.hondams.dbunit.tool.model.CatalogDefinition;
import com.github.hondams.dbunit.tool.model.CatalogNode;
import com.github.hondams.dbunit.tool.model.ColumnDefinition;
import com.github.hondams.dbunit.tool.model.ColumnKey;
import com.github.hondams.dbunit.tool.model.ColumnNode;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.DatabaseNodeBuilder;
import com.github.hondams.dbunit.tool.model.SchemaDefinition;
import com.github.hondams.dbunit.tool.model.SchemaNode;
import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.model.TableNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;

@UtilityClass
@Slf4j
public class DatabaseUtils {

    private final ObjectMapper objectMapper = new ObjectMapper();

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

        String productName = connection.getMetaData().getDatabaseProductName();
        DbUnitDataTypeFactory dataTypeFactory =//
            DbUnitDataTypeFactoryProvider.getDbUnitDataTypeFactory(productName);

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

                // see:org.dbunit.util.SQLHelper.createColumn
                int sqlType = rs.getInt("DATA_TYPE");
                //If Types.DISTINCT like SQL DOMAIN, then get Source Date Type of SQL-DOMAIN
                if (sqlType == java.sql.Types.DISTINCT) {
                    sqlType = rs.getInt("SOURCE_DATA_TYPE");
                }
                definition.setSqlType(sqlType);
                definition.setSqlTypeName(rs.getString("TYPE_NAME"));

                DataType dataType = dataTypeFactory.createDataType(sqlType,
                    definition.getSqlTypeName(), definition.getTableName(),
                    definition.getColumnName());
                definition.setDataTypeName(dataType.toString());

                definition.setColumnSize(rs.getInt("COLUMN_SIZE"));
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                if (!rs.wasNull()) {
                    definition.setDecimalDigits(decimalDigits);
                }
                definition.setRemark(rs.getString("REMARKS"));
                definition.setDefaultValue(rs.getString("COLUMN_DEF"));
                definition.setNullable(rs.getString("IS_NULLABLE"));
                definition.setAutoIncrement(rs.getString("IS_AUTOINCREMENT"));

                columns.add(definition);
            }
        } catch (DataTypeException e) {
            throw new SQLException(e);
        }

        fillKeyIndex(connection, columns);

        return columns;
    }

    public DatabaseNode createCopy(DatabaseNode databaseNode, List<TableKey> tableKeys) {

        DatabaseNode copiedDatabaseNode = new DatabaseNode();
        copiedDatabaseNode.setProductName(databaseNode.getProductName());
        for (CatalogNode catalog : databaseNode.getCatalogs()) {
            CatalogNode copiedCatalog = createCopy(catalog, tableKeys);
            if (copiedCatalog != null) {
                copiedDatabaseNode.getCatalogs().add(copiedCatalog);
            }
        }
        return copiedDatabaseNode;
    }

    private CatalogNode createCopy(CatalogNode catalogNode, List<TableKey> tableKeys) {

        CatalogNode copiedCatalog = new CatalogNode();
        copiedCatalog.setCatalogName(copiedCatalog.getCatalogName());
        for (SchemaNode schemaNode : catalogNode.getSchemas()) {
            SchemaNode copiedSchema = createCopy(catalogNode.getCatalogName(), schemaNode,
                tableKeys);
            if (copiedSchema != null) {
                copiedCatalog.getSchemas().add(copiedSchema);
            }
        }
        if (copiedCatalog.getSchemas().isEmpty()) {
            return null;
        }
        return copiedCatalog;
    }

    private SchemaNode createCopy(String catalogName, SchemaNode schemaNode,
        List<TableKey> tableKeys) {

        SchemaNode copiedSchemaNode = new SchemaNode();
        copiedSchemaNode.setSchemaName(schemaNode.getSchemaName());
        for (TableNode tableNode : schemaNode.getTables()) {
            TableKey tableKey = new TableKey(catalogName, schemaNode.getSchemaName(),
                tableNode.getTableName());
            if (tableKeys.contains(tableKey)) {
                copiedSchemaNode.getTables().add(createCopy(tableNode));
            }
        }
        if (copiedSchemaNode.getTables().isEmpty()) {
            return null;
        }
        return copiedSchemaNode;
    }

    private TableNode createCopy(TableNode tableNode) {
        TableNode copiedTable = new TableNode();
        copiedTable.setTableName(tableNode.getTableName());
        copiedTable.setTableType(tableNode.getTableType());
        for (ColumnNode columnNode : tableNode.getColumns()) {
            copiedTable.getColumns().add(createCopy(columnNode));
        }
        return copiedTable;
    }

    private ColumnNode createCopy(ColumnNode columnNode) {
        ColumnNode copiedColumn = new ColumnNode();
        copiedColumn.setColumnName(columnNode.getColumnName());
        copiedColumn.setSqlType(columnNode.getSqlType());
        copiedColumn.setSqlTypeName(columnNode.getSqlTypeName());
        copiedColumn.setDataTypeName(columnNode.getDataTypeName());
        copiedColumn.setColumnSize(columnNode.getColumnSize());
        copiedColumn.setDecimalDigits(columnNode.getDecimalDigits());
        copiedColumn.setRemark(columnNode.getRemark());
        copiedColumn.setDefaultValue(columnNode.getDefaultValue());
        copiedColumn.setNullable(columnNode.getNullable());
        copiedColumn.setAutoIncrement(columnNode.getAutoIncrement());
        copiedColumn.setKeyIndex(columnNode.getKeyIndex());
        return copiedColumn;
    }

    public DatabaseNode getDatabaseNode(Connection connection, List<TableKey> tableKeys)
        throws SQLException {

        DatabaseNodeBuilder builder = new DatabaseNodeBuilder();
        builder.setProductName(connection.getMetaData().getDatabaseProductName());
        List<TableDefinition> tables = getTables(connection, tableKeys);
        builder.appendTable(tables);
        for (TableKey tableKey : tableKeys) {
            List<ColumnDefinition> columns = getColumns(connection, tableKey);
            builder.appendColumn(columns);
        }
        return builder.build();
    }

    public DatabaseNode getDatabaseNode(Connection connection, TableKey tableKey)
        throws SQLException {

        DatabaseNodeBuilder builder = new DatabaseNodeBuilder();
        builder.setProductName(connection.getMetaData().getDatabaseProductName());
        List<TableDefinition> tables = getTables(connection, tableKey);
        builder.appendTable(tables);
        List<ColumnDefinition> columns = getColumns(connection, tableKey);
        builder.appendColumn(columns);

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

    public DatabaseNode loadDatabaseNode(File file) {
        try {
            return objectMapper.readValue(file, DatabaseNode.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void saveDatabaseNode(File file, DatabaseNode databaseNode) {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, databaseNode);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}