package com.github.hondams.dbunit.tool.model;

import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DatabaseDefinitionBuilder {

    private final Connection connection;

    private final Map<CatalogKey, CatalogDefinition> catalogMap = new HashMap<>();
    private final Map<SchemaKey, SchemaDefinition> schemaMap = new HashMap<>();
    private final Map<TableKey, TableDefinition> tableMap = new HashMap<>();

    private DatabaseDefinition databaseDefinition;

    public DatabaseDefinition build() throws SQLException {
        this.catalogMap.clear();
        this.schemaMap.clear();
        this.tableMap.clear();

        this.databaseDefinition = new DatabaseDefinition();

        fillCatalogs();
        fillSchemas();
        fillTables();
        fillColumns();

        return this.databaseDefinition;
    }

    private void fillCatalogs() throws SQLException {
        DatabaseMetaData metaData = this.connection.getMetaData();

        ResultSet rs = metaData.getCatalogs();
        while (rs.next()) {
            CatalogDefinition definition = new CatalogDefinition();
            definition.setCatalogName(rs.getString("TABLE_CAT"));
            CatalogKey key = new CatalogKey(definition.getCatalogName());
            this.databaseDefinition.getCatalogs().add(definition);
            this.catalogMap.put(key, definition);
        }
    }

    private void fillSchemas() throws SQLException {
        DatabaseMetaData metaData = this.connection.getMetaData();
        ResultSet rs = metaData.getSchemas();
        while (rs.next()) {
            SchemaDefinition schema = new SchemaDefinition();
            schema.setSchemaName(rs.getString("TABLE_SCHEM"));

            CatalogKey catalogKey = new CatalogKey(//
                rs.getString("TABLE_CATALOG"));
            SchemaKey key = new SchemaKey(catalogKey.getCatalogName(), schema.getSchemaName());
            CatalogDefinition catalog = this.catalogMap.get(catalogKey);
            if (catalog == null) {
                log.warn("Catalog not found for schema. catalog={}, schema={}",
                    catalogKey.getCatalogName(), schema.getSchemaName());
            } else {
                catalog.getSchemas().add(schema);
            }
            this.schemaMap.put(key, schema);
        }
    }

    private void fillTables() throws SQLException {
        DatabaseMetaData metaData = this.connection.getMetaData();
        // TABLE_TYPE、典型的なタイプは、「TABLE」、「VIEW」、「SYSTEM TABLE」、「GLOBAL TEMPORARY」、「LOCAL TEMPORARY」、「ALIAS」、「SYNONYM」
        ResultSet rs = metaData.getTables(null, null, "%", null);
        while (rs.next()) {
            TableDefinition table = new TableDefinition();
            table.setTableName(rs.getString("TABLE_NAME"));
            table.setTableType(rs.getString("TABLE_TYPE"));

            SchemaKey schemaKey = new SchemaKey(//
                rs.getString("TABLE_CAT"),//
                rs.getString("TABLE_SCHEM"));
            TableKey key = new TableKey(schemaKey.getCatalogName(), schemaKey.getSchemaName(),
                table.getTableName());
            SchemaDefinition schema = this.schemaMap.get(schemaKey);
            if (schema == null) {
                log.warn("Schema not found for table. catalog={}, schema={}, table={}",
                    schemaKey.getCatalogName(), schemaKey.getSchemaName(), table.getTableName());
            } else {
                schema.getTables().add(table);
            }
            this.tableMap.put(key, table);
        }
    }

    private void fillColumns() throws SQLException {
        DatabaseMetaData metaData = this.connection.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, "%", "%");
        while (rs.next()) {
            ColumnDefinition column = new ColumnDefinition();
            column.setColumnName(rs.getString("COLUMN_NAME"));

            int dataType = rs.getInt("DATA_TYPE");
            String sqlTypeName = DatabaseUtils.getSqlTypeName(dataType);
            if (sqlTypeName == null) {
                log.warn("Unknown SQL type. dataType={}", dataType);
                sqlTypeName = "UNKNOWN";
            }
            column.setSqlTypeName(sqlTypeName);

            column.setTypeName(rs.getString("TYPE_NAME"));
            column.setColumnSize(rs.getInt("COLUMN_SIZE"));
            int decimalDigits = rs.getInt("DECIMAL_DIGITS");
            if (!rs.wasNull()) {
                column.setDecimalDigits(decimalDigits);
            }
            column.setNullable(rs.getString("IS_NULLABLE"));

            TableKey tableKey = new TableKey(//
                rs.getString("TABLE_CAT"),//
                rs.getString("TABLE_SCHEM"),//
                rs.getString("TABLE_NAME"));
            TableDefinition table = this.tableMap.get(tableKey);
            if (table == null) {
                log.warn("Table not found for column. catalog={}, schema={}, table={}, column={}",
                    tableKey.getCatalogName(), tableKey.getSchemaName(), tableKey.getTableName(),
                    column.getColumnName());
            } else {
                table.getColumns().add(column);
            }
        }
    }
}
