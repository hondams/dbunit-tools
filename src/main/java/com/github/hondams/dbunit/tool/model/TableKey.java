package com.github.hondams.dbunit.tool.model;

import lombok.Value;

@Value
public class TableKey {

    String catalogName;
    String schemaName;
    String tableName;

    public static TableKey fromTableDefinition(TableDefinition table) {
        return new TableKey(table.getCatalogName(), table.getSchemaName(), table.getTableName());
    }

    public static TableKey fromColumnDefinition(ColumnDefinition column) {
        return new TableKey(column.getCatalogName(), column.getSchemaName(), column.getTableName());
    }

    public static String toQualifiedTableName(TableKey tableKey) {
        if (tableKey.getCatalogName() != null && !tableKey.getCatalogName().isEmpty()) {
            return tableKey.getCatalogName() + "." + tableKey.getSchemaName() + "."
                + tableKey.getTableName();
        } else {
            if (tableKey.getSchemaName() != null && !tableKey.getSchemaName().isEmpty()) {
                return tableKey.getSchemaName() + "." + tableKey.getTableName();
            } else {
                return tableKey.getTableName();
            }
        }
    }

    public static TableKey fromQualifiedTableName(String qualifiedTableName) {
        String catalogName = null;
        String schemaName = null;
        String tableName;
        String[] parts = qualifiedTableName.split("\\.");
        if (parts.length == 1) {
            tableName = parts[0];
        } else if (parts.length == 2) {
            schemaName = parts[0];
            tableName = parts[1];
        } else if (parts.length == 3) {
            catalogName = parts[0];
            schemaName = parts[1];
            tableName = parts[2];
        } else {
            return null;
        }
        return new TableKey(catalogName, schemaName, tableName);
    }
}
