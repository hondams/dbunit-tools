package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.model.TableKey;
import java.util.List;

public enum TableNamePattern {
    CATALOG_SCHEMA_TABLE, SCHEMA_TABLE, TABLE;

    public String getTableName(TableKey tableKey) {
        switch (this) {
            case CATALOG_SCHEMA_TABLE:
                return TableKey.toQualifiedTableName(tableKey);
            case SCHEMA_TABLE:
                return tableKey.getSchemaName() + "." + tableKey.getTableName();
            case TABLE:
                return tableKey.getTableName();
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    public static TableNamePattern fromTableName(String tableName) {
        String[] parts = tableName.split("\\.");
        switch (parts.length) {
            case 1:
                return TABLE;
            case 2:
                return SCHEMA_TABLE;
            case 3:
                return CATALOG_SCHEMA_TABLE;
            default:
                throw new IllegalStateException("Invalid table name: " + tableName);
        }
    }


    public static TableNamePattern fromTableNames(List<String> tableNames) {
        int maxPartsLength = 0;
        for (String tableName : tableNames) {
            String[] parts = tableName.split("\\.");
            if (maxPartsLength < parts.length) {
                maxPartsLength = parts.length;
            }
        }
        switch (maxPartsLength) {
            case 1:
                return TABLE;
            case 2:
                return SCHEMA_TABLE;
            case 3:
                return CATALOG_SCHEMA_TABLE;
            default:
                throw new IllegalStateException("Invalid table names: " + tableNames);
        }
    }
}
