package com.github.hondams.dbunit.tool.model;

import lombok.Value;

@Value
public class SchemaKey {

    String catalogName;
    String schemaName;

    public static SchemaKey fromTableKey(TableKey tableKey) {
        return new SchemaKey(tableKey.getCatalogName(), tableKey.getSchemaName());
    }
}
