package com.github.hondams.dbunit.tool.model;

import lombok.Value;

@Value
public class CatalogKey {

    String catalogName;

    public static CatalogKey fromSchemaKey(SchemaKey schemaKey) {
        return new CatalogKey(schemaKey.getCatalogName());
    }
}
