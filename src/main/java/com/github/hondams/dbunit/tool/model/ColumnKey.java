package com.github.hondams.dbunit.tool.model;

import lombok.Value;

@Value
public class ColumnKey {

    String catalogName;
    String schemaName;
    String tableName;
    String columnName;
}
