package com.github.hondams.dbunit.tool.model;

import lombok.Value;

@Value
public class TableKey {

    String catalogName;
    String schemaName;
    String tableName;
}
