package com.github.hondams.dbunit.tool.model;

import lombok.Data;

@Data
public class TableDefinition {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String tableType;
}
