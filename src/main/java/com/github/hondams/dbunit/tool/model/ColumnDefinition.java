package com.github.hondams.dbunit.tool.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
public class ColumnDefinition {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String columnName;
    private String sqlTypeName;

    private String typeName;
    private int columnSize;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer decimalDigits;
    private String nullable;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer keyIndex;
}
