package com.github.hondams.dbunit.tool.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
public class ColumnDefinition {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String columnName;
    private String dataTypeName;
    private int sqlType;
    private String sqlTypeName;
    private int columnSize;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer decimalDigits;
    private String remark;
    private String defaultValue;
    private String nullable;
    private String autoIncrement;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer keyIndex;
}
