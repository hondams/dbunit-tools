package com.github.hondams.dbunit.tool.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;

@Data
public class ColumnNode {

    private String columnName;
    private String sqlTypeName;

    private String typeName;
    private int columnSize;
    @JsonInclude(Include.NON_NULL)
    private Integer decimalDigits;
    private String nullable;
    private Integer keyIndex;
}
