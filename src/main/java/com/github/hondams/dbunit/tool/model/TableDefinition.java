package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class TableDefinition {

    private String tableName;
    private String tableType;
    private List<ColumnDefinition> columns = new ArrayList<>();
}
