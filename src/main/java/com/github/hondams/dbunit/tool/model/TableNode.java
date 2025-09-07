package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class TableNode {

    private String tableName;
    private List<ColumnNode> columns = new ArrayList<>();
}
