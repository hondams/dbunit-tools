package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class SchemaNode {

    private String schemaName;

    private List<TableNode> tables = new ArrayList<>();
}
