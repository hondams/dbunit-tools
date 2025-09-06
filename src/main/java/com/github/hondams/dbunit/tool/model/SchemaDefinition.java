package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class SchemaDefinition {

    private String schemaName;

    private List<TableDefinition> tables = new ArrayList<>();
}
