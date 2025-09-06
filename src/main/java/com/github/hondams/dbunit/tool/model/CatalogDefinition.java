package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class CatalogDefinition {

    private String catalogName;

    private List<SchemaDefinition> schemas = new ArrayList<>();
}
