package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class CatalogNode {

    private String catalogName;

    private List<SchemaNode> schemas = new ArrayList<>();
}
