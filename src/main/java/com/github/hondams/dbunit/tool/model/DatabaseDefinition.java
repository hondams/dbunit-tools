package com.github.hondams.dbunit.tool.model;

import java.util.List;
import lombok.Data;

@Data
public class DatabaseDefinition {

    private List<CatalogDefinition> catalogs;
}
