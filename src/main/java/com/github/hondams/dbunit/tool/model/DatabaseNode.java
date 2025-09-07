package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class DatabaseNode {

    private List<CatalogNode> catalogs = new ArrayList<>();
}
