package com.github.hondams.dbunit.tool.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DatabaseNodeBuilder {

    @Setter
    private String productName;
    private final Map<CatalogKey, CatalogNode> catalogMap = new LinkedHashMap<>();
    private final Map<SchemaKey, SchemaNode> schemaMap = new HashMap<>();
    private final Map<TableKey, TableNode> tableMap = new HashMap<>();

    public DatabaseNode build() {
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setProductName(this.productName);
        databaseNode.getCatalogs().addAll(this.catalogMap.values());
        return databaseNode;
    }

    public void appendTable(List<TableDefinition> tables) {
        for (TableDefinition table : tables) {
            TableKey tableKey = TableKey.fromTableDefinition(table);
            TableNode tableNode = getTableNode(tableKey);
            tableNode.setTableType(table.getTableType());
        }
    }

    public void appendColumn(List<ColumnDefinition> columns) {
        for (ColumnDefinition column : columns) {
            append(column);
        }
    }

    public void append(ColumnDefinition column) {

        ColumnNode node = new ColumnNode();
        node.setColumnName(column.getColumnName());
        node.setDataTypeName(column.getDataTypeName());
        node.setSqlType(column.getSqlType());
        node.setSqlTypeName(column.getSqlTypeName());
        node.setColumnSize(column.getColumnSize());
        node.setDecimalDigits(column.getDecimalDigits());
        node.setNullable(column.getNullable());
        node.setRemark(column.getRemark());
        node.setDefaultValue(column.getDefaultValue());
        node.setAutoIncrement(column.getAutoIncrement());
        node.setKeyIndex(column.getKeyIndex());

        TableKey tableKey = TableKey.fromColumnDefinition(column);
        TableNode table = getTableNode(tableKey);

        node.setLocation(table.getColumns().size() + 1);
        table.getColumns().add(node);
    }

    private TableNode getTableNode(TableKey key) {
        TableNode node = this.tableMap.get(key);
        if (node == null) {
            SchemaKey schemaKey = SchemaKey.fromTableKey(key);
            SchemaNode schemaNode = getSchemaNode(schemaKey);

            node = new TableNode();
            node.setTableName(key.getTableName());
            schemaNode.getTables().add(node);

            this.tableMap.put(key, node);
        }
        return node;
    }

    private SchemaNode getSchemaNode(SchemaKey key) {
        SchemaNode node = this.schemaMap.get(key);
        if (node == null) {
            CatalogKey catalogKey = CatalogKey.fromSchemaKey(key);
            CatalogNode catalogNode = getCatalogNode(catalogKey);

            node = new SchemaNode();
            node.setSchemaName(key.getSchemaName());
            catalogNode.getSchemas().add(node);

            this.schemaMap.put(key, node);
        }
        return node;
    }

    private CatalogNode getCatalogNode(CatalogKey key) {
        CatalogNode node = this.catalogMap.get(key);
        if (node == null) {
            node = new CatalogNode();
            node.setCatalogName(key.getCatalogName());

            this.catalogMap.put(key, node);
        }
        return node;
    }
}
