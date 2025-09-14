package com.github.hondams.dbunit.tool.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hondams.dbunit.tool.model.CatalogNode;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.SchemaNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.model.TableNode;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "data", description = "Compare two database definition files")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DiffDataCommand implements Callable<Integer> {

    private static final List<String> TABLE_DIFF_HEADER = List.of(//
        "Table",//
        "Status");
    private static final List<String> COLUMN_DIFF_HEADER = List.of(//
        "Column",//
        "Status",//
        "Property",//
        "Value1",//
        "Value2");

    private static final List<PrintLineAlignment> TABLE_DIFF_ALIGNMENTS = List.of(//
        PrintLineAlignment.LEFT,//
        PrintLineAlignment.LEFT);

    private static final List<PrintLineAlignment> COLUMN_DIFF_ALIGNMENTS = List.of(//
        PrintLineAlignment.LEFT,//
        PrintLineAlignment.LEFT,//
        PrintLineAlignment.LEFT,//
        PrintLineAlignment.LEFT,//
        PrintLineAlignment.LEFT);

    @Option(names = {"-d", "--dbdef-file"}, required = true,//
        description = "Database definition file 1. Specify a file exported by dbdef export command.")
    String dbdefFile;
    @Option(names = {"-f1", "--file1"}, required = true,//
        description = "DbUnit file 1. Specify a file exported by export command.")
    String file1;
    @Option(names = {"-f2", "--file2"}, required = true,//
        description = "DbUnit file 2. Specify a file exported by export command.")
    String file2;

    @Autowired
    ObjectMapper objectMapper;

    @Override
    public Integer call() throws Exception {

        File inputDbDefFile = new File(this.dbdefFile);
        if (!inputDbDefFile.exists()) {
            ConsolePrinter.println(log,
                "DbDef File not found: " + inputDbDefFile.getAbsolutePath());
            return 1;
        }
        File inputFile1 = new File(this.file1);
        if (!inputFile1.exists()) {
            ConsolePrinter.println(log, "File1 not found: " + inputFile1.getAbsolutePath());
            return 1;
        }
        File inputFile2 = new File(this.file2);
        if (!inputFile2.exists()) {
            ConsolePrinter.println(log, "File2 not found: " + inputFile2.getAbsolutePath());
            return 1;
        }

        try {
            DatabaseNode databaseNode = this.objectMapper.readValue(inputDbDefFile,
                DatabaseNode.class);

            Map<TableKey, TableNode[]> tableMap = new TreeMap<>(TableKey.COMPARATOR);
            for (CatalogNode catalogNode : databaseNode.getCatalogs()) {
                for (SchemaNode schemaNode : catalogNode.getSchemas()) {
                    for (TableNode tableNode : schemaNode.getTables()) {
                        TableKey tableKey = new TableKey(catalogNode.getCatalogName(),
                            schemaNode.getSchemaName(), tableNode.getTableName());
                        TableNode[] nodes = new TableNode[2];
                        nodes[0] = tableNode;
                        tableMap.put(tableKey, nodes);
                    }
                }
            }
            //            for (CatalogNode catalogNode : databaseNode2.getCatalogs()) {
            //                for (SchemaNode schemaNode : catalogNode.getSchemas()) {
            //                    for (TableNode tableNode : schemaNode.getTables()) {
            //                        TableKey tableKey = new TableKey(catalogNode.getCatalogName(),
            //                            schemaNode.getSchemaName(), tableNode.getTableName());
            //                        TableNode[] nodes = tableMap.computeIfAbsent(tableKey,
            //                            k -> new TableNode[2]);
            //                        nodes[1] = tableNode;
            //                    }
            //                }
            //            }
            //
            //            printTableDiff(tableMap);
            //
            //            printColumnDiff(tableMap);

            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }

    //    private void printTableDiff(Map<TableKey, TableNode[]> tableMap) {
    //        List<List<String>> rows = new ArrayList<>();
    //        for (Map.Entry<TableKey, TableNode[]> entry : tableMap.entrySet()) {
    //            TableKey tableKey = entry.getKey();
    //            TableNode[] nodes = entry.getValue();
    //            TableNode tableNode1 = nodes[0];
    //            TableNode tableNode2 = nodes[1];
    //
    //            String tableName = TableKey.toQualifiedTableName(tableKey);
    //            if (tableNode1 == null) {
    //                rows.add(List.of(tableName, "Only in file2"));
    //            } else if (tableNode2 == null) {
    //                rows.add(List.of(tableName, "Only in file1"));
    //            } else {
    //                if (tableNode1.equals(tableNode2)) {
    //                    rows.add(List.of(tableName, "Same"));
    //                } else {
    //                    rows.add(List.of(tableName, "Different"));
    //                }
    //            }
    //        }
    //        List<String> lines = PrintLineUtils.getTableLines("", TABLE_DIFF_HEADER,
    //            TABLE_DIFF_ALIGNMENTS, rows);
    //        for (String line : lines) {
    //            ConsolePrinter.println(log, line);
    //        }
    //        ConsolePrinter.println(log, "");
    //    }
    //
    //    private void printColumnDiff(Map<TableKey, TableNode[]> tableMap) {
    //
    //        for (Map.Entry<TableKey, TableNode[]> entry : tableMap.entrySet()) {
    //            TableKey tableKey = entry.getKey();
    //            TableNode[] nodes = entry.getValue();
    //            TableNode tableNode1 = nodes[0];
    //            TableNode tableNode2 = nodes[1];
    //
    //            if (tableNode1 == null || tableNode2 == null || tableNode1.equals(tableNode2)) {
    //                // No column comparison if the table exists in only one file.
    //                continue;
    //            }
    //
    //            Map<String, ColumnNode[]> columnMap = new LinkedHashMap<>();
    //            for (ColumnNode columnNode : tableNode1.getColumns()) {
    //                ColumnNode[] columnNodes = new ColumnNode[2];
    //                columnNodes[0] = columnNode;
    //                columnMap.put(columnNode.getColumnName(), columnNodes);
    //            }
    //            for (ColumnNode columnNode : tableNode2.getColumns()) {
    //                ColumnNode[] columnNodes = columnMap.computeIfAbsent(columnNode.getColumnName(),
    //                    k -> new ColumnNode[2]);
    //                columnNodes[1] = columnNode;
    //            }
    //
    //            List<List<String>> rows = new ArrayList<>();
    //            for (Map.Entry<String, ColumnNode[]> columnEntry : columnMap.entrySet()) {
    //                String columnName = columnEntry.getKey();
    //                ColumnNode[] columnNodes = columnEntry.getValue();
    //                ColumnNode columnNode1 = columnNodes[0];
    //                ColumnNode columnNode2 = columnNodes[1];
    //
    //                if (columnNode1 == null) {
    //                    rows.add(List.of(columnName, "Only in file2", "", "", ""));
    //                } else if (columnNode2 == null) {
    //                    rows.add(List.of(columnName, "Only in file1", "", "", ""));
    //                } else {
    //                    if (!columnNode1.equals(columnNode2)) {
    //                        if (!Objects.equals(columnNode1.getLocation(), columnNode2.getLocation())) {
    //                            rows.add(List.of(columnName, "Different", "Location",
    //                                String.valueOf(columnNode1.getLocation()),
    //                                String.valueOf(columnNode2.getLocation())));
    //                        }
    //                        if (!Objects.equals(columnNode1.getSqlTypeName(),
    //                            columnNode2.getSqlTypeName())) {
    //                            rows.add(List.of(columnName, "Different", "SqlTypeName",
    //                                columnNode1.getSqlTypeName(), columnNode2.getSqlTypeName()));
    //                        }
    //                        if (!Objects.equals(columnNode1.getTypeName(), columnNode2.getTypeName())) {
    //                            rows.add(List.of(columnName, "Different", "TypeName",
    //                                columnNode1.getTypeName(), columnNode2.getTypeName()));
    //                        }
    //                        if (!Objects.equals(columnNode1.getColumnSize(),
    //                            columnNode2.getColumnSize())) {
    //                            rows.add(List.of(columnName, "Different", "ColumnSize",
    //                                String.valueOf(columnNode1.getColumnSize()),
    //                                String.valueOf(columnNode2.getColumnSize())));
    //                        }
    //                        if (!Objects.equals(columnNode1.getDecimalDigits(),
    //                            columnNode2.getDecimalDigits())) {
    //                            rows.add(List.of(columnName, "Different", "DecimalDigits",
    //                                columnNode1.getDecimalDigits() == null ? ""
    //                                    : String.valueOf(columnNode1.getDecimalDigits()),
    //                                columnNode2.getDecimalDigits() == null ? ""
    //                                    : String.valueOf(columnNode2.getDecimalDigits())));
    //                        }
    //                        if (!Objects.equals(columnNode1.getNullable(), columnNode2.getNullable())) {
    //                            rows.add(List.of(columnName, "Different", "Nullable",
    //                                columnNode1.getNullable(), columnNode2.getNullable()));
    //                        }
    //                        if (!Objects.equals(columnNode1.getKeyIndex(), columnNode2.getKeyIndex())) {
    //                            rows.add(List.of(columnName, "Different", "KeyIndex",
    //                                columnNode1.getKeyIndex() == null ? ""
    //                                    : String.valueOf(columnNode1.getKeyIndex()),
    //                                columnNode2.getKeyIndex() == null ? ""
    //                                    : String.valueOf(columnNode2.getKeyIndex())));
    //                        }
    //                    }
    //                }
    //            }
    //            String tableName = TableKey.toQualifiedTableName(tableKey);
    //            ConsolePrinter.println(log, "Table: " + tableName);
    //
    //            List<String> lines = PrintLineUtils.getTableLines("", COLUMN_DIFF_HEADER,
    //                COLUMN_DIFF_ALIGNMENTS, rows);
    //            for (String line : lines) {
    //                ConsolePrinter.println(log, line);
    //            }
    //            ConsolePrinter.println(log, "");
    //        }
    //    }
}
