package com.github.hondams.dbunit.tool.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.dbunit.TableMetaDataUtils;
import com.github.hondams.dbunit.tool.dbunit.TableNamePattern;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "record", description = "Compare table record between two DbUnit files")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DiffRecordCommand implements Callable<Integer> {

    private static final List<String> RECORD_DIFF_HEADER = List.of(//
        "Key",//
        "Status");//Same,Different,Only in file1,Only in file2

    private static final List<PrintLineAlignment> RECORD_DIFF_ALIGNMENTS = List.of(//
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
    @Option(names = {"-t", "--table"}, required = true, //
        description = "Comparing Table name. Specify only the table name.")
    String tableName;

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
            ITable table1 = getTable(inputFile1);
            ITable table2 = getTable(inputFile2);
            ITableMetaData tableMetaData = getTableMetaData(inputDbDefFile,
                List.of(table1.getTableMetaData(), table2.getTableMetaData()));
            int index1 = 0;
            int index2 = 0;
            Object[] lastRowValues1 = null;
            Object[] lastRowValues2 = null;
            Object[] rowValues1 = DbUnitUtils.getRowValues(tableMetaData, table1, index1);
            Object[] rowValues2 = DbUnitUtils.getRowValues(tableMetaData, table2, index2);
            while (rowValues1 != null && rowValues2 != null) {
                //                int cmp = DbUnitUtils.compareRowValues(tableMetaData, rowValues1, rowValues2);
                //                if (cmp == 0) {
                //                    // Same
                //                    index1++;
                //                    index2++;
                //                    rowValues1 = DbUnitUtils.getRowValues(tableMetaData, table1, index1);
                //                    rowValues2 = DbUnitUtils.getRowValues(tableMetaData, table2, index2);
                //                } else if (cmp < 0) {
                //                    // Only in file1
                //                    index1++;
                //                    rowValues1 = DbUnitUtils.getRowValues(tableMetaData, table1, index1);
                //                } else {
                //                    // Only in file2
                //                    index2++;
                //                    rowValues2 = DbUnitUtils.getRowValues(tableMetaData, table2, index2);
                //                }
                //
            }

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


    private ITable getTable(File file) throws DataSetException {
        IDataSet dataSet = DbUnitUtils.loadStreaming(file);
        ITable table = DbUnitUtils.getTable(dataSet, this.tableName);
        if (table == null) {
            throw new NoSuchTableException(
                "Table not found: " + this.tableName + " in " + file.getAbsolutePath());
        }
        return table;
    }

    private ITableMetaData getTableMetaData(File dbDefFile, List<ITableMetaData> tableMetaDataList)
        throws DataSetException {
        DatabaseNode databaseNode = DatabaseUtils.loadDatabaseNode(dbDefFile);
        TableNamePattern tableNamePattern = TableNamePattern.fromTableName(this.tableName);
        Map<TableKey, ITableMetaData> tableMetaDataMap = TableMetaDataUtils.createTableMetaDataMap(
            databaseNode, tableNamePattern);
        Map.Entry<TableKey, ITableMetaData> entry = TableMetaDataUtils.selectTableMetaData(
            tableMetaDataMap, getSearchingTableMetaData(tableMetaDataList));
        return entry.getValue();
    }

    private ITableMetaData getSearchingTableMetaData(List<ITableMetaData> tableMetaDataList)
        throws DataSetException {

        ITableMetaData foundTableMetaData = null;
        for (ITableMetaData tableMetaData : tableMetaDataList) {
            if (foundTableMetaData == null) {
                foundTableMetaData = tableMetaData;
            } else {
                if (foundTableMetaData.getColumns().length < tableMetaData.getColumns().length) {
                    foundTableMetaData = tableMetaData;
                }
            }
        }
        return foundTableMetaData;
    }
}
