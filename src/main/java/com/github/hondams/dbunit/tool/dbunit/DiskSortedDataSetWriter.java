package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.SortedTable;

public class DiskSortedDataSetWriter {

    // DiskSortedDataSetは
    // ディレクトリを指定する。
    // ディレクトリ直下には、メタデータを構築できる、dbdef.jsonを格納する。
    // dbdef.jsonには、データを保持しているテーブルの情報のみを保持する。
    // ディレクトリの下には、テーブル名のディレクトリを作り、すべてレコードをキー、または、すべての値でソートし、
    // ファイル名順に分割して、FlatXml形式のファイルを格納する。
    // ファイル名は、テーブル名＋連番＋.xmlとする。
    //
    // 作成中は、ディレクトリの下には、テーブル名のディレクトリを作り、
    // ファイル単位で、ソートして、
    // ファイル名を、テーブル名＋連番＋.temp.xmlで、一時的に格納する。
    //
    // 最後に、テーブル単位に、
    // 複数ファイルを同時に開き、最小のレコードを返却する。
    // 主キーがある場合は、主キーでソートしておく。
    // 主キーがない場合は、全カラムでソートして、上のような全レコードでソートしたファイルを作成する。

    private final File dir;

    private final DatabaseNode databaseNode;

    private final int splitSize;

    private final Map<TableKey, ITableMetaData> metaDataMap;

    private final Map<String, WritingTable> writingTableMap = new HashMap<>();

    public DiskSortedDataSetWriter(File dir, DatabaseNode databaseNode,
        TableNamePattern tableNamePattern, int splitSize) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IllegalStateException(
                    "Failed to create directory: " + dir.getAbsolutePath());
            }
        } else {
            if (!dir.isDirectory()) {
                throw new IllegalStateException("Not a directory: " + dir.getAbsolutePath());
            } else {
                File[] files = dir.listFiles();
                if (files != null && files.length != 0) {
                    throw new IllegalStateException(
                        "Directory is not empty: " + dir.getAbsolutePath());
                }
            }
        }
        this.dir = dir;
        this.databaseNode = databaseNode;
        this.splitSize = splitSize;
        this.metaDataMap = TableMetaDataUtils.createTableMetaDataMap(databaseNode,
            tableNamePattern);
    }

    public void append(IDataSet dataSet) throws DataSetException {
        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            ITable inputTable = iterator.getTable();
            ITableMetaData inputTableMetaData = inputTable.getTableMetaData();
            WritingTable writingTable = findWritingTableByTableName(inputTableMetaData);
            writeTempTableFiles(inputTable, writingTable);
        }
    }

    public void write() throws DataSetException {
        writeDbdefFile();
        for (WritingTable writingTable : this.writingTableMap.values()) {
            writeTableFiles(writingTable);
        }
    }

    public List<String> getTableNames() {
        return new ArrayList<>(this.writingTableMap.keySet());
    }

    public void setMergeMode(String tableName, TableMergeMode mergeMode) {
        WritingTable writingTable = this.writingTableMap.get(tableName);
        if (writingTable == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        writingTable.setMergeMode(mergeMode);
    }

    private List<TableKey> getWritingTableKeys() {
        List<TableKey> tableKeys = new ArrayList<>();
        for (WritingTable writingTable : this.writingTableMap.values()) {
            tableKeys.add(writingTable.getTableKey());
        }
        return tableKeys;
    }

    private void writeTempTableFiles(ITable inputTable, WritingTable writingTable)
        throws DataSetException {

        String tableName = writingTable.getTableMetaData().getTableName();
        File tableDir = new File(this.dir, tableName);
        if (!tableDir.exists()) {
            if (!tableDir.mkdirs()) {
                throw new IllegalStateException(
                    "Failed to create directory: " + tableDir.getAbsolutePath());
            }
        } else {
            if (!tableDir.isDirectory()) {
                throw new IllegalStateException("Not a directory: " + tableDir.getAbsolutePath());
            }
        }

        Column[] columns = writingTable.getTableMetaData().getColumns();
        Column[] inputColumns = inputTable.getTableMetaData().getColumns();

        int columnCount = columns.length;
        int[] inputColumnIndexes = new int[columnCount];

        for (int i = 0; i < inputColumnIndexes.length; i++) {
            Column column = columns[i];
            inputColumnIndexes[i] = columIndexOf(inputTable.getTableMetaData(),
                column.getColumnName());
        }

        DefaultTable tempTable = new DefaultTable(writingTable.getTableMetaData());
        try {
            for (int rowIndex = 0; ; rowIndex++) {
                Object[] rowValues = new Object[columnCount];
                for (int columnIndex = 0; columnIndex < inputColumnIndexes.length; columnIndex++) {
                    int inputColumnIndex = inputColumnIndexes[columnIndex];
                    if (inputColumnIndex != -1) {
                        Column inputColumn = inputColumns[inputColumnIndex];
                        rowValues[columnIndex] = inputTable.getValue(rowIndex,
                            inputColumn.getColumnName());
                    }
                }
                tempTable.addRow(rowValues);

                if (this.splitSize <= tempTable.getRowCount()) {
                    writeTempTableFile(tableDir, tempTable, writingTable);
                    tempTable = new DefaultTable(writingTable.getTableMetaData());
                }
            }
        } catch (RowOutOfBoundsException e) {
            // end of table
            if (tempTable.getRowCount() != 0) {
                writeTempTableFile(tableDir, tempTable, writingTable);
            }
        }
    }

    private void writeTempTableFile(File tableDir, DefaultTable tempTable,
        WritingTable writingTable) throws DataSetException {

        int fileCount = writingTable.getTempFileCount() + 1;
        writingTable.setTempFileCount(fileCount);
        String fileName = String.format("%s_%04d.temp.xml",
            tempTable.getTableMetaData().getTableName(), fileCount);
        File file = new File(tableDir, fileName);
        DefaultDataSet dataSet = new DefaultDataSet();
        dataSet.addTable(toSortedTable(tempTable));
        DbUnitUtils.save(dataSet, file, DbUnitFileFormat.FLAT_XML);
    }

    private SortedTable toSortedTable(ITable table) throws DataSetException {
        Column[] sortColumns = table.getTableMetaData().getPrimaryKeys();
        if (sortColumns.length == 0) {
            sortColumns = table.getTableMetaData().getColumns();
        }
        SortedTable sortedTable = new SortedTable(table, sortColumns);
        sortedTable.setUseComparable(true);
        return sortedTable;
    }

    private WritingTable findWritingTableByTableName(ITableMetaData searchingTableMetaData)
        throws DataSetException {
        TableKey searchingTableKey = TableKey.fromQualifiedTableName(
            searchingTableMetaData.getTableName());
        ITableMetaData writingMetaData = this.metaDataMap.get(searchingTableKey);
        if (writingMetaData == null) {
            Map<TableKey, ITableMetaData> foundMetaDataMap = new LinkedHashMap<>();
            for (Map.Entry<TableKey, ITableMetaData> entry : this.metaDataMap.entrySet()) {
                TableKey tableKey = entry.getKey();
                if (matchesTableName(tableKey, searchingTableKey)) {
                    foundMetaDataMap.put(tableKey, entry.getValue());
                }
            }
            if (foundMetaDataMap.size() == 1) {
                for (Map.Entry<TableKey, ITableMetaData> entry : foundMetaDataMap.entrySet()) {
                    return getWritingTable(entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<TableKey, ITableMetaData> entry : foundMetaDataMap.entrySet()) {
                if (equalsAllColumnNames(entry.getValue(), searchingTableMetaData)) {
                    return getWritingTable(entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<TableKey, ITableMetaData> entry : foundMetaDataMap.entrySet()) {
                if (includesAllColumnNames(entry.getValue(), searchingTableMetaData)) {
                    return getWritingTable(entry.getKey(), entry.getValue());
                }
            }
            throw new IllegalStateException(
                "Multiple TableMetaData found: " + searchingTableMetaData.getTableName() + " -> "
                    + foundMetaDataMap.keySet());
        } else {
            return getWritingTable(searchingTableKey, writingMetaData);
        }
    }

    private WritingTable getWritingTable(TableKey tableKey, ITableMetaData tableMetaData) {
        String tableName = tableMetaData.getTableName();
        WritingTable writingTable = this.writingTableMap.get(tableName);
        if (writingTable == null) {
            writingTable = new WritingTable();
            writingTable.setTableKey(tableKey);
            writingTable.setTableMetaData(tableMetaData);
            writingTable.setFileCount(0);
            this.writingTableMap.put(tableName, writingTable);
            return writingTable;
        } else {
            if (!writingTable.getTableKey().equals(tableKey)) {
                throw new IllegalStateException(
                    "Duplicated TableName: tableName=" + tableName + ", tableKeys=["
                        + writingTable.getTableKey() + "," + tableKey + "]");
            }
            return writingTable;
        }
    }

    private boolean matchesTableName(TableKey tableKey, TableKey searchingTableKey) {
        return (searchingTableKey.getCatalogName() == null//
            || searchingTableKey.getCatalogName().equals(tableKey.getCatalogName()))//
            && (searchingTableKey.getSchemaName() == null//
            || searchingTableKey.getSchemaName().equals(tableKey.getSchemaName()))
            && (searchingTableKey.getTableName().equals(tableKey.getTableName()));
    }

    private boolean equalsAllColumnNames(ITableMetaData metaData1, ITableMetaData metaData2)
        throws DataSetException {
        if (metaData1.getColumns().length != metaData2.getColumns().length) {
            return false;
        }
        for (int i = 0; i < metaData1.getColumns().length; i++) {
            Column column1 = metaData1.getColumns()[i];
            Column column2 = metaData2.getColumns()[i];
            if (!column1.getColumnName().equalsIgnoreCase(column2.getColumnName())) {
                return false;
            }
        }
        return true;
    }

    private boolean includesAllColumnNames(ITableMetaData databaseMetaData,
        ITableMetaData dataMetaData) throws DataSetException {
        if (databaseMetaData.getColumns().length < dataMetaData.getColumns().length) {
            return false;
        }
        for (Column column : dataMetaData.getColumns()) {
            Column found = findColumnByName(databaseMetaData, column.getColumnName());
            if (found == null) {
                return false;
            }
        }
        return true;
    }

    private Column findColumnByName(ITableMetaData metaData, String columnName)
        throws DataSetException {
        for (Column column : metaData.getColumns()) {
            if (column.getColumnName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }


    private int columIndexOf(ITableMetaData metaData, String columnName) throws DataSetException {
        for (int i = 0; i < metaData.getColumns().length; i++) {
            Column column = metaData.getColumns()[i];
            if (column.getColumnName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private void writeDbdefFile() {
        List<TableKey> tableKeys = getWritingTableKeys();
        DatabaseNode outDatabaseNode = DatabaseUtils.createCopy(this.databaseNode, tableKeys);
        File metaDataFile = new File(this.dir, DiskSortedDataSet.METADATA_FILE_NAME);
        DatabaseUtils.saveDatabaseNode(metaDataFile, outDatabaseNode);
    }

    private void writeTableFiles(WritingTable writingTable) throws DataSetException {
        String tableName = writingTable.getTableMetaData().getTableName();
        File tableDir = new File(this.dir, tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            throw new IllegalStateException(
                "Table directory not found: " + tableDir.getAbsolutePath());
        }

        List<File> tableFiles = new ArrayList<>();
        File[] files = tableDir.listFiles((dir, name) -> name.endsWith(".temp.xml"));
        if (files != null) {
            tableFiles.addAll(Arrays.asList(files));
        }

        ITable mergingTable = createMergingTable(writingTable, tableFiles);

        Column[] columns = writingTable.getTableMetaData().getColumns();
        int columnCount = columns.length;

        DefaultTable table = new DefaultTable(writingTable.getTableMetaData());
        try {
            for (int rowIndex = 0; ; rowIndex++) {
                Object[] rowValues = new Object[columnCount];
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    Column column = columns[columnIndex];
                    rowValues[columnIndex] = mergingTable.getValue(rowIndex,
                        column.getColumnName());
                }
                table.addRow(rowValues);

                if (this.splitSize <= table.getRowCount()) {
                    writeTableFile(tableDir, table, writingTable);
                    table = new DefaultTable(writingTable.getTableMetaData());
                }
            }
        } catch (RowOutOfBoundsException e) {
            // end of table
            if (table.getRowCount() != 0) {
                writeTableFile(tableDir, table, writingTable);
            }
        }

        for (File file : tableFiles) {
            if (!file.delete()) {
                throw new IllegalStateException("Failed to delete file: " + file.getAbsolutePath());
            }
        }
    }

    private ITable createMergingTable(WritingTable writingTable, List<File> files)
        throws DataSetException {
        ITableMetaData tableMetaData = writingTable.getTableMetaData();
        if (tableMetaData.getPrimaryKeys().length == 0) {
            if (writingTable.getMergeMode() == null) {
                return new RowMergingDiskSortedTable(tableMetaData, files);
            } else {
                switch (writingTable.getMergeMode()) {
                    case DISTINCT:
                        return new RowMergingDiskSortedTable(tableMetaData, files);
                    case UNION_ALL:
                        return new AllMergingDiskSortedTable(tableMetaData, files);
                    default:
                        throw new IllegalStateException(
                            "Unexpected merge mode: " + writingTable.getMergeMode());
                }
            }
        } else {
            return new KeyMergingDiskSortedTable(tableMetaData, files);
        }
    }

    private void writeTableFile(File tableDir, DefaultTable table, WritingTable writingTable)
        throws DataSetException {

        int fileCount = writingTable.getFileCount() + 1;
        writingTable.setFileCount(fileCount);
        String fileName = String.format("%s_%04d.xml", table.getTableMetaData().getTableName(),
            fileCount);
        File file = new File(tableDir, fileName);
        DefaultDataSet dataSet = new DefaultDataSet();
        dataSet.addTable(table);
        DbUnitUtils.save(dataSet, file, DbUnitFileFormat.FLAT_XML);
    }

    @Data
    private static class WritingTable {

        private TableKey tableKey;
        private ITableMetaData tableMetaData;
        private TableMergeMode mergeMode;
        private int tempFileCount;
        private int fileCount;
    }
}
