package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableIterator;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;

public class DiskSortedDataSet extends AbstractDataSet {

    public static final String METADATA_FILE_NAME = "dbdef.json";

    // ディレクトリを指定する。
    // ディレクトリ直下には、メタデータを構築できる、dbdef.jsonを格納する。
    // ディレクトリの下には、テーブル名のディレクトリを作り、ファイル名順で、ソート済みのFlatXml形式のファイルを格納する。

    private final DiskSortedTable[] tables;

    public DiskSortedDataSet(File dir) throws DataSetException {
        List<ITableMetaData> metaDataList = createTableMetaDataList(dir);
        this.tables = createDiskSortedTables(dir, metaDataList);
    }

    private List<ITableMetaData> createTableMetaDataList(File dir) {
        List<String> tableNames = getTableNames(dir);
        TableNamePattern tableNamePattern = TableNamePattern.fromTableNames(tableNames);
        DatabaseNode databaseNode = DatabaseUtils.loadDatabaseNode(
            new File(dir, METADATA_FILE_NAME));
        return new ArrayList<>(
            TableMetaDataUtils.createTableMetaDataMap(databaseNode, tableNamePattern).values());
    }

    private List<String> getTableNames(File dir) {
        List<String> tableNames = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    tableNames.add(file.getName());
                }
            }
        }
        return tableNames;
    }

    private DiskSortedTable[] createDiskSortedTables(File dir, List<ITableMetaData> metaDataList)
        throws DataSetException {
        List<DiskSortedTable> tables = new ArrayList<>();
        for (ITableMetaData metaData : metaDataList) {
            File tableDir = new File(dir, metaData.getTableName());
            if (tableDir.exists() && tableDir.isDirectory()) {
                tables.add(new DiskSortedTable(metaData, tableDir));
            } else {
                throw new IllegalStateException(
                    "Table directory not found: " + tableDir.getAbsolutePath());
            }
        }
        return tables.toArray(new DiskSortedTable[0]);
    }

    @Override
    protected ITableIterator createIterator(boolean reversed) throws DataSetException {
        return new DefaultTableIterator(this.tables);
    }

    @Override
    public ITableIterator reverseIterator() throws DataSetException {
        throw new UnsupportedOperationException();
    }
}
