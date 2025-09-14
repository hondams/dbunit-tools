package com.github.hondams.dbunit.tool.dbunit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
    // ディレクトリの下には、テーブル名のディレクトリを作り、ソート済みのFlatXml形式のファイルを格納する。
    // 読み込むときは、複数ファイルを同時に開き、最小のレコードを返却する。
    // 主キーがある場合は、主キーでソートしておく。
    // 主キーがない場合は、全カラムでソートしておく。
    // ディレクトリ直下には、メタデータを構築できる、dbdef.jsonを格納する。

    private DiskSortedTable[] tables;

    public DiskSortedDataSet(File dir) throws DataSetException {
        List<ITableMetaData> metaDataList = createTableMetaDataList(dir);
        this.tables = createDiskSortedTables(dir, metaDataList);
    }

    private List<ITableMetaData> createTableMetaDataList(File dir) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            DatabaseNode databaseNode = objectMapper.readValue(//
                new File(dir, METADATA_FILE_NAME), DatabaseNode.class);
            return TableMetaDataUtils.createTableMetaDataList(databaseNode);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
