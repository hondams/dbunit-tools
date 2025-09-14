package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.util.FileUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.AbstractTable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.RowOutOfBoundsException;

@Slf4j
public class DiskSortedTable extends AbstractTable {

    @Getter
    private final ITableMetaData tableMetaData;

    private final List<DiskSortedTableReader> tableReaders;

    private final Map<List<Comparable<Object>>, DiskSortedTableReader> readerMap =//
        new TreeMap<>(DiskSortedTableRecordComparator.INSTANCE);

    private final boolean hasPrimaryKey;

    private int index = -1;
    private LinkedHashMap<String, Comparable<Object>> row;

    public DiskSortedTable(ITableMetaData tableMetaData, File tableDirectory)
        throws DataSetException {
        this.tableMetaData = tableMetaData;
        this.hasPrimaryKey = hasPrimaryKey(tableMetaData);
        this.tableReaders = createTableReaders(tableMetaData, tableDirectory);
    }

    private boolean hasPrimaryKey(ITableMetaData tableMetaData) throws DataSetException {
        return (tableMetaData.getPrimaryKeys().length != 0);
    }

    private List<DiskSortedTableReader> createTableReaders(ITableMetaData tableMetaData,
        File tableDirectory) throws DataSetException {
        List<DiskSortedTableReader> readers = new ArrayList<>();
        File[] files = tableDirectory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    String extension = FileUtils.getFileExtension(file);
                    if (extension != null && extension.equalsIgnoreCase("xml")) {
                        readers.add(new DiskSortedTableReader(tableMetaData, file));
                    }
                }
            }
        }
        return readers;
    }

    @Override
    public int getRowCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(int row, String column) throws DataSetException {
        if (this.index < 0) {
            for (DiskSortedTableReader reader : this.tableReaders) {
                putNextKey(reader);
            }
            this.index = 0;
            setNextRow();
        } else {
            if (this.row == null) {
                throw new RowOutOfBoundsException();
            }
        }

        int diff = row - this.index;
        if (diff < 0) {
            throw new IllegalStateException("Cannot move backward");
        } else if (diff == 0) {
            if (!this.row.containsKey(column)) {
                throw new NoSuchColumnException(this.tableMetaData.getTableName(), column);
            }
            return this.row.get(column);
        } else {
            for (int i = 0; i < diff - 1; i++) {
                this.index++;
                setNextRow();
                log.warn("Skipped row: table={}, row={}", this.tableMetaData.getTableName(),
                    this.index);
            }
            this.index++;
            setNextRow();
            if (!this.row.containsKey(column)) {
                throw new NoSuchColumnException(this.tableMetaData.getTableName(), column);
            }
            return this.row.get(column);
        }
    }

    private void setNextRow() throws DataSetException {
        List<Comparable<Object>> nexKey = getNextKey();
        if (nexKey == null) {
            this.row = null;
            throw new RowOutOfBoundsException();
        } else {
            DiskSortedTableReader reader = this.readerMap.get(nexKey);
            this.row = reader.getRow();
            this.readerMap.remove(nexKey);
            putNextKey(reader);
        }
    }

    private List<Comparable<Object>> getNextKey() {
        for (List<Comparable<Object>> key : this.readerMap.keySet()) {
            return key;
        }
        return null;
    }


    private void putNextKey(DiskSortedTableReader reader) throws DataSetException {
        while (reader.next()) {
            List<Comparable<Object>> key = getReaderKey(reader);
            DiskSortedTableReader duplicatedReader = this.readerMap.get(key);
            if (duplicatedReader != null) {
                log.warn("Duplicate key: {} files=[{}, {}]", key,//
                    duplicatedReader.getFile().getAbsolutePath(),//
                    reader.getFile().getAbsolutePath());
            } else {
                this.readerMap.put(key, reader);
                break;
            }
        }
    }

    private List<Comparable<Object>> getReaderKey(DiskSortedTableReader reader)
        throws DataSetException {
        if (this.hasPrimaryKey) {
            return reader.getKeyValues();
        } else {
            return reader.getRowValues();
        }

    }
}
