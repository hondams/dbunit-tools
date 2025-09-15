package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.util.FileUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeSet;
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

    private DiskSortedTableReader reader;
    private int readerIndex = -1;
    private int rowIndex = -1;
    private LinkedHashMap<String, Comparable<Object>> row;

    public DiskSortedTable(ITableMetaData tableMetaData, File tableDirectory)
        throws DataSetException {
        this.tableMetaData = tableMetaData;
        this.tableReaders = createTableReaders(tableMetaData, tableDirectory);
    }

    private List<DiskSortedTableReader> createTableReaders(ITableMetaData tableMetaData,
        File tableDirectory) throws DataSetException {
        List<DiskSortedTableReader> readers = new ArrayList<>();
        File[] files = tableDirectory.listFiles();
        if (files != null) {
            for (File file : new TreeSet<>(List.of(files))) {
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
        if (this.rowIndex < 0) {
            this.rowIndex = 0;
            setNextRow();
        } else {
            if (this.row == null) {
                throw new RowOutOfBoundsException();
            }
        }

        int diff = row - this.rowIndex;
        if (diff < 0) {
            throw new IllegalStateException("Cannot move backward");
        } else if (diff == 0) {
            if (!this.row.containsKey(column)) {
                throw new NoSuchColumnException(this.tableMetaData.getTableName(), column);
            }
            return this.row.get(column);
        } else {
            for (int i = 0; i < diff - 1; i++) {
                this.rowIndex++;
                setNextRow();
                log.warn("Skipped row: table={}, row={}", this.tableMetaData.getTableName(),
                    this.rowIndex);
            }
            this.rowIndex++;
            setNextRow();
            if (!this.row.containsKey(column)) {
                throw new NoSuchColumnException(this.tableMetaData.getTableName(), column);
            }
            return this.row.get(column);
        }
    }

    private void setNextRow() throws DataSetException {
        if (this.reader != null && !this.reader.next()) {
            this.row = this.reader.getRow();
        } else {
            setNextReader();
            while (this.reader != null) {
                if (!this.reader.next()) {
                    this.row = this.reader.getRow();
                    return;
                }
                setNextReader();
            }
            this.row = null;
            throw new RowOutOfBoundsException();
        }
    }

    private void setNextReader() {
        this.readerIndex++;
        if (this.readerIndex < this.tableReaders.size()) {
            this.reader = this.tableReaders.get(this.readerIndex);
        } else {
            this.reader = null;
        }
    }
}
