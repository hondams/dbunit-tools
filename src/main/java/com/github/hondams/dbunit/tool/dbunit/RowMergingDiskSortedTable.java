package com.github.hondams.dbunit.tool.dbunit;

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
public class RowMergingDiskSortedTable extends AbstractTable {

    // テーブル名のディレクトリを作り、ソート済みのFlatXml形式のファイルを格納する。
    // 読み込むときは、複数ファイルを同時に開き、最小のレコードを返却する。
    // 主キーがある場合は、主キーでソートしておく。
    // 主キーがない場合は、全カラムでソートしておく。
    // pkがない場合、重複レコードを1つだけを保持する。

    @Getter
    private final ITableMetaData tableMetaData;

    private final List<DiskSortedTableReader> tableReaders;

    private final Map<List<Comparable<Object>>, DiskSortedTableReader> readerMap =//
        new TreeMap<>(DiskSortedTableRecordComparator.INSTANCE);

    private int rowIndex = -1;
    private LinkedHashMap<String, Comparable<Object>> row;

    public RowMergingDiskSortedTable(ITableMetaData tableMetaData, List<File> files)
        throws DataSetException {
        this.tableMetaData = tableMetaData;
        this.tableReaders = createTableReaders(tableMetaData, files);
    }

    private List<DiskSortedTableReader> createTableReaders(ITableMetaData tableMetaData,
        List<File> files) throws DataSetException {
        List<DiskSortedTableReader> readers = new ArrayList<>();
        for (File file : files) {
            readers.add(new DiskSortedTableReader(tableMetaData, file));
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
            for (DiskSortedTableReader reader : this.tableReaders) {
                putNextKey(reader);
            }
            this.rowIndex = 0;
            setNextRow();
        } else if (this.row == null) {
            throw new RowOutOfBoundsException();
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
            List<Comparable<Object>> key = reader.getRowValues();
            DiskSortedTableReader duplicatedReader = this.readerMap.get(key);
            if (duplicatedReader != null) {
                int comparison = reader.getFile().getName()
                    .compareTo(duplicatedReader.getFile().getName());
                if (0 < comparison) { // duplicatedReader < reader
                    // readerの方が後なので、readerを優先する。
                    this.readerMap.put(key, reader);
                    printDuplicateRow(key);
                    putNextKey(duplicatedReader);
                    break;
                } else if (comparison < 0) {// reader < duplicatedReader
                    // duplicatedReaderの方が後なので、readerの行をスキップし次の行を評価する。
                    printDuplicateRow(key);
                } else {
                    // 同じファイル名はありえない。
                    throw new IllegalStateException(
                        "Same file name: " + reader.getFile().getName());
                }
            } else {
                this.readerMap.put(key, reader);
                break;
            }
        }
    }

    private void printDuplicateRow(List<Comparable<Object>> row) {
        log.warn("Duplicate row: skip={}", row);
    }
}
