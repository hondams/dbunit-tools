package com.github.hondams.dbunit.tool.dbunit;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.Getter;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.datatype.DataType;

public class DiskSortedTableReader {

    @Getter
    private final File file;
    private final ITableMetaData metaData;
    private final ITable table;
    private int index = -1;
    private LinkedHashMap<String, Comparable<Object>> row;

    public DiskSortedTableReader(ITableMetaData metaData, File file) throws DataSetException {
        this.file = file;
        this.metaData = metaData;
        this.table = getTable(metaData, file);
    }

    private ITable getTable(ITableMetaData metaData, File file) throws DataSetException {
        IDataSet dataSet = DbUnitUtils.loadStreamingFlatXml(file);
        ITable table = DbUnitUtils.getTable(dataSet, metaData.getTableName());
        if (table == null) {
            throw new NoSuchTableException("Table not found: " + metaData.getTableName());
        }
        return table;
    }

    @SuppressWarnings("unchecked")
    public boolean next() throws DataSetException {
        if (0 < this.index && this.row == null) {
            return false;
        }
        this.index++;
        try {
            LinkedHashMap<String, Comparable<Object>> r = new LinkedHashMap<>();
            for (Column column : this.metaData.getColumns()) {
                String columnName = column.getColumnName();
                DataType dataType = column.getDataType();
                Object v = this.table.getValue(this.index, columnName);
                Object value = dataType.typeCast(v);
                if (v != null && !(value instanceof Comparable)) {
                    throw new IllegalStateException(
                        "Value is not Comparable: table=" + this.metaData.getTableName()//
                            + ", row=" + this.index + ", column=" + columnName + ", value=" + v);
                }
                r.put(columnName, (Comparable<Object>) value);
            }
            this.row = r;
            return true;
        } catch (RowOutOfBoundsException e) {
            this.row = null;
            return false;
        }
    }

    public List<Comparable<Object>> getKeyValues() throws DataSetException {
        LinkedHashMap<String, Comparable<Object>> row = getRow();
        if (row == null) {
            return null;
        }
        List<Comparable<Object>> values = new ArrayList<>();
        for (Column column : this.metaData.getPrimaryKeys()) {
            String columnName = column.getColumnName();
            Comparable<Object> value = row.get(columnName);
            values.add(value);
        }
        return values;
    }

    public List<Comparable<Object>> getRowValues() {
        LinkedHashMap<String, Comparable<Object>> row = getRow();
        if (row == null) {
            return null;
        }
        return new ArrayList<>(row.values());
    }

    public LinkedHashMap<String, Comparable<Object>> getRow() {
        if (this.index < 0) {
            throw new IllegalStateException("No current row. Call next() before calling getRow().");
        }
        return this.row;
    }
}
