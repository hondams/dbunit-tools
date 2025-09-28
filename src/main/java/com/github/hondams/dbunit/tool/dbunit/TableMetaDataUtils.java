package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.model.CatalogNode;
import com.github.hondams.dbunit.tool.model.ColumnNode;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.SchemaNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.model.TableNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Column.AutoIncrement;
import org.dbunit.dataset.Column.Nullable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;

@UtilityClass
public class TableMetaDataUtils {

    public Map.Entry<TableKey, ITableMetaData> selectTableMetaData(
        Map<TableKey, ITableMetaData> tableMetaDataMap, ITableMetaData searchingTableMetaData)
        throws DataSetException {

        TableKey searchingTableKey = TableKey.fromQualifiedTableName(
            searchingTableMetaData.getTableName());
        ITableMetaData writingMetaData = tableMetaDataMap.get(searchingTableKey);
        if (writingMetaData != null) {
            return Map.entry(searchingTableKey, writingMetaData);
        } else {
            Map<TableKey, ITableMetaData> foundMetaDataMap = new LinkedHashMap<>();
            for (Map.Entry<TableKey, ITableMetaData> entry : tableMetaDataMap.entrySet()) {
                TableKey tableKey = entry.getKey();
                if (matchesTableName(tableKey, searchingTableKey)) {
                    foundMetaDataMap.put(tableKey, entry.getValue());
                }
            }
            if (foundMetaDataMap.size() == 1) {
                for (Map.Entry<TableKey, ITableMetaData> entry : foundMetaDataMap.entrySet()) {
                    return entry;
                }
            }
            for (Map.Entry<TableKey, ITableMetaData> entry : foundMetaDataMap.entrySet()) {
                if (equalsAllColumnNames(entry.getValue().getColumns(),
                    searchingTableMetaData.getColumns())) {
                    return entry;
                }
            }
            for (Map.Entry<TableKey, ITableMetaData> entry : foundMetaDataMap.entrySet()) {
                if (includesAllColumnNames(entry.getValue().getColumns(),
                    searchingTableMetaData.getColumns())) {
                    return entry;
                }
            }
            throw new IllegalStateException(
                "Multiple TableMetaData found: " + searchingTableMetaData.getTableName() + " -> "
                    + foundMetaDataMap.keySet());
        }
    }


    private boolean matchesTableName(TableKey tableKey, TableKey searchingTableKey) {
        return (searchingTableKey.getCatalogName() == null//
            || searchingTableKey.getCatalogName().equals(tableKey.getCatalogName()))//
            && (searchingTableKey.getSchemaName() == null//
            || searchingTableKey.getSchemaName().equals(tableKey.getSchemaName()))
            && (searchingTableKey.getTableName().equals(tableKey.getTableName()));
    }

    private boolean equalsAllColumnNames(Column[] columns1, Column[] columns2) {
        if (columns1.length != columns2.length) {
            return false;
        }
        for (int i = 0; i < columns1.length; i++) {
            Column column1 = columns1[i];
            Column column2 = columns2[i];
            if (!column1.getColumnName().equalsIgnoreCase(column2.getColumnName())) {
                return false;
            }
        }
        return true;
    }

    private boolean includesAllColumnNames(Column[] databaseColumns, Column[] dataColumns) {
        if (databaseColumns.length < dataColumns.length) {
            return false;
        }
        for (Column column : dataColumns) {
            Column found = findColumnByName(databaseColumns, column.getColumnName());
            if (found == null) {
                return false;
            }
        }
        return true;
    }


    public Column[] selectColumns(ITableMetaData tableMetaData, List<String> columnNames)
        throws DataSetException {
        Column[] columns = tableMetaData.getColumns();
        Column[] filteredColumns = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (containsColumnName(columnNames, column.getColumnName())) {
                filteredColumns[i] = column;
            }
        }
        return filteredColumns;
    }

    private boolean containsColumnName(List<String> columnNames, String searchingColumnName) {
        for (String columnName : columnNames) {
            if (columnName.equalsIgnoreCase(searchingColumnName)) {
                return true;
            }
        }
        return false;
    }


    private Column findColumnByName(Column[] columns, String columnName) {
        for (Column column : columns) {
            if (column.getColumnName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }


    public Map<TableKey, ITableMetaData> createTableMetaDataMap(DatabaseNode databaseNode,
        TableNamePattern tableNamePattern) {
        DbUnitDataTypeFactory dataTypeFactory = DbUnitDataTypeFactoryProvider.getDbUnitDataTypeFactory(
            databaseNode.getProductName());
        Map<TableKey, ITableMetaData> tableMetaDataMap = new LinkedHashMap<>();
        for (CatalogNode catalogNode : databaseNode.getCatalogs()) {
            for (SchemaNode schemaNode : catalogNode.getSchemas()) {
                for (TableNode tableNode : schemaNode.getTables()) {
                    TableKey tableKey = new TableKey(catalogNode.getCatalogName(),
                        schemaNode.getSchemaName(), tableNode.getTableName());
                    String tableName = tableNamePattern.getTableName(tableKey);
                    // ここでは、tableNameの重複チェックはしない。
                    tableMetaDataMap.put(tableKey,
                        createTableMetaData(dataTypeFactory, tableName, tableNode));
                }
            }
        }
        return tableMetaDataMap;
    }

    private ITableMetaData createTableMetaData(DbUnitDataTypeFactory dataTypeFactory,
        String tableName, TableNode tableNode) {
        List<Column> columns = new ArrayList<>();
        Map<Integer, Column> primaryKeyMap = new TreeMap<>();
        for (ColumnNode columnNode : tableNode.getColumns()) {
            Column column = createColumn(dataTypeFactory, tableNode.getTableName(), columnNode);
            columns.add(column);
            if (columnNode.getKeyIndex() != null) {
                primaryKeyMap.put(columnNode.getKeyIndex(), column);
            }
        }
        List<Column> primaryKeys = new ArrayList<>(primaryKeyMap.values());
        // see: org.dbunit.operation.AbstractOperation#getOperationMetaData
        // DatabaseDataset#createDataSetで、渡しテーブル名
        return new DefaultTableMetaData(tableName,//
            columns.toArray(new Column[0]),//
            primaryKeys.toArray(new Column[0]));
    }

    private Column createColumn(DbUnitDataTypeFactory dataTypeFactory, String tableName,
        ColumnNode columnNode) {

        try {
            String columnName = columnNode.getColumnName();
            // see: org.dbunit.util.SQLHelper#createColumn
            DataType dataType = dataTypeFactory.createDataType(columnNode.getSqlType(),
                columnNode.getSqlTypeName(), tableName, columnName);
            String sqlTypeName = columnNode.getSqlTypeName();
            Nullable nullable = toNullable(columnNode.getNullable());
            String defaultValue = columnNode.getDefaultValue();
            String remarks = columnNode.getRemark();
            AutoIncrement autoIncrement = toAutoIncrement(columnNode.getAutoIncrement());
            return new Column(columnName, dataType, sqlTypeName, nullable, defaultValue, remarks,
                autoIncrement);
        } catch (DataTypeException e) {
            throw new IllegalStateException(e);
        }
    }

    private Nullable toNullable(String nullable) {

        if (StringUtils.isEmpty(nullable)) {
            return Column.NULLABLE_UNKNOWN;
        }
        switch (nullable) {
            case "YES":
                return Column.NULLABLE;
            case "NO":
                return Column.NO_NULLS;
            default:
                throw new IllegalArgumentException("Invalid nullable: " + nullable);
        }
    }

    private AutoIncrement toAutoIncrement(String autoIncrement) {

        if (StringUtils.isEmpty(autoIncrement)) {
            return AutoIncrement.UNKNOWN;
        }
        switch (autoIncrement) {
            case "YES":
                return AutoIncrement.YES;
            case "NO":
                return AutoIncrement.NO;
            default:
                throw new IllegalArgumentException("Invalid autoIncrement: " + autoIncrement);
        }
    }
}
