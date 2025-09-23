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
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;

@UtilityClass
public class TableMetaDataUtils {

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
