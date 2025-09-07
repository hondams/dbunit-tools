package com.github.hondams.dbunit.tool.util;

import java.sql.Connection;
import java.sql.SQLException;
import lombok.experimental.UtilityClass;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;

@UtilityClass
public class DbUnitUtils {

    //    public IDataSet createEmptyDataSet(Connection connection, String schemaName, String tableName) {
    //        try {
    //            DatabaseNode databaseNode = DatabaseUtils.getDatabaseNode(connection, null, schemaName,
    //                tableName);
    //            if (databaseNode.getCatalogs().size() != 1) {
    //                throw new IllegalStateException(
    //                    "Unexpected catalogs size: " + databaseNode.getCatalogs().size());
    //            }
    //            CatalogNode catalogNode = databaseNode.getCatalogs().get(0);
    //            if (catalogNode.getSchemas().size() != 1) {
    //                throw new IllegalStateException(
    //                    "Unexpected schemas size: " + catalogNode.getSchemas().size());
    //            }
    //            SchemaNode schemaNode = catalogNode.getSchemas().get(0);
    //            if (schemaNode.getTables().size() != 1) {
    //                throw new IllegalStateException(
    //                    "Unexpected tables size: " + schemaNode.getTables().size());
    //            }
    //            TableNode tableNode = schemaNode.getTables().get(0);
    //
    //            List<String> columnNames = new ArrayList<>();
    //            for (var column : tableNode.getColumns()) {
    //                columnNames.add(column.getColumnName());
    //            }
    //            ITableMetaData
    //            DefaultTable table = new DefaultTable(tableName, columnNames.toArray(new String[0]));
    //            DefaultDataSet dataSet = new DefaultDataSet(table);
    //            return dataSet;
    //        } catch (SQLException e) {
    //            throw new IllegalStateException(e);
    //        }
    //    }
    public IDataSet createDatabaseDataSet(Connection connection, String schemaName) {
        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(connection, schemaName);
            return databaseConnection.createDataSet();
        } catch (SQLException | DatabaseUnitException e) {
            throw new IllegalStateException(e);
        }
    }

    public IDataSet createDatabaseDataSet(Connection connection, String schemaName,
        String[] tableNames) {
        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(connection, schemaName);
            return databaseConnection.createDataSet(tableNames);
        } catch (SQLException | DatabaseUnitException e) {
            throw new IllegalStateException(e);
        }
    }
}
