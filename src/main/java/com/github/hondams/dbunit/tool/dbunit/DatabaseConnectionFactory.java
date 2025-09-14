package com.github.hondams.dbunit.tool.dbunit;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import lombok.experimental.UtilityClass;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.datatype.IDataTypeFactory;

@UtilityClass
public class DatabaseConnectionFactory {

    public DatabaseConnection create(Connection connection, String schema) {

        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(connection, schema);
            DatabaseConfig databaseConfig = databaseConnection.getConfig();
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData.getDatabaseProductName();

            IDataTypeFactory dataTypeFactory = new DbUnitDataTypeFactory(productName);
            databaseConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, dataTypeFactory);

            if (productName != null && productName.equalsIgnoreCase("oracle")) {
                databaseConfig.setProperty(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES,
                    true);
            }

            return databaseConnection;
        } catch (DatabaseUnitException | SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
