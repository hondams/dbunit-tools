package com.github.hondams.dbunit.tool.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import lombok.experimental.UtilityClass;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.ForwardOnlyResultSetTableFactory;
import org.dbunit.dataset.datatype.DefaultDataTypeFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.ext.db2.Db2DataTypeFactory;
import org.dbunit.ext.h2.H2DataTypeFactory;
import org.dbunit.ext.mssql.MsSqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.oracle.Oracle10DataTypeFactory;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;

@UtilityClass
public class DatabaseConnectionFactory {

    public DatabaseConnection create(Connection connection, String schema) {

        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(connection, schema);
            DatabaseConfig databaseConfig = databaseConnection.getConfig();
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData.getDatabaseProductName();

            IDataTypeFactory dataTypeFactory = createDataTypeFactory(productName);
            databaseConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, dataTypeFactory);
            databaseConfig.setProperty(DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY,
                new ForwardOnlyResultSetTableFactory());

            if (productName != null && productName.equalsIgnoreCase("oracle")) {
                databaseConfig.setProperty(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES,
                    true);
            }

            return databaseConnection;
        } catch (DatabaseUnitException | SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private IDataTypeFactory createDataTypeFactory(String productName) {
        if (productName == null) {
            return new DefaultDataTypeFactory();
        }
        switch (productName.toLowerCase()) {
            case "mysql":
                return new MySqlDataTypeFactory();
            case "mariadb":
                // MariaDB is compatible with MySQL
                return new MySqlDataTypeFactory();
            case "postgresql":
                return new PostgresqlDataTypeFactory();
            case "oracle":
                return new Oracle10DataTypeFactory();
            case "microsoft sql server":
            case "sql server":
                return new MsSqlDataTypeFactory();
            case "db2":
                return new Db2DataTypeFactory();
            case "h2":
                return new H2DataTypeFactory();
            default:
                // Unknown database
                return new DefaultDataTypeFactory();
        }
    }
}
