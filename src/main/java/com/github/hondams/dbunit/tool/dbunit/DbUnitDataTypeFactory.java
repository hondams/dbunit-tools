package com.github.hondams.dbunit.tool.dbunit;

import lombok.RequiredArgsConstructor;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.DefaultDataTypeFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.ext.db2.Db2DataTypeFactory;
import org.dbunit.ext.h2.H2DataTypeFactory;
import org.dbunit.ext.mssql.MsSqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.oracle.Oracle10DataTypeFactory;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;

@RequiredArgsConstructor
public class DbUnitDataTypeFactory implements IDataTypeFactory {

    private final String productName;
    private IDataTypeFactory dataTypeFactory;

    @Override
    public DataType createDataType(int sqlType, String sqlTypeName) throws DataTypeException {
        IDataTypeFactory dataTypeFactory = getDataTypeFactory();
        DataType dataType = dataTypeFactory.createDataType(sqlType, sqlTypeName);
        if (dataType != DataType.UNKNOWN) {
        }
        return dataType;
    }

    @Override
    public DataType createDataType(int sqlType, String sqlTypeName, String tableName,
        String columnName) throws DataTypeException {
        IDataTypeFactory dataTypeFactory = getDataTypeFactory();
        DataType dataType = dataTypeFactory.createDataType(sqlType, sqlTypeName, tableName,
            columnName);
        if (dataType != DataType.UNKNOWN) {
        }
        return dataType;
    }

    private IDataTypeFactory getDataTypeFactory() {
        if (this.dataTypeFactory == null) {
            this.dataTypeFactory = createDataTypeFactory(this.productName);
        }
        return this.dataTypeFactory;
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
