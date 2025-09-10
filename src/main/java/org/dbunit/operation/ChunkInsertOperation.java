package org.dbunit.operation;

import java.sql.SQLException;
import java.util.BitSet;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.statement.IPreparedBatchStatement;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@RequiredArgsConstructor
public class ChunkInsertOperation extends InsertOperation {

    private static final Logger logger = LoggerFactory.getLogger(ChunkInsertOperation.class);

    public static final ChunkInsertOperation CHUNK_INSERT = new ChunkInsertOperation();

    private static final int DEFAULT_CHUNK_SIZE = 3000;

    private final int chunkSize;

    public ChunkInsertOperation() {
        this(DEFAULT_CHUNK_SIZE);
    }

    // DatabaseOperation class
    @Override
    public void execute(IDatabaseConnection connection, IDataSet dataSet)
        throws DatabaseUnitException, SQLException {
        logger.debug("execute(connection={}, dataSet={}) - start", connection, dataSet);

        DatabaseConfig databaseConfig = connection.getConfig();
        IStatementFactory factory = (IStatementFactory) databaseConfig.getProperty(
            DatabaseConfig.PROPERTY_STATEMENT_FACTORY);
        boolean allowEmptyFields = connection.getConfig()
            .getFeature(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS);

        // for each table
        ITableIterator iterator = iterator(dataSet);
        while (iterator.next()) {
            ITable table = iterator.getTable();

            String tableName = table.getTableMetaData().getTableName();
            logger.trace("execute: processing table='{}'", tableName);

            // Do not process empty table
            if (isEmpty(table)) {
                continue;
            }

            ITableMetaData metaData = getOperationMetaData(connection, table.getTableMetaData());
            BitSet ignoreMapping = null;
            OperationData operationData = null;
            IPreparedBatchStatement statement = null;

            boolean uncommited = false;
            int recordCount = 0;
            try {
                // For each row
                int start = this._reverseRowOrder ? table.getRowCount() - 1 : 0;
                int increment = this._reverseRowOrder ? -1 : 1;

                try {
                    for (int i = start; ; i = i + increment) {
                        int row = i;

                        // If current row have a different ignore value mapping
                        // than
                        // previous one, we generate a new statement
                        if (ignoreMapping == null || !equalsIgnoreMapping(ignoreMapping, table,
                            row)) {
                            // Execute and close previous statement
                            if (statement != null) {
                                statement.executeBatch();
                                statement.clearBatch();
                                statement.close();
                            }

                            ignoreMapping = getIgnoreMapping(table, row);
                            operationData = getOperationData(metaData, ignoreMapping, connection);
                            statement = factory.createPreparedBatchStatement(operationData.getSql(),
                                connection);
                        }

                        // for each column
                        Column[] columns = operationData.getColumns();
                        for (int j = 0; j < columns.length; j++) {
                            // Bind value only if not in ignore mapping
                            if (!ignoreMapping.get(j)) {
                                Column column = columns[j];
                                String columnName = column.getColumnName();
                                try {
                                    DataType dataType = column.getDataType();
                                    Object value = table.getValue(row, columnName);

                                    if ("".equals(value) && !allowEmptyFields) {
                                        handleColumnHasNoValue(tableName, columnName);
                                    }

                                    statement.addValue(value, dataType);
                                } catch (TypeCastException e) {
                                    final String msg = "Error casting value for table '" + tableName
                                        + "' and column '" + columnName + "'";
                                    logger.error("execute: {}", msg);
                                    throw new TypeCastException(msg, e);
                                }
                            }
                        }
                        statement.addBatch();
                        uncommited = true;
                        recordCount++;
                        if (recordCount % this.chunkSize == 0) {
                            statement.executeBatch();
                            statement.clearBatch();
                            connection.getConnection().commit();
                            uncommited = false;
                        }
                    }
                } catch (RowOutOfBoundsException e) {
                    // This exception occurs when records are exhausted
                    // and we reach the end of the table. Ignore this error

                    // end of table
                }

                if (uncommited) {
                    statement.executeBatch();
                    statement.clearBatch();
                    connection.getConnection().commit();
                }
            } catch (SQLException e) {
                final String msg = "Exception processing table name='" + tableName + "'";
                throw new DatabaseUnitException(msg, e);
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        }
    }
}
