package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DatabaseConnectionFactory;
import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.io.File;
import java.sql.Connection;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.ChunkInsertOperation;
import org.dbunit.operation.CompositeOperation;
import org.dbunit.operation.DatabaseOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "import", description = "Import data from file to database")
@Component
@Slf4j
public class ImportCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"},//
        description = "Schema name. If not specified, the default schema is used.")
    String scheme;

    @Option(names = {"-i", "--input"}, required = true,//
        description = "Input dbunit file path")
    String input;

    @Option(names = {"-c", "--chunk"},//
        description = "Number of rows per batch when inserting. If not specified or less than 1, all rows are inserted at once.")
    int chunk = -1;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {

        File inputFile = new File(this.input);
        if (!inputFile.exists()) {
            ConsolePrinter.println(log, "File1 not found: " + inputFile.getAbsolutePath());
            return 1;
        }

        try (Connection connection = this.dataSource.getConnection()) {
            IDataSet inputDataSet = DbUnitUtils.loadStreaming(inputFile);

            DatabaseConnection databaseConnection = DatabaseConnectionFactory.create(connection,
                this.scheme);
            if (this.chunk <= 0) {
                DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, inputDataSet);
            } else {
                boolean oldAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    DatabaseOperation importOperation = new CompositeOperation(
                        DatabaseOperation.TRUNCATE_TABLE, new ChunkInsertOperation(this.chunk));
                    importOperation.execute(databaseConnection, inputDataSet);
                    connection.commit();
                } catch (Exception e) {
                    connection.rollback();
                    throw e;
                } finally {
                    connection.setAutoCommit(oldAutoCommit);
                }
            }
            ConsolePrinter.println(log, "Imported from " + inputFile.getAbsolutePath());
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
