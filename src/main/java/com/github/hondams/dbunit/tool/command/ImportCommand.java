package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DbUnitUtils;
import java.io.File;
import java.sql.Connection;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "import", description = "Import data from file to database")
@Component
public class ImportCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"})
    String scheme;

    @Option(names = {"-i", "--input"}, required = true)
    String input;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {
            File inputFile = new File(this.input);
            IDataSet inputDataSet = DbUnitUtils.load(inputFile);

            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                DatabaseConnection databaseConnection = new DatabaseConnection(connection,
                    this.scheme);
                DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, inputDataSet);
                ConsolePrinter.println("Imported from " + inputFile.getAbsolutePath());
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        }
        return 0;

    }
}
