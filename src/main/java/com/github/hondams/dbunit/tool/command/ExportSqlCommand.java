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
import org.dbunit.database.QueryDataSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "sql", description = "Export sql result from database to file")
@Component
@Slf4j
public class ExportSqlCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "sql", arity = "1")
    String sql;

    @Option(names = {"-s", "--scheme"}, required = true)
    String scheme;

    @Option(names = {"-t", "--table"}, required = true)
    String table;

    @Option(names = {"-f", "--format"})
    String format;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {
            DatabaseConnection databaseConnection = DatabaseConnectionFactory.create(connection,
                this.scheme);
            QueryDataSet inputDataSet = new QueryDataSet(databaseConnection);
            inputDataSet.addTable(this.table, this.sql);

            File outputFile = new File(this.output);
            if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
                boolean created = outputFile.getParentFile().mkdirs();
                if (!created) {
                    throw new IllegalStateException(
                        "Failed to create directories: " + outputFile.getParentFile());
                }
            }
            DbUnitUtils.save(inputDataSet, outputFile, this.format);
            ConsolePrinter.println(log, "Exported to " + outputFile.getAbsolutePath());
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
