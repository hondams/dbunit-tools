package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DatabaseConnectionFactory;
import com.github.hondams.dbunit.tool.dbunit.DbUnitFileFormat;
import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.io.File;
import java.sql.Connection;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.ForwardOnlyResultSetTableFactory;
import org.dbunit.database.QueryDataSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "sql", description = "Export sql result from database to file")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ExportSqlCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "1",//
        description = "SQL query to export")
    String sql;

    @Option(names = {"-s", "--scheme"},//
        description = "Schema name. If not specified, the default schema is used.")
    String scheme;

    @Option(names = {"-t", "--table"}, required = true, //
        description = "Table name. Specify only the table name.")
    String table;

    @Option(names = {"-f", "--format"},//
        description = "File format. " //
            + "When outputting as XML or CSV, this option must be specified. "//
            + "If not specified, the format is inferred from the file extension.")
    DbUnitFileFormat format;

    @Option(names = {"-o", "--output"}, required = true,//
        description = "Output dbunit file path. "//
            + "If the format is CSV, specify a directory.")
    String output;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {

        File outputFile = new File(this.output);
        File outputDirectory = outputFile.getParentFile();
        if (outputDirectory != null//
            && (!outputDirectory.isDirectory() || !outputDirectory.exists())) {
            boolean created = outputFile.getParentFile().mkdirs();
            if (!created) {
                ConsolePrinter.println(log,
                    "Failed to create directories: " + outputFile.getParentFile());
                return 1;
            }
        }

        try (Connection connection = this.dataSource.getConnection()) {
            DatabaseConnection databaseConnection = DatabaseConnectionFactory.create(connection,
                this.scheme);
            if (DbUnitUtils.supportsStreamWrite(outputFile, this.format)) {
                DatabaseConfig databaseConfig = databaseConnection.getConfig();
                databaseConfig.setProperty(DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY,
                    new ForwardOnlyResultSetTableFactory());
            }
            QueryDataSet inputDataSet = new QueryDataSet(databaseConnection);
            inputDataSet.addTable(this.table, this.sql);

            DbUnitUtils.save(inputDataSet, outputFile, this.format);
            ConsolePrinter.println(log, "Exported to " + outputFile.getAbsolutePath());
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
