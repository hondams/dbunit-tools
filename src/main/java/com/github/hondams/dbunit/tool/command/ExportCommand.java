package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.DbUnitUtils;
import java.io.File;
import java.sql.Connection;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export data from database to file")
@Component
public class ExportCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"})
    String scheme;

    @Option(names = {"-t", "--table"}, split = ",")
    String[] table;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {
            DatabaseConnection databaseConnection = new DatabaseConnection(connection, this.scheme);
            IDataSet inputDataSet;
            if (this.table == null || this.table.length == 0) {
                inputDataSet = DbUnitUtils.createDatabaseDataSet(databaseConnection);
            } else {
                inputDataSet = DbUnitUtils.createDatabaseDataSet(databaseConnection, this.table);
            }

            File outputFile = new File(this.output);
            if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
                boolean createdDirs = outputFile.getParentFile().mkdirs();
                if (!createdDirs) {
                    System.err.println(
                        "Failed to create directories: " + outputFile.getParentFile());
                    return -1;
                }
            }
            DbUnitUtils.save(inputDataSet, outputFile);
            System.out.println("Exported to " + outputFile.getAbsolutePath());
        }
        return 0;
    }
}
