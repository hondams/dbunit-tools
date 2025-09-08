package com.github.hondams.dbunit.tool.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import java.io.File;
import java.sql.Connection;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export data from database to file")
@Component
public class DbDefExportCommand implements Callable<Integer> {

    @Option(names = {"-t", "--table"})
    String table;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Autowired
    DataSource dataSource;

    @Autowired
    ObjectMapper objectMapper;

    @Override
    public Integer call() throws Exception {

        TableKey tableKey = TableKey.fromQualifiedTableName(this.table);
        if (tableKey == null) {
            System.out.println("Invalid table name: " + this.table);
            return -1;
        }

        DatabaseNode databaseNode;
        try (Connection connection = this.dataSource.getConnection()) {
            databaseNode = DatabaseUtils.getDatabaseNode(connection, tableKey.getCatalogName(),
                tableKey.getSchemaName(), tableKey.getTableName());
        }

        File outputFile = new File(this.output);
        if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
            boolean createdDirs = outputFile.getParentFile().mkdirs();
            if (!createdDirs) {
                System.err.println("Failed to create directories: " + outputFile.getParentFile());
                return -1;
            }
        }
        this.objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, databaseNode);
        System.out.println("Exported to " + outputFile.getAbsolutePath());

        return 0;
    }
}
