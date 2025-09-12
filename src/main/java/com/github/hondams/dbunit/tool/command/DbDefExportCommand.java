package com.github.hondams.dbunit.tool.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export data from database to file")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DbDefExportCommand implements Callable<Integer> {

    @Option(names = {"-t", "--table"}, split = ",", required = true,//
        description = "Table name. Specify as [catalog.]schema.table. Pattern match using %% is available.")
    String[] table;

    @Option(names = {"-o", "--output"}, required = true,//
        description = "Output file path")
    String output;

    @Autowired
    DataSource dataSource;

    @Autowired
    ObjectMapper objectMapper;

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

            List<TableKey> tableKeys = TableKey.fromQualifiedTableNames(List.of(this.table));
            DatabaseNode databaseNode = DatabaseUtils.getDatabaseNode(connection, tableKeys);

            this.objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, databaseNode);
            ConsolePrinter.println(log, "Exported to " + outputFile.getAbsolutePath());

            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
