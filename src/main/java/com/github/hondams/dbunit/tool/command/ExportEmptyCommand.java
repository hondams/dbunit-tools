package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DatabaseConnectionFactory;
import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.model.CatalogNode;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.SchemaNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.model.TableNode;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import com.github.hondams.dbunit.tool.util.SqlUtils;
import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "table", description = "Export table data from database to file")
@Component
@Slf4j
public class ExportEmptyCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"}, required = true)
    String scheme;

    @Option(names = {"-t", "--table"}, split = ",", required = true)
    String[] table;

    @Option(names = {"-e", "--exclude"}, split = ",")
    String[] exclude;

    @Option(names = {"-f", "--format"})
    String format;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {

            List<TableKey> tableKeys = TableKey.fromQualifiedTableNames(List.of(this.table));
            DatabaseNode databaseNode = DatabaseUtils.getDatabaseNode(connection, tableKeys);

            if (databaseNode.getCatalogs().isEmpty()) {
                throw new IllegalStateException("Table not found: " + List.of(this.table));
            }

            DatabaseConnection databaseConnection = DatabaseConnectionFactory.create(connection,
                this.scheme);

            QueryDataSet inputDataSet = new QueryDataSet(databaseConnection);
            for (CatalogNode catalogNode : databaseNode.getCatalogs()) {
                for (SchemaNode schemaNode : catalogNode.getSchemas()) {
                    if (this.scheme.equalsIgnoreCase(schemaNode.getSchemaName())) {
                        for (TableNode tableNode : schemaNode.getTables()) {
                            if (!isExcluded(tableNode.getTableName())) {
                                inputDataSet.addTable(tableNode.getTableName(),
                                    SqlUtils.getEmpty(tableNode.getTableName()));
                            }
                        }
                    }
                }
            }

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

    private boolean isExcluded(String tableName) {
        if (this.exclude != null) {
            for (String ex : this.exclude) {
                if (ex.equalsIgnoreCase(tableName)) {
                    return true;
                }
            }
        }
        return false;
    }
}
