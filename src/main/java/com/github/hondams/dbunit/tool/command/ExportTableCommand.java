package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DatabaseConnectionFactory;
import com.github.hondams.dbunit.tool.dbunit.DbUnitFileFormat;
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
public class ExportTableCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"},//
        description = "Schema name. If not specified, the default schema is used.")
    String scheme;

    @Option(names = {"-t", "--table"}, split = ",", required = true, //
        description = "Table name. Specify only the table name. Pattern match using %% is available.")
    String[] table;

    @Option(names = {"-e", "--exclude"}, split = ",",//
        description = "Table names to exclude. "//
            + "Specify only the table name. "//
            + "Exclusion is applied only when the table name matches case-insensitively.")
    String[] exclude;

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
        if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
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

            if (databaseNode.getCatalogs().isEmpty()) {
                ConsolePrinter.println(log, "Table not found: " + List.of(this.table));
                return 1;
            }

            DatabaseConnection databaseConnection = DatabaseConnectionFactory.create(connection,
                this.scheme);

            QueryDataSet inputDataSet = new QueryDataSet(databaseConnection);
            for (CatalogNode catalogNode : databaseNode.getCatalogs()) {
                for (SchemaNode schemaNode : catalogNode.getSchemas()) {
                    if (this.scheme == null//
                        || this.scheme.equalsIgnoreCase(schemaNode.getSchemaName())) {
                        for (TableNode tableNode : schemaNode.getTables()) {
                            if (!isExcluded(tableNode.getTableName())) {
                                inputDataSet.addTable(tableNode.getTableName(),
                                    SqlUtils.getAll(tableNode.getTableName(),
                                        tableNode.getColumns()));
                            }
                        }
                    }
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
