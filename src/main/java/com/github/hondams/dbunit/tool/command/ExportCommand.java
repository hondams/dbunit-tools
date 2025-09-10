package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DatabaseConnectionFactory;
import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export data from database to file")
@Component
@Slf4j
public class ExportCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"}, required = true)
    String scheme;

    @Option(names = {"-t", "--table"}, split = ",")
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
            DatabaseConnection databaseConnection = DatabaseConnectionFactory.create(connection,
                this.scheme);
            IDataSet inputDataSet;
            if ((this.table == null || this.table.length == 0)//
                && (this.exclude == null || this.exclude.length == 0)) {
                inputDataSet = DbUnitUtils.createDatabaseDataSet(databaseConnection);
            } else {
                List<String> includeTableNames;
                if (this.table == null) {
                    includeTableNames = List.of("%");
                } else {
                    includeTableNames = List.of(this.table);
                }
                List<TableKey> tableKeys = TableKey.fromQualifiedTableNames(includeTableNames);
                List<TableDefinition> tableDefinitions = DatabaseUtils.getTables(connection,
                    tableKeys);
                //                List<String> tableNames = new ArrayList<>();
                //                for (TableDefinition tableDefinition : tableDefinitions) {
                //                    if (this.scheme.equalsIgnoreCase(tableDefinition.getSchemaName())) {
                //                        if (!isExcluded(tableDefinition.getTableName())) {
                //                            tableNames.add(tableDefinition.getTableName());
                //                        }
                //                    }
                //                }
                //                inputDataSet = DbUnitUtils.createDatabaseDataSet(databaseConnection,
                //                    tableNames.toArray(new String[0]));
                QueryDataSet queryDataSet = new QueryDataSet(databaseConnection);
                for (TableDefinition tableDefinition : tableDefinitions) {
                    if (this.scheme.equalsIgnoreCase(tableDefinition.getSchemaName())) {
                        if (!isExcluded(tableDefinition.getTableName())) {
                            TableKey tableKey = TableKey.fromTableDefinition(tableDefinition);
                            String tableName = TableKey.toQualifiedTableName(tableKey);
                            queryDataSet.addTable(tableDefinition.getTableName(),
                                "SELECT * FROM " + tableName);
                        }
                    }
                }
                inputDataSet = queryDataSet;
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
            ConsolePrinter.println(log,"Exported to " + outputFile.getAbsolutePath());
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log,"Error: " + e.getMessage(), e);
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
