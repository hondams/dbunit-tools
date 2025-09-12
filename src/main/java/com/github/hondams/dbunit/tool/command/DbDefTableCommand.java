package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.PrintLineUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "table",//
    description = "Print database table information")
@Component
@Slf4j
public class DbDefTableCommand implements Callable<Integer> {

    private static final List<String> HEADER = List.of(//
        "Catalog", "Schema",//
        "Table", "TableType");
    private static final List<PrintLineAlignment> ALIGNMENTS = List.of(//
        PrintLineAlignment.LEFT, PrintLineAlignment.LEFT,//
        PrintLineAlignment.LEFT, PrintLineAlignment.LEFT);

    @Option(names = {"-t", "--table"}, split = ",", //
        description = "Table name. Specify as [catalog.]schema.table. Pattern match using % is available.")
    String[] table;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {

            List<List<String>> rows = new ArrayList<>();

            List<TableDefinition> tableDefinitions;
            if (this.table == null || this.table.length == 0) {
                tableDefinitions = DatabaseUtils.getAllTables(connection);
            } else {
                List<TableKey> tableKeys = TableKey.fromQualifiedTableNames(List.of(this.table));
                tableDefinitions = DatabaseUtils.getTables(connection, tableKeys);
            }

            for (TableDefinition table : tableDefinitions) {
                String catalogName = table.getCatalogName();
                String schemaName = table.getSchemaName();
                if (catalogName == null) {
                    catalogName = "<null>";
                }
                if (schemaName == null) {
                    schemaName = "<null>";
                }
                String tableName = table.getTableName();
                String tableType = table.getTableType();
                rows.add(List.of(catalogName, schemaName, tableName, tableType));
            }

            List<String> lines = PrintLineUtils.getTableLines("", HEADER, ALIGNMENTS, rows);
            for (String line : lines) {
                ConsolePrinter.println(log, line);
            }
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
