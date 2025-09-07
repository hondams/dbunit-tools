package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.PrintLineUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "table",//
    description = "Print database table information")
@Component
public class DbDefTableCommand implements Callable<Integer> {

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        List<String> header = List.of(//
            "Catalog", "Schema",//
            "Table", "TableType");
        List<PrintLineAlignment> alignments = List.of(//
            PrintLineAlignment.LEFT, PrintLineAlignment.LEFT,//
            PrintLineAlignment.LEFT, PrintLineAlignment.LEFT);
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = this.dataSource.getConnection()) {
            List<TableDefinition> tables = DatabaseUtils.getAllTables(connection);
            for (TableDefinition table : tables) {
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
        }
        List<String> lines = PrintLineUtils.getTableLines("", header, alignments, rows);
        for (String line : lines) {
            System.out.println(line);
        }
        return 0;
    }
}
