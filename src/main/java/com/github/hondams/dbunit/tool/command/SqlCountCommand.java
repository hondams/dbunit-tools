package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.PrintLineUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "count", description = "Count rows in specified tables")
@Component
public class SqlCountCommand implements Callable<Integer> {

    @Option(names = {"-t", "--table"}, split = ",")
    String[] table;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        List<String> header = new ArrayList<>();
        header.add("Table");
        header.add("Count");

        List<PrintLineAlignment> alignments = new ArrayList<>();
        alignments.add(PrintLineAlignment.LEFT);
        alignments.add(PrintLineAlignment.RIGHT);

        List<List<String>> rows = new ArrayList<>();

        try (Connection connection = this.dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                List<TableDefinition> tableDefinitions;
                if (this.table == null || this.table.length == 0) {
                    tableDefinitions = DatabaseUtils.getAllTables(connection);
                } else {
                    List<TableKey> tableKeys = TableKey.fromQualifiedTableNames(
                        List.of(this.table));
                    tableDefinitions = DatabaseUtils.getTables(connection, tableKeys);
                }
                for (TableDefinition tableDefinition : tableDefinitions) {
                    TableKey tableKey = TableKey.fromTableDefinition(tableDefinition);
                    String tableName = TableKey.toQualifiedTableName(tableKey);
                    String sql = "SELECT COUNT(*) FROM " + tableName;
                    try (ResultSet resultSet = statement.executeQuery(sql)) {
                        if (resultSet.next()) {
                            int count = resultSet.getInt(1);
                            rows.add(List.of(tableName, String.valueOf(count)));
                        } else {
                            rows.add(List.of(tableName, "0"));
                        }
                    }
                }
            }
        }

        List<String> lines = PrintLineUtils.getTableLines("", header, alignments, rows);
        for (String line : lines) {
            ConsolePrinter.println(line);
        }
        return 0;
    }
}
