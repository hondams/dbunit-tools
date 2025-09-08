package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.TableDefinition;
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
                List<String> tableNames = new ArrayList<>();
                if (this.table == null || this.table.length == 0) {
                    List<TableDefinition> tables = DatabaseUtils.getAllTables(connection);
                    for (TableDefinition tableDef : tables) {
                        String tableName = tableDef.getTableName();
                        if (tableDef.getSchemaName() != null) {
                            tableName = tableDef.getSchemaName() + "." + tableName;
                            if (tableDef.getCatalogName() != null) {
                                tableName = tableDef.getCatalogName() + "." + tableName;
                            }
                        }
                        tableNames.add(tableName);
                    }
                } else {
                    tableNames.addAll(List.of(this.table));
                }
                for (String tableName : tableNames) {
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
            System.out.println(line);
        }
        return 0;
    }
}
