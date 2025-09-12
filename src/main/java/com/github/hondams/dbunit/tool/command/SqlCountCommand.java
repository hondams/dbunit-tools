package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.TableDefinition;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.DatabaseUtils;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.PrintLineUtils;
import com.github.hondams.dbunit.tool.util.SqlUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
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

@Command(name = "count", description = "Count rows in specified tables")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SqlCountCommand implements Callable<Integer> {

    private static final List<String> HEADER = List.of(//
        "Table",//
        "Count");

    private static final List<PrintLineAlignment> ALIGNMENTS = List.of(//
        PrintLineAlignment.LEFT,//
        PrintLineAlignment.RIGHT);

    @Option(names = {"-t", "--table"}, split = ",",//
        description = "Table name. Specify as [catalog.]schema.table. Pattern match using %% is available.")
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

            try (Statement statement = connection.createStatement()) {
                for (TableDefinition tableDefinition : tableDefinitions) {
                    TableKey tableKey = TableKey.fromTableDefinition(tableDefinition);
                    String tableName = TableKey.toQualifiedTableName(tableKey);
                    String sql = SqlUtils.getCount(tableName);
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
