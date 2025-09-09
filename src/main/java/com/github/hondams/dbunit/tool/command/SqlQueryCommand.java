package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.PrintLineUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "query", description = "Execute SQL query command")
@Component
public class SqlQueryCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "sql", arity = "1")
    String sql;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        List<String> header = new ArrayList<>();
        List<PrintLineAlignment> alignments = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();

        try (Connection connection = this.dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(this.sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        header.add(metaData.getColumnName(i));
                        int columnType = metaData.getColumnType(i);
                        switch (columnType) {
                            case java.sql.Types.INTEGER:
                            case java.sql.Types.BIGINT:
                            case java.sql.Types.DECIMAL:
                            case java.sql.Types.NUMERIC:
                            case java.sql.Types.FLOAT:
                            case java.sql.Types.REAL:
                            case java.sql.Types.DOUBLE:
                                alignments.add(PrintLineAlignment.RIGHT);
                                break;
                            default:
                                alignments.add(PrintLineAlignment.LEFT);
                        }
                    }

                    while (resultSet.next()) {
                        List<String> row = new ArrayList<>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.add(toString(resultSet.getObject(i)));
                        }
                        rows.add(row);
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

    private String toString(Object obj) {
        if (obj == null) {
            return "<null>";
        }
        return obj.toString();
    }
}
