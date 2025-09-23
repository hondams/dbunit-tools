package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.PrintLineUtils;
import com.github.hondams.dbunit.tool.util.SqlUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "query", description = "Execute SQL query command")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SqlQueryCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1",//
        description = "SQL query to execute")
    String sql;

    @Option(names = {"-f", "--file"},//
        description = "File containing sql to execute")
    String file;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {

        String querySql;
        if (StringUtils.isNotEmpty(this.sql) && StringUtils.isEmpty(this.file)) {
            querySql = this.sql;
        } else if (StringUtils.isEmpty(this.sql) && StringUtils.isNotEmpty(this.file)) {
            File sqlFile = new File(this.file);
            if (!sqlFile.exists()) {
                ConsolePrinter.println(log, "File not found: " + this.file);
                return 1;
            }
            querySql = Files.readString(sqlFile.toPath(), StandardCharsets.UTF_8);
        } else {
            ConsolePrinter.println(log, "sql=" + this.sql + ", file=" + this.file);
            CommandLine cmd = new CommandLine(this);
            cmd.usage(System.out);
            ConsolePrinter.println(log, "Either <sql> or -file option is required.");
            return 1;
        }

        try (Connection connection = this.dataSource.getConnection()) {

            try (Statement statement = connection.createStatement()) {
                for (String executingSql : SqlUtils.splitSqls(querySql)) {
                    executeSql(statement, executingSql);
                }
            }

            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }

    private void executeSql(Statement statement, String executingSql) throws SQLException {
        ConsolePrinter.println(log,
            "Executing SQL: " + executingSql.replace("\r\n", " ").replace("\r", " ")
                .replace("\n", " "));
        List<String> header = new ArrayList<>();
        List<PrintLineAlignment> alignments = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(executingSql)) {
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
            if (header.isEmpty()) {
                ConsolePrinter.println(log,
                    "The query did not return any columns. rows=" + rows.size());
            } else {
                List<String> lines = PrintLineUtils.getTableLines("", header, alignments, rows);
                for (String line : lines) {
                    ConsolePrinter.println(log, line);
                }
            }
        }
        ConsolePrinter.println(log, "");
    }

    private String toString(Object obj) {
        if (obj == null) {
            return "<null>";
        }
        return obj.toString();
    }
}
