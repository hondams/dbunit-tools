package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import com.github.hondams.dbunit.tool.util.ConsolePrinterLineWriter;
import com.github.hondams.dbunit.tool.util.ConsoleUtils;
import com.github.hondams.dbunit.tool.util.PrintLineAlignment;
import com.github.hondams.dbunit.tool.util.SqlUtils;
import com.github.hondams.dbunit.tool.util.TableWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.Data;
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

    @Option(names = {"-a", "--adjust-value-width"},//
        description = "Adjust column width based on actual data length")
    boolean adjustValueWidth;

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

        try (TableWriter tableWriter = new TableWriter(new ConsolePrinterLineWriter(log))) {
            boolean needsReloadResultSet = false;
            List<QueryColumnMetaData> metaDataList = new ArrayList<>();

            try (ResultSet resultSet = statement.executeQuery(executingSql)) {

                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnCount = resultSetMetaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    QueryColumnMetaData metaData = new QueryColumnMetaData();
                    metaData.setColumnLabel(resultSetMetaData.getColumnLabel(i));
                    metaData.setColumnName(resultSetMetaData.getColumnName(i));
                    metaData.setColumnType(resultSetMetaData.getColumnType(i));
                    metaData.setPrecision(resultSetMetaData.getPrecision(i));
                    metaData.setScale(resultSetMetaData.getScale(i));
                    metaData.setColumnDisplaySize(resultSetMetaData.getColumnDisplaySize(i));

                    metaDataList.add(metaData);
                }

                for (QueryColumnMetaData metaData : metaDataList) {
                    metaData.setHeaderName(metaData.getColumnLabel());
                }
                tableWriter.setHeaderNames(metaDataList.stream()//
                    .map(QueryColumnMetaData::getHeaderName).collect(Collectors.toList()));

                for (QueryColumnMetaData metaData : metaDataList) {
                    metaData.setAlignment(getAlignment(metaData));
                }
                tableWriter.setAlignments(metaDataList.stream()//
                    .map(QueryColumnMetaData::getAlignment).collect(Collectors.toList()));

                if (this.adjustValueWidth) {
                    for (QueryColumnMetaData metaData : metaDataList) {
                        metaData.setDisplaySize(
                            ConsoleUtils.getDisplaySize(metaData.getHeaderName()));
                    }
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            QueryColumnMetaData metaData = metaDataList.get(i - 1);
                            String value = toStringValue(resultSet.getObject(i), metaData);
                            int displaySize = ConsoleUtils.getDisplaySize(value);
                            if (metaData.getDisplaySize() < displaySize) {
                                metaData.setDisplaySize(displaySize);
                            }
                        }
                    }
                    try {
                        boolean moved = resultSet.first();
                        if (!moved) {
                            needsReloadResultSet = true;
                        }
                    } catch (SQLException e) {
                        needsReloadResultSet = true;
                    }
                } else {
                    for (QueryColumnMetaData metaData : metaDataList) {
                        metaData.setDisplaySize(getDisplaySize(metaData));
                    }
                }
                tableWriter.setDisplaySizes(metaDataList.stream()//
                    .map(QueryColumnMetaData::getDisplaySize).collect(Collectors.toList()));

                if (!needsReloadResultSet) {
                    writeRows(resultSet, metaDataList, tableWriter);
                }
            }

            if (needsReloadResultSet) {
                try (ResultSet resultSet = statement.executeQuery(executingSql)) {
                    writeRows(resultSet, metaDataList, tableWriter);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ConsolePrinter.println(log, "");
    }

    private void writeRows(ResultSet resultSet, List<QueryColumnMetaData> metaDataList,
        TableWriter tableWriter) throws SQLException, IOException {
        int rowCount = 0;
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            int columnIndex = 1;
            for (QueryColumnMetaData metaData : metaDataList) {
                String value = toStringValue(resultSet.getObject(columnIndex), metaData);
                row.add(value);
                columnIndex++;
            }
            tableWriter.writeRow(row);
            rowCount++;
        }

        if (metaDataList.isEmpty()) {
            ConsolePrinter.println(log, "The query did not return any columns. rows=" + rowCount);
        }
    }

    private PrintLineAlignment getAlignment(QueryColumnMetaData metaData) {
        switch (metaData.getColumnType()) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
            case java.sql.Types.DOUBLE:
                return PrintLineAlignment.RIGHT;
            default:
                return PrintLineAlignment.LEFT;
        }
    }

    private int getDisplaySize(QueryColumnMetaData metaData) {
        int displaySizes = 0;
        switch (metaData.getColumnType()) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
                displaySizes = metaData.getPrecision() + 1; // 符号
                break;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
            case java.sql.Types.DOUBLE:
                displaySizes = metaData.getPrecision() + 2; //　小数点と符号
                break;
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
                // for "<blob>"
                displaySizes = 6;
                break;
            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
                // for "<clob>"
                displaySizes = 6;
                break;
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
                // for "<clob>"
                displaySizes = metaData.getPrecision() * 2;
                break;
            default:
                displaySizes = metaData.getDisplaySize();
                break;
        }
        if (displaySizes <= 6) {
            // for "<null>"
            displaySizes = 6;
        }
        return displaySizes;
    }

    private String toStringValue(Object value, QueryColumnMetaData metaData) {
        if (value == null) {
            return "<null>";
        }
        switch (metaData.getColumnType()) {
            case java.sql.Types.BLOB:
                return "<bloc>";
            case java.sql.Types.CLOB:
                return "<clob>";
            default:
                return String.valueOf(value);
        }
    }

    @Data
    private static class QueryColumnMetaData {

        private String columnLabel;
        private String columnName;
        private int columnType;
        private int precision;
        private int scale;
        private int columnDisplaySize;

        private String headerName;
        private PrintLineAlignment alignment;
        private int displaySize;
    }
}
