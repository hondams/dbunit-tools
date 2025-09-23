package com.github.hondams.dbunit.tool.util;

import com.github.hondams.dbunit.tool.model.ColumnNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

@UtilityClass
public class SqlUtils {

    public String getEmpty(String tableName) {
        return "SELECT * FROM " + tableName + " WHERE 1=0";
    }

    public String getAll(String tableName, List<ColumnNode> columns) {
        List<ColumnNode> keyColumns = getKeyColumns(columns);
        if (keyColumns.isEmpty()) {
            return "SELECT * FROM " + tableName;
        } else {
            return "SELECT * FROM " + tableName + " ORDER BY " + keyColumns.stream()
                .map(ColumnNode::getColumnName).collect(Collectors.joining(", "));
        }
    }

    public String getCount(String tableName) {
        return "SELECT COUNT(*) FROM " + tableName;
    }

    public List<String> splitSqls(String sqlsText) {
        List<String> sqls = new ArrayList<>();
        try {
            for (net.sf.jsqlparser.statement.Statement sqlStatement ://
                CCJSqlParserUtil.parseStatements(sqlsText)) {
                sqls.add(sqlStatement.toString());
            }
        } catch (JSQLParserException e) {
            throw new IllegalStateException(e);
        }
        return sqls;
    }

    private List<ColumnNode> getKeyColumns(List<ColumnNode> columns) {
        List<ColumnNode> keyColumns = new ArrayList<>();
        for (ColumnNode column : columns) {
            if (column.getKeyIndex() != null) {
                keyColumns.add(column);
            }
        }
        keyColumns.sort(Comparator.comparing(ColumnNode::getKeyIndex));
        return keyColumns;
    }
}
