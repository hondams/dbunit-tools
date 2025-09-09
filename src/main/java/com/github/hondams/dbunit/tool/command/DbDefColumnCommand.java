package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.CatalogNode;
import com.github.hondams.dbunit.tool.model.ColumnNode;
import com.github.hondams.dbunit.tool.model.DatabaseNode;
import com.github.hondams.dbunit.tool.model.SchemaNode;
import com.github.hondams.dbunit.tool.model.TableKey;
import com.github.hondams.dbunit.tool.model.TableNode;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
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
import picocli.CommandLine.Parameters;

@Command(name = "column",//
    description = "Print database column information")
@Component
public class DbDefColumnCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "table", arity = "1")
    String table;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        List<String> header = List.of(//
            "ColumnName", "SqlTypeName", "TypeName",//
            "ColumnSize", "DecimalDigits", "Nullable",//
            "KeyIndex");
        List<PrintLineAlignment> alignments = List.of(//
            PrintLineAlignment.LEFT, PrintLineAlignment.LEFT, PrintLineAlignment.LEFT,//
            PrintLineAlignment.RIGHT, PrintLineAlignment.RIGHT, PrintLineAlignment.LEFT,//
            PrintLineAlignment.RIGHT);

        TableKey tableKey = TableKey.fromQualifiedTableName(this.table);
        if (tableKey == null) {
            throw new IllegalStateException("Invalid table name: " + this.table);
        }

        DatabaseNode databaseNode;
        try (Connection connection = this.dataSource.getConnection()) {
            databaseNode = DatabaseUtils.getDatabaseNode(connection, tableKey.getCatalogName(),
                tableKey.getSchemaName(), tableKey.getTableName());
        }

        if (databaseNode.getCatalogs().isEmpty()) {
            throw new IllegalStateException("Table not found: " + this.table);
        }

        for (CatalogNode catalogNode : databaseNode.getCatalogs()) {
            for (SchemaNode schemaNode : catalogNode.getSchemas()) {
                for (TableNode tableNode : schemaNode.getTables()) {
                    List<List<String>> rows = new ArrayList<>();
                    ConsolePrinter.println("Table: " + catalogNode.getCatalogName()//
                        + "." + schemaNode.getSchemaName()//
                        + "." + tableNode.getTableName());
                    for (ColumnNode columnNode : tableNode.getColumns()) {
                        String columnName = columnNode.getColumnName();
                        String sqlTypeName = columnNode.getSqlTypeName();
                        String typeName = columnNode.getTypeName();
                        String columnSize = String.valueOf(columnNode.getColumnSize());
                        String decimalDigits = columnNode.getDecimalDigits() == null ? ""
                            : String.valueOf(columnNode.getDecimalDigits());
                        String nullable = columnNode.getNullable();
                        String keyIndex = columnNode.getKeyIndex() == null ? ""
                            : String.valueOf(columnNode.getKeyIndex());

                        rows.add(
                            List.of(columnName, sqlTypeName, typeName, columnSize, decimalDigits,
                                nullable, keyIndex));
                    }
                    List<String> lines = PrintLineUtils.getTableLines("", header, alignments, rows);
                    for (String line : lines) {
                        ConsolePrinter.println(line);
                    }
                    ConsolePrinter.println("");
                }
            }
        }
        return 0;
    }
}