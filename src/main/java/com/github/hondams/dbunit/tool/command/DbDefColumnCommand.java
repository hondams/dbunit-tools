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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "column",//
    description = "Print database column information")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DbDefColumnCommand implements Callable<Integer> {

    private static final List<String> HEADER = List.of(//
        "ColumnName", "SqlTypeName", "TypeName",//
        "ColumnSize", "DecimalDigits", "Nullable",//
        "KeyIndex");
    private static final List<PrintLineAlignment> ALIGNMENTS = List.of(//
        PrintLineAlignment.LEFT, PrintLineAlignment.LEFT, PrintLineAlignment.LEFT,//
        PrintLineAlignment.RIGHT, PrintLineAlignment.RIGHT, PrintLineAlignment.LEFT,//
        PrintLineAlignment.RIGHT);

    @Option(names = {"-t", "--table"}, split = ",", required = true,//
        description = "Table name. Specify as [catalog.]schema.table. Pattern match using %% is available.")
    String[] table;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {

            List<TableKey> tableKeys = TableKey.fromQualifiedTableNames(List.of(this.table));
            DatabaseNode databaseNode = DatabaseUtils.getDatabaseNode(connection, tableKeys);

            if (databaseNode.getCatalogs().isEmpty()) {
                ConsolePrinter.println(log, "Table not found: " + List.of(this.table));
                return 1;
            }

            for (CatalogNode catalogNode : databaseNode.getCatalogs()) {
                for (SchemaNode schemaNode : catalogNode.getSchemas()) {
                    for (TableNode tableNode : schemaNode.getTables()) {
                        List<List<String>> rows = new ArrayList<>();

                        ConsolePrinter.println(log, "Table: " + TableKey.toQualifiedTableName(//
                            new TableKey(catalogNode.getCatalogName(),//
                                schemaNode.getSchemaName(),//
                                tableNode.getTableName())));
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

                            rows.add(List.of(columnName, sqlTypeName, typeName, columnSize,
                                decimalDigits, nullable, keyIndex));
                        }
                        List<String> lines = PrintLineUtils.getTableLines("", HEADER, ALIGNMENTS,
                            rows);
                        for (String line : lines) {
                            ConsolePrinter.println(log, line);
                        }
                        ConsolePrinter.println(log, "");
                    }
                }
            }
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}