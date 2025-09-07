package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.SchemaDefinition;
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

@Command(name = "schema",//
    description = "Print database schema information")
@Component
public class DbDefSchemaCommand implements Callable<Integer> {

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        List<String> header = List.of(//
            "Catalog", "Schema");
        List<PrintLineAlignment> alignments = List.of(//
            PrintLineAlignment.LEFT, PrintLineAlignment.LEFT);
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = this.dataSource.getConnection()) {
            List<SchemaDefinition> schemas = DatabaseUtils.getAllSchemas(connection);
            for (SchemaDefinition schema : schemas) {
                String catalogName = schema.getCatalogName();
                String schemaName = schema.getSchemaName();
                if (catalogName == null) {
                    catalogName = "<null>";
                }
                if (schemaName == null) {
                    schemaName = "<null>";
                }
                rows.add(List.of(catalogName, schemaName));
            }
        }
        List<String> lines = PrintLineUtils.getTableLines("", header, alignments, rows);
        for (String line : lines) {
            System.out.println(line);
        }
        return 0;
    }
}
