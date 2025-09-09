package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.model.CatalogDefinition;
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

@Command(name = "catalog",//
    description = "Print database catalog information")
@Component
public class DbDefCatalogCommand implements Callable<Integer> {

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {

        List<String> header = List.of(//
            "Catalog");
        List<PrintLineAlignment> alignments = List.of(//
            PrintLineAlignment.LEFT);
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = this.dataSource.getConnection()) {
            List<CatalogDefinition> catalogs = DatabaseUtils.getAllCatalogs(connection);
            for (CatalogDefinition catalog : catalogs) {
                String catalogName = catalog.getCatalogName();
                if (catalogName == null) {
                    catalogName = "<null>";
                }
                rows.add(List.of(catalogName));
            }
        }
        List<String> lines = PrintLineUtils.getTableLines("", header, alignments, rows);
        for (String line : lines) {
            ConsolePrinter.println(line);
        }
        return 0;
    }
}
