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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "catalog",//
    description = "Print database catalog information")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DbDefCatalogCommand implements Callable<Integer> {

    private static final List<String> HEADER = List.of(//
        "Catalog");
    private static final List<PrintLineAlignment> ALIGNMENTS = List.of(//
        PrintLineAlignment.LEFT);

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {

        try (Connection connection = this.dataSource.getConnection()) {
            List<List<String>> rows = new ArrayList<>();
            List<CatalogDefinition> catalogs = DatabaseUtils.getAllCatalogs(connection);
            for (CatalogDefinition catalog : catalogs) {
                String catalogName = catalog.getCatalogName();
                if (catalogName == null) {
                    catalogName = "<null>";
                }
                rows.add(List.of(catalogName));
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
