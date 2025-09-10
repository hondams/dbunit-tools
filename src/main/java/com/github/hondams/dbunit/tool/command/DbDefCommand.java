package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;

@Command(name = "dbdef",//
    description = "Print database definition information",//
    subcommands = {DbDefCatalogCommand.class, DbDefSchemaCommand.class, DbDefTableCommand.class,
        DbDefColumnCommand.class, DbDefExportCommand.class, DbDefDiffCommand.class})
@Component
@Slf4j
public class DbDefCommand implements Callable<Integer> {

    // picocliのCommandには、デフォルトコンストラクタが必要のため、@Autowiredを利用する
    @Autowired
    IFactory factory;

    @Override
    public Integer call() throws Exception {
        CommandLine commandLine = new CommandLine(this, this.factory);
        commandLine.usage(System.out);
        log.info("Invalid command.");
        return 0;
    }
}
