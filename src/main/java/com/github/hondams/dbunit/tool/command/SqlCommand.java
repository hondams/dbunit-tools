package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "sql", description = "Execute SQL command",//
    subcommands = {SqlQueryCommand.class, SqlUpdateCommand.class, SqlCountCommand.class})
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SqlCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(this);
        cmd.usage(System.out);
        log.info("Invalid command.");
        return 0;
    }
}
