package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;

@Command(name = "",//
    description = "",//
    subcommands = {BatchCommand.class, ConfigCommand.class, ConvertCommand.class,
        ExportCommand.class, ImportCommand.class, DbDefCommand.class, SqlCommand.class,
        ExitCommand.class})
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ConversationCommand implements Callable<Integer> {

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
