package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;

@Command(name = "",//
    description = "",//
    subcommands = {ConfigCommand.class, ConvertCommand.class, ExportCommand.class,
        ImportCommand.class, TableDefCommand.class, ExitCommand.class})
@Component
public class ConversationCommand implements Callable<Integer> {

    // picocliのCommandには、デフォルトコンストラクタが必要のため、@Autowiredを利用する
    @Autowired
    IFactory factory;

    @Override
    public Integer call() throws Exception {
        CommandLine commandLine = new CommandLine(this, this.factory);
        commandLine.usage(System.out);
        return 0;
    }
}
