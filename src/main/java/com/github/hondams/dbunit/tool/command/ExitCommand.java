package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "exit",//
    description = "Exit dbunit-tools")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ExitCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        ConsolePrinter.println(log,"exited.");
        return 0;
    }
}
