package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "clear",//
    description = "Clear console")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ClearCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        System.out.print("\033[H\033[2J");
        System.out.flush();
        return 0;
    }
}
