package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "list",//
    description = "List configurations")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConfigListCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        System.out.println("ConfigListCommand:");
        return 0;
    }
}
