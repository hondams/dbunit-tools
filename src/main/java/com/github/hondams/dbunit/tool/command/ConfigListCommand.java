package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "list",//
    description = "")
@Component
public class ConfigListCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        System.out.println("ConfigListCommand:");
        return 0;
    }
}
