package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "set",//
    description = "Set configuration")
@Component
public class ConfigSetCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "key", arity = "1")
    String key;
    @Parameters(index = "1", description = "value", arity = "1")
    String value;

    @Override
    public Integer call() throws Exception {
        System.out.println("ConfigSetCommand: key=" + this.key + ", value=" + this.value);
        return 0;
    }
}
