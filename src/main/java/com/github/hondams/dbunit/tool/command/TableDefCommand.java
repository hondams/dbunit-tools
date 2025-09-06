package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "tabledef", description = "")
@Component
public class TableDefCommand implements Callable<Integer> {

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Override
    public Integer call() throws Exception {
        System.out.println("TableDefCommand: output=" + this.output);
        return 0;
    }
}
