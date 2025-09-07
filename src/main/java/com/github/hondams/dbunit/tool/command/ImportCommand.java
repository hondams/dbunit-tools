package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "import", description = "Import data from file to database")
@Component
public class ImportCommand implements Callable<Integer> {

    @Option(names = {"-i", "--input"}, required = true)
    String input;

    @Override
    public Integer call() throws Exception {
        System.out.println("ImportCommand: input=" + this.input);
        return 0;
    }
}
