package com.github.hondams.dbunit.tool.command;

import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export data from database to file")
@Component
public class ExportCommand implements Callable<Integer> {

    @Option(names = {"-s", "--scheme"})
    String scheme;

    @Option(names = {"-t", "--table"}, split = ",")
    String[] table;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Override
    public Integer call() throws Exception {
        System.out.println("ExportCommand: output=" + this.output);
        return 0;
    }
}
