package com.github.hondams.dbunit.tool.command;

import java.util.Arrays;
import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "convert", description = "Convert or merge data file format")
@Component
public class ConvertCommand implements Callable<Integer> {

    @Option(names = {"-i", "--input"}, split = ",", required = true)
    String[] input;
    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Override
    public Integer call() throws Exception {

        System.out.println(
            "ConvertCommand: input=" + Arrays.asList(this.input) + ", output=" + this.output);
        return 0;
    }
}
