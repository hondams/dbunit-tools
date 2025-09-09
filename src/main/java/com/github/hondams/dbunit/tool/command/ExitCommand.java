package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

@Command(name = "exit",//
    description = "Exit dbunit-tools")
@Component
public class ExitCommand implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        ConsolePrinter.println("exited.");
        return 0;
    }
}
