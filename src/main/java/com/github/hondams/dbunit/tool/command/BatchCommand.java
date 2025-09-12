package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Option;

@Command(name = "batch", description = "Execute commands from a file")
@Component
@Slf4j
public class BatchCommand implements Callable<Integer> {

    // picocliのCommandには、デフォルトコンストラクタが必要のため、@Autowiredを利用する
    @Autowired
    IFactory factory;
    @Autowired
    ApplicationContext applicationContext;

    @Option(names = {"-f", "--file"}, required = true,//
        description = "File containing commands to execute")
    String file;

    @Override
    public Integer call() throws Exception {

        File batchFile = new File(this.file);
        if (!batchFile.exists()) {
            ConsolePrinter.println(log, "File not found: " + batchFile.getAbsolutePath());
            return 1;
        }

        try {
            ConsolePrinter.println(log, "Start batch mode. file=" + batchFile.getAbsolutePath());

            ConversationCommand conversationCommand = this.applicationContext.getBean(
                ConversationCommand.class);
            CommandLine commandLine = new CommandLine(conversationCommand, this.factory);

            List<String> lines = Files.readAllLines(batchFile.toPath());
            for (String line : lines) {
                ConsolePrinter.println(log, "> " + line);
                if (!line.trim().isEmpty() && !line.startsWith("#")) {
                    String[] args = org.apache.commons.exec.CommandLine.parse(line).toStrings();
                    commandLine.execute(args);
                    if ("exit".equals(line)) {
                        break;
                    }
                }
            }
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
