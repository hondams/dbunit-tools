package com.github.hondams.dbunit.tool.command;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Option;

@Command(name = "batch", description = "")
@Component
public class BatchCommand implements Callable<Integer> {

    // picocliのCommandには、デフォルトコンストラクタが必要のため、@Autowiredを利用する
    @Autowired
    IFactory factory;
    @Autowired
    ApplicationContext applicationContext;

    @Option(names = {"-f", "--file"}, required = true)
    String file;

    @Override
    public Integer call() throws Exception {
        Path path = Path.of(this.file);
        System.out.println("Start batch mode. file=" + path.toAbsolutePath());

        ConversationCommand conversationCommand = this.applicationContext.getBean(
            ConversationCommand.class);
        CommandLine commandLine = new CommandLine(conversationCommand, this.factory);

        List<String> lines = Files.readAllLines(path);
        for (String line : lines) {
            System.out.println("> " + line);
            if (!line.trim().isEmpty() && !line.startsWith("#")) {
                String[] args = org.apache.commons.exec.CommandLine.parse(line).toStrings();
                commandLine.execute(args);
                if ("exit".equals(line)) {
                    break;
                }
            }
        }
        return 0;
    }
}
