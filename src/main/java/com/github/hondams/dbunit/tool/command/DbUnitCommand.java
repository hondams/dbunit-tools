package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.util.Scanner;
import java.util.concurrent.Callable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;

@Component
@Command(name = "",//
    mixinStandardHelpOptions = true,//
    version = "dbunit-tools 1.0.0",//
    description = "A set of tools for DbUnit",//
    subcommands = {BatchCommand.class, ConfigCommand.class, ConvertCommand.class,
        ExportCommand.class, ImportCommand.class, DbDefCommand.class, SqlCommand.class})
public class DbUnitCommand implements Callable<Integer> {

    // picocliのCommandには、デフォルトコンストラクタが必要のため、@Autowiredを利用する
    @Autowired
    IFactory factory;
    @Autowired
    ApplicationContext applicationContext;

    @Override
    public Integer call() throws Exception {

        try {
            ConsolePrinter.println("Start conversation mode.");
            ConsolePrinter.println(" Use '-h' option to see usage.");
            ConsolePrinter.println(" Input 'exit' to exit.");
            ConversationCommand conversationCommand = this.applicationContext.getBean(
                ConversationCommand.class);
            CommandLine commandLine = new CommandLine(conversationCommand, this.factory);
            boolean exit = false;
            try (Scanner scanner = new Scanner(System.in, "Windows-31J")) {
                while (!exit) {
                    ConsolePrinter.printPrompt();
                    String line = scanner.nextLine();
                    ConsolePrinter.printInput(line);
                    if (line.trim().isEmpty()) {
                        commandLine.usage(System.out);
                    } else {
                        String[] args = getArgs(line);
                        commandLine.execute(args);
                        if ("exit".equals(line)) {
                            exit = true;
                        }
                    }
                }
            }
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError("Error: " + e.getMessage(), e);
            return 1;
        }
    }

    private String[] getArgs(String line) {
        String[] args = org.apache.commons.exec.CommandLine.parse(line).toStrings();
        for (int i = 0; i < args.length; i++) {
            args[i] = unescapeArgument(args[i]);
        }
        return args;
    }

    private String unescapeArgument(String arg) {
        // 先頭と末尾のクォートを除去
        if ((arg.startsWith("\"") && arg.endsWith("\""))//
            || (arg.startsWith("'") && arg.endsWith("'"))) {
            arg = arg.substring(1, arg.length() - 1);
        }
        // バックスラッシュでエスケープされたクォートやバックスラッシュを元に戻す
        arg = arg.replace("\\\"", "\"")//
            .replace("\\'", "'")//
            .replace("\\\\", "\\");
        return arg;
    }
}
