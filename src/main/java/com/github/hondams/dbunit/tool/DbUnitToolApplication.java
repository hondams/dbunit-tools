package com.github.hondams.dbunit.tool;

import com.github.hondams.dbunit.tool.command.DbUnitCommand;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import picocli.CommandLine;
import picocli.CommandLine.IFactory;

@SpringBootApplication
@RequiredArgsConstructor
@Profile("!test")
public class DbUnitToolApplication implements CommandLineRunner, ExitCodeGenerator {

    private final IFactory factory;
    private final DbUnitCommand command;
    private int exitCode;

    @Override
    public void run(String... args) {
        CommandLine commandLine = new CommandLine(this.command, this.factory);
        this.exitCode = commandLine.execute(args);
    }

    @Override
    public int getExitCode() {
        return this.exitCode;
    }

    public static void main(String[] args) {

        ApplicationContext applicationContext =//
            SpringApplication.run(DbUnitToolApplication.class, args);
        int exitCode = SpringApplication.exit(applicationContext);
        System.exit(exitCode);
    }

}
