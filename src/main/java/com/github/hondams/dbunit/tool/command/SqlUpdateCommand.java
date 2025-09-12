package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "update", description = "Execute SQL update command")
@Component
@Slf4j
public class SqlUpdateCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1",//
        description = "SQL update to execute")
    String sql;

    @Option(names = {"-f", "--file"},//
        description = "File containing sql to execute")
    String file;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        String updateSql;
        if (StringUtils.isNotEmpty(this.sql) && StringUtils.isEmpty(this.file)) {
            updateSql = this.sql;
        } else if (StringUtils.isEmpty(this.sql) && StringUtils.isNotEmpty(this.file)) {
            File sqlFile = new File(this.file);
            if (!sqlFile.exists()) {
                ConsolePrinter.println(log, "File not found: " + this.file);
                return 1;
            }
            updateSql = Files.readString(sqlFile.toPath(), StandardCharsets.UTF_8);
        } else {
            CommandLine cmd = new CommandLine(this);
            cmd.usage(System.out);
            ConsolePrinter.println(log, "Either <sql> or -file option is required.");
            return 1;
        }

        try (Connection connection = this.dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                int result = statement.executeUpdate(updateSql);
                ConsolePrinter.println(log, "Result: " + result);
            }
            return 0;
        } catch (Exception e) {
            ConsolePrinter.printError(log, "Error: " + e.getMessage(), e);
            return 1;
        }
    }
}
