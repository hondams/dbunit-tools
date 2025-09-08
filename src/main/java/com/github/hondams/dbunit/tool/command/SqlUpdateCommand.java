package com.github.hondams.dbunit.tool.command;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "update", description = "Execute SQL update command")
@Component
public class SqlUpdateCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "sql", arity = "1")
    String sql;

    @Autowired
    DataSource dataSource;

    @Override
    public Integer call() throws Exception {
        try (Connection connection = this.dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                int result = statement.executeUpdate(this.sql);
                System.out.println("Result: " + result);
            }
        }
        return 0;
    }
}
