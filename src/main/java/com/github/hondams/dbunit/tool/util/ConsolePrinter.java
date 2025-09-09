package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class ConsolePrinter {

    public void println(String message) {
        System.out.println(message);
        log.info(message);
    }

    public void printPrompt() {
        System.out.print("> ");
    }

    public void printInput(String input) {
        log.info("> " + input);
    }
}
