package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;
import org.slf4j.Logger;

@UtilityClass
public class ConsolePrinter {

    public void println(Logger log, String message) {
        System.out.println(message);
        log.info(message);
    }

    public void printPrompt() {
        System.out.print("> ");
    }

    public void printInput(Logger log, String input) {
        log.info("> {}", input);
    }

    public void printError(Logger log, String message, Throwable t) {
        t.printStackTrace(System.err);
        log.error(message, t);
    }
}
