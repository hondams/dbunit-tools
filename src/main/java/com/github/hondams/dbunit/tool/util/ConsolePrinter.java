package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

@UtilityClass
public class ConsolePrinter {

    private static final LocationAwareLogger logger = (LocationAwareLogger) LoggerFactory.getLogger(
        ConsolePrinter.class);
    private static final String FQCN = ConsolePrinter.class.getName();

    public void println(String message) {
        System.out.println(message);
        logger.log(null, FQCN, LocationAwareLogger.INFO_INT, message, null, null);
    }

    public void printPrompt() {
        System.out.print("> ");
    }

    public void printInput(String input) {
        logger.log(null, FQCN, LocationAwareLogger.INFO_INT, "> " + input, null, null);
    }

    public void printError(String message, Throwable t) {
        t.printStackTrace(System.err);
        logger.log(null, FQCN, LocationAwareLogger.ERROR_INT, message, null, t);
    }
}
