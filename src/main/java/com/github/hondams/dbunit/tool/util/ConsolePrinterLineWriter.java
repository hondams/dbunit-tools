package com.github.hondams.dbunit.tool.util;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;

@RequiredArgsConstructor
public class ConsolePrinterLineWriter implements LineWriter {

    private final Logger log;

    @Override
    public void println(String line) throws IOException {
        ConsolePrinter.println(this.log, line);
    }

    @Override
    public void close() throws IOException {
    }
}
