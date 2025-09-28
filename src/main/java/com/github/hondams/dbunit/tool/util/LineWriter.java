package com.github.hondams.dbunit.tool.util;

import java.io.Closeable;
import java.io.IOException;

public interface LineWriter extends Closeable {

    void println(String line) throws IOException;
}
