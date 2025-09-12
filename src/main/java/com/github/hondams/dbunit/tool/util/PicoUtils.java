package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PicoUtils {

    public String[] getArgs(String line) {
        String[] args = org.apache.commons.exec.CommandLine.parse(line).toStrings();
        for (int i = 0; i < args.length; i++) {
            args[i] = unescape(args[i]);
        }
        return args;
    }

    private String unescape(String str) {
        String temp = str;
        if (str.startsWith("\"") && str.endsWith("\"")) {
            temp = temp.substring(1, temp.length() - 1);
            temp = temp.replace("\\\"", "\"");
            temp = temp.replace("\"\"", "\"");
        }

        temp = temp.replace("\\", "\\\\");
        return temp;
    }
}
