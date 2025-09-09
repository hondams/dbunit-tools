package com.github.hondams.dbunit.tool.util;

import java.io.File;
import lombok.experimental.UtilityClass;

@UtilityClass
public class FileUtils {

    public String getFileExtension(File file) {
        String fileName = file.getName();
        return fileName.contains(".")//
            ? fileName.substring(fileName.lastIndexOf(".") + 1)//
            : "";
    }
}
