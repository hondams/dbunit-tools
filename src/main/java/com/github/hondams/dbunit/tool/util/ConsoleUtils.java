package com.github.hondams.dbunit.tool.util;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class ConsoleUtils {

    @Getter
    private final Charset charset;
    private final CharsetEncoder charsetEncoder;

    static {
        charset = Charset.forName(System.getProperty("sun.stdout.encoding"));
        charsetEncoder = charset.newEncoder();
        log.info("System charset: {}", charset);
    }

    public int getDisplaySize(String text) {
        int displaySize = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (CharUtils.isAscii(c) || CharUtils.isHalfWidthKatakana(c)) {
                // cはASCII文字です
                displaySize++;
            } else {
                displaySize += 2;
            }
        }
        return displaySize;
    }

    public String fillLeft(String text, int displaySize) {
        int textDisplaySize = getDisplaySize(text);
        if (textDisplaySize >= displaySize) {
            return text;
        } else {
            return " ".repeat(displaySize - textDisplaySize) + text;
        }
    }

    public String fillRight(String text, int displaySize) {
        int textDisplaySize = getDisplaySize(text);
        if (textDisplaySize >= displaySize) {
            return text;
        } else {
            return text + " ".repeat(displaySize - textDisplaySize);
        }
    }

}
