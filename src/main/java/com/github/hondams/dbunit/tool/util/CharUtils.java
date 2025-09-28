package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CharUtils {

    public boolean isAscii(int codePoint) {
        return codePoint <= 0x7F;
    }

    public boolean isAsciiPrintable(int codePoint) {
        return 0x20 <= codePoint && codePoint <= 0x7E;
    }

    public boolean isAsciiControl(int codePoint) {
        return codePoint <= 0x1F || codePoint == 0x7F;
    }

    public boolean isHalfWidthKatakana(int codePoint) {
        return '\uFF61' <= codePoint && codePoint <= '\uFF9F'; // Half-width Katakana range
    }
}
