package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CharUtils {

    public boolean isAscii(char ch) {
        return ch <= 0x7F;
    }

    public boolean isAsciiPrintable(char ch) {
        return ch >= 0x20 && ch <= 0x7E;
    }

    public boolean isAsciiControl(char ch) {
        return ch <= 0x1F || ch == 0x7F;
    }

    public boolean isHalfWidthKatakana(char ch) {
        return '\uFF61' <= ch && ch <= '\uFF9F'; // Half-width Katakana range
    }
}
