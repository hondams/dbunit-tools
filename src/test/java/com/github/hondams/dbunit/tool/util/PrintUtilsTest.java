package com.github.hondams.dbunit.tool.util;

import org.junit.jupiter.api.Test;

public class PrintUtilsTest {

    @Test
    void test() {
        System.out.println(
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
        System.out.println("\tabc");
        System.out.println("\u0000");
    }
}
