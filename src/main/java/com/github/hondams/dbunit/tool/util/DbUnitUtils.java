package com.github.hondams.dbunit.tool.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class DbUnitUtils {

    public static String getSamplePackage() {
        testAbc();
        return DbUnitUtils.class.getPackageName().replace(".util", "");
    }

    private static void testAbc() {
        System.out.println("testAbc");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
