package com.github.hondams.dbunit.tool.dbunit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DbUnitDataTypeFactoryProvider {

    private final Map<String, DbUnitDataTypeFactory> factoryMap = new ConcurrentHashMap<>();

    public DbUnitDataTypeFactory getDbUnitDataTypeFactory(String productName) {
        return factoryMap.computeIfAbsent(productName, k -> new DbUnitDataTypeFactory(productName));
    }
}
