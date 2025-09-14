package com.github.hondams.dbunit.tool.dbunit;

import java.io.File;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.junit.jupiter.api.Test;

public class DbUnitToolsTest {

    @Test
    void test() throws Exception {
        IDataSet dataSet = DbUnitUtils.loadStreamingFlatXml(
            new File("C:\\projects\\github\\hondams\\dbunit-tools\\data\\data.xml"));
        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            ITable t = iterator.getTable();
            break;
        }
    }

    @Test
    void test2() throws Exception {
        IDataSet dataSet = DbUnitUtils.loadStreamingFlatXml(
            new File("C:\\projects\\github\\hondams\\dbunit-tools\\data\\data.xml"));
        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            ITable t = iterator.getTable();
        }
    }
}
