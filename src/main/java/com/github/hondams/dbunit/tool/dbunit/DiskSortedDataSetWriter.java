package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.model.DatabaseNode;
import java.io.File;

public class DiskSortedDataSetWriter {

    private final File dir;

    private final DatabaseNode databaseNode;

    private final int limit;

    public DiskSortedDataSetWriter(File dir, DatabaseNode databaseNode, int limit) {
        this.dir = dir;
        this.databaseNode = databaseNode;
        this.limit = limit;
    }
}
