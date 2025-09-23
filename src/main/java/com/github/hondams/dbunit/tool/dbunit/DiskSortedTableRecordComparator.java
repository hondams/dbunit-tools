package com.github.hondams.dbunit.tool.dbunit;

import java.util.Comparator;
import java.util.List;

public class DiskSortedTableRecordComparator implements Comparator<List<Comparable<Object>>> {

    public static final Comparator<List<Comparable<Object>>> INSTANCE = new DiskSortedTableRecordComparator();

    // org.dbunit.dataset.SortedTable.AbstractRowComparator.compare(java.lang.Object, java.lang.Object)で、
    // nullを最小値としているので、それに合わせる。
    private static final Comparator<Comparable<Object>> VALUE_COMPARATOR = Comparator.nullsFirst(
        Comparator.naturalOrder());

    @Override
    public int compare(List<Comparable<Object>> o1, List<Comparable<Object>> o2) {
        if (o1 == null) {
            throw new IllegalArgumentException("o1 is null");
        }
        if (o2 == null) {
            throw new IllegalArgumentException("o2 is null");
        }
        if (o1.size() != o2.size()) {
            throw new IllegalArgumentException("o1.size() != o2.size()");
        }
        for (int i = 0; i < o1.size(); i++) {
            Comparable<Object> v1 = o1.get(i);
            Comparable<Object> v2 = o2.get(i);
            int c = VALUE_COMPARATOR.compare(v1, v2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }
}
