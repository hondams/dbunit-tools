package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("rawtypes")
public class RecordKey implements Comparable<RecordKey> {

    private final List<Comparable> values;

    public RecordKey(List<Comparable> values) {
        List<Comparable> copy = new ArrayList<>(values);
        this.values = Collections.unmodifiableList(copy);
    }

    @Override
    public int compareTo(RecordKey o) {
        int size = Math.min(this.values.size(), o.values.size());
        for (int i = 0; i < size; i++) {
            Comparable thisValue = this.values.get(i);
            Comparable otherValue = o.values.get(i);
            if (thisValue == null && otherValue == null) {
                // skip
            } else if (thisValue == null) {
                return -1;
            } else if (otherValue == null) {
                return 1;
            } else {
                int cmp = thisValue.compareTo(otherValue);
                if (cmp != 0) {
                    return cmp;
                }
            }
        }
        return Integer.compare(this.values.size(), o.values.size());
    }
}
