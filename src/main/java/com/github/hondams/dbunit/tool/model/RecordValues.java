package com.github.hondams.dbunit.tool.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@SuppressWarnings("rawtypes")
@Getter
public class RecordValues implements Comparable<RecordValues> {

    private final List<Comparable> values = new ArrayList<>();

    @Override
    public int compareTo(RecordValues o) {
        int size = Math.min(this.values.size(), o.values.size());
        for (int i = 0; i < size; i++) {
            Comparable thisValue = this.values.get(i);
            Comparable otherValue = o.values.get(i);
            if (thisValue == null && otherValue == null) {
                continue;
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
