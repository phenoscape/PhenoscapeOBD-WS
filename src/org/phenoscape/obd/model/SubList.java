package org.phenoscape.obd.model;

import java.util.List;

public class SubList<T> {

    private final List<T> list;
    private final long total;

    public SubList(List<T> list, long total) {
        this.list = list;
        this.total = total;
    }

    public List<T> getList() {
        return this.list;
    }

    public long getTotal() {
        return this.total;
    }

}
