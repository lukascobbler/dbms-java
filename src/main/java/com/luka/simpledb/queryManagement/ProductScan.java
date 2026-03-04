package com.luka.simpledb.queryManagement;

import com.luka.simpledb.queryManagement.expressions.Constant;
import com.luka.simpledb.queryManagement.scanTypes.Scan;

public class ProductScan implements Scan {
    private final Scan scan1, scan2;

    public ProductScan(Scan scan1, Scan scan2) {
        this.scan1 = scan1;
        this.scan2 = scan2;
    }

    @Override
    public void beforeFirst() {
        scan1.beforeFirst();
        scan1.next();
        scan2.beforeFirst();
    }

    @Override
    public boolean next() {
        if (scan2.next()) {
            return true;
        } else {
            scan2.beforeFirst();
            return scan2.next() && scan1.next();
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getInt(fieldName);
        }

        return scan2.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getString(fieldName);
        }

        return scan2.getString(fieldName);
    }

    @Override
    public boolean getBoolean(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getBoolean(fieldName);
        }

        return scan2.getBoolean(fieldName);
    }

    @Override
    public Constant getValue(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getValue(fieldName);
        }

        return scan2.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan1.hasField(fieldName) || scan1.hasField(fieldName);
    }
}
