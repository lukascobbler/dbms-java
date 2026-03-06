package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.BinaryScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;

public class ProductScan extends BinaryScan {
    public ProductScan(Scan scan1, Scan scan2) {
        super(scan1, scan2);
    }

    @Override
    public void beforeFirst() {
        childScan1.beforeFirst();
        childScan1.next();
        childScan2.beforeFirst();
    }

    @Override
    public void afterLast() {
        childScan2.afterLast();
        childScan2.previous();
        childScan1.afterLast();
    }

    @Override
    public boolean next() {
        if (childScan2.next()) {
            return true;
        } else {
            childScan2.beforeFirst();
            return childScan2.next() && childScan1.next();
        }
    }

    @Override
    public boolean previous() {
        if (childScan2.previous()) {
            return true;
        } else {
            childScan2.afterLast();
            return childScan2.previous() && childScan1.previous();
        }
    }

    @Override
    public int internalGetInt(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getInt(fieldName);
        }

        return childScan2.getInt(fieldName);
    }

    @Override
    public String internalGetString(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getString(fieldName);
        }

        return childScan2.getString(fieldName);
    }

    @Override
    public boolean internalGetBoolean(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getBoolean(fieldName);
        }

        return childScan2.getBoolean(fieldName);
    }

    @Override
    public Constant internalGetValue(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getValue(fieldName);
        }

        return childScan2.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return childScan1.hasField(fieldName) || childScan1.hasField(fieldName);
    }
}
