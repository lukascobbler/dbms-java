package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.BinaryScan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

/// Assumes the scans operate on identical schemas.
public class UnionScan extends BinaryScan {
    private boolean isFirstScanSelected = true;

    public UnionScan(Scan childScan1, Scan childScan2) {
        super(childScan1, childScan2);
    }

    @Override
    public void beforeFirst() {
        childScan1.beforeFirst();
        childScan2.beforeFirst();
        isFirstScanSelected = true;
    }

    @Override
    public void afterLast() {
        childScan1.afterLast();
        childScan2.afterLast();
        isFirstScanSelected = false;
    }

    @Override
    public boolean next() {
        if (isFirstScanSelected) {
            if (childScan1.next()) {
                return true;
            }
            isFirstScanSelected = false;
        }
        return childScan2.next();
    }

    @Override
    public boolean previous() {
        if (!isFirstScanSelected) {
            if (childScan2.previous()) {
                return true;
            }
            isFirstScanSelected = true;
        }
        return childScan1.previous();
    }

    @Override
    protected int internalGetInt(String fieldName) {
        if (isFirstScanSelected) {
            return childScan1.getInt(fieldName);
        }

        return childScan2.getInt(fieldName);
    }

    @Override
    protected String internalGetString(String fieldName) {
        if (isFirstScanSelected) {
            return childScan1.getString(fieldName);
        }

        return childScan2.getString(fieldName);
    }

    @Override
    protected boolean internalGetBoolean(String fieldName) {
        if (isFirstScanSelected) {
            return childScan1.getBoolean(fieldName);
        }

        return childScan2.getBoolean(fieldName);
    }

    @Override
    protected Constant internalGetValue(String fieldName) {
        if (isFirstScanSelected) {
            return childScan1.getValue(fieldName);
        }

        return childScan2.getValue(fieldName);
    }
}
