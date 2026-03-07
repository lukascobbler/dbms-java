package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

/// Binary scans do not have default implementations for navigation methods because
/// there are no sensible defaults for scans that operate on two child scans.
/// The rest of the methods that do have default implementations assume that
/// the scans operate on different schemas with all different fields.
public abstract class BinaryScan extends Scan {
    protected final Scan childScan1;
    protected final Scan childScan2;

    public BinaryScan(Scan s1, Scan s2) {
        this.childScan1 = s1;
        this.childScan2 = s2;
    }

    @Override
    public void close() {
        childScan1.close();
        childScan2.close();
    }

    @Override
    public boolean hasField(String fieldName) {
        return childScan1.hasField(fieldName) || childScan1.hasField(fieldName);
    }

    // todo check that the field only exists in one of the scans
    @Override
    protected int internalGetInt(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getInt(fieldName);
        }

        return childScan2.getInt(fieldName);
    }

    @Override
    protected String internalGetString(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getString(fieldName);
        }

        return childScan2.getString(fieldName);
    }

    @Override
    protected boolean internalGetBoolean(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getBoolean(fieldName);
        }

        return childScan2.getBoolean(fieldName);
    }

    @Override
    protected Constant internalGetValue(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getValue(fieldName);
        }

        return childScan2.getValue(fieldName);
    }
}
