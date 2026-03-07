package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

public abstract class UnaryScan extends Scan {
    protected final Scan childScan;

    public UnaryScan(Scan childScan) { this.childScan = childScan; }

    // default implementations for a typical scan
    @Override public void beforeFirst() { childScan.beforeFirst(); }
    @Override public void afterLast() { childScan.afterLast(); }
    @Override public boolean next() { return childScan.next(); }
    @Override public boolean previous() { return childScan.previous(); }
    @Override public boolean hasField(String fieldName) { return childScan.hasField(fieldName); }
    @Override public void close() { childScan.close(); }

    // default getter implementations
    @Override protected int internalGetInt(String fieldName) { return childScan.getInt(fieldName); }
    @Override protected String internalGetString(String fieldName) { return childScan.getString(fieldName); }
    @Override protected boolean internalGetBoolean(String fieldName) { return childScan.getBoolean(fieldName); }
    @Override protected Constant internalGetValue(String fieldName) { return childScan.getValue(fieldName); }
}
