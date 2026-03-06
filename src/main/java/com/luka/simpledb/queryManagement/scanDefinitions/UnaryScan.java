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
    @Override public boolean hasField(String fld) { return childScan.hasField(fld); }
    @Override public void close() { childScan.close(); }

    // default getter implementations
    @Override protected int internalGetInt(String fld) { return childScan.getInt(fld); }
    @Override protected String internalGetString(String fld) { return childScan.getString(fld); }
    @Override protected boolean internalGetBoolean(String fld) { return childScan.getBoolean(fld); }
    @Override protected Constant internalGetValue(String fld) { return childScan.getValue(fld); }
}
