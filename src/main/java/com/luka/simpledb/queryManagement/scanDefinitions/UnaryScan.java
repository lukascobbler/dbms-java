package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

/// A scan class that defines default functionality for all scans
/// that build on top of **one** child scan, which are the scans
/// that operate on one table. All scans that operate on one table
/// should extend this class and redefine only needed behaviors.
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
    @Override protected Constant internalGetValue(String fieldName) { return childScan.getValue(fieldName); }
}
