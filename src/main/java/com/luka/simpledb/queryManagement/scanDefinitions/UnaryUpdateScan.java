package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.RecordId;

public abstract class UnaryUpdateScan extends UpdateScan {
    protected final UpdateScan childScan;

    public UnaryUpdateScan(UpdateScan childScan) { this.childScan = childScan; }

    // default implementations for a typical scan
    @Override public void beforeFirst() { childScan.beforeFirst(); }
    @Override public void afterLast() { childScan.afterLast(); }
    @Override public boolean next() { return childScan.next(); }
    @Override public boolean previous() { return childScan.previous(); }
    @Override public boolean hasField(String fld) { return childScan.hasField(fld); }
    @Override public void close() { childScan.close(); }
    // default implementations for a typical update scan
    @Override public void insert() { childScan.insert(); }
    @Override public void delete() { childScan.delete(); }
    @Override public RecordId getRecordId() { return childScan.getRecordId(); }
    @Override public void moveToRecordId(RecordId rid) { childScan.moveToRecordId(rid); }

    // default getter implementations
    @Override protected int internalGetInt(String fld) { return childScan.getInt(fld); }
    @Override protected String internalGetString(String fld) { return childScan.getString(fld); }
    @Override protected boolean internalGetBoolean(String fld) { return childScan.getBoolean(fld); }
    @Override protected Constant internalGetValue(String fld) { return childScan.getValue(fld); }

    // default setter implementations
    @Override protected void internalSetInt(String fld, int v) { childScan.setInt(fld, v); }
    @Override protected void internalSetString(String fld, String v) { childScan.setString(fld, v); }
    @Override protected void internalSetBoolean(String fld, boolean v) { childScan.setBoolean(fld, v); }
    @Override protected void internalSetValue(String fld, Constant v) { childScan.setValue(fld, v); }
}