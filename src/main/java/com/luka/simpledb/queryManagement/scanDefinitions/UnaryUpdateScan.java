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
    @Override public boolean hasField(String fieldName) { return childScan.hasField(fieldName); }
    @Override public void close() { childScan.close(); }
    // default implementations for a typical update scan
    @Override public void insert() { childScan.insert(); }
    @Override public void delete() { childScan.delete(); }
    @Override public RecordId getRecordId() { return childScan.getRecordId(); }
    @Override public void moveToRecordId(RecordId rid) { childScan.moveToRecordId(rid); }

    // default getter implementations
    @Override protected int internalGetInt(String fieldName) { return childScan.getInt(fieldName); }
    @Override protected String internalGetString(String fieldName) { return childScan.getString(fieldName); }
    @Override protected boolean internalGetBoolean(String fieldName) { return childScan.getBoolean(fieldName); }
    @Override protected Constant internalGetValue(String fieldName) { return childScan.getValue(fieldName); }

    // default setter implementations
    @Override protected void internalSetInt(String fieldName, int v) { childScan.setInt(fieldName, v); }
    @Override protected void internalSetString(String fieldName, String v) { childScan.setString(fieldName, v); }
    @Override protected void internalSetBoolean(String fieldName, boolean v) { childScan.setBoolean(fieldName, v); }
    @Override protected void internalSetValue(String fieldName, Constant v) { childScan.setValue(fieldName, v); }
}