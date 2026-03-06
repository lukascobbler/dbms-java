package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

public abstract class Scan implements AutoCloseable {
    // general scan features that must have redefined behavior
    public abstract void beforeFirst();
    public abstract void afterLast();
    public abstract boolean next();
    public abstract boolean previous();
    public abstract boolean hasField(String fieldName);
    @Override
    public abstract void close();

    // getters with field exist checks that mustn't have redefined behavior
    public final int getInt(String fieldName) { validate(fieldName); return internalGetInt(fieldName); }
    public final String getString(String fieldName) { validate(fieldName); return internalGetString(fieldName); }
    public final boolean getBoolean(String fieldName) { validate(fieldName); return internalGetBoolean(fieldName); }
    public final Constant getValue(String fieldName) { validate(fieldName); return internalGetValue(fieldName); }

    // getters with no field exist checks that must have redefined behavior
    protected abstract int internalGetInt(String fieldName);
    protected abstract String internalGetString(String fieldName);
    protected abstract boolean internalGetBoolean(String fieldName);
    protected abstract Constant internalGetValue(String fieldName);

    // helper method to disallow any field access without checking for its
    // existence first
    protected void validate(String fieldName) {
        if (!hasField(fieldName)) {
            throw new FieldNotFoundInScanException(fieldName);
        }
    }
}
