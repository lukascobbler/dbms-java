package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.expressions.Constant;

public interface Scan {
    void beforeFirst();
    boolean next();
    int getInt(String fieldName);
    String getString(String fieldName);
    boolean getBoolean(String fieldName);
    Constant getValue(String fieldName);
    boolean hasField(String fieldName);
    // void close todo
}
