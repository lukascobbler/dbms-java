package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constants.Constant;

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
