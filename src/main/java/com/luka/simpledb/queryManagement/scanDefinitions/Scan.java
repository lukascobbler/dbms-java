package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

public interface Scan extends AutoCloseable {
    void beforeFirst();
    void afterLast();
    boolean next();
    boolean previous();
    int getInt(String fieldName);
    String getString(String fieldName);
    boolean getBoolean(String fieldName);
    Constant getValue(String fieldName);
    boolean hasField(String fieldName);
}
