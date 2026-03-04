package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.expressions.Constant;
import com.luka.simpledb.recordManagement.RecordId;

public interface UpdateScan extends Scan {
    void setInt(String fieldName, int value);
    void setString(String fieldName, String value);
    void setBoolean(String fieldName, boolean value);
    void setValue(String fieldName, Constant value);
    void insert();
    void delete();

    RecordId getRecordId();
    void moveToRecordId(RecordId recordId);
}
