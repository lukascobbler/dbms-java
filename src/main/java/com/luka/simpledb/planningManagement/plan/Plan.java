package com.luka.simpledb.planningManagement.plan;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;

public interface Plan<T extends Scan> {
    T open();
    int blocksAccessed();
    int recordsOutput();
    int distinctValues(String fieldName);
    Schema outputSchema();
//    @Override public abstract String toString(); todo add toString for nice printing of plans in the EXPLAIN command
}
