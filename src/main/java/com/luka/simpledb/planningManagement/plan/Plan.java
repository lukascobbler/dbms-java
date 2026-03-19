package com.luka.simpledb.planningManagement.plan;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;

public abstract class Plan<T extends Scan> {
    public abstract T open();
    public abstract int blocksAccessed();
    public abstract int recordsOutput();
    public abstract int distinctValues(String fieldName);
    public abstract Schema schema();
//    @Override public abstract String toString(); todo add toString for nice printing of plans in the EXPLAIN command
}
