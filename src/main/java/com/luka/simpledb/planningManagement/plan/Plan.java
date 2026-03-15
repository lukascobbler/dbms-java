package com.luka.simpledb.planningManagement.plan;

// todo decide when to call folding for predicates
//  of SELECT, DELETE, UPDATE (AND CREATE VIEW since it uses SELECT)
//  (they aren't folded immediately in the
//  parser because they can contain non-constant values)
//  folding predicates is part of planning

// todo decide when to call folding for expressions of
//  UPDATE (they aren't folded immediately in the
//  parser because they can contain non-constant values)
//  folding expressions is part of planning

// todo decide when to call the running of expressions of
//  SELECT
//  running expressions is part of scanning

// todo when are scans closed - query scans are closed in the jdbc when the iteration is done,
//  update scans are closed after the plan finishes

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
