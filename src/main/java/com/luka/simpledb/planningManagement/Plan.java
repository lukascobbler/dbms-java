package com.luka.simpledb.planningManagement;

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
//  UPDATE, SELECT
//  running expressions is part of scanning

// todo decide when to call the running of predicates of
//  UPDATE, SELECT, DELETE
//  running predicates is part of scanning and is in the select scan

public interface Plan {
    int distinctValues(String name);
}
