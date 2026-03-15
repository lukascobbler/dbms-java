package com.luka.simpledb.planningManagement.planner.plannerDefinitions;

import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.transactionManagement.Transaction;

public interface QueryPlanner {
    Plan<Scan> createPlan(SelectStatement selectStatement, Transaction transaction);
}
