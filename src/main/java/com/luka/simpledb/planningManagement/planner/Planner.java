package com.luka.simpledb.planningManagement.planner;

import com.luka.simpledb.parsingManagement.parser.Parser;
import com.luka.simpledb.parsingManagement.statement.*;
import com.luka.simpledb.planningManagement.exceptions.PlanValidationException;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.QueryPlanner;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.UpdatePlanner;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.transactionManagement.Transaction;

public class Planner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan<Scan> createQueryPlan(String query, Transaction transaction) throws PlanValidationException {
        Parser parser = new Parser(query);

        Statement statement = parser.parse();

        switch (statement) {
            case SelectStatement selectStatement -> {
                return queryPlanner.createValidatedPlan(selectStatement, transaction);
            }
            case ExplainStatement explainStatement -> { // todo statement explaining (plan printing)
                throw new UnsupportedOperationException();
            }
            default -> throw new PlanValidationException("The given statement is not a query statement");
        }
    }

    public int executeUpdate(String updateQuery, Transaction transaction) throws PlanValidationException {
        Parser parser = new Parser(updateQuery);

        Statement statement = parser.parse();

        return switch (statement) {
            case CreateIndexStatement createIndexStatement ->
                updatePlanner.executeCreateIndexValidated(createIndexStatement, transaction);
            case CreateTableStatement createTableStatement ->
                updatePlanner.executeCreateTableValidated(createTableStatement, transaction);
            case CreateViewStatement createViewStatement ->
                updatePlanner.executeCreateViewValidated(createViewStatement, transaction);
            case DeleteStatement deleteStatement ->
                updatePlanner.executeDeleteValidated(deleteStatement, transaction);
            case InsertStatement insertStatement ->
                updatePlanner.executeInsertValidated(insertStatement, transaction);
            case UpdateStatement updateStatement ->
                updatePlanner.executeUpdateValidated(updateStatement, transaction);
            case ExplainStatement explainStatement -> { // todo statement explaining (plan printing)
                throw new UnsupportedOperationException();
            }
            default -> throw new PlanValidationException("The given statement is not an update statement.");
        };
    }
}
