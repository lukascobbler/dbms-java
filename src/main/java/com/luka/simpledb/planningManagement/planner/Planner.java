package com.luka.simpledb.planningManagement.planner;

import com.luka.simpledb.parsingManagement.parser.Parser;
import com.luka.simpledb.parsingManagement.statement.*;
import com.luka.simpledb.planningManagement.exceptions.QueryPlanForNonQueryStatementException;
import com.luka.simpledb.planningManagement.exceptions.UpdateQueryExecutionForNonUpdateQueryException;
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

    public Plan<Scan> createQueryPlan(String query, Transaction transaction) {
        Parser parser = new Parser(query);

        Statement statement = parser.parse();

        switch (statement) {
            case SelectStatement selectStatement -> {
                return queryPlanner.createPlan(selectStatement, transaction);
            }
            case ExplainStatement explainStatement -> { // todo statement explaining (plan printing)
                throw new UnsupportedOperationException();
            }
            default -> throw new QueryPlanForNonQueryStatementException();
        }
    }

    public int executeUpdate(String updateQuery, Transaction transaction) {
        Parser parser = new Parser(updateQuery);

        Statement statement = parser.parse();

        return switch (statement) {
            case CreateIndexStatement createIndexStatement ->
                updatePlanner.executeCreateIndex(createIndexStatement, transaction);
            case CreateTableStatement createTableStatement ->
                updatePlanner.executeCreateTable(createTableStatement, transaction);
            case CreateViewStatement createViewStatement ->
                updatePlanner.executeCreateView(createViewStatement, transaction);
            case DeleteStatement deleteStatement ->
                updatePlanner.executeDelete(deleteStatement, transaction);
            case InsertStatement insertStatement ->
                updatePlanner.executeInsert(insertStatement, transaction);
            case UpdateStatement updateStatement ->
                updatePlanner.executeUpdate(updateStatement, transaction);
            case ExplainStatement explainStatement -> { // todo statement explaining (plan printing)
                throw new UnsupportedOperationException();
            }
            default -> throw new UpdateQueryExecutionForNonUpdateQueryException();
        };
    }
}
