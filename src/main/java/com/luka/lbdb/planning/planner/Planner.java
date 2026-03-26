package com.luka.lbdb.planning.planner;

import com.luka.lbdb.parsing.parser.Parser;
import com.luka.lbdb.parsing.statement.*;
import com.luka.lbdb.planning.exceptions.PlanValidationException;
import com.luka.lbdb.planning.plan.Plan;
import com.luka.lbdb.planning.planner.plannerDefinitions.QueryPlanner;
import com.luka.lbdb.planning.planner.plannerDefinitions.UpdatePlanner;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.transactions.Transaction;

/// The main entry point class for executing / creating plans. Does not
/// have any special logic, it is just a wrapper over the system's query and
/// update planners.
public class Planner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    /// A planner needs some concrete implementation of the query and update
    /// planners.
    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    /// Creates a query plan, but does not execute or process it.
    ///
    /// @return The plan for a `SELECT` query.
    public Plan<Scan> createQueryPlan(String query, Transaction transaction) throws PlanValidationException {
        Parser parser = new Parser(query);

        Statement statement = parser.parse();

        switch (statement) {
            case SelectStatement selectStatement -> {
                return queryPlanner.createValidatedPlan(selectStatement, transaction);
            }
            default -> throw new PlanValidationException("The given statement is not a query statement");
        }
    }

    /// Creates a plan for any modification query and executes it.
    ///
    /// @return The number of rows affected.
    public int executeUpdate(String updateQuery, Transaction transaction) throws PlanValidationException {
        Parser parser = new Parser(updateQuery);

        Statement statement = parser.parse();

        return switch (statement) {
            case CreateIndexStatement createIndexStatement ->
                updatePlanner.executeCreateIndexValidated(createIndexStatement, transaction);
            case CreateTableStatement createTableStatement ->
                updatePlanner.executeCreateTableValidated(createTableStatement, transaction);
            case DeleteStatement deleteStatement ->
                updatePlanner.executeDeleteValidated(deleteStatement, transaction);
            case InsertStatement insertStatement ->
                updatePlanner.executeInsertValidated(insertStatement, transaction);
            case UpdateStatement updateStatement ->
                updatePlanner.executeUpdateValidated(updateStatement, transaction);
            default -> throw new PlanValidationException("The given statement is not an update statement.");
        };
    }

    /// @return The string that explains some `SELECT` query.
    public String explainStatement(String query, Transaction transaction) {
        Parser parser = new Parser(query);

        Statement statement = parser.parse();

        return switch (statement) {
            case ExplainStatement explainStatement ->
                switch (explainStatement.explainingStatement()) {
                    case SelectStatement s ->
                            queryPlanner.createValidatedPlan(s, transaction).explainedPlan();
                    case ExplainStatement e ->
                            "You can't explain an explain statement :)";
                    default ->
                            "Update plan explaining is sadly not supported";
                };
            default -> throw new PlanValidationException("The given statement is not an explain statement.");
        };
    }
}
