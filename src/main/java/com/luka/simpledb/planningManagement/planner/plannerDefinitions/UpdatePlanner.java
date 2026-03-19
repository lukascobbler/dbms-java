package com.luka.simpledb.planningManagement.planner.plannerDefinitions;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.*;
import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
import com.luka.simpledb.parsingManagement.statement.*;
import com.luka.simpledb.parsingManagement.statement.insert.NewFieldValueInfo;
import com.luka.simpledb.parsingManagement.statement.update.NewFieldExpressionAssignment;
import com.luka.simpledb.planningManagement.exceptions.PlanValidationException;
import com.luka.simpledb.queryManagement.exceptions.ZeroDivisionException;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.PartialEvaluator;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.recordManagement.exceptions.RecordTooLongException;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.sql.Types.NULL;

/// Abstraction over all implementations of update planners which performs
/// all semantic checks and prepares the planner for update execution. Any
/// implementation of update planning only needs to extend this class and
/// define the execution part.
public abstract class UpdatePlanner {
    protected final MetadataManager metadataManager;

    /// An update planner needs the system's metadata to validate queries against.
    public UpdatePlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    // Public API, representing the operations that the update planner is able to execute checked

    public int executeUpdateChecked(UpdateStatement updateStatement, Transaction transaction) {
        Layout tableLayout = getTableLayout(updateStatement.tableName(), transaction);

        List<NewFieldExpressionAssignment> foldedNewExprs = new ArrayList<>();
        for (NewFieldExpressionAssignment newFieldValue : updateStatement.newValues()) {
            Expression foldedExpr;
            try {
                foldedExpr = PartialEvaluator.evaluate(newFieldValue.newValueExpression());
            } catch (ZeroDivisionException e) {
                throw new PlanValidationException("Constant zero division");
            }

            Set<String> exprFields = new HashSet<>(foldedExpr.getFields());

            for (String f : exprFields) {
                if (!tableLayout.getSchema().hasField(f)) {
                    throw new PlanValidationException(String.format("Field '%s' doesn't exist in the table", f));
                }
            }

            if (!tableLayout.getSchema().isNullable(newFieldValue.fieldName())
                    && foldedExpr.type(tableLayout.getSchema()) == NULL) {
                throw new ParsingException(
                        String.format("Field '%s' isn't nullable", newFieldValue.fieldName())
                );
            }

            if (tableLayout.getSchema().type(newFieldValue.fieldName()) != foldedExpr.type(tableLayout.getSchema())) {
                throw new PlanValidationException(String.format(
                        "Expression '%s' has the wrong type",
                        newFieldValue.newValueExpression())
                );
            }

            foldedNewExprs.add(new NewFieldExpressionAssignment(newFieldValue.fieldName(), foldedExpr));
        }

        validateAndFoldPredicate(updateStatement.predicate(), tableLayout.getSchema());

        UpdateStatement foldedUpdateStatement = new UpdateStatement(
                updateStatement.tableName(),
                foldedNewExprs,
                updateStatement.predicate()
        );

        return executeUpdate(foldedUpdateStatement, transaction);
    }

    public int executeInsertChecked(InsertStatement insertStatement, Transaction transaction) {
        Layout tableLayout = getTableLayout(insertStatement.tableName(), transaction);

        if (insertStatement.newFieldValues().size() != tableLayout.getSchema().getFields().size()) {
            throw new ParsingException("Incorrect number of fields in statement");
        }

        for (NewFieldValueInfo newFieldValue : insertStatement.newFieldValues()) {
            if (!tableLayout.getSchema().hasField(newFieldValue.fieldName())) {
                throw new ParsingException(
                        String.format("Field '%s' doesn't exist in the table", newFieldValue.fieldName())
                );
            }

            if (!tableLayout.getSchema().isNullable(newFieldValue.fieldName())
                    && newFieldValue.newValue().type() == NULL) {
                throw new ParsingException(
                        String.format("Field '%s' isn't nullable", newFieldValue.fieldName())
                );
            }

            if (tableLayout.getSchema().type(newFieldValue.fieldName()) != newFieldValue.newValue().type()
                    && newFieldValue.newValue().type() != NULL) {
                throw new ParsingException(
                        String.format("Field '%s' has the wrong type", newFieldValue.fieldName())
                );
            }
        }

        return executeInsert(insertStatement, transaction);
    }

    public int executeDeleteChecked(DeleteStatement deleteStatement, Transaction transaction) {
        Layout tableLayout = getTableLayout(deleteStatement.tableName(), transaction);
        validateAndFoldPredicate(deleteStatement.predicate(), tableLayout.getSchema());

        return executeDelete(deleteStatement, transaction);
    }

    public int executeCreateViewChecked(CreateViewStatement createViewStatement, Transaction transaction) {
        // todo check
        return executeCreateView(createViewStatement, transaction);
    }

    public int executeCreateTableChecked(CreateTableStatement createTableStatement, Transaction transaction) {
        try {
            return executeCreateTable(createTableStatement, transaction);
        } catch (TableDuplicateNameException e) {
            throw new PlanValidationException(String.format(
                    "Table '%s' already exists",
                    createTableStatement.tableName()
            ));
        } catch (RecordTooLongException e) {
            throw new PlanValidationException(String.format(
                    "Record size for table '%s' is above the limit (%sB > %dB)",
                    createTableStatement.tableName(),
                    e.getMessage(),
                    transaction.blockSize()
            ));
        }
    }

    public int executeCreateIndexChecked(CreateIndexStatement createIndexStatement, Transaction transaction) {
        try {
            return executeCreateIndex(createIndexStatement, transaction);
        } catch (TableNotFoundException e) {
            throw new PlanValidationException(String.format(
                    "Table '%s' does not exist",
                    createIndexStatement.tableName()
            ));
        } catch (IndexTableIncorrectException e) {
            throw new PlanValidationException(String.format(
                    "Table '%s' does not have the fieldName '%s'",
                    createIndexStatement.tableName(),
                    createIndexStatement.fieldName()
            ));
        } catch (IndexDuplicateNameException e) {
            throw new PlanValidationException(String.format(
                    "Index with name '%s' already exists",
                    createIndexStatement.indexName()
            ));
        } catch (IndexAlreadyExistsException e) {
            throw new PlanValidationException(String.format(
                    "Index on fieldName '%s' already exists",
                    createIndexStatement.fieldName()
            ));
        }
    }

    // Internal API, that runs actual plan creations

    protected abstract int executeUpdate(UpdateStatement updateStatement, Transaction transaction);
    protected abstract int executeInsert(InsertStatement insertStatement, Transaction transaction);
    protected abstract int executeDelete(DeleteStatement deleteStatement, Transaction transaction);
    protected abstract int executeCreateView(CreateViewStatement createViewStatement, Transaction transaction);
    protected abstract int executeCreateTable(CreateTableStatement createTableStatement, Transaction transaction);
    protected abstract int executeCreateIndex(CreateIndexStatement createIndexStatement, Transaction transaction);

    // Private API, for checking statements

    /// Gets the table layout for validating fields against some
    /// statement.
    ///
    /// @return The layout of the table.
    /// @throws PlanValidationException if the table doesn't exist.
    private Layout getTableLayout(String tableName, Transaction transaction) {
        try {
            return metadataManager.getLayout(tableName, transaction);
        } catch (TableNotFoundException e) {
            throw new PlanValidationException(String.format(
                    "Table '%s' does not exist",
                    tableName
            ));
        }
    }

    /// Validates the predicate against a schema.
    ///
    /// @throws PlanValidationException if the predicate isn't valid.
    private void validateAndFoldPredicate(Predicate predicate, Schema schema) {
        try {
            predicate.fold();
        } catch (ZeroDivisionException e) {
            throw new PlanValidationException("Constant zero division");
        }

        for (Term term : predicate.getTerms()) {
            Expression lhs = term.getLhs();
            Expression rhs = term.getRhs();

            Set<String> termFields = new HashSet<>(lhs.getFields());
            termFields.addAll(rhs.getFields());

            for (String f : termFields) {
                if (!schema.hasField(f)) {
                    throw new PlanValidationException(String.format("Field '%s' doesn't exist in the table", f));
                }
            }

            if (lhs.type(schema) != rhs.type(schema)) {
                throw new PlanValidationException("Different types are compared in the 'WHERE' predicate.");
            }
        }
    }
}
