package com.luka.lbdb.planning.planner.plannerDefinitions;

import com.luka.lbdb.metadataManagement.MetadataManager;
import com.luka.lbdb.metadataManagement.exceptions.*;
import com.luka.lbdb.parsing.statement.*;
import com.luka.lbdb.parsing.statement.insert.NewFieldValueInfo;
import com.luka.lbdb.parsing.statement.update.NewFieldExpressionAssignment;
import com.luka.lbdb.planning.exceptions.PlanValidationException;
import com.luka.lbdb.querying.exceptions.ZeroDivisionException;
import com.luka.lbdb.querying.virtualEntities.Predicate;
import com.luka.lbdb.querying.virtualEntities.constant.Constant;
import com.luka.lbdb.querying.virtualEntities.expression.Expression;
import com.luka.lbdb.querying.virtualEntities.expression.PartialEvaluator;
import com.luka.lbdb.querying.virtualEntities.term.Term;
import com.luka.lbdb.records.DatabaseType;
import com.luka.lbdb.records.Layout;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.records.exceptions.RecordTooLongException;
import com.luka.lbdb.transactionManagement.Transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /// Validates every aspect of an update statement and folds constant expressions.
    /// Checks for:
    /// - tables and fields existing
    /// - nullability of fields
    /// - types of fields
    ///
    /// @return The number of rows affected after the statement execution.
    /// @throws PlanValidationException on various failed checks.
    public int executeUpdateValidated(UpdateStatement updateStatement, Transaction transaction) {
        Layout tableLayout = getTableLayout(updateStatement.tableName(), transaction);

        List<NewFieldExpressionAssignment> foldedNewExprs = new ArrayList<>();
        for (NewFieldExpressionAssignment newFieldValue : updateStatement.newValues()) {
            if (!tableLayout.getSchema().hasField(newFieldValue.fieldName())) {
                throw new PlanValidationException(String.format(
                        "Field '%s' doesn't exist in the table",
                        newFieldValue.fieldName()
                ));
            }

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
                    && foldedExpr.type(tableLayout.getSchema()) == DatabaseType.NULL) {
                throw new PlanValidationException(
                        String.format("Field '%s' isn't nullable", newFieldValue.fieldName())
                );
            }

            if (tableLayout.getSchema().type(newFieldValue.fieldName()) != foldedExpr.type(tableLayout.getSchema())
                    && newFieldValue.newValueExpression().type(tableLayout.getSchema()) != DatabaseType.NULL) {
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

    /// Validates every aspect of an insert statement and folds constant expressions.
    /// Checks for:
    /// - tables and fields existing
    /// - correct number of fields existing
    /// - nullability of fields
    /// - types of fields
    ///
    /// @return The number of rows affected after the statement execution.
    /// @throws PlanValidationException on various failed checks.
    public int executeInsertValidated(InsertStatement insertStatement, Transaction transaction) {
        Layout tableLayout = getTableLayout(insertStatement.tableName(), transaction);

        List<NewFieldValueInfo> filledValueInfos;

        if (insertStatement.implicitFieldNames()) {
            filledValueInfos = new ArrayList<>();
            Schema tableSchema = tableLayout.getSchema();

            if (tableSchema.getFields().size() != insertStatement.newFieldValues().size()) {
                throw new PlanValidationException("Incorrect number of fields in statement");
            }

            for (int i = 0; i < tableSchema.getFields().size(); i++) {
                String fieldName = tableSchema.getFields().get(i);
                Constant newValue = insertStatement.newFieldValues().get(i).newValue();

                filledValueInfos.add(new NewFieldValueInfo(fieldName, newValue));
            }
        } else {
            filledValueInfos = insertStatement.newFieldValues();
        }

        InsertStatement filledInsertStatement = new InsertStatement(
            insertStatement.tableName(), filledValueInfos, false
        );

        if (filledInsertStatement.newFieldValues().size() != tableLayout.getSchema().getFields().size()) {
            throw new PlanValidationException("Incorrect number of fields in statement");
        }

        for (NewFieldValueInfo newFieldValue : filledInsertStatement.newFieldValues()) {
            if (!tableLayout.getSchema().hasField(newFieldValue.fieldName())) {
                throw new PlanValidationException(
                        String.format("Field '%s' doesn't exist in the table", newFieldValue.fieldName())
                );
            }

            if (!tableLayout.getSchema().isNullable(newFieldValue.fieldName())
                    && newFieldValue.newValue().type() == DatabaseType.NULL) {
                throw new PlanValidationException(
                        String.format("Field '%s' isn't nullable", newFieldValue.fieldName())
                );
            }

            if (tableLayout.getSchema().type(newFieldValue.fieldName()) != newFieldValue.newValue().type()
                    && newFieldValue.newValue().type() != DatabaseType.NULL) {
                throw new PlanValidationException(
                        String.format("Field '%s' has the wrong type", newFieldValue.fieldName())
                );
            }
        }

        return executeInsert(filledInsertStatement, transaction);
    }

    /// Validates every aspect of a delete statement and folds constant expressions.
    /// Checks for:
    /// - tables and fields existing
    ///
    /// @return The number of rows affected after the statement execution.
    /// @throws PlanValidationException on various failed checks.
    public int executeDeleteValidated(DeleteStatement deleteStatement, Transaction transaction) {
        Layout tableLayout = getTableLayout(deleteStatement.tableName(), transaction);
        validateAndFoldPredicate(deleteStatement.predicate(), tableLayout.getSchema());

        return executeDelete(deleteStatement, transaction);
    }

    /// Validates every aspect of a create table statement.
    /// Checks for:
    /// - table already existing
    /// - maximum record size
    ///
    /// @return The number of rows affected after the statement execution.
    /// @throws PlanValidationException on various failed checks.
    public int executeCreateTableValidated(CreateTableStatement createTableStatement, Transaction transaction) {
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

    /// Validates every aspect of a create index statement.
    /// Checks for:
    /// - tables and fields existing
    /// - same name index already existing
    /// - same field index already existing
    ///
    /// @return The number of rows affected after the statement execution.
    /// @throws PlanValidationException on various failed checks.
    public int executeCreateIndexValidated(CreateIndexStatement createIndexStatement, Transaction transaction) {
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

    /// Knows for sure that the update statement is valid and executes it without any check.
    ///
    /// @return The number of rows affected after the statement execution.
    protected abstract int executeUpdate(UpdateStatement updateStatement, Transaction transaction);

    /// Knows for sure that the insert statement is valid and executes it without any check.
    ///
    /// @return The number of rows affected after the statement execution.
    protected abstract int executeInsert(InsertStatement insertStatement, Transaction transaction);

    /// Knows for sure that the delete statement is valid and executes it without any check.
    ///
    /// @return The number of rows affected after the statement execution.
    protected abstract int executeDelete(DeleteStatement deleteStatement, Transaction transaction);

    /// Knows for sure that the create table statement is valid and executes it without any check.
    ///
    /// @return The number of rows affected after the statement execution.
    protected abstract int executeCreateTable(CreateTableStatement createTableStatement, Transaction transaction);

    /// Knows for sure that the create index statement is valid and executes it without any check.
    ///
    /// @return The number of rows affected after the statement execution.
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

            if (lhs.type(schema) != rhs.type(schema)
                    && lhs.type(schema) != DatabaseType.NULL && rhs.type(schema) != DatabaseType.NULL) {
                throw new PlanValidationException("Different types are compared in the 'WHERE' predicate.");
            }
        }
    }
}
