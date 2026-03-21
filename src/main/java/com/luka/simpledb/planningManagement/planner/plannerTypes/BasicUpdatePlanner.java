package com.luka.simpledb.planningManagement.planner.plannerTypes;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.parsingManagement.statement.*;
import com.luka.simpledb.parsingManagement.statement.insert.NewFieldValueInfo;
import com.luka.simpledb.parsingManagement.statement.update.NewFieldExpressionAssignment;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.planningManagement.plan.planTypes.update.SelectPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.update.TablePlan;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.UpdatePlanner;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.RecordId;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.Map;
import java.util.Optional;

// todo docs
public class BasicUpdatePlanner extends UpdatePlanner {
    private final Map<String, RecordId> lastInsertions;

    public BasicUpdatePlanner(MetadataManager metadataManager, Map<String, RecordId> lastInsertions) {
        super(metadataManager);
        this.lastInsertions = lastInsertions;
    }

    /// Inserts a new record from the position of the lastly inserted record.
    /// This insertion strategy allows faster insertion performance, but wastes
    /// space of earlier deleted records until the server restarts.
    ///
    /// @return 1, because only one row can be inserted at a time.
    @Override
    protected int executeInsert(InsertStatement insertStatement, Transaction transaction) {
        Plan<UpdateScan> plan = new TablePlan(transaction, insertStatement.tableName(), metadataManager);

        Optional<RecordId> lastInsertionForTable = getLastInsertion(insertStatement.tableName());

        try (UpdateScan insertScan = plan.open()) {
            lastInsertionForTable.ifPresent(insertScan::moveToRecordId);

            insertScan.insert();

            for (NewFieldValueInfo newFieldValueInfo : insertStatement.newFieldValues()) {
                insertScan.setValue(newFieldValueInfo.fieldName(), newFieldValueInfo.newValue());
            }

            setLastInsertion(insertStatement.tableName(), insertScan.getRecordId());
        }

        return 1;
    }

    /// Updates all records that match a predicate. Only provided fields' will be changed.
    ///
    /// @return The number of rows that matched the predicate.
    @Override
    protected int executeUpdate(UpdateStatement updateStatement, Transaction transaction) {
        Plan<UpdateScan> plan = new TablePlan(transaction, updateStatement.tableName(), metadataManager);
        plan = new SelectPlan(plan, updateStatement.predicate());

        int count = 0;
        try (UpdateScan updateScan = plan.open()) {
            while (updateScan.next()) {
                count++;

                for (NewFieldExpressionAssignment newFieldValue : updateStatement.newValues()) {
                    Constant computedValue = newFieldValue.newValueExpression().evaluate(updateScan);
                    updateScan.setValue(newFieldValue.fieldName(), computedValue);
                }
            }
        }

        return count;
    }

    /// Deletes all records that match a predicate.
    ///
    /// @return The number of rows that matched the predicate.
    @Override
    protected int executeDelete(DeleteStatement deleteStatement, Transaction transaction) {
        Plan<UpdateScan> plan = new TablePlan(transaction, deleteStatement.tableName(), metadataManager);
        plan = new SelectPlan(plan, deleteStatement.predicate());

        int count = 0;
        try (UpdateScan deleteScan = plan.open()) {
            while (deleteScan.next()) {
                deleteScan.delete();
                count++;
            }
        }

        return count;
    }

    /// Puts the new table's metadata in the system - its name and schema.
    ///
    /// @return 0, because no rows were changed.
    @Override
    protected int executeCreateTable(CreateTableStatement createTableStatement, Transaction transaction) {
        metadataManager.createTable(
                createTableStatement.tableName(),
                createTableStatement.schema(),
                transaction
        );
        return 0;
    }

    /// Puts the new view's metadata in the system - its name and definition.
    ///
    /// @return 0, because no rows were changed.
    @Override
    protected int executeCreateView(CreateViewStatement createViewStatement, Transaction transaction) {
        metadataManager.createView(
                createViewStatement.viewName(),
                createViewStatement.selectStatement().toString(),
                transaction
        );
        return 0;
    }

    /// Puts the new index's metadata in the system - its name, table, field and type.
    ///
    /// @return 0, because no rows were changed.
    @Override
    protected int executeCreateIndex(CreateIndexStatement createIndexStatement, Transaction transaction) {
        metadataManager.createIndex(
                createIndexStatement.indexName(),
                createIndexStatement.tableName(),
                createIndexStatement.fieldName(),
                createIndexStatement.type(),
                transaction
        );
        return 0;
    }

    /// Synchronously gets the last insert record id position from a given table.
    private synchronized Optional<RecordId> getLastInsertion(String tableName) {
        return Optional.ofNullable(lastInsertions.get(tableName));
    }

    /// Synchronously sets the last insert record id position for a given table.
    private synchronized void setLastInsertion(String tableName, RecordId recordId) {
        lastInsertions.put(tableName, recordId);
    }
}
