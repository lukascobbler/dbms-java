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
import com.luka.simpledb.transactionManagement.Transaction;

public class BasicUpdatePlanner extends UpdatePlanner {
    public BasicUpdatePlanner(MetadataManager metadataManager) {
        super(metadataManager);
    }

    @Override
    protected int executeInsert(InsertStatement insertStatement, Transaction transaction) {
        Plan<UpdateScan> plan = new TablePlan(transaction, insertStatement.tableName(), metadataManager);

        try (UpdateScan insertScan = plan.open()) {
            insertScan.insert();

            for (NewFieldValueInfo newFieldValueInfo : insertStatement.newFieldValues()) {
                insertScan.setValue(newFieldValueInfo.fieldName(), newFieldValueInfo.newValue());
            }
        }

        return 1;
    }

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

    @Override
    protected int executeCreateTable(CreateTableStatement createTableStatement, Transaction transaction) {
        metadataManager.createTable(
                createTableStatement.tableName(),
                createTableStatement.schema(),
                transaction
        );
        return 0;
    }

    @Override
    protected int executeCreateView(CreateViewStatement createViewStatement, Transaction transaction) {
        metadataManager.createView(
                createViewStatement.viewName(),
                createViewStatement.selectStatement().toString(),
                transaction
        );
        return 0;
    }

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
}
