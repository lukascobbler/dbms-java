package com.luka.simpledb.planningManagement.planner.plannerTypes;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.IndexType;
import com.luka.simpledb.parsingManagement.statement.*;
import com.luka.simpledb.parsingManagement.statement.update.NewFieldExpressionAssignment;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.planningManagement.plan.planTypes.update.SelectPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.update.TablePlan;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.UpdatePlanner;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.PartialEvaluator;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.Iterator;

public class BasicUpdatePlanner implements UpdatePlanner {
    private final MetadataManager metadataManager;

    public BasicUpdatePlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public int executeInsert(InsertStatement insertStatement, Transaction transaction) {
        Plan<UpdateScan> plan = new TablePlan(transaction, insertStatement.tableName(), metadataManager);

        try (UpdateScan insertScan = plan.open()) {
            insertScan.insert();

            Iterator<Constant> constantIterator = insertStatement.values().iterator();
            for (String fieldName : insertStatement.fields()) {
                Constant newValue = constantIterator.next();
                insertScan.setValue(fieldName, newValue); // todo check appropriate types
            }
        }

        return 1;
    }

    @Override
    public int executeUpdate(UpdateStatement updateStatement, Transaction transaction) {
        Plan<UpdateScan> plan = new TablePlan(transaction, updateStatement.tableName(), metadataManager);
        plan = new SelectPlan(plan, updateStatement.predicate());

        for (NewFieldExpressionAssignment newFieldValue : updateStatement.newValues()) {
            // we do folding of every new expression before calculating the values in the scan
            // to ensure as little calculation as possible on the virtual machine
            PartialEvaluator.evaluate(newFieldValue.newValueExpression());
        }

        int count = 0;
        try (UpdateScan updateScan = plan.open()) {
            while (updateScan.next()) {
                count++;

                for (NewFieldExpressionAssignment newFieldValue : updateStatement.newValues()) {
                    Constant computedValue = newFieldValue.newValueExpression().evaluate(updateScan);
                    updateScan.setValue(newFieldValue.fieldName(), computedValue); // todo check appropriate types
                }
            }
        }

        return count;
    }

    @Override
    public int executeDelete(DeleteStatement deleteStatement, Transaction transaction) {
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
    public int executeCreateTable(CreateTableStatement createTableStatement, Transaction transaction) {
        metadataManager.createTable(
                createTableStatement.tableName(),
                createTableStatement.schema(),
                transaction
        );
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewStatement createViewStatement, Transaction transaction) {
        metadataManager.createView(
                createViewStatement.viewName(),
                createViewStatement.selectStatement().toString(),
                transaction
        );
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexStatement createIndexStatement, Transaction transaction) {
        metadataManager.createIndex(
                createIndexStatement.indexName(),
                createIndexStatement.tableName(),
                createIndexStatement.fieldName(),
                IndexType.B_TREE, // todo different index types in parsing management and here
                transaction
        );
        return 0;
    }
}
