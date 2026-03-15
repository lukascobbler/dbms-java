package com.luka.simpledb.planningManagement.plan.planTypes.update;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.SelectScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.recordManagement.Schema;

public class SelectPlan extends Plan<UpdateScan> {
    private final Plan<UpdateScan> childPlan;
    private final Predicate predicate;

    public SelectPlan(Plan<UpdateScan> childPlan, Predicate predicate) {
        this.childPlan = childPlan;
        this.predicate = predicate;
    }

    @Override
    public UpdateScan open() {
        UpdateScan openedChildScan = childPlan.open();
        return new SelectScan(openedChildScan, predicate);
    }

    @Override
    public int blocksAccessed() {
        return childPlan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return childPlan.recordsOutput() / predicate.reductionFactor(childPlan);
    }

    @Override
    public int distinctValues(String fieldName) {
        if (predicate.equatesWithConstant(fieldName) != null) {
            return 1;
        } else {
            String secondFieldName = predicate.equatesWithFieldName(fieldName);

            if (secondFieldName != null) {
                return Math.min(
                        childPlan.distinctValues(fieldName),
                        childPlan.distinctValues(secondFieldName)
                );
            } else {
                return childPlan.distinctValues(fieldName);
            }
        }
    }

    @Override
    public Schema schema() {
        return childPlan.schema();
    }
}
