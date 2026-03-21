package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.SelectReadOnlyScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.recordManagement.Schema;

public class SelectReadOnlyPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final Predicate predicate;

    public SelectReadOnlyPlan(Plan<Scan> childPlan, Predicate predicate) {
        this.childPlan = childPlan;
        this.predicate = predicate;
    }

    @Override
    public Scan open() {
        Scan openedChildScan = childPlan.open();
        return new SelectReadOnlyScan(openedChildScan, predicate);
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
            // todo check these equations here and in the non-read only select scan
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
    public Schema outputSchema() {
        return childPlan.outputSchema();
    }
}
