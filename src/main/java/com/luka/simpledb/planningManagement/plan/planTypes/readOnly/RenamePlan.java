package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.RenameScan;
import com.luka.simpledb.recordManagement.Schema;

import java.util.Map;

public class RenamePlan implements Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final Map<String, String> newToOldNames;
    private final Schema outputSchema = new Schema();

    public RenamePlan(Plan<Scan> childPlan, Map<String, String> newToOldNames) {
        this.childPlan = childPlan;
        this.newToOldNames = newToOldNames;

        Schema oldSchema = childPlan.outputSchema();

        for (var mapEntry : newToOldNames.entrySet()) {
            int fieldType = oldSchema.type(mapEntry.getValue());
            int length = oldSchema.length(mapEntry.getValue());
            boolean isNullable = oldSchema.isNullable(mapEntry.getValue());

            outputSchema.addField(mapEntry.getKey(), fieldType, length, isNullable);
        }
    }

    @Override
    public Scan open() {
        Scan childScan = childPlan.open();
        return new RenameScan(childScan, newToOldNames);
    }

    @Override
    public int blocksAccessed() {
        return childPlan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return childPlan.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return childPlan.distinctValues(newToOldNames.get(fieldName));
    }

    @Override
    public Schema outputSchema() {
        return outputSchema;
    }
}