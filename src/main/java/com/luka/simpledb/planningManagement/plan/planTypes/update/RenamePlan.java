package com.luka.simpledb.planningManagement.plan.planTypes.update;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.RenameScan;
import com.luka.simpledb.recordManagement.Schema;

public class RenamePlan extends Plan<UpdateScan> {
    private final Plan<UpdateScan> childPlan;
    private final String oldFieldName, newFieldName;
    private final Schema schema = new Schema();

    public RenamePlan(Plan<UpdateScan> childPlan, String oldFieldName, String newFieldName) {
        this.childPlan = childPlan;
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;

        Schema oldSchema = childPlan.schema();
        for (String fieldName : schema.getFields()) {
            if (!fieldName.equals(oldFieldName)) {
                schema.add(fieldName, oldSchema);
            }

            int fieldType = oldSchema.type(fieldName);
            int length = oldSchema.length(fieldName);
            boolean isNullable = oldSchema.isNullable(fieldName);

            schema.addField(fieldName, fieldType, length, isNullable);
        }
    }

    @Override
    public UpdateScan open() {
        UpdateScan childScan = childPlan.open();
        return new RenameScan(childScan, oldFieldName, newFieldName);
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
        if (fieldName.equals(newFieldName)) {
            return childPlan.distinctValues(oldFieldName);
        }

        return childPlan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
