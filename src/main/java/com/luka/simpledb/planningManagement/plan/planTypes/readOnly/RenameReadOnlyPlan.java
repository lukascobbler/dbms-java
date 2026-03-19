package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.RenameReadOnlyScan;
import com.luka.simpledb.recordManagement.Schema;

public class RenameReadOnlyPlan extends Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final String oldFieldName, newFieldName;
    private final Schema schema = new Schema();

    public RenameReadOnlyPlan(Plan<Scan> childPlan, String oldFieldName, String newFieldName) {
        this.childPlan = childPlan;
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;

        Schema oldSchema = childPlan.schema();
        for (String fieldName : oldSchema.getFields()) {
            if (!fieldName.equals(oldFieldName)) {
                schema.add(fieldName, oldSchema);
            }
        }

        int fieldType = oldSchema.type(oldFieldName);
        int length = oldSchema.length(oldFieldName);
        boolean isNullable = oldSchema.isNullable(oldFieldName);

        schema.addField(newFieldName, fieldType, length, isNullable);
    }

    @Override
    public Scan open() {
        Scan childScan = childPlan.open();
        return new RenameReadOnlyScan(childScan, oldFieldName, newFieldName);
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