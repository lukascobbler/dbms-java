package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.ExtendScan;
import com.luka.simpledb.recordManagement.Schema;

public class ExtendReadOnlyPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final ProjectionFieldInfo projectionFieldInfo;
    private final Schema schema = new Schema();

    public ExtendReadOnlyPlan(Plan<Scan> childPlan, ProjectionFieldInfo projectionFieldInfo) {
        this.childPlan = childPlan;
        this.projectionFieldInfo = projectionFieldInfo;

        schema.addAll(childPlan.schema());

        int type = projectionFieldInfo.expression().type(childPlan.schema());
        int length = projectionFieldInfo.expression().length(childPlan.schema());
        boolean isNullable = false; // extended fields aren't nullable
        schema.addField(projectionFieldInfo.name(), type, length, isNullable);
    }

    @Override
    public Scan open() {
        Scan childScan = childPlan.open();
        return new ExtendScan(childScan, projectionFieldInfo.expression(), projectionFieldInfo.name());
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
        if (fieldName.equals(projectionFieldInfo.name())) {
            return childPlan.recordsOutput(); // todo comment
        }
        return childPlan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
