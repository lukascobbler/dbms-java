package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.ProjectReadOnlyScan;
import com.luka.simpledb.recordManagement.Schema;

import java.util.List;

public class ProjectReadOnlyPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final Schema schema = new Schema();

    public ProjectReadOnlyPlan(Plan<Scan> childPlan, List<String> fieldList) {
        this.childPlan = childPlan;

        for (String field : fieldList) {
            schema.add(field, childPlan.schema());
        }
    }

    @Override
    public Scan open() {
        Scan openedChildScan = childPlan.open();
        return new ProjectReadOnlyScan(openedChildScan, schema.getFields());
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
        return childPlan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
