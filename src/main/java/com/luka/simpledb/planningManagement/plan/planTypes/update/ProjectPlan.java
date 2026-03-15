package com.luka.simpledb.planningManagement.plan.planTypes.update;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.ProjectScan;
import com.luka.simpledb.recordManagement.Schema;

import java.util.List;

public class ProjectPlan extends Plan<UpdateScan> {
    private final Plan<UpdateScan> childPlan;
    private final Schema schema = new Schema();

    public ProjectPlan(Plan<UpdateScan> childPlan, List<String> fieldList) {
        this.childPlan = childPlan;

        for (String field : fieldList) {
            schema.add(field, childPlan.schema());
        }
    }

    @Override
    public UpdateScan open() {
        UpdateScan openedChildScan = childPlan.open();
        return new ProjectScan(openedChildScan, schema.getFields());
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
