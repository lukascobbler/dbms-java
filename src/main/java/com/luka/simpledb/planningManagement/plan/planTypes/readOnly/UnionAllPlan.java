package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.UnionAllScan;
import com.luka.simpledb.recordManagement.DatabaseType;
import com.luka.simpledb.recordManagement.schema.Schema;

public class UnionAllPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan1, childPlan2;
    private final Schema outputSchema = new Schema();

    public UnionAllPlan(Plan<Scan> childPlan1, Plan<Scan> childPlan2) {
        this.childPlan1 = childPlan1;
        this.childPlan2 = childPlan2;

        Schema child1Schema = childPlan1.outputSchema();
        Schema child2Schema = childPlan2.outputSchema();

        for (String fieldName : child1Schema.getFields()) {
            DatabaseType type = child1Schema.type(fieldName);
            int runtimeLength = Math.max(child1Schema.runtimeLength(fieldName), child2Schema.runtimeLength(fieldName));
            boolean isNullable = child1Schema.isNullable(fieldName) || child2Schema.isNullable(fieldName);

            outputSchema.addField(fieldName, type, runtimeLength, isNullable);
        }
    }

    @Override
    public Scan open() {
        Scan openedChildScan1 = childPlan1.open();
        Scan openedChildScan2 = childPlan2.open();
        return new UnionAllScan(openedChildScan1, openedChildScan2);
    }

    @Override
    public int blocksAccessed() {
        return childPlan1.blocksAccessed() + childPlan2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return childPlan1.recordsOutput() + childPlan2.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return childPlan1.distinctValues(fieldName) + childPlan2.distinctValues(fieldName);
    }

    @Override
    public Schema outputSchema() {
        return outputSchema;
    }
}
