package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.ProductScan;
import com.luka.simpledb.recordManagement.Schema;

public class ProductPlan extends Plan<Scan> {
    private final Plan<Scan> childPlan1, childPlan2;
    private final Schema schema = new Schema();

    public ProductPlan(Plan<Scan> childPlan1, Plan<Scan> childPlan2) {
        this.childPlan1 = childPlan1;
        this.childPlan2 = childPlan2;
        schema.addAll(childPlan1.schema());
        schema.addAll(childPlan2.schema());
    }

    @Override
    public Scan open() {
        Scan openedChildScan1 = childPlan1.open();
        Scan openedChildScan2 = childPlan1.open();
        return new ProductScan(openedChildScan1, openedChildScan2);
    }

    @Override
    public int blocksAccessed() {
        return childPlan1.blocksAccessed() + (childPlan1.recordsOutput() * childPlan2.blocksAccessed());
    }

    @Override
    public int recordsOutput() {
        return childPlan1.recordsOutput() * childPlan2.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if (childPlan1.schema().hasField(fieldName)) {
            return childPlan1.distinctValues(fieldName);
        } else {
            return childPlan2.distinctValues(fieldName);
            // todo check if field name exists
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
