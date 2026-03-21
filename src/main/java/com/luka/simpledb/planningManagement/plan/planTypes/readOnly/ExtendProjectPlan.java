package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.ExtendProjectScan;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.recordManagement.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtendProjectPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final Map<String, Expression> projectionFieldInfos;
    private final Schema inputSchema;
    private final Schema outputSchema = new Schema();

    public ExtendProjectPlan(Plan<Scan> childPlan, List<ProjectionFieldInfo> projectionFieldInfoList) {
        this.childPlan = childPlan;
        inputSchema = childPlan.outputSchema();
        projectionFieldInfos = new HashMap<>();

        for (ProjectionFieldInfo projectionFieldInfo : projectionFieldInfoList) {
            int type = projectionFieldInfo.expression().type(childPlan.outputSchema());
            int length = projectionFieldInfo.expression().length(childPlan.outputSchema());
            boolean isNullable = false; // extended fields aren't nullable

            outputSchema.addField(projectionFieldInfo.name(), type, length, isNullable);
            projectionFieldInfos.put(projectionFieldInfo.name(), projectionFieldInfo.expression());
        }
    }

    @Override
    public Scan open() {
        Scan childScan = childPlan.open();
        return new ExtendProjectScan(childScan, projectionFieldInfos);
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
        // todo more sophisticated algorithm for distinct values
        //  t1_intfield1 should be the same as table1.intfield1 as well as t1_intfield1 as A
        //  only the fields that this plan truly added should have records output as their distinct values
        //  even all operations that involve one field should have the child plan's distinct values
        if (inputSchema.hasField(fieldName)) {
            return childPlan.distinctValues(fieldName);
        }
        return childPlan.recordsOutput(); // todo comment
    }

    @Override
    public Schema outputSchema() {
        return outputSchema;
    }
}
