package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.ExplainData;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.UnionAllScan;
import com.luka.simpledb.recordManagement.DatabaseType;
import com.luka.simpledb.recordManagement.schema.Schema;

import java.util.List;

/// Plan for the "union all" relational algebra operator.
/// Does not remove duplicate rows. Read-only operations only.
public class UnionAllPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan1, childPlan2;
    private final Schema outputSchema = new Schema();

    /// Requires two subplans that will create a bigger table. Assumes that all fields at
    /// same positions have the same type and name and that there is the same number of
    /// fields in both subplans. Usually, the type and number of fields requirements are
    /// satisfied in the checker, but names almost always need to be renamed. Use [RenamePlan]
    /// for that.
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
        return new UnionAllScan(childPlan1.open(), childPlan2.open());
    }

    /// Since the subplans are processed one after the other, the total
    /// number of block accesses will be the sum of both subplans' block
    /// accesses.
    ///
    /// @return The sum of subplans' block accesses.
    @Override
    public int blocksAccessed() {
        return childPlan1.blocksAccessed() + childPlan2.blocksAccessed();
    }

    /// Since the subplans are processed one after the other, the total
    /// number of records in the output scan will be the sum of both
    /// subplans' records.
    ///
    /// @return The sum of subplans' records outputs.
    @Override
    public int recordsOutput() {
        return childPlan1.recordsOutput() + childPlan2.recordsOutput();
    }

    /// Assumes that both subplans have no overlapping distinct values, which
    /// is almost always false but is a good estimation regardless.
    ///
    /// @return The number of distinct values for a field in both subplans.
    @Override
    public int distinctValues(String fieldName) {
        return childPlan1.distinctValues(fieldName) + childPlan2.distinctValues(fieldName);
    }

    /// Null value counts can simply be summed.
    ///
    /// @return The number of null values for a field in both subplans.
    @Override
    public int nullValues(String fieldName) {
        return childPlan1.nullValues(fieldName) + childPlan2.nullValues(fieldName);
    }

    /// The final schema is just the schema of the left (first) subplan,
    /// since the resulting plan assumes that both subplans have the same
    /// schema.
    ///
    /// @return The schema of the left subplan.
    @Override
    public Schema outputSchema() {
        return outputSchema;
    }

    @Override
    public void explainPlan(List<ExplainData> previousExplanations, int ident) {
        previousExplanations.add(new ExplainData(
                ident,
                this.getClass().getSimpleName(),
                blocksAccessed(),
                recordsOutput(),
                ""
        ));

        childPlan1.explainPlan(previousExplanations, ident + 1);
        childPlan2.explainPlan(previousExplanations, ident + 1);
    }
}
