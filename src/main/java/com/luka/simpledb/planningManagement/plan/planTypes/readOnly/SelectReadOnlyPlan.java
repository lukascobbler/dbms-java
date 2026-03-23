package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.SelectReadOnlyScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.schema.Schema;

import java.util.List;

/// Plan for the "select" relational algebra operator. Read-only operations only.
public class SelectReadOnlyPlan implements Plan<Scan> {
    private final Plan<Scan> childPlan;
    private final Predicate predicate;

    /// Requires the plan that will be filtered, and the predicate that will filter it.
    public SelectReadOnlyPlan(Plan<Scan> childPlan, Predicate predicate) {
        this.childPlan = childPlan;
        this.predicate = predicate;
    }

    @Override
    public Scan open() {
        return new SelectReadOnlyScan(childPlan.open(), predicate);
    }

    /// Selections (filtering) need to check if the filter matches for every
    /// record in the subplan, so the number of blocks will be the same as the
    /// subplan.
    ///
    /// @return The number of blocks of the subplan.
    @Override
    public int blocksAccessed() {
        return childPlan.blocksAccessed();
    }

    /// The number of records after the selection (filtering) depends on the
    /// reduction factor of the predicate, which internally depends on the product
    /// of the reduction factor of each term.
    ///
    /// @return The number of records after the selection, reduced from the subplan
    /// by the predicate's reduction factor.
    @Override
    public int recordsOutput() {
        return (int) Math.ceil(childPlan.recordsOutput() / predicate.reductionFactor(childPlan));
    }

    /// If any of the terms of a predicate equate the requested field to a constant (all
    /// terms are joined by `AND`), the number of distinct values for that field will
    /// be exactly 1 since all other rows where that isn't the case failed the filter.
    /// If multiple different constants are found, no rows match the predicate so there
    /// will be 0 distinct values.
    ///
    /// Else, if the requested field doesn't match any constant, but matches a list of
    /// other fields, the number of distinct values for the requested field will be the
    /// minimum of all distinct values of the fields that it matched.
    ///
    /// @return The number of distinct values for a field after the selection (filtering).
    @Override
    public int distinctValues(String fieldName) {
        List<Constant> constants = predicate.allEquatedConstants(fieldName)
                .distinct()
                .toList();

        if (constants.size() > 1) {
            return 0;
        } else if (constants.size() == 1) {
            return 1;
        }

        int minDistinct = childPlan.distinctValues(fieldName);

        int mostRestrictiveEquated = predicate.allEquatedFields(fieldName)
                .mapToInt(childPlan::distinctValues)
                .min()
                .orElse(minDistinct);

        return Math.min(minDistinct, mostRestrictiveEquated);
    }

    /// If any of the terms of a predicate equate the requested field to a NULL (all
    /// terms are joined by `AND`), the number of null values for that field will
    /// be exactly the record output of this plan since all other rows where that isn't
    /// the case failed the filter.
    ///
    /// Else, if at least one term explicitly excludes nulls by doing operations like
    /// F = NULL or F > NULL, 0 rows will be NULL for the requested field.
    ///
    /// Finally, the number of NULL values for predicates where the NULL constant isn't
    /// mentioned is assumed to be proportional to the number of NULL values in the child
    /// plan.
    ///
    /// @return The number of distinct values for a field after the selection (filtering).
    @Override
    public int nullValues(String fieldName) {
        if (predicate.equatesWithNull(fieldName)) {
            return recordsOutput();
        }

        if (predicate.excludesNulls(fieldName)) {
            return 0;
        }

        return (int) Math.ceil(childPlan.nullValues(fieldName) / predicate.reductionFactor(childPlan));
    }

    /// The output schema after selection is the same as the subplan's
    /// output schema because selection doesn't transform data in any way,
    /// it only removes rows.
    ///
    /// @return The subplan's output schema.
    @Override
    public Schema outputSchema() {
        return childPlan.outputSchema();
    }
}
