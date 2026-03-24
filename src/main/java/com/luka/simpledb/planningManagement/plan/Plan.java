package com.luka.simpledb.planningManagement.plan;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/// Plans objects have two purposes:
/// - doing different estimations on data in order to produce cost functions'
/// results for the query subtree
/// - tracking column transformations for different operations and having the
/// most up-to-date output schema
///
/// Each plan can be converted to its resulting scan when it is time to use the
/// data.
///
/// Be aware that cost functions do not produce exact results, only estimations.
/// Three cost functions exist:
/// - blocks accessed: the number of blocks that the resulting scan
/// needs to access to produce all records
/// - records output: the number of records that will be in the resulting
/// scan
/// - distinct values for a field: the number of distinct values of a field
/// in the resulting scan
/// - null values for a field: the number of null values of a field in the
/// resulting scan
///
/// Generally, the goal of an optimal plan is to reduce block accesses as much
/// as possible, but building plans based only on that cost function is not
/// always possible, so using the record output cost function is okay as well.
/// However, building an optimal plan isn't the plan object's job, see
/// [Planner types][com.luka.simpledb.planningManagement.planner.plannerTypes].
///
/// Plans are generic over any `T` that implements scan, which in turn
/// allows type safe plans for both scans that update data, and scans that
/// only read the data.
public interface Plan<T extends Scan> {
    /// Creates the downward query subtree starting from this plan.
    /// Generic over T, so that both updating and read-only scans can
    /// define the same function.
    ///
    /// @return The topmost scan object containing the root of the
    /// query subtree.
    T open();

    /// The blocks accessed cost function. Determines the number of blocks
    /// that the resulting scan will need to access to produce all records.
    ///
    /// @return Estimated number of block accesses.
    int blocksAccessed();

    /// The records output cost function. Determines the number of records
    /// that will be in the resulting scan.
    ///
    /// @return Estimated number of records in the resulting scan.
    int recordsOutput();

    /// The distinct values cost function. Determines the number of distinct
    /// values for a given field name in the resulting scan. If the field does
    /// not exist in the output schema, this function should return 0.
    ///
    /// @return The estimated number of distinct values for a given field.
    int distinctValues(String fieldName);

    /// The null values cost function. Determines the number of null
    /// values for a given field name in the resulting scan. If the field does
    /// not exist in the output schema, this function should return 0.
    ///
    /// @return The estimated number of null values for a given field.
    int nullValues(String fieldName);

    /// Since plans can transform data, those transformations need to be tracked so that
    /// plan consumers can know what the output data represents. All data is part of some
    /// column, and the system's already existing mechanism for storing column metadata is
    /// the `Schema` object which can be perfectly reused here.
    ///
    /// @return The schema of this plan's output data. If this plan doesn't transform the
    /// data, it will probably return the subplan's output schema.
    Schema outputSchema();

    /// @return The string representing a table with all subplan explanations.
    default String explainedPlan() {
        List<ExplainData> explainData = new ArrayList<>();

        this.explainPlan(explainData, 0);

        return ExplainData.explainAllPlans(explainData);
    }

    /// Populates the plan recursively with correct indentations.
    void explainPlan(List<ExplainData> previousExplanations, int ident);
}
