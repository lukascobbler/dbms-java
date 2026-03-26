package com.luka.lbdb.planning.planTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.planning.plan.planTypes.readOnly.SelectReadOnlyPlan;
import com.luka.lbdb.planning.plan.planTypes.readOnly.TableReadOnlyPlan;
import com.luka.lbdb.querying.virtualEntities.Predicate;
import com.luka.lbdb.querying.virtualEntities.constant.BooleanConstant;
import com.luka.lbdb.querying.virtualEntities.constant.IntConstant;
import com.luka.lbdb.querying.virtualEntities.expression.ConstantExpression;
import com.luka.lbdb.querying.virtualEntities.expression.FieldNameExpression;
import com.luka.lbdb.querying.virtualEntities.term.Term;
import com.luka.lbdb.querying.virtualEntities.term.TermOperator;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SelectPlanTests {
    @Test
    public void testSelectReducesRecordsOutput() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        Predicate filterPredicate = new Predicate(new Term(
                new FieldNameExpression("t1_intfield1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(5))
        ));

        SelectReadOnlyPlan selectPlan = new SelectReadOnlyPlan(table1Plan, filterPredicate);

        assertEquals(table1Plan.blocksAccessed(), selectPlan.blocksAccessed());
        assertTrue(selectPlan.recordsOutput() <= table1Plan.recordsOutput());

        PlanTestUtils.verifyRecordOutputEstimation(testData, selectPlan);
    }

    @Test
    public void testSelectWithEqualityReducesDistinctValuesToOne() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        Predicate filterPredicate = new Predicate(new Term(
                new FieldNameExpression("t1_intfield1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        ));

        SelectReadOnlyPlan selectPlan = new SelectReadOnlyPlan(table1Plan, filterPredicate);

        assertEquals(1, selectPlan.distinctValues("t1_intfield1"));

        PlanTestUtils.verifyRecordOutputEstimation(testData, selectPlan);
    }

    @Test
    public void testDistinctValuesOnSkewedBooleanDistributions() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        assertEquals(1, table1Plan.distinctValues("t1_boolfield1"));
        assertEquals(2, table1Plan.distinctValues("samebool"));

        Predicate falsePredicate = new Predicate(new Term(
                new FieldNameExpression("t1_boolfield1"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(false)))
        );
        SelectReadOnlyPlan selectPlan = new SelectReadOnlyPlan(table1Plan, falsePredicate);

        assertEquals(1, selectPlan.distinctValues("t1_boolfield1"));

        // This test will not be correct because boolean values' distinct value count can be
        // at most 2, so the reduction factor calculator can't estimate properly because of
        // such a low cardinality
        // PlanTestUtils.verifyRecordOutputEstimation(testData, selectPlan);
    }

    @Test
    public void testSelectPlanExcludesNullValuesWhenEquatingToConstant() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        assertEquals(250, table1Plan.nullValues("t1_intfield3"));

        Predicate filterPredicate = new Predicate(new Term(
                new FieldNameExpression("t1_intfield3"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(5)))
        );
        SelectReadOnlyPlan selectPlan = new SelectReadOnlyPlan(table1Plan, filterPredicate);

        assertEquals(0, selectPlan.nullValues("t1_intfield3"));

        PlanTestUtils.verifyRecordOutputEstimation(testData, selectPlan);
    }
}
