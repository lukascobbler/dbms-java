package com.luka.planningManagement.planTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.*;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CombiningPlanTests {
    @Test
    public void testSelectOverProductOverRename() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());
        TableReadOnlyPlan table2Plan =
                new TableReadOnlyPlan(testData.tx(), "table2", testData.db().getMetadataManager());

        RenamePlan renamePlan = new RenamePlan(table2Plan, Map.of(
                "matching_id", "t2_intfield1"
        ));

        ProductPlan productPlan = new ProductPlan(table1Plan, renamePlan);

        Predicate joinPredicate = new Predicate(new Term(
                new FieldNameExpression("t1_intfield1"),
                TermOperator.EQUALS,
                new FieldNameExpression("matching_id"))
        );
        SelectReadOnlyPlan joinPlan = new SelectReadOnlyPlan(productPlan, joinPredicate);

        int minDistinctExpected = Math.min(
                table1Plan.distinctValues("t1_intfield1"),
                table2Plan.distinctValues("t2_intfield1")
        );

        assertTrue(joinPlan.distinctValues("t1_intfield1") <= minDistinctExpected);

        PlanTestUtils.verifyRecordOutputEstimation(testData, joinPlan);
    }

    @Test
    public void testExtendProjectOverUnionAll() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1PlanA =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());
        TableReadOnlyPlan table1PlanB =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        UnionAllPlan unionPlan = new UnionAllPlan(table1PlanA, table1PlanB);

        ExtendProjectPlan projectPlan = new ExtendProjectPlan(unionPlan, List.of(
                new ProjectionFieldInfo(
                        "t1_intfield1",
                        new FieldNameExpression("t1_intfield1")
                )
        ));

        assertEquals(table1PlanA.recordsOutput() * 2, projectPlan.recordsOutput());
        assertEquals(table1PlanA.blocksAccessed() * 2, projectPlan.blocksAccessed());

        PlanTestUtils.verifyRecordOutputEstimation(testData, projectPlan);
    }

    @Test
    public void testJoinEstimationWithPartialOverlap() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());
        TableReadOnlyPlan table2Plan =
                new TableReadOnlyPlan(testData.tx(), "table2", testData.db().getMetadataManager());

        RenamePlan renamePlan1 = new RenamePlan(table1Plan, Map.of(
                "table1.sameint", "sameint"
        ));

        RenamePlan renamePlan2 = new RenamePlan(table2Plan, Map.of(
                "table2.sameint", "sameint"
        ));

        ProductPlan productPlan = new ProductPlan(renamePlan1, renamePlan2);

        Predicate joinPredicate = new Predicate(new Term(
                new FieldNameExpression("sameint", "table1"),
                TermOperator.EQUALS,
                new FieldNameExpression("sameint", "table2"))
        );
        SelectReadOnlyPlan joinPlan = new SelectReadOnlyPlan(productPlan, joinPredicate);

        PlanTestUtils.verifyRecordOutputEstimation(testData, joinPlan);

        assertEquals(250, joinPlan.distinctValues("table1.sameint"), 15);
    }
}
