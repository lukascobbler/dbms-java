package com.luka.planningManagement.planTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.ExtendProjectPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.RenamePlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.TableReadOnlyPlan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.ConstantExpression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExtendProjectPlanTests {
    @Test
    public void testDistinctValueOfDirectlyProjectedFieldGoesThrough() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        RenamePlan renamePlan = new RenamePlan(table1Plan, Map.of(
                "table1.t1_intfield1", "t1_intfield1"
        ));

        ExtendProjectPlan extendProjectPlan = new ExtendProjectPlan(renamePlan, List.of(
                new ProjectionFieldInfo(
                        "t1_intfield1",
                        new FieldNameExpression("t1_intfield1", "table1")
                ),
                new ProjectionFieldInfo(
                        "table1.t1_intfield1",
                        new FieldNameExpression("t1_intfield1", "table1")
                )
        ));

        assertEquals(250, extendProjectPlan.distinctValues("t1_intfield1"), 10);
        assertEquals(250, extendProjectPlan.distinctValues("table1.t1_intfield1"), 10);

        PlanTestUtils.verifyRecordOutputEstimation(testData, extendProjectPlan);
    }

    @Test
    public void testExtendProjectPassesThroughMetrics() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        ExtendProjectPlan projectPlan = new ExtendProjectPlan(table1Plan, List.of(
                new ProjectionFieldInfo(
                        "t1_intfield1",
                        new FieldNameExpression("t1_intfield1")
                )
        ));

        assertEquals(table1Plan.blocksAccessed(), projectPlan.blocksAccessed());
        assertEquals(table1Plan.recordsOutput(), projectPlan.recordsOutput());

        assertEquals(table1Plan.distinctValues("t1_intfield1"),
                projectPlan.distinctValues("t1_intfield1"), 10);

        assertEquals(0, projectPlan.distinctValues("t1_boolfield1"));

        PlanTestUtils.verifyRecordOutputEstimation(testData, projectPlan);
    }

    @Test
    public void testExtendProjectWithConstantExpression() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        ExtendProjectPlan projectPlan = new ExtendProjectPlan(table1Plan, List.of(
                new ProjectionFieldInfo(
                        "constant_field",
                        new ConstantExpression(new IntConstant(5))
                )
        ));

        assertEquals(1, projectPlan.distinctValues("constant_field"));
        assertEquals(0, projectPlan.nullValues("constant_field"));

        PlanTestUtils.verifyRecordOutputEstimation(testData, projectPlan);
    }
}
