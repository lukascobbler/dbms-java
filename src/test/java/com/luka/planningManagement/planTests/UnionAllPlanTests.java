package com.luka.planningManagement.planTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.ExtendProjectPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.TableReadOnlyPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.UnionAllPlan;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnionAllPlanTests {
    @Test
    public void testUnionAllSumsRecordsAndBlocks() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1PlanA =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());
        TableReadOnlyPlan table1PlanB =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        UnionAllPlan unionPlan = new UnionAllPlan(table1PlanA, table1PlanB);

        int expectedRecords = table1PlanA.recordsOutput() * 2;
        int expectedBlocks = table1PlanA.blocksAccessed() * 2;

        assertEquals(expectedRecords, unionPlan.recordsOutput());
        assertEquals(expectedBlocks, unionPlan.blocksAccessed());

        assertEquals(table1PlanA.distinctValues("t1_intfield1") * 2,
                unionPlan.distinctValues("t1_intfield1"));

        PlanTestUtils.verifyRecordOutputEstimation(testData, unionPlan);
    }

    @Test
    public void testUnionAllDistinctAndNullSummations() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table2Plan =
                new TableReadOnlyPlan(testData.tx(), "table2", testData.db().getMetadataManager());
        TableReadOnlyPlan table3Plan =
                new TableReadOnlyPlan(testData.tx(), "table3", testData.db().getMetadataManager());

        ExtendProjectPlan extendProjectPlan2 = new ExtendProjectPlan(table2Plan, List.of(
                new ProjectionFieldInfo("sameint", new FieldNameExpression("sameint"))
        ));

        ExtendProjectPlan extendProjectPlan3 = new ExtendProjectPlan(table3Plan, List.of(
                new ProjectionFieldInfo("sameint", new FieldNameExpression("sameint"))
        ));

        UnionAllPlan unionPlan = new UnionAllPlan(extendProjectPlan2, extendProjectPlan3);

        assertEquals(500, unionPlan.recordsOutput());
        assertEquals(500, unionPlan.distinctValues("sameint"), 20);

        if (unionPlan.outputSchema().hasField("t2_intfield3")) {
            assertEquals(250, unionPlan.nullValues("t2_intfield3"), 10);
        }

        PlanTestUtils.verifyRecordOutputEstimation(testData, unionPlan);
    }
}
