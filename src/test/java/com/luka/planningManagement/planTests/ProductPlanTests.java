package com.luka.planningManagement.planTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.ProductPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.TableReadOnlyPlan;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProductPlanTests {
    @Test
    public void testProductMultipliesRecordsAndAdjustsBlocks() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());
        TableReadOnlyPlan table2Plan =
                new TableReadOnlyPlan(testData.tx(), "table2", testData.db().getMetadataManager());

        ProductPlan productPlan = new ProductPlan(table1Plan, table2Plan);

        int expectedRecords = table1Plan.recordsOutput() * table2Plan.recordsOutput();
        int expectedBlocks = table1Plan.blocksAccessed() + (table1Plan.recordsOutput() * table2Plan.blocksAccessed());

        assertEquals(expectedRecords, productPlan.recordsOutput());
        assertEquals(expectedBlocks, productPlan.blocksAccessed());

        int expectedNulls = table1Plan.nullValues("t1_intfield1") * table2Plan.recordsOutput();
        assertEquals(expectedNulls, productPlan.nullValues("t1_intfield1"));

        PlanTestUtils.verifyRecordOutputEstimation(testData, productPlan);
    }

    @Test
    public void testProductPlanMultipliesNullValuesCorrectly() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());
        TableReadOnlyPlan table2Plan =
                new TableReadOnlyPlan(testData.tx(), "table2", testData.db().getMetadataManager());

        assertEquals(250, table1Plan.nullValues("t1_intfield3"), 10);

        ProductPlan productPlan = new ProductPlan(table1Plan, table2Plan);

        int expectedNulls = 250 * 250;
        assertEquals(expectedNulls, productPlan.nullValues("t1_intfield3"), expectedNulls * 0.05);

        PlanTestUtils.verifyRecordOutputEstimation(testData, productPlan);
    }
}
