package com.luka.lbdb.planning.planTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.planning.plan.planTypes.readOnly.RenamePlan;
import com.luka.lbdb.planning.plan.planTypes.readOnly.TableReadOnlyPlan;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RenamePlanTests {
    @Test
    public void testRenameChangesSchemaAndDistinctValues() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        TableReadOnlyPlan table1Plan =
                new TableReadOnlyPlan(testData.tx(), "table1", testData.db().getMetadataManager());

        RenamePlan renamePlan = new RenamePlan(table1Plan, Map.of(
                "new_intfield", "t1_intfield1"
        ));

        assertEquals(table1Plan.recordsOutput(), renamePlan.recordsOutput());
        assertEquals(table1Plan.blocksAccessed(), renamePlan.blocksAccessed());

        assertEquals(0, renamePlan.distinctValues("t1_intfield1"));

        assertEquals(table1Plan.distinctValues("t1_intfield1"),
                renamePlan.distinctValues("new_intfield"), 10);

        PlanTestUtils.verifyRecordOutputEstimation(testData, renamePlan);
    }
}
