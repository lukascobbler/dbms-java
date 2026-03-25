package com.luka.planningManagement.planTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;

@Execution(ExecutionMode.SAME_THREAD)
public class ExplainPlanTests {
    // these tests can't fail, they are used as a way to debug
    // plan explanation and / or see the results of it

    @Test
    public void explainPlan1() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        String query = "SELECT t1_intfield1 FROM table1, table2, table3 " +
                "WHERE table1.sameint = table3.sameint AND table2.t2_boolfield3 IS NULL " +
                "UNION ALL " +
                "SELECT t2_intfield3 FROM table2 WHERE t2_intfield1 = 5 " +
                "UNION ALL " +
                "SELECT sameint FROM table3;";

        Plan<Scan> plan = PlanTestUtils.createQueryPlan(testData, query);

        System.out.println(query);
        System.out.println(plan.explainedPlan());
    }

    @Test
    public void explainPlan2() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        String query = "SELECT t1_intfield1, table1.t1_intfield1 FROM table1, table2 WHERE table1.sameint = 5;";

        Plan<Scan> plan = PlanTestUtils.createQueryPlan(testData, query);

        System.out.println(query);
        System.out.println(plan.explainedPlan());
    }

    @Test
    public void explainPlan3() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeFullTables(tmpDir);
        PlanTestUtils.refreshStatistics(testData);

        String query = "SELECT *, 1 + t1_intfield2 + 2 + t2_intfield1 + table3.sameint FROM table1, table2, table3;";

        Plan<Scan> plan = PlanTestUtils.createQueryPlan(testData, query);

        System.out.println(query);
        System.out.println(plan.explainedPlan());
    }
}
