package com.luka.planningManagement.plannerTests.runTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.simpleDB.settings.SimpleDBSettings;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpdateStatementRunTests {
    @Test
    public void testUpdateSuccessfulConstantsNoPredicate() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeFullTables(tmpDir);

        String updateQuery = "UPDATE table1 SET t1_intfield1 = 5, t1_intfield3 = 0;";

        int totalRowsAffected = PlanTestUtils.executeUpdate(testDataThreeTablesTransaction, updateQuery);

        assertEquals(250, totalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";

        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            for (int i = 0; i < 250; i++) {
                assertTrue(s.next());
                assertEquals(5, s.getValue("t1_intfield1").asInt());
                assertEquals(0, s.getValue("t1_intfield3").asInt());
            }
        }
    }

    @Test
    public void testUpdateSuccessfulFieldnamesNoPredicate() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeFullTables(tmpDir);

        String updateQuery = "UPDATE table1 " +
                "SET t1_intfield1 = t1_intfield1 + 100, t1_intfield3 = t1_intfield2;";

        int totalRowsAffected = PlanTestUtils.executeUpdate(testDataThreeTablesTransaction, updateQuery);

        assertEquals(250, totalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";

        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            for (int i = 0; i < 250; i++) {
                assertTrue(s.next());
                assertEquals(i + 100, s.getValue("t1_intfield1").asInt());
                assertEquals(s.getValue("t1_intfield2").asInt(), s.getValue("t1_intfield3").asInt());
            }
        }
    }

    @Test
    public void testUpdateSuccessfulRepeatedUpdate() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeFullTables(tmpDir);

        String updateQuery = "UPDATE table1 SET t1_intfield1 = 5, t1_intfield1 = 0;";

        int totalRowsAffected = PlanTestUtils.executeUpdate(testDataThreeTablesTransaction, updateQuery);

        assertEquals(250, totalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";

        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            for (int i = 0; i < 250; i++) {
                assertTrue(s.next());
                assertEquals(0, s.getValue("t1_intfield1").asInt());
            }
        }
    }

    @Test
    public void testUpdateSuccessfulFieldnamesPredicate() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeFullTables(tmpDir);

        String updateQuery = "UPDATE table1 " +
                "SET t1_intfield1 = t1_intfield1 + 100, t1_intfield3 = t1_intfield2 " +
                "WHERE t1_intfield1 <= 50 AND t1_intfield2 < 100;";

        int totalRowsAffected = PlanTestUtils.executeUpdate(testDataThreeTablesTransaction, updateQuery);

        assertEquals(51, totalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";

        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            int count = 0;
            while (s.next()) {
                int originalI = count;
                int currentInt1 = s.getValue("t1_intfield1").asInt();
                int currentInt2 = s.getValue("t1_intfield2").asInt();

                if (originalI <= 50) {
                    int currentInt3 = s.getValue("t1_intfield3").asInt();

                    assertEquals(originalI + 100, currentInt1);
                    assertEquals(currentInt2, currentInt3);
                } else {
                    assertEquals(originalI, currentInt1);
                    assertEquals(NullConstant.INSTANCE, s.getValue("t1_intfield3"));
                }
                count++;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUpdateRollback(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeFullTables(tmpDir, settings);
        testDataThreeTablesTransaction.tx().commit();

        var testDataThreeTablesUpdateTransaction = PlanTestUtils.newTransaction(testDataThreeTablesTransaction);

        String updateQuery = "UPDATE table1 SET t1_intfield1 = 5, t1_intfield3 = 0;";

        int totalRowsAffected =
                PlanTestUtils.executeUpdate(testDataThreeTablesUpdateTransaction, updateQuery);

        assertEquals(250, totalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";

        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesUpdateTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            for (int i = 0; i < 250; i++) {
                assertTrue(s.next());
            }
        }

        testDataThreeTablesUpdateTransaction.tx().rollback();

        var testDataThreeTablesAfterRollbackTransaction =
                PlanTestUtils.newTransaction(testDataThreeTablesUpdateTransaction);

        Plan<Scan> queryPlanAfterRollback =
                PlanTestUtils.createQueryPlan(testDataThreeTablesUpdateTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            for (int i = 0; i < 250; i++) {
                assertTrue(s.next());
                assertEquals(i, s.getValue("t1_intfield1").asInt());
                assertEquals(i + 1, s.getValue("t1_intfield2").asInt());
                assertEquals(NullConstant.INSTANCE, s.getValue("t1_intfield3"));
            }
            assertFalse(s.next());
        }
    }
}
