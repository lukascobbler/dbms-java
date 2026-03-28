package com.luka.lbdb.planning.plannerTests.runTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.planning.plan.Plan;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.querying.virtualEntities.constant.BooleanConstant;
import com.luka.lbdb.querying.virtualEntities.constant.Constant;
import com.luka.lbdb.querying.virtualEntities.constant.IntConstant;
import com.luka.lbdb.querying.virtualEntities.constant.StringConstant;
import com.luka.lbdb.records.DatabaseType;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.db.settings.LBDBSettings;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertStatementRunTests {
    /// Helper record for checking inserted data.
    private record InsertTestData(String insertQuery, Map<String, Constant> generatedData) { }

    /// @return Correct insert statement with all fields and random data.
    private InsertTestData buildInsertQueryWithRandomData(Schema currentSchema, boolean implicitFields) {
        StringBuilder fieldsBuilder = new StringBuilder();
        StringBuilder valuesBuilder = new StringBuilder();
        Map<String, Constant> actualData = new HashMap<>();

        for (String field : currentSchema.getFields()) {
            DatabaseType type = currentSchema.type(field);
            boolean isNullable = currentSchema.isNullable(field);

            fieldsBuilder.append(field).append(", ");

            if (isNullable) {
                valuesBuilder.append("NULL").append(", ");
                continue;
            }

            int randomInt = (int) (Math.random() * 500);
            switch (type) {
                case DatabaseType.INT -> {
                    valuesBuilder.append(randomInt).append(", ");
                    actualData.put(field, new IntConstant(randomInt));
                }
                case DatabaseType.VARCHAR -> {
                    valuesBuilder.append("'str").append(randomInt).append("', ");
                    actualData.put(field, new StringConstant("str" + randomInt));
                }
                case DatabaseType.BOOLEAN -> {
                    boolean value = randomInt < 250;
                    valuesBuilder.append(value).append(", ");
                    actualData.put(field, new BooleanConstant(value));
                }
            }
        }

        fieldsBuilder = new StringBuilder(fieldsBuilder.substring(0, fieldsBuilder.length() - 2));
        valuesBuilder = new StringBuilder(valuesBuilder.substring(0, valuesBuilder.length() - 2));

        String finalQuery = String.format(
                "INSERT INTO table1 %s VALUES (%s);",
                implicitFields ? "" : "(" + fieldsBuilder + ")",
                valuesBuilder
        );

        return new InsertTestData(finalQuery, actualData);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInsertionSuccessful(boolean implicitFields) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        InsertTestData insertData = buildInsertQueryWithRandomData(
                testDataThreeTablesTransaction.layouts().getFirst().getSchema(),
                implicitFields
        );

        int totalRowsAffected = 0;
        for (int i = 0; i < 1000; i++) {
            totalRowsAffected += PlanTestUtils.executeUpdate(testDataThreeTablesTransaction, insertData.insertQuery);
        }

        assertEquals(1000, totalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";

        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();
            for (int i = 0; i < 1000; i++) {
                s.next();
                for (var field : insertData.generatedData.entrySet()) {
                    assertEquals(field.getValue(), s.getValue(field.getKey()));
                }
            }

            assertFalse(s.next());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInsertionRollbackNoValues(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDBSettings settings = new LBDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeEmptyTables(tmpDir, settings);
        testDataThreeTablesTransaction.tx().commit();

        var testDataThreeTablesInsertionTransaction = PlanTestUtils.newTransaction(testDataThreeTablesTransaction);

        InsertTestData insertData = buildInsertQueryWithRandomData(
                testDataThreeTablesInsertionTransaction.layouts().getFirst().getSchema(), false);

        int totalRowsAffected = 0;
        for (int i = 0; i < 1000; i++) {
            totalRowsAffected +=
                    PlanTestUtils.executeUpdate(testDataThreeTablesInsertionTransaction, insertData.insertQuery);
        }

        assertEquals(1000, totalRowsAffected);

        String selectQueryTableBeforeRollback = "SELECT * FROM table1;";

        Plan<Scan> queryPlanBeforeRollback =
                PlanTestUtils.createQueryPlan(testDataThreeTablesInsertionTransaction, selectQueryTableBeforeRollback);

        try (Scan s = queryPlanBeforeRollback.open()) {
            s.beforeFirst();
            for (int i = 0; i < 1000; i++) {
                s.next();
                for (var field : insertData.generatedData.entrySet()) {
                    assertEquals(field.getValue(), s.getValue(field.getKey()));
                }
            }

            assertFalse(s.next());
        }

        testDataThreeTablesInsertionTransaction.tx().rollback();

        var testDataAfterRollbackTransaction = PlanTestUtils.newTransaction(testDataThreeTablesInsertionTransaction);

        String selectQueryTableAfterRollback = "SELECT * FROM table1;";

        Plan<Scan> queryPlanAfterRollback =
                PlanTestUtils.createQueryPlan(testDataAfterRollbackTransaction, selectQueryTableAfterRollback);

        try (Scan s = queryPlanAfterRollback.open()) {
            s.beforeFirst();
            for (int i = 0; i < 1000; i++) {
                assertFalse(s.next());
            }
        }
    }
}
