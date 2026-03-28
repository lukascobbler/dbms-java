package com.luka.lbdb.planning.plannerTests.runTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.planning.plan.Plan;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.querying.virtualEntities.constant.*;
import com.luka.lbdb.records.DatabaseType;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.db.settings.LBDBSettings;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertStatementRunTests {
    /// Helper record for checking inserted data.
    private record InsertTestData(String insertQuery, List<Map<String, Constant>> generatedData) { }

    /// @return Correct insert statement with all fields and n random rows.
    private InsertTestData buildInsertQueryWithRandomData(Schema currentSchema, boolean implicitFields, int numTuples) {
        List<String> fieldList = currentSchema.getFields();
        StringJoiner fieldsJoiner = new StringJoiner(", ");
        StringJoiner allTuplesJoiner = new StringJoiner(", ");
        List<Map<String, Constant>> allGeneratedData = new ArrayList<>();

        for (String field : fieldList) {
            fieldsJoiner.add(field);
        }

        for (int t = 0; t < numTuples; t++) {
            StringJoiner tupleValuesJoiner = new StringJoiner(", ", "(", ")");
            Map<String, Constant> currentTupleData = new HashMap<>();

            for (String field : fieldList) {
                DatabaseType type = currentSchema.type(field);
                boolean isNullable = currentSchema.isNullable(field);
                int randomInt = (int) (Math.random() * 500);

                if (isNullable && Math.random() > 0.8) {
                    tupleValuesJoiner.add("NULL");
                    currentTupleData.put(field, NullConstant.INSTANCE);
                    continue;
                }

                switch (type) {
                    case INT -> {
                        tupleValuesJoiner.add(String.valueOf(randomInt));
                        currentTupleData.put(field, new IntConstant(randomInt));
                    }
                    case VARCHAR -> {
                        String strVal = "str" + randomInt;
                        tupleValuesJoiner.add("'" + strVal + "'");
                        currentTupleData.put(field, new StringConstant(strVal));
                    }
                    case BOOLEAN -> {
                        boolean boolVal = randomInt < 250;
                        tupleValuesJoiner.add(String.valueOf(boolVal));
                        currentTupleData.put(field, new BooleanConstant(boolVal));
                    }
                    default -> throw new UnsupportedOperationException("Unknown type: " + type);
                }
            }
            allTuplesJoiner.add(tupleValuesJoiner.toString());
            allGeneratedData.add(currentTupleData);
        }

        String fieldPart = implicitFields ? "" : "(" + fieldsJoiner + ") ";
        String finalQuery = String.format(
                "INSERT INTO table1 %sVALUES %s;",
                fieldPart,
                allTuplesJoiner
        );

        return new InsertTestData(finalQuery, allGeneratedData);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInsertionSuccessful(boolean implicitFields) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeEmptyTables(tmpDir);
        Schema schema = testDataThreeTablesTransaction.layouts().getFirst().getSchema();

        int iterations = 100;
        int tuplesPerIteration = 100;
        int totalExpectedRows = iterations * tuplesPerIteration;

        List<Map<String, Constant>> masterExpectedData = new ArrayList<>();
        int actualTotalRowsAffected = 0;

        for (int i = 0; i < iterations; i++) {
            InsertTestData iterationData = buildInsertQueryWithRandomData(
                    schema,
                    implicitFields,
                    tuplesPerIteration
            );

            actualTotalRowsAffected += PlanTestUtils.executeUpdate(testDataThreeTablesTransaction, iterationData.insertQuery);
            masterExpectedData.addAll(iterationData.generatedData());
        }

        assertEquals(totalExpectedRows, actualTotalRowsAffected);

        String selectQueryTable = "SELECT * FROM table1;";
        Plan<Scan> queryPlan = PlanTestUtils.createQueryPlan(testDataThreeTablesTransaction, selectQueryTable);

        try (Scan s = queryPlan.open()) {
            s.beforeFirst();

            for (int i = 0; i < totalExpectedRows; i++) {
                assertTrue(s.next());

                Map<String, Constant> expectedRow = masterExpectedData.get(i);
                for (var entry : expectedRow.entrySet()) {
                    String fieldName = entry.getKey();
                    Constant expectedValue = entry.getValue();
                    assertEquals(expectedValue, s.getValue(fieldName));
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
        Schema schema = testDataThreeTablesInsertionTransaction.layouts().getFirst().getSchema();

        int iterations = 100;
        int tuplesPerIteration = 100;
        int totalExpectedRows = iterations * tuplesPerIteration;

        List<Map<String, Constant>> masterExpectedData = new ArrayList<>();
        int actualTotalRowsAffected = 0;

        for (int i = 0; i < iterations; i++) {
            InsertTestData iterationData = buildInsertQueryWithRandomData(
                    schema,
                    false,
                    tuplesPerIteration
            );

            actualTotalRowsAffected +=
                    PlanTestUtils.executeUpdate(testDataThreeTablesInsertionTransaction, iterationData.insertQuery);
            masterExpectedData.addAll(iterationData.generatedData());
        }

        assertEquals(totalExpectedRows, actualTotalRowsAffected);

        String selectQueryTableBeforeRollback = "SELECT * FROM table1;";

        Plan<Scan> queryPlanBeforeRollback =
                PlanTestUtils.createQueryPlan(testDataThreeTablesInsertionTransaction, selectQueryTableBeforeRollback);

        try (Scan s = queryPlanBeforeRollback.open()) {
            s.beforeFirst();
            for (int i = 0; i < totalExpectedRows; i++) {
                assertTrue(s.next());
                Map<String, Constant> expectedRow = masterExpectedData.get(i);
                for (var entry : expectedRow.entrySet()) {
                    assertEquals(entry.getValue(), s.getValue(entry.getKey()));
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
            assertFalse(s.next());
        }
    }
}
