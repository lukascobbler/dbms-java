package com.luka.lbdb.planning.plannerTests.runTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.metadataManagement.exceptions.TableNotFoundException;
import com.luka.lbdb.planning.exceptions.PlanValidationException;
import com.luka.lbdb.planning.plan.Plan;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.records.DatabaseType;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.db.settings.LBDBSettings;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class CreateTableStatementRunTests {
    /// Helper record for verifying field correctness.
    private record FieldInfoAndName(String fieldName, DatabaseType type, int runtimeLength, boolean nullable) { }

    /// @return True if the schema has all the correct information.
    private boolean verifySchema(Schema schema, FieldInfoAndName[] fieldInfos) {
        if (schema.getFields().size() != fieldInfos.length) {
            return false;
        }

        for (FieldInfoAndName info : fieldInfos) {
            if (!(schema.hasField(info.fieldName) &&
                    schema.isNullable(info.fieldName) == info.nullable &&
                    schema.type(info.fieldName) == info.type &&
                    schema.runtimeLength(info.fieldName) == info.runtimeLength)) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testSuccessful() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE TABLE table4 (int1 INT, string1 VARCHAR(5));";
        FieldInfoAndName[] correctFields = new FieldInfoAndName[] {
                new FieldInfoAndName("int1", DatabaseType.INT, DatabaseType.INT.length, true),
                new FieldInfoAndName("string1", DatabaseType.VARCHAR, 5, true)
        };

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query);
            testData.tx().commit();
        });
        assertTrue(verifySchema(
                testData.db().getMetadataManager().getLayout("table4", testData.tx()).getSchema(),
                correctFields
        ));
    }

    @Test
    public void testSuccessfulWithMultipleFields() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE TABLE table4 " +
                "(int1 INT, string1 VARCHAR(5), bool1 BOOLEAN, int2 INT NOT NULL, bool2 BOOLEAN NOT NULL);";
        FieldInfoAndName[] correctFields = new FieldInfoAndName[] {
                new FieldInfoAndName("int1", DatabaseType.INT, DatabaseType.INT.length, true),
                new FieldInfoAndName("string1", DatabaseType.VARCHAR, 5, true),
                new FieldInfoAndName("bool1", DatabaseType.BOOLEAN, DatabaseType.BOOLEAN.length, true),
                new FieldInfoAndName("int2", DatabaseType.INT, DatabaseType.INT.length, false),
                new FieldInfoAndName("bool2", DatabaseType.BOOLEAN, DatabaseType.BOOLEAN.length, false)
        };

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query);
            testData.tx().commit();
        });
        assertTrue(verifySchema(
                testData.db().getMetadataManager().getLayout("table4", testData.tx()).getSchema(),
                correctFields
        ));
    }

    @Test
    public void testFailTableAlreadyExists() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE TABLE table1 (int1 INT, string1 VARCHAR(5));";

        assertThrowsExactly(PlanValidationException.class, () -> {
            PlanTestUtils.executeUpdate(testData, query);
            testData.tx().commit();
        });
        assertThrowsExactly(TableNotFoundException.class, () ->
                testData.db().getMetadataManager().getLayout("table4", testData.tx())
        );
    }

    @Test
    public void testFailTableRecordTooLong() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE TABLE table5 (string VARCHAR(10^5));";

        assertThrowsExactly(PlanValidationException.class, () -> {
            PlanTestUtils.executeUpdate(testData, query);
            testData.tx().commit();
        });
        assertThrowsExactly(TableNotFoundException.class, () ->
                testData.db().getMetadataManager().getLayout("table4", testData.tx())
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRollbackTableCreation(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDBSettings settings = new LBDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir, settings);

        String query = "CREATE TABLE table4 " +
                "(int1 INT, string1 VARCHAR(5), bool1 BOOLEAN, int2 INT NOT NULL, bool2 BOOLEAN NOT NULL);";
        FieldInfoAndName[] correctFields = new FieldInfoAndName[] {
                new FieldInfoAndName("int1", DatabaseType.INT, DatabaseType.INT.length, true),
                new FieldInfoAndName("string1", DatabaseType.VARCHAR, 5, true),
                new FieldInfoAndName("bool1", DatabaseType.BOOLEAN, DatabaseType.BOOLEAN.length, true),
                new FieldInfoAndName("int2", DatabaseType.INT, DatabaseType.INT.length, false),
                new FieldInfoAndName("bool2", DatabaseType.BOOLEAN, DatabaseType.BOOLEAN.length, false)
        };

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query);
        });
        assertTrue(verifySchema(
                testData.db().getMetadataManager().getLayout("table4", testData.tx()).getSchema(),
                correctFields
        ));

        testData.tx().rollback();

        var newTestDataTransaction = PlanTestUtils.newTransaction(testData);

        assertThrowsExactly(TableNotFoundException.class, () -> verifySchema(
                newTestDataTransaction.db().getMetadataManager().getLayout("table4", testData.tx()).getSchema(),
                correctFields
        ));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRollbackTableCreationAndInsertion(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDBSettings settings = new LBDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        var testDataThreeTablesTransaction = PlanTestUtils.initializeThreeFullTables(tmpDir, settings);
        testDataThreeTablesTransaction.tx().commit();

        var testDataTable4Transaction = PlanTestUtils.newTransaction(testDataThreeTablesTransaction);

        String query = "CREATE TABLE table4 " +
                "(int1 INT, string1 VARCHAR(1000), bool1 BOOLEAN, int2 INT NOT NULL, bool2 BOOLEAN NOT NULL);";
        FieldInfoAndName[] correctFields = new FieldInfoAndName[] {
                new FieldInfoAndName("int1", DatabaseType.INT, DatabaseType.INT.length, true),
                new FieldInfoAndName("string1", DatabaseType.VARCHAR, 1000, true),
                new FieldInfoAndName("bool1", DatabaseType.BOOLEAN, DatabaseType.BOOLEAN.length, true),
                new FieldInfoAndName("int2", DatabaseType.INT, DatabaseType.INT.length, false),
                new FieldInfoAndName("bool2", DatabaseType.BOOLEAN, DatabaseType.BOOLEAN.length, false)
        };

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testDataTable4Transaction, query);
        });
        assertTrue(verifySchema(
                testDataTable4Transaction.db().getMetadataManager()
                        .getLayout("table4", testDataTable4Transaction.tx()).getSchema(),
                correctFields
        ));

        String insertQuery = "INSERT INTO table4 (int1, string1, bool1, int2, bool2) VALUES (1, 'a', true, 2, false);";

        for (int i = 0; i < 1000; i++) {
            PlanTestUtils.executeUpdate(testDataTable4Transaction, insertQuery);
        }

        testDataTable4Transaction.tx().rollback();

        var testDataAfterTable4RollBackTransation = PlanTestUtils.newTransaction(testDataTable4Transaction);

        assertThrowsExactly(TableNotFoundException.class, () -> verifySchema(
                testDataAfterTable4RollBackTransation.db().getMetadataManager()
                        .getLayout("table4", testDataAfterTable4RollBackTransation.tx()).getSchema(),
                correctFields
        ));
        assertDoesNotThrow(() ->
                testDataAfterTable4RollBackTransation.db().getMetadataManager()
                        .getLayout("table1", testDataAfterTable4RollBackTransation.tx()));
        assertDoesNotThrow(() ->
                testDataAfterTable4RollBackTransation.db().getMetadataManager()
                        .getLayout("table2", testDataAfterTable4RollBackTransation.tx()));
        assertDoesNotThrow(() ->
                testDataAfterTable4RollBackTransation.db().getMetadataManager()
                        .getLayout("table3", testDataAfterTable4RollBackTransation.tx()));

        String selectQueryTable1 = "SELECT sameint FROM table1;";
        String selectQueryTable2 = "SELECT sameint FROM table2;";
        String selectQueryTable3 = "SELECT sameint FROM table3;";

        Plan<Scan> selectTable1Plan = PlanTestUtils.createQueryPlan(testDataAfterTable4RollBackTransation, selectQueryTable1);
        Plan<Scan> selectTable2Plan = PlanTestUtils.createQueryPlan(testDataAfterTable4RollBackTransation, selectQueryTable2);
        Plan<Scan> selectTable3Plan = PlanTestUtils.createQueryPlan(testDataAfterTable4RollBackTransation, selectQueryTable3);

        assertTrue(selectTable1Plan.outputSchema().hasField("sameint"));
        assertTrue(selectTable2Plan.outputSchema().hasField("sameint"));
        assertTrue(selectTable3Plan.outputSchema().hasField("sameint"));

        try (Scan s1 = selectTable1Plan.open();
             Scan s2 = selectTable2Plan.open();
             Scan s3 = selectTable3Plan.open()) {
            s1.beforeFirst();
            s2.beforeFirst();
            s3.beforeFirst();
            for (int i = 0; i < 250; i++) {
                s1.next();
                s2.next();
                s3.next();
                assertEquals(i + 50, s1.getValue("sameint").asInt());
                assertEquals(i, s2.getValue("sameint").asInt());
                assertEquals(i, s3.getValue("sameint").asInt());
            }
        }
    }
}
