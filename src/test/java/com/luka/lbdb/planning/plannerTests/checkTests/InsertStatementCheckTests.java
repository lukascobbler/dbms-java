package com.luka.lbdb.planning.plannerTests.checkTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.planning.exceptions.PlanValidationException;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class InsertStatementCheckTests {
    @Test
    public void testSuccessfulInsert() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (" +
                    "t1_intfield1, t1_intfield2, t1_intfield3, " +
                    "t1_stringField1, t1_stringField2, t1_stringField3, " +
                    "t1_boolField1, t1_boolField2, t1_boolField3, " +
                    "sameint, samestring, samebool" +
                ") VALUES (" +
                    "1, 2, 3, " +
                    "'test1', 'test2', 'test3', " +
                    "true, TRUE, False, " +
                    "1234, 'test', false" +
                ");";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testSuccessfulInsertImplicitFieldNames() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 VALUES (" +
                    "1, 2, NULL, 3, " +
                    "'test1', 'test2', NULL, 'test3', " +
                    "true, TRUE, NULL, False" +
                ");";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testSuccessNullOnNullableField() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool" +
            ") VALUES (" +
                "1, 2, NULL, " +
                "'test1', 'test2', NULL, " +
                "true, TRUE, NULL, " +
                "1234, 'test', false" +
            ");";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailNoTable() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table5 (" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool" +
            ") VALUES (" +
                "1, 2, NULL, " +
                "'test1', 'test2', NULL, " +
                "true, TRUE, NULL, " +
                "1234, 'test', false" +
            ");";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailWrongImplicitFieldType() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 VALUES (" +
                "1, 2, 'a', 3, " +
                "'test1', 'test2', NULL, 'test3', " +
                "true, TRUE, NULL, False" +
                ");";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailWrongImplicitNumberLess() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 VALUES (" +
                "1, 2, 'a', 3, " +
                "'test1', 'test2', NULL, 'test3', " +
                "true, TRUE, NULL" +
                ");";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailWrongImplicitNumberMore() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 VALUES (" +
                "1, 2, 'a', 3, " +
                "'test1', 'test2', NULL, 'test3', " +
                "true, TRUE, NULL, TRUE, TRUE" +
                ");";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testMultipleTuplesSuccess() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String tuple = "(1, 2, NULL, 3, 'test1', 'test2', NULL, 'test3', true, TRUE, NULL, False)";

        String query = "INSERT INTO table1 VALUES " + tuple + ", " + tuple + ", " + tuple + ";";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testMultipleTuplesOneWrongType() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String correctTuple = "(1, 2, NULL, 3, 'test1', 'test2', NULL, 'test3', true, TRUE, NULL, False)";
        String incorrectTuple = "(1, 2, 'a', 3, 'test1', 'test2', 123, 'test3', true, TRUE, NULL, False)";

        String query = "INSERT INTO table1 VALUES " + correctTuple + ", " + incorrectTuple + ", " + correctTuple + ";";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testMultipleTuplesExplicitFieldNamesCorrect() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String fieldNames = "(" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool" +
            ")";
        String correctTuple = "(1, 2, NULL, 'test1', 'test2', NULL, true, TRUE, NULL, 3, 'test3', False)";

        String query = "INSERT INTO table1 " + fieldNames + " VALUES " + correctTuple + ", " + correctTuple + ", " + correctTuple + ";";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailIncorrectNumberOfFieldsTooFew() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (t1_intfield1) VALUES (1);";

        assertThrowsExactly(PlanValidationException.class, () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailIncorrectNumberOfFieldsTooMany() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool, sameint" +
            ") VALUES (" +
                "1, 2, NULL, " +
                "'test1', 'test2', NULL, " +
                "true, TRUE, NULL, " +
                "1234, 'test', false, " +
                "123" +
            ");";

        assertThrowsExactly(PlanValidationException.class, () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailOnNoExistingField() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool, fake_field" +
            ") VALUES (" +
                "1, 2, NULL, " +
                "'test1', 'test2', NULL, " +
                "true, TRUE, NULL, " +
                "1234, 'test', false, " +
                "123" +
            ");";

        assertThrowsExactly(PlanValidationException.class, () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailSetNullOnNonNullableField() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool, sameint" +
            ") VALUES (" +
                "1, NULL, 3, " +
                "'test1', 'test2', NULL, " +
                "true, TRUE, NULL, " +
                "1234, 'test', false, " +
                "123" +
            ");";

        assertThrowsExactly(PlanValidationException.class, () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }

    @Test
    public void testFailWrongType() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "INSERT INTO table1 (" +
                "t1_intfield1, t1_intfield2, t1_intfield3, " +
                "t1_stringField1, t1_stringField2, t1_stringField3, " +
                "t1_boolField1, t1_boolField2, t1_boolField3, " +
                "sameint, samestring, samebool, sameint" +
            ") VALUES (" +
                "1, 2, 'a', " +
                "'test1', 'test2', NULL, " +
                "true, TRUE, NULL, " +
                "1234, 'test', false, " +
                "123" +
            ");";

        assertThrowsExactly(PlanValidationException.class, () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
    }
}
