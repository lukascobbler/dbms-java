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

        assertDoesNotThrow(() -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
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

        assertDoesNotThrow(() -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
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

        assertThrowsExactly(PlanValidationException.class, () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeInsertValidated"));
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
