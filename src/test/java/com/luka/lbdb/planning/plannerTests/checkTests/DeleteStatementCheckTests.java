package com.luka.lbdb.planning.plannerTests.checkTests;

import com.luka.lbdb.planning.PlanTestUtils;
import com.luka.lbdb.planning.exceptions.PlanValidationException;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class DeleteStatementCheckTests {
    @Test
    public void testSuccessfulNoPredicate() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "DELETE FROM table1;";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeDeleteValidated"));
    }

    @Test
    public void testSuccessfulPredicate() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "DELETE FROM table1 WHERE sameint = 123;";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeDeleteValidated"));
    }

    @Test
    public void testSuccessfulNullPredicate() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "DELETE FROM table1 WHERE t1_intfield3 IS NULL;";

        assertDoesNotThrow(
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeDeleteValidated"));
    }

    @Test
    public void testFailNoTable() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "DELETE FROM table5;";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeDeleteValidated"));
    }

    @Test
    public void testFailNoFieldInPredicate() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "DELETE FROM table1 WHERE abc = 14;";

        assertThrowsExactly(PlanValidationException.class,
                () -> PlanTestUtils.checkUpdateStatement(testData, query, "executeDeleteValidated"));
    }
}
