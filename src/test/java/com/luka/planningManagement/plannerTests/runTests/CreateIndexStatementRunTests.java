package com.luka.planningManagement.plannerTests.runTests;

import com.luka.planningManagement.PlanTestUtils;
import com.luka.simpledb.planningManagement.exceptions.PlanValidationException;
import com.luka.simpledb.simpleDB.settings.SimpleDBSettings;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class CreateIndexStatementRunTests {
    @Test
    public void testSuccessful() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE INDEX named_index ON table1 (sameint);";

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query);
            testData.tx().commit();
        });

        assertTrue(testData
                .db()
                .getMetadataManager()
                .getIndexInfo("table1", testData.tx())
                .containsKey("sameint")
        );
    }

    @Test
    public void testSuccessfulWithType() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE INDEX named_index ON table1 (sameint) TYPE HASH;";

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query);
            testData.tx().commit();
        });

        assertTrue(testData
                .db()
                .getMetadataManager()
                .getIndexInfo("table1", testData.tx())
                .containsKey("sameint")
        );
    }

    @Test
    public void testFailNoTable() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE INDEX named_index ON table5 (sameint);";

        assertThrowsExactly(PlanValidationException.class, () -> {
            testData.tx().commit();
            PlanTestUtils.executeUpdate(testData, query);
        });
    }

    @Test
    public void testFailNoField() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query = "CREATE INDEX named_index ON table1 (sameintaaa);";

        assertThrowsExactly(PlanValidationException.class, () -> {
            testData.tx().commit();
            PlanTestUtils.executeUpdate(testData, query);
        });
    }

    @Test
    public void testAlreadyExistsSameName() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query1 = "CREATE INDEX named_index ON table1 (sameint);";
        String query2 = "CREATE INDEX named_index ON table1 (samestring);";

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query1);
            testData.tx().commit();
        });
        assertTrue(testData
                .db()
                .getMetadataManager()
                .getIndexInfo("table1", testData.tx())
                .containsKey("sameint")
        );
        assertThrowsExactly(PlanValidationException.class, () -> {
            testData.tx().commit();
            PlanTestUtils.executeUpdate(testData, query2);
        });
    }

    @Test
    public void testAlreadyExistsSameField() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir);

        String query1 = "CREATE INDEX named_index1 ON table1 (sameint);";
        String query2 = "CREATE INDEX named_index2 ON table1 (sameint);";

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query1);
            testData.tx().commit();
        });
        assertTrue(testData
                .db()
                .getMetadataManager()
                .getIndexInfo("table1", testData.tx())
                .containsKey("sameint")
        );
        assertThrowsExactly(PlanValidationException.class, () -> {
            testData.tx().commit();
            PlanTestUtils.executeUpdate(testData, query2);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRollbackIndexCreation(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        var testData = PlanTestUtils.initializeThreeEmptyTables(tmpDir, settings);

        String query = "CREATE INDEX named_index ON table1 (sameint);";

        assertDoesNotThrow(() -> {
            PlanTestUtils.executeUpdate(testData, query);
        });
        assertTrue(testData
                .db()
                .getMetadataManager()
                .getIndexInfo("table1", testData.tx())
                .containsKey("sameint")
        );

        testData.tx().rollback();

        assertFalse(testData
                .db()
                .getMetadataManager()
                .getIndexInfo("table1", testData.tx())
                .containsKey("sameint")
        );
    }
}
