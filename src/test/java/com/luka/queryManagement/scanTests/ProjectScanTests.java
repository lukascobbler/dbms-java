package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.ProjectScan;
import com.luka.simpledb.queryManagement.scanTypes.update.SelectScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.BooleanConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.ConstantExpression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ProjectScanTests {
    @Test
    public void testProjectingSubsetOfFields() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);
        List<String> fields = Arrays.asList("t1_intField1", "t1_stringField1");

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ProjectScan scan = new ProjectScan(tableScan, fields)) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertTrue(scan.hasField("t1_intField1"));
            assertTrue(scan.hasField("t1_stringField1"));
            assertFalse(scan.hasField("t1_intField2"));

            assertEquals(0, scan.getInt("t1_intField1"));
            assertEquals("str0", scan.getString("t1_stringField1"));

            assertThrows(FieldNotFoundInScanException.class, () -> scan.getInt("t1_intField2"));
        }
    }

    @Test
    public void testProjectingWithPredicateFilter() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(200))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_boolField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(true))
        );
        Predicate pred = new Predicate(t1, t2);

        List<String> fields = Arrays.asList("t1_intField1", "t1_stringField1");

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan selectScan = new SelectScan(tableScan, pred);
             ProjectScan scan = new ProjectScan(selectScan, fields)) {

            assertFalse(scan.hasField("t1_intField2"));

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                assertTrue(scan.getInt("t1_intField1") > 200);
                count++;
            }
            assertEquals(49, count);
        }
    }

    @Test
    public void testUpdatingProjectedFields() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);
        List<String> fields = Arrays.asList("t1_intField1", "t1_stringField1");

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ProjectScan scan = new ProjectScan(tableScan, fields)) {

            scan.beforeFirst();
            assertTrue(scan.next());

            scan.setInt("t1_intField1", 888);
            assertEquals(888, scan.getInt("t1_intField1"));

            assertThrows(FieldNotFoundInScanException.class, () -> scan.setInt("t1_intField2", 999));
        }
    }

    @Test
    public void testAccessingNullValuesThroughProjection() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Predicate pred = new Predicate(t1, t2);

        List<String> fields = Arrays.asList("t1_intField1", "t1_intField3");

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan selectScan = new SelectScan(tableScan, pred);
             ProjectScan scan = new ProjectScan(selectScan, fields)) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertEquals(10, scan.getInt("t1_intField1"));
            assertEquals(NullConstant.INSTANCE, scan.getValue("t1_intField3"));
        }
    }

    @Test
    public void testProjectScanNavigation() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);
        List<String> fields = List.of("t1_intField1");

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ProjectScan scan = new ProjectScan(tableScan, fields)) {

            scan.afterLast();
            assertTrue(scan.previous());
            assertEquals(249, scan.getInt("t1_intField1"));

            scan.beforeFirst();
            assertTrue(scan.next());
            assertEquals(0, scan.getInt("t1_intField1"));
        }
    }
}
