package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.SelectScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.BooleanConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.StringConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.ConstantExpression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class SelectScanTests {
    @Test
    public void testScanningForEquality() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(150))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_stringField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new StringConstant("str150"))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();

            assertTrue(scan.next());
            assertEquals(150, scan.getInt("t1_intField1"));
            assertEquals(151, scan.getInt("t1_intField2"));
            assertEquals("str150", scan.getString("t1_stringField1"));

            assertFalse(scan.next());
        }
    }

    @Test
    public void testScanningForInequality() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.NOT_EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.NOT_EQUALS,
                new ConstantExpression(new IntConstant(20))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();

            int matchCount = 0;
            while (scan.next()) {
                int val = scan.getInt("t1_intField1");
                assertNotEquals(10, val);
                assertNotEquals(20, val);
                matchCount++;
            }

            assertEquals(248, matchCount);
        }
    }

    @Test
    public void testScanningForGreaterThan() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(240))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField2"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(245))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();

            int matchCount = 0;
            while (scan.next()) {
                assertTrue(scan.getInt("t1_intField1") > 240);
                assertTrue(scan.getInt("t1_intField2") > 245);
                matchCount++;
            }

            assertEquals(5, matchCount);
        }
    }

    @Test
    public void testEmptyScanAllRowsFiltered() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_boolField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(false))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testNoRowsFiltered() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.GREATER_OR_EQUAL,
                new ConstantExpression(new IntConstant(0))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_boolField2"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(false))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();

            int matchCount = 0;
            while (scan.next()) {
                matchCount++;
            }

            assertEquals(250, matchCount);
        }
    }

    @Test
    public void testUpdateOperations() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(100))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_boolField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(true))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();
            assertTrue(scan.next());

            scan.setInt("t1_intField2", 9999);
            assertEquals(9999, scan.getInt("t1_intField2"));

            scan.delete();

            scan.beforeFirst();
            assertFalse(scan.next());

            scan.insert();
            scan.setInt("t1_intField1", 100);
            scan.setInt("t1_intField2", 101);
            scan.setString("t1_stringField1", "str100");
            scan.setString("t1_stringField2", "str101");
            scan.setBoolean("t1_boolField1", true);
            scan.setBoolean("t1_boolField2", false);

            scan.beforeFirst();
            assertTrue(scan.next());
            assertEquals("str100", scan.getString("t1_stringField1"));
        }
    }

    @Test
    public void testScanningForNullWithEqualsOperator() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.EQUALS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testScanningForNullWithIsOperator() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_stringField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(20))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan scan = new SelectScan(tableScan, pred)) {

            scan.beforeFirst();
            assertTrue(scan.next());
            assertEquals(20, scan.getInt("t1_intField1"));
            assertEquals("str20", scan.getString("t1_stringField1"));
            assertFalse(scan.next());
        }
    }
}
