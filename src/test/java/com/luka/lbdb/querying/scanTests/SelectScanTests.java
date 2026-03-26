package com.luka.lbdb.querying.scanTests;

import com.luka.lbdb.querying.QueryTestUtils;
import com.luka.lbdb.querying.scanDefinitions.UpdateScan;
import com.luka.lbdb.querying.scanTypes.update.SelectScan;
import com.luka.lbdb.querying.virtualEntities.Predicate;
import com.luka.lbdb.querying.virtualEntities.constant.BooleanConstant;
import com.luka.lbdb.querying.virtualEntities.constant.IntConstant;
import com.luka.lbdb.querying.virtualEntities.constant.NullConstant;
import com.luka.lbdb.querying.virtualEntities.constant.StringConstant;
import com.luka.lbdb.querying.virtualEntities.expression.ConstantExpression;
import com.luka.lbdb.querying.virtualEntities.expression.FieldNameExpression;
import com.luka.lbdb.querying.virtualEntities.term.Term;
import com.luka.lbdb.querying.virtualEntities.term.TermOperator;
import com.luka.lbdb.querying.scanTypes.update.TableScan;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class SelectScanTests {
    @Test
    public void testScanningForEquality() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
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
            assertEquals(150, scan.getValue("t1_intField1").asInt());
            assertEquals(151, scan.getValue("t1_intField2").asInt());
            assertEquals("str150", scan.getValue("t1_stringField1").asString());

            assertFalse(scan.next());
        }
    }

    @Test
    public void testScanningForInequality() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
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
                int val = scan.getValue("t1_intField1").asInt();
                assertNotEquals(10, val);
                assertNotEquals(20, val);
                matchCount++;
            }

            assertEquals(248, matchCount);
        }
    }

    @Test
    public void testScanningForGreaterThan() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
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
                assertTrue(scan.getValue("t1_intField1").asInt() > 240);
                assertTrue(scan.getValue("t1_intField2").asInt() > 245);
                matchCount++;
            }

            assertEquals(5, matchCount);
        }
    }

    @Test
    public void testEmptyScanAllRowsFiltered() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
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
        Path tmpDir = TestUtils.setUpTempDirectory();
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
        Path tmpDir = TestUtils.setUpTempDirectory();
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

            scan.setValue("t1_intField2", new IntConstant(9999));
            assertEquals(9999, scan.getValue("t1_intField2").asInt());

            scan.delete();

            scan.beforeFirst();
            assertFalse(scan.next());

            scan.insert();
            scan.setValue("t1_intField1", new IntConstant(100));
            scan.setValue("t1_intField2", new IntConstant(101));
            scan.setValue("t1_stringField1", new StringConstant("str100"));
            scan.setValue("t1_stringField2", new StringConstant("str101"));
            scan.setValue("t1_boolField1", new BooleanConstant(true));
            scan.setValue("t1_boolField2", new BooleanConstant(false));

            scan.beforeFirst();
            assertTrue(scan.next());
            assertEquals("str100", scan.getValue("t1_stringField1").asString());
        }
    }

    @Test
    public void testScanningForNullWithEqualsOperator() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
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
        Path tmpDir = TestUtils.setUpTempDirectory();
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
            assertEquals(20, scan.getValue("t1_intField1").asInt());
            assertEquals("str20", scan.getValue("t1_stringField1").asString());
            assertFalse(scan.next());
        }
    }
}
