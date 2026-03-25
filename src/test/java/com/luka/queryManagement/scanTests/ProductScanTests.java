package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.ProductScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.SelectReadOnlyScan;
import com.luka.simpledb.queryManagement.scanTypes.update.SelectScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ProductScanTests {
    @Test
    public void testProductTotalRecordCount() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ProductScan scan = new ProductScan(s1, s2)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                count++;
            }
            assertEquals(62500, count);
        }
    }

    @Test
    public void testProductFieldRetrievalFromDisjointSchemas() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ProductScan scan = new ProductScan(s1, s2)) {

            for (String fieldName : testData.layouts().get(0).getSchema().getFields()) {
                assertTrue(scan.hasField(fieldName));
            }

            for (String fieldName : testData.layouts().get(1).getSchema().getFields()) {
                assertTrue(scan.hasField(fieldName));
            }

            scan.beforeFirst();
            assertTrue(scan.next());

            assertTrue(scan.hasField("t1_intField1"));
            assertEquals(0, scan.getValue("t1_intField1").asInt());

            assertTrue(scan.hasField("t2_intField1"));
            assertEquals(0, scan.getValue("t2_intField1").asInt());
        }
    }

    @Test
    public void testProductWithMidComplexityPredicate() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(5))
        );
        Term t2 = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ProductScan product = new ProductScan(s1, s2);
             SelectReadOnlyScan scan = new SelectReadOnlyScan(product, pred)) {

            scan.beforeFirst();
            assertTrue(scan.next());
            assertEquals(5, scan.getValue("t1_intField1").asInt());
            assertEquals(10, scan.getValue("t2_intField1").asInt());
            assertFalse(scan.next());
        }
    }

    @Test
    public void testProductWithFirstEmptyChild() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred = new Predicate(emptyTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS1 = new SelectScan(s1, emptyPred);
             ProductScan scan = new ProductScan(emptyS1, s2)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testProductWithSecondEmptyChild() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred = new Predicate(emptyTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS2 = new SelectScan(s2, emptyPred);
             ProductScan scan = new ProductScan(s1, emptyS2)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testProductWithBothEmptyChildren() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred1 = new Predicate(emptyTerm1);

        Term emptyTerm2 = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred2 = new Predicate(emptyTerm2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS1 = new SelectScan(s1, emptyPred1);
             SelectScan emptyS2 = new SelectScan(s2, emptyPred2);
             ProductScan scan = new ProductScan(emptyS1, emptyS2)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }
    @Test
    public void testProductBackwardsWithFirstEmptyChild() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred = new Predicate(emptyTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS1 = new SelectScan(s1, emptyPred);
             ProductScan scan = new ProductScan(emptyS1, s2)) {

            scan.afterLast();
            assertFalse(scan.previous());
        }
    }

    @Test
    public void testProductBackwardsWithSecondEmptyChild() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred = new Predicate(emptyTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS2 = new SelectScan(s2, emptyPred);
             ProductScan scan = new ProductScan(s1, emptyS2)) {

            scan.afterLast();
            assertFalse(scan.previous());
        }
    }

    @Test
    public void testProductBackwardsWithBothEmptyChildren() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred1 = new Predicate(emptyTerm1);

        Term emptyTerm2 = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred2 = new Predicate(emptyTerm2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS1 = new SelectScan(s1, emptyPred1);
             SelectScan emptyS2 = new SelectScan(s2, emptyPred2);
             ProductScan scan = new ProductScan(emptyS1, emptyS2)) {

            scan.afterLast();
            assertFalse(scan.previous());
        }
    }

    @Test
    public void testProductNavigationBackwards() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ProductScan scan = new ProductScan(s1, s2)) {

            scan.afterLast();

            assertTrue(scan.previous());
            assertEquals(249, scan.getValue("t1_intField1").asInt());
            assertEquals(249, scan.getValue("t2_intField1").asInt());

            assertTrue(scan.previous());
            assertEquals(249, scan.getValue("t1_intField1").asInt());
            assertEquals(248, scan.getValue("t2_intField1").asInt());
        }
    }

    @Test
    public void testProductWithNullCheckThroughJoin() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(0))
        );
        Term t2 = new Term(
                new FieldNameExpression("t2_intField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ProductScan product = new ProductScan(s1, s2);
             SelectReadOnlyScan scan = new SelectReadOnlyScan(product, pred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                assertEquals(NullConstant.INSTANCE, scan.getValue("t2_intField3"));
                count++;
            }

            assertEquals(250, count);
        }
    }
}