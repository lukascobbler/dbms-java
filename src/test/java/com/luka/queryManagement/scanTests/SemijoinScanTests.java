package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.SemijoinScan;
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

import static org.junit.jupiter.api.Assertions.*;

public class SemijoinScanTests {
    @Test
    public void testSemijoinWithBasicMatches() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate pred = new Predicate(t1);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SemijoinScan scan = new SemijoinScan(s1, s2, pred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                assertTrue(scan.hasField("t1_intField1"));
                assertFalse(scan.hasField("t2_intField1"));
                count++;
            }
            assertEquals(250, count);
        }
    }

    @Test
    public void testSemijoinYieldsOuterRecordOnceDespiteMultipleInnerMatches() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.GREATER_OR_EQUAL,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate pred = new Predicate(t1);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SemijoinScan scan = new SemijoinScan(s1, s2, pred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                count++;
            }
            assertEquals(250, count);
        }
    }

    @Test
    public void testSemijoinWithEmptyInnerTable() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        Term emptyTerm = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate emptyPred = new Predicate(emptyTerm);

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate joinPred = new Predicate(joinTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS2 = new SelectScan(s2, emptyPred);
             SemijoinScan scan = new SemijoinScan(s1, emptyS2, joinPred)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testSemijoinBackwardsNavigation() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Term t2 = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SemijoinScan scan = new SemijoinScan(s1, s2, pred)) {

            scan.afterLast();
            assertTrue(scan.previous());
            assertEquals(10, scan.getInt("t1_intField1"));
            assertFalse(scan.previous());
        }
    }

    @Test
    public void testSemijoinWithNullComparisons() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Term t2 = new Term(
                new FieldNameExpression("t2_intField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SemijoinScan scan = new SemijoinScan(s1, s2, pred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                count++;
            }
            assertEquals(250, count);
        }
    }
}
