package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.AntijoinScan;
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

public class AntijoinScanTests {
    @Test
    public void testAntijoinWithEmptyInnerTableReturnsAllOuter() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term emptyTerm = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate innerEmptyPred = new Predicate(emptyTerm);

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate joinPred = new Predicate(joinTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan emptyS2 = new SelectScan(s2, innerEmptyPred);
             AntijoinScan scan = new AntijoinScan(s1, emptyS2, joinPred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                count++;
            }
            assertEquals(250, count);
        }
    }

    @Test
    public void testAntijoinWithPerfectMatchReturnsNothing() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate pred = new Predicate(t1);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             AntijoinScan scan = new AntijoinScan(s1, s2, pred)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testAntijoinFilteringSpecificRange() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term filterInner = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Predicate innerPred = new Predicate(filterInner);

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate joinPred = new Predicate(joinTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan filteredS2 = new SelectScan(s2, innerPred);
             AntijoinScan scan = new AntijoinScan(s1, filteredS2, joinPred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                assertNotEquals(10, scan.getValue("t1_intField1").asInt());
                count++;
            }
            assertEquals(249, count);
        }
    }

    @Test
    public void testAntijoinBackwardsNavigation() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        Term excludeFive = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.NOT_EQUALS,
                new ConstantExpression(new IntConstant(5))
        );
        Predicate innerPred = new Predicate(excludeFive);

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate joinPred = new Predicate(joinTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan filteredS2 = new SelectScan(s2, innerPred);
             AntijoinScan scan = new AntijoinScan(s1, filteredS2, joinPred)) {

            scan.afterLast();
            assertTrue(scan.previous());
            assertEquals(5, scan.getValue("t1_intField1").asInt());
            assertFalse(scan.previous());
        }
    }

    @Test
    public void testAntijoinWithNullCheck() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

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
             AntijoinScan scan = new AntijoinScan(s1, s2, pred)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }
}
