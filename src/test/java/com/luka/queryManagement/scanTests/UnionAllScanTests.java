package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.RenameScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.SelectReadOnlyScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.UnionAllScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.BooleanConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class UnionAllScanTests {
    @Test
    public void testUnionAllRowsFromBothTables() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan renamedS2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             UnionAllScan scan = new UnionAllScan(s1, renamedS2)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                count++;
            }
            assertEquals(500, count);
        }
    }

    @Test
    public void testUnionAllWithMidComplexityPredicate() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_boolField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(true))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan renamedS2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             RenameScan renamedS2_2 = new RenameScan(renamedS2, Map.of("t1_boolField1", "t2_boolField1"));
             UnionAllScan union = new UnionAllScan(s1, renamedS2_2);
             SelectReadOnlyScan scan = new SelectReadOnlyScan(union, pred)) {

            scan.beforeFirst();
            int count = 0;
            while (scan.next()) {
                count++;
                assertEquals(10, scan.getInt("t1_intField1"));
            }
            assertEquals(2, count);
        }
    }

    @Test
    public void testUnionAllNavigationBackwards() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan renamedS2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             UnionAllScan scan = new UnionAllScan(s1, renamedS2)) {

            scan.afterLast();

            assertTrue(scan.previous());
            assertEquals(249, scan.getInt("t1_intField1"));

            for (int i = 0; i < 300; i++) {
                scan.previous();
            }

            assertTrue(scan.getInt("t1_intField1") < 250);
        }
    }

    @Test
    public void testUnionAllEmptyScans() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(0))
        );
        Predicate pred = new Predicate(t1);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan renamedS2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             SelectReadOnlyScan sel1 = new SelectReadOnlyScan(s1, pred);
             SelectReadOnlyScan sel2 = new SelectReadOnlyScan(renamedS2, pred);
             UnionAllScan scan = new UnionAllScan(sel1, sel2)) {

            scan.beforeFirst();
            assertFalse(scan.next());
        }
    }

    @Test
    public void testUnionAllNullCheckWithIs() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(248))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan renamedS2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             RenameScan fullyRenamedS2 = new RenameScan(renamedS2, Map.of("t1_intField3", "t2_intField3"));
             UnionAllScan union = new UnionAllScan(s1, fullyRenamedS2);
             SelectReadOnlyScan scan = new SelectReadOnlyScan(union, pred)) {

            scan.beforeFirst();
            assertTrue(scan.next());
            assertEquals(249, scan.getInt("t1_intField1"));
            assertTrue(scan.next());
            assertEquals(249, scan.getInt("t1_intField1"));
            assertFalse(scan.next());
        }
    }
}
