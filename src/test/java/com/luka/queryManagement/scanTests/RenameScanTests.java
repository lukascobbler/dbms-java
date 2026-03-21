package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.RenameScan;
import com.luka.simpledb.queryManagement.scanTypes.update.SelectScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.ConstantExpression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RenameScanTests {
    @Test
    public void testAccessingFieldWithNewName() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             RenameScan scan = new RenameScan(tableScan, Map.of("renamedInt", "t1_intField1"))) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertTrue(scan.hasField("renamedInt"));
            assertEquals(0, scan.getInt("renamedInt"));

            assertTrue(scan.hasField("t1_intField2"));
            assertEquals(1, scan.getInt("t1_intField2"));
        }
    }

    @Test
    public void testOldNameIsNoLongerAccessible() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             RenameScan scan = new RenameScan(tableScan, Map.of("newStringName", "t1_stringField1"))) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertFalse(scan.hasField("t1_stringField1"));

            assertThrows(FieldNotFoundInScanException.class, () -> scan.getString("t1_stringField1"));
        }
    }

    @Test
    public void testRenamingWithPredicateFilter() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(100))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_intField2"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(110))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             SelectScan selectScan = new SelectScan(tableScan, pred);
             RenameScan scan = new RenameScan(selectScan, Map.of("description", "t1_stringField1"))) {

            scan.beforeFirst();

            int count = 0;
            while (scan.next()) {
                assertTrue(scan.getString("description").startsWith("str"));
                count++;
            }
            assertEquals(8, count);
        }
    }

    @Test
    public void testNullCheckThroughRename() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             RenameScan scan = new RenameScan(tableScan, Map.of("nullableInt", "t1_intField3"))) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertEquals(NullConstant.INSTANCE, scan.getValue("nullableInt"));

            Term t1 = new Term(
                    new FieldNameExpression("nullableInt"),
                    TermOperator.IS,
                    new ConstantExpression(NullConstant.INSTANCE)
            );
            Term t2 = new Term(
                    new FieldNameExpression("t1_intField1"),
                    TermOperator.EQUALS,
                    new ConstantExpression(new IntConstant(0))
            );
            Predicate pred = new Predicate(t1, t2);

            assertTrue(pred.isSatisfied(scan));
        }
    }
}
