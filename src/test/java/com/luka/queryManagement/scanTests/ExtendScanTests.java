package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.ExtendScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.SelectReadOnlyScan;
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

import static org.junit.jupiter.api.Assertions.*;

public class ExtendScanTests {
    @Test
    public void testAccessingExtendedField() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression expr = new BinaryArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.ADD,
                new ConstantExpression(new IntConstant(100))
        );

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ExtendScan scan = new ExtendScan(tableScan, expr, "calculatedField")) {

            scan.beforeFirst();

            int i = 0;
            while(scan.next()) {
                assertTrue(scan.hasField("calculatedField"));
                assertEquals(100 + i, scan.getInt("calculatedField"));

                assertTrue(scan.hasField("t1_intField1"));
                assertEquals(i, scan.getInt("t1_intField1"));
                i++;
            }

            assertTrue(i > 0);
        }
    }

    @Test
    public void testExtendedFieldInPredicate() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression expr = new BinaryArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.MUL,
                new ConstantExpression(new IntConstant(2))
        );

        Term t1 = new Term(
                new FieldNameExpression("doubledVal"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(20))
        );
        Term t2 = new Term(
                new FieldNameExpression("t1_boolField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new BooleanConstant(true))
        );
        Predicate pred = new Predicate(t1, t2);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ExtendScan extendScan = new ExtendScan(tableScan, expr, "doubledVal");
             SelectReadOnlyScan scan = new SelectReadOnlyScan(extendScan, pred)) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertEquals(20, scan.getInt("doubledVal"));
            assertEquals(10, scan.getInt("t1_intField1"));
            assertFalse(scan.next());
        }
    }

    @Test
    public void testExtendWithNullValues() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression expr = new FieldNameExpression("t1_intField3");

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ExtendScan scan = new ExtendScan(tableScan, expr, "copyOffNull")) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertEquals(NullConstant.INSTANCE, scan.getValue("copyOffNull"));

            Term t1 = new Term(
                    new FieldNameExpression("copyOffNull"),
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

    @Test
    public void testExtendWithNullValueLiteral() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression expr = new ConstantExpression(NullConstant.INSTANCE);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
             ExtendScan scan = new ExtendScan(tableScan, expr, "nullLiteral")) {

            scan.beforeFirst();
            assertTrue(scan.next());

            assertEquals(NullConstant.INSTANCE, scan.getValue("nullLiteral"));
        }
    }
}