package com.luka.queryManagement.vritualEntitiesTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.BooleanConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.simpledb.recordManagement.schema.PhysicalSchema;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class TermTests {
    Plan<Scan> mockPlan = new Plan<>() {
        @Override public Scan open() { return null; }
        @Override public int blocksAccessed() { return 0; }
        @Override public int recordsOutput() { return 0; }
        @Override public int distinctValues(String fieldName) {
            if (fieldName.equals("t1_intField1")) return 250;
            if (fieldName.equals("t1_boolField1")) return 2;
            return 10;
        }
        @Override public PhysicalSchema outputSchema() { return null; }
    };

    @Test
    public void testIsOperatorWithNulls() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Scan ts = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
        ts.next();

        Term t1 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        assertTrue(t1.isSatisfied(ts));

        Term t2 = new Term(
                new FieldNameExpression("t1_stringField1"),
                TermOperator.IS,
                new ConstantExpression(NullConstant.INSTANCE)
        );
        assertFalse(t2.isSatisfied(ts));
    }

    @Test
    public void testNullPoisoningStandardOperators() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Scan ts = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
        ts.next();

        Term t1 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.EQUALS,
                new FieldNameExpression("t1_intField3")
        );
        assertFalse(t1.isSatisfied(ts));

        Term t2 = new Term(
                new FieldNameExpression("t1_intField3"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(5))
        );
        assertFalse(t2.isSatisfied(ts));
    }

    @Test
    public void testComplexExpressionWithinTerm() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Scan ts = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
        ts.next();

        Expression math = new BinaryArithmeticExpression(
                new BinaryArithmeticExpression(
                        new FieldNameExpression("t1_intField1"),
                        ArithmeticOperator.ADD,
                        new ConstantExpression(new IntConstant(10))
                ),
                ArithmeticOperator.DIV,
                new ConstantExpression(new IntConstant(2))
        );

        Term t = new Term(math, TermOperator.GREATER_THAN, new FieldNameExpression("t1_intField1"));
        assertTrue(t.isSatisfied(ts));
    }

    @Test
    public void testStringAndBooleanLogic() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Scan ts = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());

        while (ts.next()) {
            Term t1 = new Term(
                    new FieldNameExpression("t1_stringField1"),
                    TermOperator.NOT_EQUALS,
                    new FieldNameExpression("t1_stringField2")
            );
            assertTrue(t1.isSatisfied(ts));

            Term t2 = new Term(
                    new FieldNameExpression("t1_boolField1"),
                    TermOperator.EQUALS,
                    new ConstantExpression(new BooleanConstant(true))
            );
            assertTrue(t2.isSatisfied(ts));
        }
    }

    @Test
    public void testNumericComparisons() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        Scan ts = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());
        ts.next();

        Term t1 = new Term(
                new FieldNameExpression("t1_intField2"),
                TermOperator.GREATER_OR_EQUAL,
                new ConstantExpression(new IntConstant(1))
        );
        assertTrue(t1.isSatisfied(ts));

        while(ts.next()) {
            Term t2 = new Term(
                    new FieldNameExpression("t1_intField1"),
                    TermOperator.LESS_THAN,
                    new FieldNameExpression("t1_intField2")
            );

            assertTrue(t2.isSatisfied(ts));
        }
    }

    @Test
    public void testTermFoldingOptimization() throws Exception {
        Expression fivePlusFive = new BinaryArithmeticExpression(
                new ConstantExpression(new IntConstant(5)),
                ArithmeticOperator.ADD,
                new ConstantExpression(new IntConstant(5))
        );

        Term t = new Term(
                fivePlusFive,
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );

        t.foldExpressions();

        Expression lhs = (Expression) TestUtils.getPrivateField(t, "lhs");

        assertEquals(new ConstantExpression(new IntConstant(10)), lhs);
        assertTrue(t.isSatisfied(null));
    }

    @Test
    public void testEquatesLogic() {
        Expression f1 = new FieldNameExpression("t1_intField1");
        Expression f2 = new FieldNameExpression("t1_intField2");
        Constant c10 = new IntConstant(10);
        Expression e10 = new ConstantExpression(c10);

        Term t1 = new Term(f1, TermOperator.EQUALS, e10);
        assertEquals(c10, t1.equatesWithConstant("t1_intField1"));
        assertNull(t1.equatesWithFieldName("t1_intField1"));

        Term t2 = new Term(e10, TermOperator.EQUALS, f1);
        assertEquals(c10, t2.equatesWithConstant("t1_intField1"));

        Term t3 = new Term(f1, TermOperator.EQUALS, f2);
        assertEquals("t1_intField2", t3.equatesWithFieldName("t1_intField1"));
        assertEquals("t1_intField1", t3.equatesWithFieldName("t1_intField2"));
        assertNull(t3.equatesWithConstant("t1_intField1"));

        Term t4 = new Term(f1, TermOperator.GREATER_THAN, e10);
        assertNull(t4.equatesWithConstant("t1_intField1"));
    }

    @Test
    public void testReductionFactors() {
        Expression f1 = new FieldNameExpression("t1_intField1");
        Expression f_bool = new FieldNameExpression("t1_boolField1");
        Expression c10 = new ConstantExpression(new IntConstant(10));

        Term rangeTerm = new Term(f1, TermOperator.GREATER_THAN, c10);
        assertEquals(2, rangeTerm.reductionFactor(mockPlan));

        Term eqTerm = new Term(f1, TermOperator.EQUALS, c10);
        assertEquals(250, eqTerm.reductionFactor(mockPlan));

        Term joinTerm = new Term(f1, TermOperator.EQUALS, f_bool);
        assertEquals(250, joinTerm.reductionFactor(mockPlan)); // max(250, 2)

        Term trueTerm = new Term(c10, TermOperator.EQUALS, c10);
        assertEquals(1, trueTerm.reductionFactor(mockPlan));

        Term falseTerm = new Term(c10, TermOperator.EQUALS, new ConstantExpression(new IntConstant(99)));
        assertEquals(Integer.MAX_VALUE, falseTerm.reductionFactor(mockPlan));
    }

    @Test
    public void testScanConsistencyWithEquates() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        var testData = QueryTestUtils.initializeOneFullTable(tmpDir);

        String targetField = "t1_intField1";
        Constant targetVal = new IntConstant(42);
        Term term = new Term(
                new FieldNameExpression(targetField),
                TermOperator.EQUALS,
                new ConstantExpression(targetVal)
        );

        Constant equated = term.equatesWithConstant(targetField);
        assertEquals(targetVal, equated);

        try (TableScan ts = new TableScan(testData.tx(), "table1", testData.layouts().getFirst())) {
            int matchCount = 0;
            while (ts.next()) {
                boolean satisfied = term.isSatisfied(ts);
                int actualVal = ts.getInt(targetField);

                if (satisfied) {
                    assertEquals(targetVal.asInt(), actualVal);
                    matchCount++;
                } else {
                    assertNotEquals(targetVal.asInt(), actualVal);
                }
            }
            assertEquals(1, matchCount);
        }
    }
}
