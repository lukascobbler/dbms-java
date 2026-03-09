package com.luka.queryManagement.vritualEntitiesTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.*;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class ExpressionTests {
    @Test
    public void testConstantEvaluationExpression() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression eInt = new ConstantExpression(new IntConstant(843));
        Expression eString = new ConstantExpression(new StringConstant("test"));
        Expression eBool = new ConstantExpression(new BooleanConstant(true));
        Expression eNull = new ConstantExpression(NullConstant.INSTANCE);

        Scan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());

        try (ts1) {
            while (ts1.next()) {
                assertEquals(843, eInt.evaluate(ts1).asInt());
                assertEquals("test", eString.evaluate(ts1).asString());
                assertTrue(eBool.evaluate(ts1).asBoolean());
                assertEquals(NullConstant.INSTANCE, eNull.evaluate(ts1));
            }
        }
    }

    @Test
    public void testFieldNameExpression() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression eFieldNameInt1 = new FieldNameExpression("t1_intField1");
        Expression eFieldNameInt2 = new FieldNameExpression("t1_intField2");
        Expression eFieldNameInt3 = new FieldNameExpression("t1_intField3");
        Expression eFieldNameString1 = new FieldNameExpression("t1_stringField1");
        Expression eFieldNameString2 = new FieldNameExpression("t1_stringField2");
        Expression eFieldNameString3 = new FieldNameExpression("t1_stringField3");
        Expression eFieldNameBool1 = new FieldNameExpression("t1_boolField1");
        Expression eFieldNameBool2 = new FieldNameExpression("t1_boolField2");
        Expression eFieldNameBool3 = new FieldNameExpression("t1_boolField3");

        Scan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());

        int i = 0;
        try (ts1) {
            while (ts1.next()) {
                assertEquals(i, eFieldNameInt1.evaluate(ts1).asInt());
                assertEquals(i + 1, eFieldNameInt2.evaluate(ts1).asInt());
                assertEquals(NullConstant.INSTANCE, eFieldNameInt3.evaluate(ts1));
                assertEquals("str" + i, eFieldNameString1.evaluate(ts1).asString());
                assertEquals("str" + i + 1, eFieldNameString2.evaluate(ts1).asString());
                assertEquals(NullConstant.INSTANCE, eFieldNameString3.evaluate(ts1));
                assertTrue(eFieldNameBool1.evaluate(ts1).asBoolean());
                assertFalse(eFieldNameBool2.evaluate(ts1).asBoolean());
                assertEquals(NullConstant.INSTANCE, eFieldNameBool3.evaluate(ts1));

                i += 1;
            }
        }
    }

    @Test
    public void testArithmeticExpressionConstant() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression eInt1 = new ConstantExpression(new IntConstant(1));
        Expression eInt2 = new ConstantExpression(new IntConstant(2));
        Expression ar1 = new ArithmeticExpression(eInt1, ArithmeticOperator.ADD, eInt2);

        Scan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());

        try (ts1) {
            while (ts1.next()) {
                assertEquals(3, ar1.evaluate(ts1).asInt());
            }
        }
    }

    @Test
    public void testArithmeticExpressionFieldNameSimple() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Expression eInt1 = new ConstantExpression(new IntConstant(500));
        Expression eInt2 = new FieldNameExpression("t1_intField2");
        Expression ar1 = new ArithmeticExpression(eInt1, ArithmeticOperator.ADD, eInt2);

        Scan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());

        int i = 0;
        try (ts1) {
            while (ts1.next()) {
                assertEquals(500 + i + 1, ar1.evaluate(ts1).asInt());

                i += 1;
            }
        }
    }

    @Test
    public void testArithmeticExpressionFieldNameComplex() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        // ((f1 * 10) + (f2 - 5)) / 2
        Expression complexExpr = new ArithmeticExpression(
                new ArithmeticExpression(
                        new ArithmeticExpression(
                                new FieldNameExpression("t1_intField1"),
                                ArithmeticOperator.MUL,
                                new ConstantExpression(new IntConstant(10))
                        ),
                        ArithmeticOperator.ADD,
                        new ArithmeticExpression(
                                new FieldNameExpression("t1_intField2"),
                                ArithmeticOperator.SUB,
                                new ConstantExpression(new IntConstant(5))
                        )
                ),
                ArithmeticOperator.DIV,
                new ConstantExpression(new IntConstant(2))
        );

        Scan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst());

        try (ts1) {
            int i = 0;
            while (ts1.next()) {
                int expected = ((i * 10) + ((i + 1) - 5)) / 2;
                Constant result = complexExpr.evaluate(ts1);
                assertEquals(expected, result.asInt());
                i++;
            }
        }
    }

    @Test
    public void testComplexFoldingAndEvaluation1() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        // Target: ((t1_intField1 + (10 + 5)) * (20 / 10)) - (t1_intField1 + 15)
        // 1. (10 + 5) -> 15
        // 2. (20 / 10) -> 2
        // 3. ((t1_intField1 + 15) * 2) - (t1_intField1 + 15)

        Expression f1 = new FieldNameExpression("t1_intField1");

        Expression const10 = new ConstantExpression(new IntConstant(10));
        Expression const5 = new ConstantExpression(new IntConstant(5));
        Expression innerAdd = new ArithmeticExpression(const10, ArithmeticOperator.ADD, const5);

        Expression leftWithField = new ArithmeticExpression(f1, ArithmeticOperator.ADD, innerAdd);

        Expression const20 = new ConstantExpression(new IntConstant(20));
        Expression const10_2 = new ConstantExpression(new IntConstant(10));
        Expression innerDiv = new ArithmeticExpression(const20, ArithmeticOperator.DIV, const10_2);

        Expression branch1 = new ArithmeticExpression(leftWithField, ArithmeticOperator.MUL, innerDiv);

        Expression branch2 = new ArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.ADD,
                new ConstantExpression(new IntConstant(15))
        );

        Expression totalExpr = new ArithmeticExpression(branch1, ArithmeticOperator.SUB, branch2);

        Expression foldedExpr = PartialEvaluator.evaluate(totalExpr);

        String expectedString = "(((t1_intField1 + 15) * 2) - (t1_intField1 + 15))";
        assertEquals(expectedString, foldedExpr.toString());

        try (TableScan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst())) {
            int i = 0;
            while (ts1.next()) {
                int expectedValue = i + 15;

                Constant resultTotal = totalExpr.evaluate(ts1);
                Constant resultFolded = foldedExpr.evaluate(ts1);
                assertEquals(expectedValue, resultTotal.asInt());
                assertEquals(expectedValue, resultFolded.asInt());
                i++;
            }
        }
    }

    @Test
    public void testComplexFoldingAndEvaluation2() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        // Target: ((t1_intField1 + (10 + 5)) * (20 / 20)) - (t1_intField1 + 15)
        // 1. (10 + 5) -> 15
        // 2. (20 / 10) -> 1
        // 3. ((t1_intField1 + 15) * 1) - (t1_intField1 + 15)
        // 4. ((t1_intField1 + 15) - (t1_intField1 + 15)
        // 5. 0

        Expression f1 = new FieldNameExpression("t1_intField1");

        Expression const10 = new ConstantExpression(new IntConstant(10));
        Expression const5 = new ConstantExpression(new IntConstant(5));
        Expression innerAdd = new ArithmeticExpression(const10, ArithmeticOperator.ADD, const5);

        Expression leftWithField = new ArithmeticExpression(f1, ArithmeticOperator.ADD, innerAdd);

        Expression const20 = new ConstantExpression(new IntConstant(20));
        Expression const10_2 = new ConstantExpression(new IntConstant(20));
        Expression innerDiv = new ArithmeticExpression(const20, ArithmeticOperator.DIV, const10_2);

        Expression branch1 = new ArithmeticExpression(leftWithField, ArithmeticOperator.MUL, innerDiv);

        Expression branch2 = new ArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.ADD,
                new ConstantExpression(new IntConstant(15))
        );

        Expression totalExpr = new ArithmeticExpression(branch1, ArithmeticOperator.SUB, branch2);

        Expression foldedExpr = PartialEvaluator.evaluate(totalExpr);

        String expectedString = "0";
        assertEquals(expectedString, foldedExpr.toString());

        try (TableScan ts1 = new TableScan(testData.tx(), "table1", testData.layouts().getFirst())) {
            int i = 0;
            while (ts1.next()) {
                int expectedValue = 0;

                Constant resultTotal = totalExpr.evaluate(ts1);
                Constant resultFolded = foldedExpr.evaluate(ts1);
                assertEquals(expectedValue, resultTotal.asInt());
                assertEquals(expectedValue, resultFolded.asInt());
                i++;
            }
        }
    }

    @Test
    public void appliesToSchema() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Schema schema = testData.layouts().getFirst().getSchema();

        Expression e1 = new FieldNameExpression("t1_intField1");
        assertTrue(e1.appliesTo(schema));

        Expression e2 = new FieldNameExpression("garbage_field");
        assertFalse(e2.appliesTo(schema));

        Expression e3 = new ConstantExpression(new IntConstant(42));
        assertTrue(e3.appliesTo(schema));

        Expression complexSuccess = new ArithmeticExpression(
                new ArithmeticExpression(
                        new FieldNameExpression("t1_intField1"),
                        ArithmeticOperator.ADD,
                        new FieldNameExpression("t1_intField2")
                ),
                ArithmeticOperator.MUL,
                new ConstantExpression(new IntConstant(10))
        );
        assertTrue(complexSuccess.appliesTo(schema));

        Expression complexFailure = new ArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.ADD,
                new FieldNameExpression("hidden_field")
        );
        assertFalse(complexFailure.appliesTo(schema));
    }
}
