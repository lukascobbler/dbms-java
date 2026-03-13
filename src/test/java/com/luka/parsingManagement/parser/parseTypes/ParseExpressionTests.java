package com.luka.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.parser.parseTypes.ParseExpression;
import com.luka.simpledb.queryManagement.exceptions.WildcardExpressionEvaluationException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.BooleanConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.StringConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParseExpressionTests {
    private Expression parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseExpression(ctx).parse();
    }

    @Test
    public void testIntegerConstant() {
        Expression expr = parse("42");
        ConstantExpression c = assertInstanceOf(ConstantExpression.class, expr);
        IntConstant i42 = assertInstanceOf(IntConstant.class, c.constant());
        assertEquals(42, i42.asInt());
        assertEquals(42, c.evaluate(null).asInt());
    }

    @Test
    public void testStringConstant() {
        Expression expr = parse("'hello'");
        ConstantExpression c = assertInstanceOf(ConstantExpression.class, expr);
        StringConstant sHello = assertInstanceOf(StringConstant.class, c.constant());
        assertEquals("hello", sHello.asString());
        assertEquals("hello", c.evaluate(null).asString());
    }

    @Test
    public void testBooleanTrueConstant() {
        Expression expr = parse("TRUE");
        ConstantExpression c = assertInstanceOf(ConstantExpression.class, expr);
        BooleanConstant bT = assertInstanceOf(BooleanConstant.class, c.constant());
        assertTrue(bT.asBoolean());
        assertTrue(c.evaluate(null).asBoolean());
    }

    @Test
    public void testBooleanFalseConstant() {
        Expression expr = parse("FALSE");
        ConstantExpression c = assertInstanceOf(ConstantExpression.class, expr);
        assertInstanceOf(BooleanConstant.class, c.constant());
        assertFalse(c.evaluate(null).asBoolean());
    }

    @Test
    public void testNullConstant() {
        Expression expr = parse("NULL");
        ConstantExpression c = assertInstanceOf(ConstantExpression.class, expr);
        assertSame(NullConstant.INSTANCE, c.constant());
        assertEquals(NullConstant.INSTANCE, c.evaluate(null));
    }

    @Test
    public void testIdentifierField() {
        Expression expr = parse("my_column");
        FieldNameExpression f = assertInstanceOf(FieldNameExpression.class, expr);
        assertEquals("my_column", f.fieldName());
    }
    @Test
    public void testBasicSubtraction() {
        Expression expr = parse("10 - b");
        BinaryArithmeticExpression bin = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.SUB, bin.op());

        ConstantExpression left = assertInstanceOf(ConstantExpression.class, bin.left());
        assertEquals(10, left.constant().asInt());

        FieldNameExpression right = assertInstanceOf(FieldNameExpression.class, bin.right());
        assertEquals("b", right.fieldName());
    }

    @Test
    public void testBasicMultiplication() {
        Expression expr = parse("x * y");
        BinaryArithmeticExpression bin = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.MUL, bin.op());

        assertEquals("x", assertInstanceOf(FieldNameExpression.class, bin.left()).fieldName());
        assertEquals("y", assertInstanceOf(FieldNameExpression.class, bin.right()).fieldName());
    }

    @Test
    public void testBasicDivision() {
        Expression expr = parse("100 / 4");
        BinaryArithmeticExpression bin = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.DIV, bin.op());

        assertEquals(100, assertInstanceOf(ConstantExpression.class, bin.left()).constant().asInt());
        assertEquals(4, assertInstanceOf(ConstantExpression.class, bin.right()).constant().asInt());
    }

    @Test
    public void testLeftAssociativityAddition() {
        Expression expr = parse("a + b + c");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.ADD, outer.op());
        assertEquals("c", assertInstanceOf(FieldNameExpression.class, outer.right()).fieldName());

        BinaryArithmeticExpression inner = assertInstanceOf(BinaryArithmeticExpression.class, outer.left());
        assertEquals(ArithmeticOperator.ADD, inner.op());
        assertEquals("a", assertInstanceOf(FieldNameExpression.class, inner.left()).fieldName());
        assertEquals("b", assertInstanceOf(FieldNameExpression.class, inner.right()).fieldName());
    }

    @Test
    public void testLeftAssociativityDivision() {
        Expression expr = parse("100 / 10 / 2");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.DIV, outer.op());
        assertEquals(2, assertInstanceOf(ConstantExpression.class, outer.right()).constant().asInt());

        BinaryArithmeticExpression inner = assertInstanceOf(BinaryArithmeticExpression.class, outer.left());
        assertEquals(ArithmeticOperator.DIV, inner.op());
        assertEquals(100, assertInstanceOf(ConstantExpression.class, inner.left()).constant().asInt());
        assertEquals(10, assertInstanceOf(ConstantExpression.class, inner.right()).constant().asInt());
    }

    @Test
    public void testPrecedenceMultiplicationOverAddition() {
        Expression expr = parse("2 + 3 * 4");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.ADD, outer.op());
        assertEquals(2, assertInstanceOf(ConstantExpression.class, outer.left()).constant().asInt());

        BinaryArithmeticExpression innerMul = assertInstanceOf(BinaryArithmeticExpression.class, outer.right());
        assertEquals(ArithmeticOperator.MUL, innerMul.op());
        assertEquals(3, assertInstanceOf(ConstantExpression.class, innerMul.left()).constant().asInt());
        assertEquals(4, assertInstanceOf(ConstantExpression.class, innerMul.right()).constant().asInt());
    }

    @Test
    public void testPrecedenceAdditionAfterMultiplication() {
        Expression expr = parse("2 * 3 + 4");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.ADD, outer.op());
        assertEquals(4, assertInstanceOf(ConstantExpression.class, outer.right()).constant().asInt());

        BinaryArithmeticExpression innerMul = assertInstanceOf(BinaryArithmeticExpression.class, outer.left());
        assertEquals(ArithmeticOperator.MUL, innerMul.op());
        assertEquals(2, assertInstanceOf(ConstantExpression.class, innerMul.left()).constant().asInt());
        assertEquals(3, assertInstanceOf(ConstantExpression.class, innerMul.right()).constant().asInt());
    }

    @Test
    public void testBasicUnaryMinus() {
        Expression expr = parse("-5");
        UnaryArithmeticExpression u = assertInstanceOf(UnaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.SUB, u.op());

        ConstantExpression operand = assertInstanceOf(ConstantExpression.class, u.operand());
        assertEquals(5, operand.constant().asInt());
    }

    @Test
    public void testBasicUnaryPlus() {
        Expression expr = parse("+x");
        UnaryArithmeticExpression u = assertInstanceOf(UnaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.ADD, u.op());

        FieldNameExpression operand = assertInstanceOf(FieldNameExpression.class, u.operand());
        assertEquals("x", operand.fieldName());
    }

    @Test
    public void testChainedUnaryOperators() {
        Expression expr = parse("--5");
        UnaryArithmeticExpression outer = assertInstanceOf(UnaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.SUB, outer.op());

        UnaryArithmeticExpression inner = assertInstanceOf(UnaryArithmeticExpression.class, outer.operand());
        assertEquals(ArithmeticOperator.SUB, inner.op());
        assertEquals(5, assertInstanceOf(ConstantExpression.class, inner.operand()).constant().asInt());
    }

    @Test
    public void testUnaryPrecedenceOverMultiplication() {
        Expression expr = parse("-a * b");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.MUL, outer.op());
        assertEquals("b", assertInstanceOf(FieldNameExpression.class, outer.right()).fieldName());

        UnaryArithmeticExpression leftUnary = assertInstanceOf(UnaryArithmeticExpression.class, outer.left());
        assertEquals(ArithmeticOperator.SUB, leftUnary.op());
        assertEquals("a", assertInstanceOf(FieldNameExpression.class, leftUnary.operand()).fieldName());
    }

    @Test
    public void testUnaryOnRightSideOfBinary() {
        Expression expr = parse("a * -b");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.MUL, outer.op());
        assertEquals("a", assertInstanceOf(FieldNameExpression.class, outer.left()).fieldName());

        UnaryArithmeticExpression rightUnary = assertInstanceOf(UnaryArithmeticExpression.class, outer.right());
        assertEquals(ArithmeticOperator.SUB, rightUnary.op());
        assertEquals("b", assertInstanceOf(FieldNameExpression.class, rightUnary.operand()).fieldName());
    }

    @Test
    public void testParenthesesOverridePrecedence() {
        Expression expr = parse("(2 + 3) * 4");
        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.MUL, outer.op());
        assertEquals(4, assertInstanceOf(ConstantExpression.class, outer.right()).constant().asInt());

        BinaryArithmeticExpression innerAdd = assertInstanceOf(BinaryArithmeticExpression.class, outer.left());
        assertEquals(ArithmeticOperator.ADD, innerAdd.op());
        assertEquals(2, assertInstanceOf(ConstantExpression.class, innerAdd.left()).constant().asInt());
        assertEquals(3, assertInstanceOf(ConstantExpression.class, innerAdd.right()).constant().asInt());
    }

    @Test
    public void testDeeplyNestedParentheses() {
        Expression expr = parse("(((x)))");
        FieldNameExpression f = assertInstanceOf(FieldNameExpression.class, expr);
        assertEquals("x", f.fieldName());
    }

    @Test
    public void testUnaryAppliedToParentheses() {
        Expression expr = parse("-(a + b)");
        UnaryArithmeticExpression outer = assertInstanceOf(UnaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.SUB, outer.op());

        BinaryArithmeticExpression inner = assertInstanceOf(BinaryArithmeticExpression.class, outer.operand());
        assertEquals(ArithmeticOperator.ADD, inner.op());
        assertEquals("a", assertInstanceOf(FieldNameExpression.class, inner.left()).fieldName());
        assertEquals("b", assertInstanceOf(FieldNameExpression.class, inner.right()).fieldName());
    }

    @Test
    public void testEmptyExpressionThrowsException() {
        assertThrows(ParserException.class, () -> parse(""));
    }

    @Test
    public void testDanglingOperatorThrowsException() {
        assertThrows(ParserException.class, () -> parse("a +"));
    }

    @Test
    public void testMissingRightParenthesisThrowsException() {
        assertThrows(ParserException.class, () -> parse("(a + b"));
    }

    @Test
    public void testUnexpectedDelimiterInPrefixThrowsException() {
        assertThrows(ParserException.class, () -> parse("/ 5"));
    }

    @Test
    public void testUnexpectedKeywordThrowsException() {
        assertThrows(ParserException.class, () -> parse("SELECT"));
    }

    @Test
    public void testConsecutiveBinaryOperatorsThrowsException() {
        assertThrows(ParserException.class, () -> parse("a + / b"));
    }

    @Test
    public void testWildcardAsOperand() {
        Expression expr = parse("5 + *");
        BinaryArithmeticExpression bin = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertInstanceOf(WildcardExpression.class, bin.right());
    }

    @Test
    public void testUnmatchedRightParenthesisReturnsEarly() {
        Expression expr = parse("a + b )");
        assertInstanceOf(BinaryArithmeticExpression.class, expr);
    }

    @Test
    public void testLeftAssociativityAdditionEdgeCase() {
        Expression expr = parse("y + n - c");

        BinaryArithmeticExpression outer = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.SUB, outer.op());
        assertEquals("c", assertInstanceOf(FieldNameExpression.class, outer.right()).fieldName());

        BinaryArithmeticExpression inner = assertInstanceOf(BinaryArithmeticExpression.class, outer.left());
        assertEquals(ArithmeticOperator.ADD, inner.op());
        assertEquals("y", assertInstanceOf(FieldNameExpression.class, inner.left()).fieldName());
        assertEquals("n", assertInstanceOf(FieldNameExpression.class, inner.right()).fieldName());
    }

    @Test
    public void testHighlyComplexExpression() {
        Expression expr = parse("a * (b - 17) / 4 + 22 * (c + 50 / 25)");

        BinaryArithmeticExpression root = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        assertEquals(ArithmeticOperator.ADD, root.op());

        BinaryArithmeticExpression leftHalf = assertInstanceOf(BinaryArithmeticExpression.class, root.left());
        assertEquals(ArithmeticOperator.DIV, leftHalf.op());
        assertEquals(4, assertInstanceOf(ConstantExpression.class, leftHalf.right()).evaluate(null).asInt());

        BinaryArithmeticExpression aTimesGroup = assertInstanceOf(BinaryArithmeticExpression.class, leftHalf.left());
        assertEquals(ArithmeticOperator.MUL, aTimesGroup.op());
        assertEquals("a", assertInstanceOf(FieldNameExpression.class, aTimesGroup.left()).fieldName());

        BinaryArithmeticExpression bMinus17 = assertInstanceOf(BinaryArithmeticExpression.class, aTimesGroup.right());
        assertEquals(ArithmeticOperator.SUB, bMinus17.op());
        assertEquals("b", assertInstanceOf(FieldNameExpression.class, bMinus17.left()).fieldName());
        assertEquals(17, assertInstanceOf(ConstantExpression.class, bMinus17.right()).evaluate(null).asInt());

        BinaryArithmeticExpression rightHalf = assertInstanceOf(BinaryArithmeticExpression.class, root.right());
        assertEquals(ArithmeticOperator.MUL, rightHalf.op());
        assertEquals(22, assertInstanceOf(ConstantExpression.class, rightHalf.left()).evaluate(null).asInt());

        BinaryArithmeticExpression cPlusDiv = assertInstanceOf(BinaryArithmeticExpression.class, rightHalf.right());
        assertEquals(ArithmeticOperator.ADD, cPlusDiv.op());
        assertEquals("c", assertInstanceOf(FieldNameExpression.class, cPlusDiv.left()).fieldName());

        BinaryArithmeticExpression div50_25 = assertInstanceOf(BinaryArithmeticExpression.class, cPlusDiv.right());
        assertEquals(ArithmeticOperator.DIV, div50_25.op());
        assertEquals(50, assertInstanceOf(ConstantExpression.class, div50_25.left()).evaluate(null).asInt());
        assertEquals(25, assertInstanceOf(ConstantExpression.class, div50_25.right()).evaluate(null).asInt());
    }

    @Test
    public void testWildcardType() {
        Expression expr = parse("*");
        assertInstanceOf(WildcardExpression.class, expr);
        assertEquals("*", expr.toString());
    }

    @Test
    public void testWildcardArithmeticFailsEvaluation() {
        Expression expr = parse("* + 5");
        BinaryArithmeticExpression bin = assertInstanceOf(BinaryArithmeticExpression.class, expr);

        assertThrows(WildcardExpressionEvaluationException.class, () -> bin.evaluate(null));
    }

    @Test
    public void testWildcardInComplexExpression() {
        Expression expr = parse("(5 * *) + 1");
        BinaryArithmeticExpression root = assertInstanceOf(BinaryArithmeticExpression.class, expr);
        BinaryArithmeticExpression leftParen = assertInstanceOf(BinaryArithmeticExpression.class, root.left());

        assertInstanceOf(WildcardExpression.class, leftParen.right());
        assertThrows(WildcardExpressionEvaluationException.class, () -> leftParen.evaluate(null));
    }
}
