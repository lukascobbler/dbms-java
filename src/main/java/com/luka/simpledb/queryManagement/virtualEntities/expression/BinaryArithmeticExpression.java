package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.exceptions.NonNumericArithmeticCalculationException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.*;
import com.luka.simpledb.recordManagement.Schema;

/// Binary arithmetic expressions encapsulate logic for calculating the constant value
/// a binary arithmetic AST can evaluate to. It has three components: the left sub-expression,
/// the arithmetic operator and the right sub-expression.
public record BinaryArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) implements Expression {
    /// Evaluates the left and right sub-expressions (can be recursive if they are also
    /// some sort of AST) and performs the arithmetic operation defined by the operator
    /// on the evaluated sub-expressions.
    @Override
    public Constant evaluate(Scan scan) {
        Constant lVal = left.evaluate(scan);
        Constant rVal = right.evaluate(scan);

        if (!(lVal instanceof IntConstant) || !(rVal instanceof IntConstant)) {
            throw new NonNumericArithmeticCalculationException();
        }

        int result = switch (op) {
            case ADD -> lVal.asInt() + rVal.asInt();
            case SUB -> lVal.asInt() - rVal.asInt();
            case MUL -> lVal.asInt() * rVal.asInt();
            case DIV -> lVal.asInt() / rVal.asInt();
        };

        return new IntConstant(result);
    }

    /// @return True if both sub-expressions apply to the given schema. Will
    /// return false only if some of the sub-expressions in the tree contain
    /// a field name that is not part of the given schema.
    @Override
    public boolean appliesTo(Schema schema) {
        return left.appliesTo(schema) && right.appliesTo(schema);
    }

    /// A binary arithmetic expression is only constant if both sub-expressions
    /// are constant.
    ///
    /// @return True if both sub-expressions are constant.
    @Override
    public boolean isConstant() {
        return left.isConstant() && right.isConstant();
    }

    @Override
    public String toString() {
        return "(%s %s %s)".formatted(left, op, right);
    }
}
