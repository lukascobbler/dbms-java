package com.luka.lbdb.querying.virtualEntities.expression;

import com.luka.lbdb.querying.exceptions.NonNumericArithmeticCalculationException;
import com.luka.lbdb.querying.exceptions.ZeroDivisionException;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.querying.virtualEntities.constant.*;
import org.jetbrains.annotations.NotNull;

/// Binary arithmetic expressions encapsulate logic for calculating the constant value
/// a binary arithmetic AST can evaluate to. It has three components: the left sub-expression,
/// the arithmetic operator and the right sub-expression.
public record BinaryArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) implements Expression {
    /// Evaluates the left and right sub-expressions (can be recursive if they are also
    /// some sort of AST) and performs the arithmetic operation defined by the operator
    /// on the evaluated sub-expressions. If any of the operands is NULL, returns NULL;
    ///
    /// @return The evaluated value as a constant.
    /// @throws NonNumericArithmeticCalculationException if any of the operands is not an integer.
    /// @throws ZeroDivisionException if division by zero is done.
    @Override
    public Constant evaluate(Scan scan) {
        Constant lVal = left.evaluate(scan);
        Constant rVal = right.evaluate(scan);

        if (lVal.isNull() || rVal.isNull()) {
            return NullConstant.INSTANCE;
        }

        if (!(lVal instanceof IntConstant) || !(rVal instanceof IntConstant)) {
            throw new NonNumericArithmeticCalculationException();
        }

        // todo overflow exceptions
        int result = switch (op) {
            case ADD -> lVal.asInt() + rVal.asInt();
            case SUB -> lVal.asInt() - rVal.asInt();
            case MUL -> lVal.asInt() * rVal.asInt();
            case DIV -> {
                if (rVal.asInt() == 0) {
                    throw new ZeroDivisionException();
                }
                yield lVal.asInt() / rVal.asInt();
            }
            case POWER -> (int) Math.pow(lVal.asInt(), rVal.asInt());
        };

        return new IntConstant(result);
    }

    @Override
    public @NotNull String toString() {
        return "(%s %s %s)".formatted(left, op, right);
    }
}
