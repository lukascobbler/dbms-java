package com.luka.lbdb.querying.virtualEntities.expression;

import com.luka.lbdb.querying.exceptions.RuntimeExecutionException;
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
    /// @throws RuntimeExecutionException if any of the operands is not an integer,
    /// if division by zero is performed or if overflowing occurs.
    @Override
    public Constant evaluate(Scan scan) {
        Constant lVal = left.evaluate(scan);
        Constant rVal = right.evaluate(scan);

        if (lVal.isNull() || rVal.isNull()) {
            return NullConstant.INSTANCE;
        }

        if (!(lVal instanceof IntConstant) || !(rVal instanceof IntConstant)) {
            throw new RuntimeExecutionException();
        }

        try {
            int l = lVal.asInt();
            int r = rVal.asInt();

            int result = switch (op) {
                case ADD -> Math.addExact(l, r);
                case SUB -> Math.subtractExact(l, r);
                case MUL -> Math.multiplyExact(l, r);
                case DIV -> {
                    if (l == Integer.MIN_VALUE && r == -1) {
                        throw new ArithmeticException("integer overflow");
                    }
                    yield l / r;
                }
                case POWER -> {
                    double p = Math.pow(l, r);
                    if (p > Integer.MAX_VALUE || p < Integer.MIN_VALUE || Double.isInfinite(p)) {
                        throw new ArithmeticException("integer overflow");
                    }
                    yield (int) p;
                }
            };

            return new IntConstant(result);
        } catch (ArithmeticException e) {
            throw new RuntimeExecutionException(e.getMessage());
        }
    }

    @Override
    public @NotNull String toString() {
        return "(%s %s %s)".formatted(left, op, right);
    }
}
