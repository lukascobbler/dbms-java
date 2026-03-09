package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;

/// A set of algorithms that performs obvious calculations on arithmetic
/// expressions thus reducing the need for them to be calculated on the
/// database virtual machine.
public class PartialEvaluator {
    /// Reduces the number of steps for a given expression.
    /// For example: `3 + 5` is a trivial operation that can
    /// be calculated before the expression goes further down the
    /// pipeline where it's more expensive to calculate it.
    /// On encountering a non-arithmetic expression,
    /// it stops the folding process.
    ///
    /// @return The evaluated arithmetic expression.
    public static Expression evaluate(Expression expr) {
        return switch (expr) {
            case ArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) ->
                    fold(evaluate(left), op, evaluate(right));
            default -> expr;
        };
    }

    /// Performs checks if both expressions are constant, does the
    /// arithmetic operation and returns the constant expression.
    /// Also, it checks for simple operations like multiplying by 1,
    /// multiplying by zero, adding zero and in that case the calculation
    /// isn't even done, the result is just returned.
    ///
    /// @return The folded arithmetic expression.
    private static Expression fold(Expression left, ArithmeticOperator op, Expression right) {
        if (left instanceof ConstantExpression(Constant lVal) &&
                right instanceof ConstantExpression(Constant rVal)) {

            int result = switch (op) {
                case ADD -> lVal.asInt() + rVal.asInt();
                case SUB -> lVal.asInt() - rVal.asInt();
                case MUL -> lVal.asInt() * rVal.asInt();
                case DIV -> lVal.asInt() / rVal.asInt();
            };
            return new ConstantExpression(new IntConstant(result));
        }

        if (op == ArithmeticOperator.MUL) {
            if (isConstant(left, 1)) return right;
            if (isConstant(right, 1)) return left;
            if (isConstant(left, 0) || isConstant(right, 0))
                return new ConstantExpression(new IntConstant(0));
        }

        if (op == ArithmeticOperator.ADD) {
            if (isConstant(left, 0)) return right;
            if (isConstant(right, 0)) return left;
        }

        return new ArithmeticExpression(left, op, right);
    }

    /// @return Whether the expression is exactly `val`.
    private static boolean isConstant(Expression e, int val) {
        return e instanceof ConstantExpression(Constant c) && c.asInt() == val;
    }
}