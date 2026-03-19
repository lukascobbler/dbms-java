package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.exceptions.ZeroDivisionException;
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
    /// @throws ZeroDivisionException if division by zero is done.
    public static Expression evaluate(Expression expr) {
        return switch (expr) {
            case BinaryArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) ->
                    foldBinary(evaluate(left), op, evaluate(right));
            case UnaryArithmeticExpression(ArithmeticOperator op, Expression operand) ->
                    foldUnary(op, evaluate(operand));
            default -> expr;
        };
    }

    /// Performs checks if both expressions are constant, does the
    /// arithmetic operation and returns the constant expression.
    /// Also, it checks for simple operations like multiplying by 1,
    /// multiplying by zero, subtracting value from itself,
    /// adding zero and in that case the calculation
    /// isn't even done, the result is just returned.
    ///
    /// @return The folded binary arithmetic expression.
    /// @throws ZeroDivisionException if division by zero is done.
    private static Expression foldBinary(Expression left, ArithmeticOperator op, Expression right) {
        if (left instanceof ConstantExpression(Constant lVal) &&
                right instanceof ConstantExpression(Constant rVal)) {

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
            return new ConstantExpression(new IntConstant(result));
        }

        if (op == ArithmeticOperator.ADD && right instanceof
                UnaryArithmeticExpression(var rOp, var rInner) && rOp == ArithmeticOperator.SUB) {
            return foldBinary(left, ArithmeticOperator.SUB, rInner);
        }

        if (op == ArithmeticOperator.SUB && right instanceof
                UnaryArithmeticExpression(var rOp, var rInner) && rOp == ArithmeticOperator.SUB) {
            return foldBinary(left, ArithmeticOperator.ADD, rInner);
        }

        if (op == ArithmeticOperator.MUL) {
            if (isExactConstant(left, 1)) return right;
            if (isExactConstant(right, 1)) return left;
            if (isExactConstant(left, 0) || isExactConstant(right, 0))
                return new ConstantExpression(new IntConstant(0));
            if (isExactConstant(right, -1)) return foldUnary(ArithmeticOperator.SUB, left);
        }

        if (op == ArithmeticOperator.ADD) {
            if (left instanceof UnaryArithmeticExpression(var lOp, var lInner) && lOp == ArithmeticOperator.SUB) {
                if (lInner.equals(right)) {
                    return new ConstantExpression(new IntConstant(0));
                }
            }

            if (isExactConstant(left, 0)) return right;
            if (isExactConstant(right, 0)) return left;
        }

        if (op == ArithmeticOperator.SUB) {
            if (isExactConstant(right, 0)) return left;
            if (left.equals(right)) {
                return new ConstantExpression(new IntConstant(0));
            }
        }

        return new BinaryArithmeticExpression(left, op, right);
    }

    /// Performs the obvious unary arithmetic operations
    /// like double negation, removing the + unary operation
    /// and constant folding.
    ///
    /// @return The folded unary arithmetic expression.
    private static Expression foldUnary(ArithmeticOperator op, Expression operand) {
        if (operand instanceof ConstantExpression(Constant c)) {
            return new ConstantExpression(new IntConstant(
                    op == ArithmeticOperator.SUB ? -c.asInt() : c.asInt()
            ));
        }

        if (op == ArithmeticOperator.ADD) {
            return operand;
        }

        if (op == ArithmeticOperator.SUB && operand instanceof UnaryArithmeticExpression(var innerOp, Expression innerOperand)) {
            if (innerOp == ArithmeticOperator.SUB) {
                return innerOperand;
            }
        }

        return new UnaryArithmeticExpression(op, operand);
    }

    /// @return Whether the expression is exactly `val`.
    private static boolean isExactConstant(Expression e, int val) {
        return e instanceof ConstantExpression(Constant c) && c.asInt() == val;
    }
}