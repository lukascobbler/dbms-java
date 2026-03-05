package com.luka.simpledb.queryManagement.virtualEntities.expressions;

import com.luka.simpledb.queryManagement.virtualEntities.constants.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.constants.IntConstant;

public class PartialEvaluator {
    public static Expression evaluate(Expression expr) {
        return switch (expr) {
            case ArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) ->
                    fold(evaluate(left), op, evaluate(right));
            default -> expr;
        };
    }

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

    private static boolean isConstant(Expression e, int val) {
        return e instanceof ConstantExpression(Constant c) && c.asInt() == val;
    }
}