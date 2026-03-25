package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.exceptions.NonNumericArithmeticCalculationException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import org.jetbrains.annotations.NotNull;

/// Unary arithmetic expressions encapsulate logic for calculating the constant value
/// a unary arithmetic AST can evaluate to. It has two components: the sub-expression
/// and the arithmetic operator.
public record UnaryArithmeticExpression(ArithmeticOperator op, Expression operand) implements Expression {
    /// Evaluates the sub-expression (can be recursive if it is also some sort of AST)
    /// and performs the arithmetic operation defined by the operator on the evaluated
    /// sub-expression. If the operand is NULL, returns NULL.
    @Override
    public Constant evaluate(Scan scan) {
        Constant val = operand.evaluate(scan);

        if (val.isNull()) {
            return NullConstant.INSTANCE;
        }

        if (!(val instanceof IntConstant intVal)) {
            throw new NonNumericArithmeticCalculationException();
        }

        return switch (op) {
            case SUB -> new IntConstant(-intVal.asInt());
            case ADD -> intVal;
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    public @NotNull String toString() {
        return "%s(%s)".formatted(op == ArithmeticOperator.SUB ? "-" : "+", operand);
    }
}
