package com.luka.simpledb.queryManagement.virtualEntities.expressions;

import com.luka.simpledb.queryManagement.exceptions.NonNumericArithmeticCalculationException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constants.*;
import com.luka.simpledb.recordManagement.Schema;

public record ArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) implements Expression {
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

    @Override
    public boolean appliesTo(Schema schema) {
        return left.appliesTo(schema) && right.appliesTo(schema);
    }

    @Override
    public String toString() {
        return "(%s %s %s)".formatted(left, op, right);
    }
}
