package com.luka.simpledb.queryManagement.virtualEntities;

import com.luka.simpledb.planningManagement.Plan;
import com.luka.simpledb.queryManagement.virtualEntities.constants.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.expressions.*;
import com.luka.simpledb.recordManagement.Schema;

public class Term {
    private final Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = PartialEvaluator.evaluate(lhs);
        this.rhs = PartialEvaluator.evaluate(rhs);
    }

    public boolean isSatisfied(Scan scan) {
        Constant rhsValue = rhs.evaluate(scan);
        Constant lhsValue = lhs.evaluate(scan);

        return rhsValue.equals(lhsValue);
    }

    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    public int reductionFactor(Plan plan) {
        return switch (lhs) {
            case FieldNameExpression(String leftFieldName) -> switch (rhs) {
                case FieldNameExpression(String rightFieldName) ->
                        Math.max(plan.distinctValues(leftFieldName), plan.distinctValues(rightFieldName));
                case ConstantExpression(Constant constant) ->
                        plan.distinctValues(leftFieldName);
                case ArithmeticExpression(Expression left, ArithmeticOperator op, Expression right) ->
                        plan.distinctValues(leftFieldName);
            };
            case ConstantExpression(Constant leftConstant) -> switch (rhs) {
                case FieldNameExpression(String rightFieldName) ->
                        plan.distinctValues(rightFieldName);
                case ConstantExpression(Constant rightConstant) ->
                        leftConstant.equals(rightConstant) ? 1 : Integer.MAX_VALUE;
                default -> Integer.MAX_VALUE;
            };
            default -> Integer.MAX_VALUE;
        };
    }

    public Constant equatesWithConstant(String fieldName) {
        if (lhs instanceof FieldNameExpression(String f) && f.equals(fieldName) && rhs instanceof ConstantExpression(Constant c)) {
            return c;
        }
        if (rhs instanceof FieldNameExpression(String f) && f.equals(fieldName) && lhs instanceof ConstantExpression(Constant c)) {
            return c;
        }
        return null;
    }

    public String equatesWithFieldName(String fieldName) {
        if (lhs instanceof FieldNameExpression(String f1) && f1.equals(fieldName) && rhs instanceof FieldNameExpression(String f2)) {
            return f2;
        }
        if (rhs instanceof FieldNameExpression(String f1) && f1.equals(fieldName) && lhs instanceof FieldNameExpression(String f2)) {
            return f2;
        }
        return null;
    }

    @Override
    public String toString() {
        return lhs.toString() + " = " + rhs.toString();
    }
}
