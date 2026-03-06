package com.luka.simpledb.queryManagement.virtualEntities.term;

import com.luka.simpledb.planningManagement.Plan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.recordManagement.Schema;

public class Term {
    private final Expression lhs, rhs;
    private final TermOperator termOperator;

    public Term(Expression lhs, TermOperator termOperator, Expression rhs) {
        this.lhs = PartialEvaluator.evaluate(lhs);
        this.termOperator = termOperator;
        this.rhs = PartialEvaluator.evaluate(rhs);
    }

    public boolean isSatisfied(Scan scan) {
        Constant rhsValue = rhs.evaluate(scan);
        Constant lhsValue = lhs.evaluate(scan);

        if (lhsValue instanceof NullConstant || rhsValue instanceof NullConstant) {
            return false;
        }

        return switch (termOperator) {
            case EQUALS -> lhsValue.equals(rhsValue);
            case NOT_EQUALS -> !lhsValue.equals(rhsValue);
            case GREATER_THAN -> lhsValue.compareTo(rhsValue) > 0;
            case LESS_THAN -> lhsValue.compareTo(rhsValue) < 0;
            case GREATER_OR_EQUAL -> lhsValue.compareTo(rhsValue) >= 0;
            case LESS_OR_EQUAL -> lhsValue.compareTo(rhsValue) <= 0;
        };
    }

    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    public int reductionFactor(Plan plan) {
        if (termOperator != TermOperator.EQUALS) {
            return 2;
        }

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
                case FieldNameExpression(String rightFieldName) -> {
                    if (leftConstant instanceof NullConstant) yield Integer.MAX_VALUE;
                    yield plan.distinctValues(rightFieldName);
                }
                case ConstantExpression(Constant rightConstant) ->
                        leftConstant.equals(rightConstant) ? 1 : Integer.MAX_VALUE;
                default -> Integer.MAX_VALUE;
            };
            default -> Integer.MAX_VALUE;
        };
    }

    public Constant equatesWithConstant(String fieldName) {
        if (termOperator != TermOperator.EQUALS) return null;

        return switch (new Pair(lhs, rhs)) {
            case Pair(FieldNameExpression(String f), ConstantExpression(Constant c)) when f.equals(fieldName) -> c;
            case Pair(ConstantExpression(Constant c), FieldNameExpression(String f)) when f.equals(fieldName) -> c;
            default -> null;
        };
    }

    public String equatesWithFieldName(String fieldName) {
        if (termOperator != TermOperator.EQUALS) return null;

        return switch (new Pair(lhs, rhs)) {
            case Pair(FieldNameExpression(String f1), FieldNameExpression(String f2)) when f1.equals(fieldName) -> f2;
            case Pair(FieldNameExpression(String f1), FieldNameExpression(String f2)) when f2.equals(fieldName) -> f1;
            default -> null;
        };
    }

    private record Pair(Expression left, Expression right) {}

    @Override
    public String toString() {
        return lhs.toString() + " " + termOperator.toString() + " " + rhs.toString();
    }
}
