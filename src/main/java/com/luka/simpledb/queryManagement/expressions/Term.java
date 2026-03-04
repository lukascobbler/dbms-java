package com.luka.simpledb.queryManagement.expressions;

import com.luka.simpledb.planningManagement.Plan;
import com.luka.simpledb.queryManagement.expressions.constants.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;

public class Term {
    private final Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
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
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();

            return Math.max(plan.distinctValues(lhsName), plan.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return plan.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return plan.distinctValues(rhsName);
        }

        if (lhs.asConstant().equals(rhs.asConstant())) {
            return 1;
        }

        return Integer.MAX_VALUE;
    }

    public Constant equatesWithConstant(String fieldName) {
        if (
            lhs.isFieldName() &&
            lhs.asFieldName().equals(fieldName) &&
            !rhs.isFieldName()
        ) {
            return rhs.asConstant();
        } else if (
            rhs.isFieldName() &&
            rhs.asFieldName().equals(fieldName) &&
            !lhs.isFieldName()
        ) {
            return lhs.asConstant();
        } else {
            return null;
        }
    }

    public String equatesWithFieldName(String fieldName) {
        if (
            lhs.isFieldName() &&
            lhs.asFieldName().equals(fieldName) &&
            !rhs.isFieldName()
        ) {
            return rhs.asFieldName();
        } else if (
            rhs.isFieldName() &&
            rhs.asFieldName().equals(fieldName) &&
            !lhs.isFieldName()
        ) {
            return lhs.asFieldName();
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return lhs.toString() + " = " + rhs.toString();
    }
}
