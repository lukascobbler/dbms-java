package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.exceptions.WildcardExpressionEvaluationException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.Schema;

public record WildcardExpression() implements Expression {
    /// A wildcard expression can't be evaluated.
    ///
    /// @throws WildcardExpressionEvaluationException on every scan.
    @Override
    public Constant evaluate(Scan scan) {
        throw new WildcardExpressionEvaluationException();
    }

    /// A wildcard expression applies to every schema.
    ///
    /// @return True.
    @Override
    public boolean appliesTo(Schema schema) {
        return true;
    }

    /// A wildcard expression is never a constant.
    ///
    /// @return False.
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String toString() {
        return "*";
    }
}
