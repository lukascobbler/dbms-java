package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.exceptions.WildcardExpressionEvaluationException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

import java.util.Optional;

public record WildcardExpression(Optional<String> rangeVariableName) implements Expression {
    /// Initialization with no range variable.
    public WildcardExpression() {
        this(Optional.empty());
    }

    /// Initialization with a range variable.
    public WildcardExpression(String rangeVariableName) {
        this(Optional.of(rangeVariableName));
    }

    /// A wildcard expression can't be evaluated.
    ///
    /// @throws WildcardExpressionEvaluationException on every scan.
    @Override
    public Constant evaluate(Scan scan) {
        throw new WildcardExpressionEvaluationException();
    }

    @Override
    public String toString() {
        return rangeVariableName.map(s -> s + ".").orElse("") + "*";
    }
}
