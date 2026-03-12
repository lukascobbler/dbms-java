package com.luka.simpledb.parsingManagement.statement.update;

import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

public record NewFieldExpressionAssignment(String fieldName, Expression newValueExpression) {
    @Override
    public String toString() {
        return fieldName + " = " +
                newValueExpression.toString();
    }
}
