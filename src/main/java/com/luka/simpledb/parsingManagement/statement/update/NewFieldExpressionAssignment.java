package com.luka.simpledb.parsingManagement.statement.update;

import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

/// A small record to unify the field name to the new value
/// it should be for `UPDATE` queries.
public record NewFieldExpressionAssignment(String fieldName, Expression newValueExpression) {
    @Override
    public String toString() {
        return fieldName + " = " +
                newValueExpression.toString();
    }
}
