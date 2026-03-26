package com.luka.lbdb.parsing.statement.update;

import com.luka.lbdb.querying.virtualEntities.expression.Expression;
import org.jetbrains.annotations.NotNull;

/// A small record to unify the field name to the new value
/// it should be for `UPDATE` queries.
public record NewFieldExpressionAssignment(String fieldName, Expression newValueExpression) {
    @Override
    public @NotNull String toString() {
        return fieldName + " = " +
                newValueExpression.toString();
    }
}
