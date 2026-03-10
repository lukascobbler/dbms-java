package com.luka.simpledb.parsingManagement.data;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

public record UpdateData(String tableName, String fieldName, Expression newValue, Predicate predicate) {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("UPDATE ");
        result.append(tableName);
        result.append(" SET ");

        result.append(fieldName).append(" = ");
        result.append(newValue.toString());

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        result.append(';');

        return result.toString();
    }
}
