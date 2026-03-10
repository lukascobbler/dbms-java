package com.luka.simpledb.parsingManagement.data;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

public record DeleteData(String tableName, Predicate predicate) {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("DELETE FROM ");
        result.append(tableName);

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        result.append(';');

        return result.toString();
    }
}
