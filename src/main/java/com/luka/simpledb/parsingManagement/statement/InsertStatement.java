package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

import java.util.List;

public record InsertStatement(String tableName, List<String> fields, List<Constant> values) implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(tableName);

        result.append('(');
        for (String fieldName : fields)
            result.append(fieldName).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2));
        result.append(')');

        result.append(" VALUES (");
        for (Constant value : values)
            result.append(value.toString()).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2));
        result.append(')');

        result.append(';');

        return result.toString();
    }
}
