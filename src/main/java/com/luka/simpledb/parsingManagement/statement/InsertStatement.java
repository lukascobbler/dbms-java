package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.parsingManagement.statement.insert.NewFieldValueInfo;

import java.util.List;

/// Represents the parsed data of `INSERT` queries.
/// `INSERT` queries need the table they are inserting into,
/// a list of fields, and a list of constant values that these
/// fields should be set to in the new row.
public record InsertStatement(String tableName, List<NewFieldValueInfo> newFieldValues) implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(tableName).append(" ");

        result.append('(');
        for (NewFieldValueInfo field : newFieldValues)
            result.append(field.fieldName()).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2));
        result.append(')');

        result.append(" VALUES (");
        for (NewFieldValueInfo field : newFieldValues)
            result.append(field.newValue().toString()).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2));
        result.append(')');

        result.append(';');

        return result.toString();
    }
}
