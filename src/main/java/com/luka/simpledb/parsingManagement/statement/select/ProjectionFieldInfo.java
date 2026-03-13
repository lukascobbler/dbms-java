package com.luka.simpledb.parsingManagement.statement.select;

import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.Objects;

/// A field that should be sent to the user (a projected field) can be a virtual field,
/// or a real field directly from the table on the disk. Both of those types of fields
/// need to be processed in the same way and this is the record that describes one
/// projected field.
public record ProjectionFieldInfo(String name, Expression expression) {
    @Override
    public String toString() {
        String exprStr = expression.toString();
        if (exprStr.equals(name)) {
            return exprStr;
        }
        return exprStr + " AS " + name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression);
    }
}
