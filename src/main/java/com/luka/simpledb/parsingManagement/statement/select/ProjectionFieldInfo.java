package com.luka.simpledb.parsingManagement.statement.select;

import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.Objects;

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
