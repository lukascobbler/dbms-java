package com.luka.simpledb.parsingManagement.statement;

public record CreateViewStatement(String viewName, SelectStatement selectStatement) implements Statement {
    @Override
    public String toString() {
        return "CREATE VIEW " +
                viewName +
                " AS " +
                selectStatement.toString();
    }
}
