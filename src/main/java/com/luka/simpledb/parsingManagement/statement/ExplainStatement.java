package com.luka.simpledb.parsingManagement.statement;

public record ExplainStatement(Statement explainingStatement) implements Statement {
    @Override public String toString() {
        return "EXPLAIN " + explainingStatement.toString();
    }
}
