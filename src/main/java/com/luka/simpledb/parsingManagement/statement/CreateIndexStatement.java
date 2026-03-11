package com.luka.simpledb.parsingManagement.statement;

public record CreateIndexStatement(String indexName, String tableName, String fieldName) implements Statement {
    @Override
    public String toString() {
        return "CREATE INDEX " + indexName + " ON " +
                tableName + " (" +
                fieldName + ");";
    }
}
