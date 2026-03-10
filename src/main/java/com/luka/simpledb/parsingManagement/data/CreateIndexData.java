package com.luka.simpledb.parsingManagement.data;

public record CreateIndexData(String indexName, String tableName, String fieldName) {
    @Override
    public String toString() {
        return "CREATE INDEX " + indexName + " ON " +
                tableName + " (" +
                fieldName + ");";
    }
}
