package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.metadataManagement.infoClasses.IndexType;

/// Represents the parsed data of `CREATE INDEX` queries.
/// `CREATE INDEX` queries need the index name, the table
/// that index is created for and the field that index is
/// created for.
public record CreateIndexStatement(String indexName, String tableName, String fieldName, IndexType type)
        implements Statement {
    @Override
    public String toString() {
        return "CREATE INDEX " + indexName + " ON " +
                tableName + " (" +
                fieldName + ") TYPE " +
                type.name() +
                ";";
    }
}
