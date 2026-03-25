package com.luka.simpledb.parsingManagement.statement;

/// A statement represents the parsed query input from the user
/// into data structures that the system uses to execute it.
/// It is represented as a sealed interface to allow easier
/// structure matching using `switch`. Only the `toString` method
/// is required for its implementors, and the string conversion
/// should be implemented as "unparsing" the data to the original
/// query.
public sealed interface Statement permits
        SelectStatement, UpdateStatement, DeleteStatement,
        InsertStatement, CreateTableStatement, CreateIndexStatement,
        ExplainStatement {
    /// Unparses a statement into its original string form. May produce
    /// some differences because of how expressions and predicate order
    /// is handled during the parsing process.
    @Override
    String toString();
}