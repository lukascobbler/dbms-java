package com.luka.simpledb.parsingManagement.statement;

public sealed interface Statement permits
        SelectStatement, UpdateStatement, DeleteStatement,
        InsertStatement, CreateTableStatement, CreateIndexStatement,
        CreateViewStatement {}