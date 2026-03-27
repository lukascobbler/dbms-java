package com.luka.lbdb.parsing.tokenizer.token;

/// Group of tokens that describe different keywords supported by the
/// database.
public enum KeywordToken implements Token {
    SELECT, FROM, WHERE, AND, INSERT, INTO, VALUES,
    DELETE, UPDATE, SET, CREATE, TABLE, VARCHAR,
    INT, VIEW, AS, INDEX, ON, NULL, TRUE, FALSE,
    IS, NOT, BOOLEAN, JOIN, UNION, EXPLAIN, BTREE, HASH,
    TYPE, ALL, START, TRANSACTION, COMMIT, ROLLBACK
}