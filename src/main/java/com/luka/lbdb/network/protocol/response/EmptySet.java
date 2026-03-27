package com.luka.lbdb.network.protocol.response;

/// Rows affected response. Used for queries that modify data.
public record EmptySet(int rowsAffected) implements Response { }
