package com.luka.lbdb.network.protocol.response;

/// If the system couldn't process a statement, the error is retuned
/// as a string.
public record ErrorResponse(String error) implements Response { }
