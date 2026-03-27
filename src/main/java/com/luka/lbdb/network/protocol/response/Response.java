package com.luka.lbdb.network.protocol.response;

/// Different types of responses from the server.
public sealed interface Response permits EmptySet, QuerySet, ErrorResponse { }
