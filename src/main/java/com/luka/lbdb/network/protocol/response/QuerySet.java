package com.luka.lbdb.network.protocol.response;

import com.luka.lbdb.querying.virtualEntities.constant.Constant;
import com.luka.lbdb.records.schema.Schema;

import java.util.List;

/// Query response, containing a list of tuples and a schema.
/// Used for queries that return tuples, such as the SELECT query, and
/// the EXPLAIN query.
public record QuerySet(Schema schema, List<List<Constant>> tuples) implements Response { }
