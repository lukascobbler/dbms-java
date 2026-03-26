package com.luka.lbdb.parsing.statement.insert;

import com.luka.lbdb.querying.virtualEntities.constant.Constant;

/// Holds information about one field's name, and it's new value.
public record NewFieldValueInfo(String fieldName, Constant newValue) { }
