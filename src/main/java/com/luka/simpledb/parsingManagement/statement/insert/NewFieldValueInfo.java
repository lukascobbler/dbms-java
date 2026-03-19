package com.luka.simpledb.parsingManagement.statement.insert;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

/// Holds information about one field's name, and it's new value.
public record NewFieldValueInfo(String fieldName, Constant newValue) { }
