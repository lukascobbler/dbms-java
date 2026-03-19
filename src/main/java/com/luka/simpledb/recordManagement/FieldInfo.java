package com.luka.simpledb.recordManagement;

/// Represents everything that can describe an arbitrary field.
public record FieldInfo(int type, int length, boolean nullable) {}