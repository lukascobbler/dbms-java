package com.luka.simpledb.queryManagement.virtualEntities.constant;

/// A boolean constant value implementation.
public record BooleanConstant(Boolean value) implements Constant {
    @Override public String toString() { return value.toString(); }
}
