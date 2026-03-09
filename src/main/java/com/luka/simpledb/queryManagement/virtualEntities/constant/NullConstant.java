package com.luka.simpledb.queryManagement.virtualEntities.constant;

/// A NULL constant value implementation.
public record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }
}