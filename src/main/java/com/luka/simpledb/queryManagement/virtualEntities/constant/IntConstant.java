package com.luka.simpledb.queryManagement.virtualEntities.constant;

/// An integer constant value implementation.
public record IntConstant(Integer value) implements Constant {
    @Override public String toString() { return value.toString(); }
}
