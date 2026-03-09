package com.luka.simpledb.queryManagement.virtualEntities.constant;

/// A NULL constant value implementation.
public record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }

    /// Global null constant instance to prevent unnecessary allocations.
    public static final NullConstant INSTANCE = new NullConstant();
}