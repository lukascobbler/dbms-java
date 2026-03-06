package com.luka.simpledb.queryManagement.virtualEntities.constant;

public record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }
    @Override public boolean equals(Object obj) { return false; }
    @Override public int hashCode() { return 0; }
}