package com.luka.simpledb.queryManagement.virtualEntities.constants;

public record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }
}