package com.luka.simpledb.queryManagement.expressions.constants;

public record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }
}