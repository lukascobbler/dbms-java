package com.luka.simpledb.queryManagement.virtualEntities.constant;

public record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }
}