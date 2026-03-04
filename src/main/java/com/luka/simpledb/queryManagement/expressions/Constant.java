package com.luka.simpledb.queryManagement.expressions;

import com.luka.simpledb.queryManagement.exceptions.IncompatibleConstantTypeException;

public sealed interface Constant extends Comparable<Constant>
        permits IntConstant, StringConstant, BoolConstant, NullConstant {

    @Override
    default int compareTo(Constant other) {
        if (this.getClass() != other.getClass()) {
            throw new IncompatibleConstantTypeException();
        }

        return switch (this) {
            case IntConstant i    -> i.value().compareTo(((IntConstant) other).value());
            case StringConstant s -> s.value().compareTo(((StringConstant) other).value());
            case BoolConstant b   -> b.value().compareTo(((BoolConstant) other).value());
            case NullConstant n   -> 0;
        };
    }

    default int asInt() {
        if (this instanceof IntConstant(Integer value)) return value;
        throw new IncompatibleConstantTypeException();
    }

    default String asString() {
        if (this instanceof StringConstant(String value)) return value;
        throw new IncompatibleConstantTypeException();
    }

    default boolean asBoolean() {
        if (this instanceof BoolConstant(Boolean value)) return value;
        throw new IncompatibleConstantTypeException();
    }
}

record IntConstant(Integer value) implements Constant {}
record StringConstant(String value) implements Constant {}
record BoolConstant(Boolean value) implements Constant {}
record NullConstant() implements Constant {
    @Override public String toString() { return "null"; }
}