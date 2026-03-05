package com.luka.simpledb.queryManagement.virtualEntities.constant;

import com.luka.simpledb.queryManagement.exceptions.IncompatibleConstantTypeException;

public sealed interface Constant extends Comparable<Constant>
        permits IntConstant, StringConstant, BooleanConstant, NullConstant {

    @Override
    default int compareTo(Constant other) {
        if (this.getClass() != other.getClass()) {
            throw new IncompatibleConstantTypeException();
        }

        return switch (this) {
            case IntConstant i -> i.value().compareTo(((IntConstant) other).value());
            case StringConstant s -> s.value().compareTo(((StringConstant) other).value());
            case BooleanConstant b -> b.value().compareTo(((BooleanConstant) other).value());
            case NullConstant n -> 0;
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
        if (this instanceof BooleanConstant(Boolean value)) return value;
        throw new IncompatibleConstantTypeException();
    }
}