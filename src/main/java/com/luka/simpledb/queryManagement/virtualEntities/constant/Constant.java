package com.luka.simpledb.queryManagement.virtualEntities.constant;

import com.luka.simpledb.queryManagement.exceptions.IncompatibleConstantTypeException;
import com.luka.simpledb.queryManagement.exceptions.NullComparisonException;

import static java.sql.Types.*;

/// A constant represents a generic value whose concrete type
/// is not known until checking. It is an abstraction over all the
/// types a database engine implements along with any special type
/// like the NULL value. It is comparable to other constants via the
/// `Comparable` interface and can be converted to a concrete Java type
///  when an outside interface like JDBC needs to provide a Java
/// type to the API caller.
public sealed interface Constant extends Comparable<Constant>
        permits IntConstant, StringConstant, BooleanConstant, NullConstant {

    /// @return A value that tells the caller how do two constants compare.
    /// @throws IncompatibleConstantTypeException if two constants of different
    /// types are compared.
    /// @throws NullComparisonException if a value is compared to null. Cases
    /// like these should be handled elsewhere.
    @Override
    default int compareTo(Constant other) {
        if (this.getClass() != other.getClass()) {
            throw new IncompatibleConstantTypeException();
        }

        return switch (this) {
            case IntConstant i -> i.value().compareTo(((IntConstant) other).value());
            case StringConstant s -> s.value().compareTo(((StringConstant) other).value());
            case BooleanConstant b -> b.value().compareTo(((BooleanConstant) other).value());
            case NullConstant n -> throw new NullComparisonException();
        };
    }

    /// @return The constant as a Java integer.
    /// @throws IncompatibleConstantTypeException if the constant does not
    /// represent an integer value.
    default int asInt() {
        if (this instanceof IntConstant(Integer value)) return value;
        throw new IncompatibleConstantTypeException();
    }

    /// @return The constant as a Java string.
    /// @throws IncompatibleConstantTypeException if the constant does not
    /// represent a string value.
    default String asString() {
        if (this instanceof StringConstant(String value)) return value;
        throw new IncompatibleConstantTypeException();
    }

    /// @return The constant as a Java boolean.
    /// @throws IncompatibleConstantTypeException if the constant does not
    /// represent a boolean value.
    default boolean asBoolean() {
        if (this instanceof BooleanConstant(Boolean value)) return value;
        throw new IncompatibleConstantTypeException();
    }

    /// @return The SQL integer representing the type of this constant.
    default int type() {
        return switch (this) {
            case BooleanConstant b -> BOOLEAN;
            case IntConstant i -> INTEGER;
            case NullConstant n -> NULL;
            case StringConstant s -> VARCHAR; // todo combine length and type into a custom enum wrapper for safety
        };
    }

    /// @return The length of the field.
    default int length() {
        return switch (this) {
            case BooleanConstant b -> 1;
            case IntConstant i -> 4;
            case NullConstant n -> 0;
            case StringConstant s -> s.asString().length();
        };
    }
}