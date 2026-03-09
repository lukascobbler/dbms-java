package com.luka.simpledb.queryManagement.scanTypes.readOnly;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UnaryScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

import java.util.function.Function;

/// A rename read only scan represents the "rename" relational algebra operator.
/// It is a unary table read-only scan. The user specifies the old and the
/// new name for a given field, and from this scan upwards, that field
/// can only be accessed by the new name.
public class RenameReadOnlyScan extends UnaryScan {
    private final String oldFieldName, newFieldName;

    /// A rename scan requires the old and the new field name and a
    /// child scan.
    public RenameReadOnlyScan(Scan updateScan, String oldFieldName, String newFieldName) {
        super(updateScan);
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;
    }

    /// The field exists if it's either the new name for
    /// the renamed field, or any other field name.
    ///
    /// @return True if the renamed or any other field exists.
    @Override
    public boolean hasField(String fieldName) {
        String translated = map(fieldName);
        return translated != null && super.hasField(translated);
    }

    /// Maps the field name to the new one and calls the super class'
    /// retrieval function because this operation only adds the logic
    /// for field rename, it does not retrieve the actual value in any
    /// other way.
    ///
    /// @return The integer for the corresponding renamed or any other field.
    @Override
    protected int internalGetInt(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetInt);
    }

    /// Maps the field name to the new one and calls the super class'
    /// retrieval function because this operation only adds the logic
    /// for field rename, it does not retrieve the actual value in any
    /// other way.
    ///
    /// @return The string for the corresponding renamed or any other field.
    @Override
    protected String internalGetString(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetString);
    }

    /// Maps the field name to the new one and calls the super class'
    /// retrieval function because this operation only adds the logic
    /// for field rename, it does not retrieve the actual value in any
    /// other way.
    ///
    /// @return The boolean for the corresponding renamed or any other field.
    @Override
    protected boolean internalGetBoolean(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetBoolean);
    }

    /// Maps the field name to the new one and calls the super class'
    /// retrieval function because this operation only adds the logic
    /// for field rename, it does not retrieve the actual value in any
    /// other way.
    ///
    /// @return The constant for the corresponding renamed or any other field.
    @Override
    protected Constant internalGetValue(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetValue);
    }

    /// Does the actual mapping from `newFieldName` -> `oldFieldName`.
    /// If `oldFieldName` is passed as the argument, it returns null
    /// as a special case to indicate its non-existence.
    private String map(String fieldName) {
        if (fieldName.equals(newFieldName)) return oldFieldName;
        if (fieldName.equals(oldFieldName)) return null;
        return fieldName;
    }

    /// Maps the field name, and if its non-existent it errors out.
    /// Otherwise, it calls the passed function and returns its result.
    ///
    /// @return The result of the passed function.
    /// @throws FieldNotFoundInScanException if the old field name is passed as
    /// an argument.
    private <T> T wrapGetMapping(String fieldName, Function<String, T> sourceOperation) {
        String translated = map(fieldName);
        if (translated == null) {
            throw new FieldNotFoundInScanException(fieldName);
        }
        return sourceOperation.apply(translated);
    }
}