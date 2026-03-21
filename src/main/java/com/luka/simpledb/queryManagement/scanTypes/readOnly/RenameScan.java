package com.luka.simpledb.queryManagement.scanTypes.readOnly;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UnaryScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

import java.util.Map;
import java.util.function.Function;

/// A rename read only scan represents the "rename" relational algebra operator.
/// It is a unary table read-only scan. The user specifies the map from new field
/// names to old field names and from this scan upward, renamed fields can only
/// be accessed through the new name.
public class RenameScan extends UnaryScan {
    private final Map<String, String> fieldNameMapper;

    /// A rename scan requires the field name mapping and a
    /// child scan.
    public RenameScan(Scan updateScan, Map<String, String> newToOldMapper) {
        super(updateScan);
        this.fieldNameMapper = newToOldMapper;
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

    /// Does the actual mapping from the new field name to the old field name.
    private String map(String fieldName) {
        if (!fieldNameMapper.containsKey(fieldName)) {
            if (fieldNameMapper.containsValue(fieldName)) return null;
            return fieldName;
        }
        return fieldNameMapper.get(fieldName);
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