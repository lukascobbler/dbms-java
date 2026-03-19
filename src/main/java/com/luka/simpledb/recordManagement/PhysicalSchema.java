package com.luka.simpledb.recordManagement;

import com.luka.simpledb.recordManagement.exceptions.FieldDuplicateNameException;
import com.luka.simpledb.recordManagement.exceptions.FieldLimitException;

/// A schema object that introduces the logic for limiting the number of
/// fields to the physical limit of the system.
public class PhysicalSchema extends Schema {
    public static final int MAX_FIELDS = 31;

    /// Generic field adder, can accept any SQL type (can be dangerous) along with
    /// the length of that type.
    ///
    /// @throws FieldLimitException if the maximum number of fields is reached.
    /// @throws FieldDuplicateNameException if the field already exists within this schema.
    @Override
    public void addField(String fieldName, int type, int length, boolean isNullable) {
        if (fields.size() + 1 > MAX_FIELDS) {
            throw new FieldLimitException();
        }
        if (fields.contains(fieldName)) {
            throw new FieldDuplicateNameException();
        }
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(type, length, isNullable));
    }

    /// Add a field described by its name from some other schema.
    ///
    /// @throws FieldLimitException if the maximum number of fields is reached.
    @Override
    public void add(String fieldName, Schema otherSchema) {
        if (fields.size() + 1 > MAX_FIELDS) {
            throw new FieldLimitException();
        }
        int type = otherSchema.type(fieldName);
        int length = otherSchema.length(fieldName);
        boolean isNullable = otherSchema.isNullable(fieldName);
        addField(fieldName, type, length, isNullable);
    }
}
