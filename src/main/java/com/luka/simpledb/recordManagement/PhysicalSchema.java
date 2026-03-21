package com.luka.simpledb.recordManagement;

import com.luka.simpledb.recordManagement.exceptions.FieldDuplicateNameException;
import com.luka.simpledb.recordManagement.exceptions.FieldLimitException;

/// A schema object that introduces the logic for limiting the number of
/// fields to the physical limit of the system as well as limiting duplicate
/// names.
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
        super.addField(fieldName, type, length, isNullable);
    }
}
