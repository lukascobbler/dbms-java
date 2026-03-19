package com.luka.simpledb.recordManagement;

import com.luka.simpledb.recordManagement.exceptions.FieldDuplicateNameException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.*;

/// `Schema` objects hold information about a table's field names, lengths
/// and types. Does not limit the number of records per field.
public class Schema {
    protected final List<String> fields = new ArrayList<>();
    protected final Map<String, FieldInfo> info = new HashMap<>();

    /// Generic field adder, can accept any SQL type (can be dangerous) along with
    /// the length of that type.
    ///
    /// @throws FieldDuplicateNameException if the field already exists within this schema.
    public void addField(String fieldName, int type, int length, boolean isNullable) {
        if (fields.contains(fieldName)) {
            throw new FieldDuplicateNameException();
        }
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(type, length, isNullable));
    }

    /// Add an integer field.
    public void addIntField(String fieldName, boolean isNullable) {
        addField(fieldName, INTEGER, 4, isNullable);
    }

    /// Add a string field with the maximum length (VARCHAR type).
    public void addStringField(String fieldName, int length, boolean isNullable) {
        addField(fieldName, VARCHAR, length, isNullable);
    }

    /// Add a boolean field.
    public void addBooleanField(String fieldName, boolean isNullable) {
        addField(fieldName, BOOLEAN, 1, isNullable);
    }

    /// Add a field described by its name from some other schema.
    public void add(String fieldName, Schema otherSchema) {
        int type = otherSchema.type(fieldName);
        int length = otherSchema.length(fieldName);
        boolean isNullable = otherSchema.isNullable(fieldName);
        addField(fieldName, type, length, isNullable);
    }

    /// Add all fields from other schema to this schema.
    public void addAll(Schema otherSchema) {
        for (String fieldName : otherSchema.getFields()) {
            add(fieldName, otherSchema);
        }
    }

    /// @return The list of field names from this schema.
    public List<String> getFields() {
        return fields;
    }

    /// @return Whether this schema contains a field described by its name.
    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    /// @return The type of the field described by its name from the SQL constants.
    public int type(String fieldName) {
        return info.get(fieldName).type();
    }

    /// @return The length of the field described by its name.
    public int length(String fieldName) {
        return info.get(fieldName).length();
    }

    /// @return Whether the field is nullable.
    public boolean isNullable(String fieldName) {
        return info.get(fieldName).nullable();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        Schema schema = (Schema) o;
        return getFields().equals(schema.getFields()) && info.equals(schema.info);
    }
}