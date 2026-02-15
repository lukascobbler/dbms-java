package com.luka.simpledb.recordManagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.*;

/// `Schema` objects hold information about a table's field names, lengths
/// and types.
public class Schema {
    private final List<String> fields = new ArrayList<>();
    private final Map<String, FieldInfo> info = new HashMap<>();

    /// Generic field adder, can accept any SQL type (can be dangerous) along with
    /// the length of that type.
    public void addField(String fieldName, int type, int length) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(type, length));
    }

    /// Add an integer field.
    public void addIntField(String fieldName) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(INTEGER, 0));
    }

    /// Add a string field with the maximum length (VARCHAR type).
    public void addStringField(String fieldName, int length) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(VARCHAR, length));
    }

    /// Add a boolean field.
    public void addBooleanField(String fieldName) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(BOOLEAN, 0));
    }

    /// Add a field described by its name from some other schema.
    public void add(String fieldName, Schema otherSchema) {
        int type = otherSchema.type(fieldName);
        int length = otherSchema.length(fieldName);
        addField(fieldName, type, length);
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

    /// @return The type of the field described by its name.
    public int type(String fieldName) {
        return info.get(fieldName).type();
    }

    /// @return The length of the field described by its name.
    public int length(String fieldName) {
        return info.get(fieldName).length();
    }
}

/// Represents everything that can describe an arbitrary field.
record FieldInfo(int type, int length) {}