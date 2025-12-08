package com.luka.simpledb.recordManagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.*;

public class Schema {
    private final List<String> fields = new ArrayList<>();
    private final Map<String, FieldInfo> info = new HashMap<>();

    public void addField(String fieldName, int type, int length) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(type, length));
    }

    public void addIntField(String fieldName) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(INTEGER, 0));
    }

    public void addStringField(String fieldName, int length) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(VARCHAR, length));
    }

    public void addBooleanField(String fieldName) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(BOOLEAN, 0));
    }

    public void add(String fieldName, Schema otherSchema) {
        int type = otherSchema.type(fieldName);
        int length = otherSchema.length(fieldName);
        addField(fieldName, type, length);
    }

    public void addAll(Schema otherSchema) {
        for (String fieldName : otherSchema.getFields()) {
            add(fieldName, otherSchema);
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    public int type(String fieldName) {
        return info.get(fieldName).type();
    }

    public int length(String fieldName) {
        return info.get(fieldName).length();
    }
}

record FieldInfo(int type, int length) {}