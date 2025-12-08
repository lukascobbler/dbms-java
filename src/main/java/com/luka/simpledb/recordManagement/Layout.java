package com.luka.simpledb.recordManagement;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;

public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int position = Integer.BYTES; // space for the in use flag (basic record manager)
        for (String fieldName : schema.getFields()) {
            offsets.put(fieldName, position);
            position += lengthInBytes(fieldName);
        }
        slotSize = position;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    private int lengthInBytes(String fieldName) {
        int type = schema.type(fieldName);
        switch (type) {
            case INTEGER -> { return Integer.BYTES; }
            case BOOLEAN -> { return Byte.BYTES; }
            case VARCHAR -> { return Page.maxLength(schema.length(fieldName)); }
            default -> throw new DatabaseTypeNotImplementedException();
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public int getSlotSize() {
        return slotSize;
    }

    public int getOffset(String fieldName) {
        // todo error handling
        return offsets.get(fieldName);
    }
}
