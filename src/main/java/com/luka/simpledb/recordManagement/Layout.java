package com.luka.simpledb.recordManagement;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;

/// A `Layout` represents a physical description of some table's records. It
/// contains information about where can fields be found, the current size of the record,
/// and any additional information that is required for the physical navigation of a record.
public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final int slotSize;

    /// A `Layout` can be instantiated from a schema (when a table is created).
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

    /// A `Layout` can be instantiated from a schema, precalculated offsets, and a
    /// precalculated slot size.
    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    /// @return The number of bytes for a given field.
    private int lengthInBytes(String fieldName) {
        int type = schema.type(fieldName);
        switch (type) {
            case INTEGER -> { return Integer.BYTES; }
            case BOOLEAN -> { return Byte.BYTES; }
            case VARCHAR -> { return Page.maxLength(schema.length(fieldName)); }
            default -> throw new DatabaseTypeNotImplementedException();
        }
    }

    /// @return The schema of this layout.
    public Schema getSchema() {
        return schema;
    }

    /// @return The slot size for this layout.
    public int getSlotSize() {
        return slotSize;
    }

    /// @return The offset of the field i.e. where the field starts.
    public int getOffset(String fieldName) {
        // todo error handling
        return offsets.get(fieldName);
    }
}
