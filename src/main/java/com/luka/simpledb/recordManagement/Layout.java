package com.luka.simpledb.recordManagement;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.recordManagement.exceptions.FieldMissingException;
import com.luka.simpledb.recordManagement.exceptions.RecordTooLongException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.sql.Types.*;

/// A `Layout` represents a physical description of some table's records. It
/// contains information about where can fields be found, the current size of the record,
/// and any additional information that is required for the physical navigation of a record.
public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final Map<String, Integer> fieldPositions;
    private final int slotSize;

    /// A `Layout` can be instantiated from a schema (when a table is created).
    ///
    /// @throws RecordTooLongException if the slot size is greater than the block size of the system.
    public Layout(Schema schema, int blockSize) {
        this.schema = schema;
        offsets = new HashMap<>();
        fieldPositions = new HashMap<>();

        int position = Integer.BYTES; // space for the in use flag
        int i = 0;

        for (String fieldName : schema.getFields()) {
            fieldPositions.put(fieldName, i);
            offsets.put(fieldName, position);
            position += lengthInBytes(fieldName);
            i += 1;
        }
        slotSize = position;

        if (slotSize > blockSize) {
            throw new RecordTooLongException();
        }
    }

    /// A `Layout` can be instantiated from a schema, previously calculated offsets and
    /// previously calculated slot size.
    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;

        this.fieldPositions = new HashMap<>();

        int i = 0;
        for (String fieldName : schema.getFields()) {
            fieldPositions.put(fieldName, i);
            i += 1;
        }
    }

    /// @return The number of bytes for a given field.
    /// @throws DatabaseTypeNotImplementedException if the type for that field is not implemented in the system.
    private int lengthInBytes(String fieldName) {
        int type = schema.type(fieldName);
        switch (type) {
            case INTEGER -> { return Integer.BYTES; }
            case BOOLEAN -> { return Byte.BYTES; }
            case VARCHAR -> {
                int rawLen = Page.maxLength(schema.length(fieldName));
                return padToFour(rawLen);
            }
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
    /// @throws FieldMissingException if the field does not exist on this layout.
    public int getOffset(String fieldName) {
        return Optional.ofNullable(offsets.get(fieldName))
                .orElseThrow(FieldMissingException::new);
    }

    /// @return The ordered position of the field.
    /// @throws FieldMissingException if the field does not exist on this layout.
    public int fieldOrderPosition(String fieldName) {
        return Optional.ofNullable(fieldPositions.get(fieldName))
                .orElseThrow(FieldMissingException::new);
    }

    /// @return Length value padded to nearest 4-byte multiple.
    private int padToFour(int len) {
        return (len + 3) & ~3;
    }
}
