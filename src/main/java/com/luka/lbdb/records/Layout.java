package com.luka.lbdb.records;

import com.luka.lbdb.fileManagement.Page;
import com.luka.lbdb.records.exceptions.DatabaseTypeNotImplementedException;
import com.luka.lbdb.records.exceptions.RecordTooLongException;
import com.luka.lbdb.records.schema.Schema;

import java.util.HashMap;
import java.util.Map;

/// A `Layout` represents a physical description of some table's records. It
/// contains information about where can fields be found, the current size of the record,
/// and any additional information that is required for the physical navigation of a record.
public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final Map<String, Integer> fieldPositions;
    private final int recordSize;

    /// A `Layout` can be instantiated from a schema (when a table is created).
    ///
    /// @throws RecordTooLongException if the record size is greater than the block size of the system.
    public Layout(Schema schema, int blockSize) {
        this.schema = schema;
        offsets = new HashMap<>();
        fieldPositions = new HashMap<>();

        int position = Integer.BYTES; // space for the in use flag
        int i = 0;

        for (String fieldName : schema.getFields()) {
            fieldPositions.put(fieldName, i);
            offsets.put(fieldName, position);
            position += runtimeLengthInBytes(fieldName);
            i += 1;
        }
        recordSize = position;

        if (recordSize > blockSize) {
            throw new RecordTooLongException(String.valueOf(recordSize));
        }
    }

    /// A `Layout` can be instantiated from a schema, previously calculated offsets and
    /// previously calculated record size.
    public Layout(Schema schema, Map<String, Integer> offsets, int recordSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.recordSize = recordSize;

        this.fieldPositions = new HashMap<>();

        int i = 0;
        for (String fieldName : schema.getFields()) {
            fieldPositions.put(fieldName, i);
            i += 1;
        }
    }

    /// @return The number of bytes for a given field.
    /// @throws DatabaseTypeNotImplementedException if the type for that field is not implemented in the system.
    private int runtimeLengthInBytes(String fieldName) {
        DatabaseType type = schema.type(fieldName);
        return switch (type) {
            case DatabaseType.INT -> DatabaseType.INT.length;
            case DatabaseType.BOOLEAN -> DatabaseType.BOOLEAN.length;
            case DatabaseType.VARCHAR -> {
                int rawLen = Page.maxLength(schema.runtimeLength(fieldName));
                yield padToFour(rawLen);
            }
            default -> throw new DatabaseTypeNotImplementedException();
        };
    }

    /// @return The schema of this layout.
    public Schema getSchema() {
        return schema;
    }

    /// @return The record size for this layout.
    public int recordLength() {
        return recordSize;
    }

    /// @return The offset of the field i.e. where the field starts.
    public int getOffset(String fieldName) {
        return offsets.get(fieldName);
    }

    /// @return The ordered position of the field.
    public int fieldOrderPosition(String fieldName) {
        return fieldPositions.get(fieldName);
    }

    /// @return Length value padded to nearest 4-byte multiple.
    private int padToFour(int len) {
        return (len + 3) & ~3;
    }
}
