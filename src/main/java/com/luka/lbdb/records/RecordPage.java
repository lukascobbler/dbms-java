package com.luka.lbdb.records;

import com.luka.lbdb.fileManagement.BlockId;
import com.luka.lbdb.records.exceptions.DatabaseTypeNotImplementedException;
import com.luka.lbdb.records.exceptions.FieldCannotBeNullException;
import com.luka.lbdb.records.exceptions.FieldIncorrectTypeException;
import com.luka.lbdb.records.exceptions.FieldLengthExceededException;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.transactions.Transaction;

/// A `RecordPage` object represents an interface to the records of some block.
/// It is an abstraction over getters and setters of fields, but leaves the
/// exact record that is being manipulated to its caller.
public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    private final Transaction transaction;
    private final BlockId blockId;
    private final Layout layout;

    /// A record page must be tied to some transaction, must point to some block and
    /// must have a layout. The block related to this record page is pinned upon creation
    /// of a `RecordPage` object because it will most likely be needed for more than one operation
    /// but not for too long.
    public RecordPage(Transaction transaction, BlockId blockId, Layout layout) {
        this.transaction = transaction;
        this.blockId = blockId;
        this.layout = layout;

        transaction.pin(blockId);
    }

    /// @return Whether the field currently has a null value.
    public boolean isNull(int slot, String fieldName) {
        if (!layout.getSchema().isNullable(fieldName)) {
            return false;
        }

        int nullBitPosition = layout.fieldOrderPosition(fieldName) + 1;
        int isNullMask = 1 << nullBitPosition;
        int slotPosition = offset(slot);

        int slotMarker = transaction.getInt(blockId, slotPosition);

        return (slotMarker & isNullMask) != 0;
    }

    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// @return An integer in the field described by its name, in some slot.
    /// @throws FieldIncorrectTypeException if the field name defined in the schema
    /// isn't of the type of this get method.
    public int getInt(int slot, String fieldName) {
        if (layout.getSchema().type(fieldName) != DatabaseType.INT) {
            throw new FieldIncorrectTypeException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        return transaction.getInt(blockId, fieldPosition);
    }

    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// @return A string in the field described by its name, in some slot.
    /// @throws FieldIncorrectTypeException if the field name defined in the schema
    /// isn't of the type of this get method.
    public String getString(int slot, String fieldName) {
        if (layout.getSchema().type(fieldName) != DatabaseType.VARCHAR) {
            throw new FieldIncorrectTypeException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        return transaction.getString(blockId, fieldPosition);
    }

    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// @return A boolean in the field described by its name, in some slot.
    /// @throws FieldIncorrectTypeException if the field name defined in the schema
    /// isn't of the type of this get method.
    public boolean getBoolean(int slot, String fieldName) {
        if (layout.getSchema().type(fieldName) != DatabaseType.BOOLEAN) {
            throw new FieldIncorrectTypeException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        return transaction.getBoolean(blockId, fieldPosition);
    }

    /// Sets the field name null bit to 1, meaning the value for that field is null.
    ///
    /// @throws FieldCannotBeNullException if the field isn't nullable according to the schema.
    public void setNull(int slot, String fieldName) {
        if (!layout.getSchema().isNullable(fieldName)) {
            throw new FieldCannotBeNullException();
        }

        int nullBitPosition = layout.fieldOrderPosition(fieldName) + 1;
        int setNotNullMask = 1 << nullBitPosition;
        int slotPosition = offset(slot);

        int slotMarker = transaction.getInt(blockId, slotPosition);
        slotMarker = slotMarker | setNotNullMask;
        transaction.setInt(blockId, slotPosition, slotMarker, true);
    }

    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets an integer in some slot, in some field with some value.
    /// @throws FieldIncorrectTypeException if the field name defined in the schema
    /// isn't of the type of this set method.
    public void setInt(int slot, String fieldName, int value) {
        if (layout.getSchema().type(fieldName) != DatabaseType.INT) {
            throw new FieldIncorrectTypeException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.setInt(blockId, fieldPosition, value, true);
        setNonNull(slot, fieldName);
    }

    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets a string in some slot, in some field with some value.
    ///
    /// @throws FieldLengthExceededException if the runtimeLength of the passed values exceeds
    /// the maximum runtimeLength defined by the schema for that field.
    /// @throws FieldIncorrectTypeException if the field name defined in the schema
    /// isn't of the type of this set method.
    public void setString(int slot, String fieldName, String value) {
        if (layout.getSchema().type(fieldName) != DatabaseType.VARCHAR) {
            throw new FieldIncorrectTypeException();
        }
        if (value.length() > layout.getSchema().runtimeLength(fieldName)) {
            throw new FieldLengthExceededException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.setString(blockId, fieldPosition, value, true);
        setNonNull(slot, fieldName);
    }

    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets a boolean in some slot, in some field with some value.
    /// @throws FieldIncorrectTypeException if the field name defined in the schema
    /// isn't of the type of this set method.
    public void setBoolean(int slot, String fieldName, boolean value) {
        if (layout.getSchema().type(fieldName) != DatabaseType.BOOLEAN) {
            throw new FieldIncorrectTypeException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.setBoolean(blockId, fieldPosition, value, true);
        setNonNull(slot, fieldName);
    }

    /// Marks the slot as empty so that new records can be written in that slot.
    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    /// Marks every slot in the block as empty and sets all field values to empty
    /// (0 for ints, false for booleans, "" for strings).
    ///
    /// @throws DatabaseTypeNotImplementedException if the type for that field is not implemented in the system.
    public void format() {
        int slot = 0;

        while (isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), EMPTY, false);
            Schema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                int fieldPosition = offset(slot) + layout.getOffset(fieldName);
                switch (schema.type(fieldName)) {
                    case DatabaseType.INT -> transaction.setInt(blockId, fieldPosition, 0, false);
                    case DatabaseType.BOOLEAN -> transaction.setBoolean(blockId, fieldPosition, false, false);
                    case DatabaseType.VARCHAR -> transaction.setString(blockId, fieldPosition, "", false);
                    default -> throw new DatabaseTypeNotImplementedException();
                }
            }
            slot++;
        }
    }

    /// @return The first next used slot after `slot`.
    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    /// @return The first next used slot before `slot`.
    public int nextBefore(int slot) {
        return searchBefore(slot, USED);
    }

    /// Marks the first empty slot after `slot` as used.
    ///
    /// @return The first empty slot after `slot`. `-1` if no empty slot was found.
    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot >= 0) {
            setFlag(newSlot, USED);
        }
        return newSlot;
    }

    /// @return The number of records in this page.
    public int numRecords() {
        return transaction.blockSize() / layout.recordLength();
    }

    /// @return The first slot number after `slot` that is of the flag `flag`.
    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (getSlotFlag(slot) == flag) {
                return slot;
            }
            slot++;
        }

        return -1;
    }

    /// @return The first slot number before `slot` that is of the flag `flag`.
    private int searchBefore(int slot, int flag) {
        slot--;
        while (slot >= 0) {
            if (getSlotFlag(slot) == flag) {
                return slot;
            }
            slot--;
        }

        return -1;
    }

    /// @return Whether the slot number is valid for this block.
    private boolean isValidSlot(int slot) {
        return offset(slot + 1) <= transaction.blockSize();
    }

    /// @return The flag at the given slot. Can be 0 or 1, corresponding to `USED` or `EMPTY`.
    private int getSlotFlag(int slot) {
        int slotPosition = offset(slot);

        int slotMarker = transaction.getInt(blockId, slotPosition);

        return slotMarker & 1;
    }

    /// Sets the flag of `slot` to `flag`. The flag is stored at the start
    /// of the slot, in the first bit. If the `flag` about to be set denotes
    /// an empty value, the whole 4 byte integer is set to exactly 0 because
    /// previous null value information for that slot is not important anymore.
    private void setFlag(int slot, int flag) {
        int slotPosition = offset(slot);

        int slotMarker = transaction.getInt(blockId, slotPosition);
        if (flag == USED) {
            slotMarker |= 1;
        } else if (flag == EMPTY) {
            slotMarker = 0;
        }
        transaction.setInt(blockId, slotPosition, slotMarker, true);
    }

    /// Sets the field name null bit to 0, meaning the value for that field is non-null.
    /// Should be called only from the corresponding set \[type\] functions.
    private void setNonNull(int slot, String fieldName) {
        int nullBitPosition = layout.fieldOrderPosition(fieldName) + 1;
        int setNullMask = ~(1 << nullBitPosition);
        int slotPosition = offset(slot);

        int slotMarker = transaction.getInt(blockId, slotPosition);
        slotMarker = slotMarker & setNullMask;
        transaction.setInt(blockId, slotPosition, slotMarker, true);
    }

    /// @return The offset of `slot`.
    private int offset(int slot) {
        return slot * layout.recordLength();
    }

    /// @return The block identification of this record page.
    public BlockId getBlockId() {
        return this.blockId;
    }
}
