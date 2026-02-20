package com.luka.simpledb.recordManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.recordManagement.exceptions.FieldLengthExceededException;
import com.luka.simpledb.transactionManagement.Transaction;

import static java.sql.Types.*;

/// A `RecordPage` object represents an interface to the records of some block.
/// It is an abstraction over getters and setters of fields, but leaves the
/// exact record where fields should be stored to the API caller.
public class RecordPage {
    public static final int EMPTY = 0, USED = 1;

    private final Transaction transaction;
    private final BlockId blockId;
    private final Layout layout;

    /// A record page must be tied to some transaction, must point to some block and
    /// must have a layout.
    public RecordPage(Transaction transaction, BlockId blockId, Layout layout) {
        this.transaction = transaction;
        this.blockId = blockId;
        this.layout = layout;
    }

    /// @return Whether the field currently has a null value.
    public boolean isNull(int slot, String fieldName) {
        int nullBitPosition = layout.fieldOrderPosition(fieldName) + 1;
        int isNullMask = 1 << nullBitPosition;
        int slotPosition = offset(slot);

        transaction.pin(blockId);
        int slotMarker = transaction.getInt(blockId, slotPosition);
        transaction.unpin(blockId);

        return (slotMarker & isNullMask) != 0;
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// @return An integer in the field described by its name, in some slot.
    public int getInt(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        int returnInt = transaction.getInt(blockId, fieldPosition);
        transaction.unpin(blockId);

        return returnInt;
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// @return A string in the field described by its name, in some slot.
    public String getString(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        String returnString = transaction.getString(blockId, fieldPosition);
        transaction.unpin(blockId);

        return returnString;
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// @return A boolean in the field described by its name, in some slot.
    public boolean getBoolean(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        boolean returnBoolean = transaction.getBoolean(blockId, fieldPosition);
        transaction.unpin(blockId);

        return returnBoolean;
    }

    /// Sets the field name null bit to 1, meaning the value for that field is null.
    public void setNull(int slot, String fieldName) {
        int nullBitPosition = layout.fieldOrderPosition(fieldName) + 1;
        int setNotNullMask = 1 << nullBitPosition;
        int slotPosition = offset(slot);

        transaction.pin(blockId);
        int slotMarker = transaction.getInt(blockId, slotPosition);
        slotMarker = slotMarker | setNotNullMask;
        transaction.setInt(blockId, slotPosition, slotMarker, true);
        transaction.unpin(blockId);
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets an integer in some slot, in some field with some value.
    public void setInt(int slot, String fieldName, int value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setInt(blockId, fieldPosition, value, true);
        setNonNull(slot, fieldName);
        transaction.unpin(blockId);
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets a string in some slot, in some field with some value.
    ///
    /// @throws FieldLengthExceededException if the length of the passed values exceeds
    /// the maximum length defined by the schema for that field.
    public void setString(int slot, String fieldName, String value) {
        if (value.length() > layout.getSchema().length(fieldName)) {
            throw new FieldLengthExceededException();
        }
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setString(blockId, fieldPosition, value, true);
        setNonNull(slot, fieldName);
        transaction.unpin(blockId);
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets a boolean in some slot, in some field with some value.
    public void setBoolean(int slot, String fieldName, boolean value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setBoolean(blockId, fieldPosition, value, true);
        setNonNull(slot, fieldName);
        transaction.unpin(blockId);
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

        transaction.pin(blockId);

        while(isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), EMPTY, false);
            Schema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                int fieldPosition = offset(slot) + layout.getOffset(fieldName);
                switch (schema.type(fieldName)) {
                    case INTEGER -> transaction.setInt(blockId, fieldPosition, 0, false);
                    case BOOLEAN -> transaction.setBoolean(blockId, fieldPosition, false, false);
                    case VARCHAR -> transaction.setString(blockId, fieldPosition, "", false);
                    default -> throw new DatabaseTypeNotImplementedException();
                }
            }
            slot++;
        }

        transaction.unpin(blockId);
    }

    /// @return The first next used slot after `slot`.
    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
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

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    ///
    /// @return The first slot number after `slot` that is of the flag `flag`.
    private int searchAfter(int slot, int flag) {
        slot++;
        transaction.pin(blockId);
        while (isValidSlot(slot)) {
            if (getSlotFlag(slot) == flag) {
                transaction.unpin(blockId);
                return slot;
            }
            slot++;
        }

        transaction.unpin(blockId);
        return -1;
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    ///
    /// @return The first slot number before `slot` that is of the flag `flag`, starting from the last record.
    private int searchBefore(int slot, int flag) {
        slot++;
        transaction.pin(blockId);
        while (isValidSlot(slot)) {
            if (getSlotFlag(slot) == flag) {
                transaction.unpin(blockId);
                return slot;
            }
            slot++;
        }

        transaction.unpin(blockId);
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
    /// of the slot, in the first bit.
    private void setFlag(int slot, int flag) {
        int slotPosition = offset(slot);

        transaction.pin(blockId);
        int slotMarker = transaction.getInt(blockId, slotPosition);
        if (flag == USED) {
            slotMarker |= 1;
        } else if (flag == EMPTY) {
            slotMarker &= ~1;
        }
        transaction.setInt(blockId, slotPosition, slotMarker, true);
        transaction.unpin(blockId);
    }

    /// Sets the field name null bit to 0, meaning the value for that field is non-null.
    /// Should be called only from the corresponding set \[type\] functions, and assumes the
    /// transaction pinned the block.
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
        return slot * layout.getSlotSize();
    }

    /// @return The block identification of this record page.
    public BlockId getBlockId() {
        return this.blockId;
    }
}
