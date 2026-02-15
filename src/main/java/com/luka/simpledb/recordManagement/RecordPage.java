package com.luka.simpledb.recordManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.transactionManagement.Transaction;

import static java.sql.Types.*;

/// A record page is an array of records stored in some block.
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

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets an integer in some slot, in some field with some value.
    public void setInt(int slot, String fieldName, int value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setInt(blockId, fieldPosition, value, true);
        transaction.unpin(blockId);
    }

    /// Pins the block id to the tied transaction so that the block must stay in memory.
    /// Calculates the offset of the record in the block using the slot number and calculates
    /// the offset of the field in that record.
    ///
    /// Sets a string in some slot, in some field with some value.
    public void setString(int slot, String fieldName, String value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setString(blockId, fieldPosition, value, true);
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
        transaction.unpin(blockId);
    }

    /// Marks the slot as empty so that new records can be written in that slot.
    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    /// Marks every slot in the block as empty and sets all field values to empty
    /// (0 for ints, false for booleans, "" for strings).
    public void format() {
        int slot = 0;

        transaction.pin(blockId);

        while(isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), EMPTY, false);
            Schema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                int fieldPosition = offset(slot) + layout.getOffset(fieldName);
                switch (schema.type(fieldName)) {
                    case INTEGER -> transaction.setInt(blockId, slot, 0, false);
                    case BOOLEAN -> transaction.setBoolean(blockId, slot, false, false);
                    case VARCHAR -> transaction.setString(blockId, slot, "", false);
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
            if (transaction.getInt(blockId, offset(slot)) == flag) {
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

    /// Sets the flag of `slot` to `flag`. The flag is stored at the start
    /// of the slot.
    private void setFlag(int slot, int flag) {
        transaction.pin(blockId);
        transaction.setInt(blockId, offset(slot), flag, true);
        transaction.unpin(blockId);
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
