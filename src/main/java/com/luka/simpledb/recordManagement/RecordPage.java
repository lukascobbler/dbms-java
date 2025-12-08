package com.luka.simpledb.recordManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.transactionManagement.Transaction;

import static java.sql.Types.*;

public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    private final Transaction transaction;
    private final BlockId blockId;
    private final Layout layout;

    public RecordPage(Transaction transaction, BlockId blockId, Layout layout) {
        this.transaction = transaction;
        this.blockId = blockId;
        this.layout = layout;
    }

    public int getInt(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        int returnInt = transaction.getInt(blockId, fieldPosition);
        transaction.unpin(blockId);

        return returnInt;
    }

    public String getString(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        String returnString = transaction.getString(blockId, fieldPosition);
        transaction.unpin(blockId);

        return returnString;
    }

    public boolean getBoolean(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        boolean returnBoolean = transaction.getBoolean(blockId, fieldPosition);
        transaction.unpin(blockId);

        return returnBoolean;
    }

    public void setInt(int slot, String fieldName, int value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setInt(blockId, fieldPosition, value, true);
        transaction.unpin(blockId);
    }

    public void setString(int slot, String fieldName, String value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setString(blockId, fieldPosition, value, true);
        transaction.unpin(blockId);
    }

    public void setBoolean(int slot, String fieldName, boolean value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        transaction.pin(blockId);
        transaction.setBoolean(blockId, fieldPosition, value, true);
        transaction.unpin(blockId);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

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

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot >= 0) {
            setFlag(newSlot, USED);
        }
        return newSlot;
    }

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

    private boolean isValidSlot(int slot) {
        return offset(slot + 1) <= transaction.blockSize();
    }

    private void setFlag(int slot, int flag) {
        transaction.pin(blockId);
        transaction.setInt(blockId, offset(slot), flag, true);
        transaction.unpin(blockId);
    }

    private int offset(int slot) {
        return slot * layout.getSlotSize();
    }

    public BlockId getBlockId() {
        return this.blockId;
    }
}
