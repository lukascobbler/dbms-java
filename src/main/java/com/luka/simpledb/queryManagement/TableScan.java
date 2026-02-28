package com.luka.simpledb.queryManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.RecordId;
import com.luka.simpledb.recordManagement.RecordPage;
import com.luka.simpledb.recordManagement.exceptions.FieldCannotBeNullException;
import com.luka.simpledb.recordManagement.exceptions.FieldLengthExceededException;
import com.luka.simpledb.transactionManagement.Transaction;

/// A table scan is a temporary object that is used for accessing one
/// table. It has the power to go through every sequentially and get / set
/// data along the way. It is used with `AutoCloseable` to allow seamless
/// releasing of managed resources, like unpinning the pins on buffers.
public class TableScan implements AutoCloseable {
    private final Transaction transaction;
    private final Layout layout;
    private final String filename;
    private RecordPage recordPage;
    private int currentSlot;

    /// A table scan is defined for a table, it's layout and for a given transaction.
    public TableScan(Transaction transaction, String tableName, Layout layout) {
        this.transaction = transaction;
        this.layout = layout;
        filename = tableName + ".table";
        if (transaction.lengthInBlocks(filename) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    /// Manually unpin the buffers
    public void close() {
        if (recordPage != null) transaction.unpin(recordPage.getBlockId());
    }

    /// Move the scan to before the first record.
    public void beforeFirst() {
        moveToBlock(0);
    }

    /// Move the scan to the next record, indicating whether it exists or not.
    ///
    /// @return True if the end of the table isn't reached, otherwise false.
    public boolean next() {
        currentSlot = recordPage.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.getBlockId().blockNum() + 1);
            currentSlot = recordPage.nextAfter(currentSlot);
        }

        return true;
    }

    /// @return The integer for the current record, with the provided field name.
    public int getInt(String fieldName) {
        return recordPage.getInt(currentSlot, fieldName);
    }

    /// @return The string for the current record, with the provided field name.
    public String getString(String fieldName) {
        return recordPage.getString(currentSlot, fieldName);
    }

    /// @return The boolean for the current record, with the provided field name.
    public boolean getBoolean(String fieldName) {
        return recordPage.getBoolean(currentSlot, fieldName);
    }

    // todo this should be checked in the usage of higher abstraction managers because a value shouldn't be returned
    //  if it's marked as null (null marking is done separately from value retrival at some location)
    //
    // todo solution: use `Constant` from the java sql when this class is implemented fully, and change it's usage in other
    //  places as well
    public boolean isNull(String fieldName) {
        return recordPage.isNull(currentSlot, fieldName);
    }

    // todo uncomment and give docs once chap 8. is complete
//    public Constant getValue(String fieldName) {
//        switch (layout.getSchema().type(fieldName)) {
//            case INTEGER -> { return new IntConstant(getInt(fieldName)); }
//            case BOOLEAN -> { return new BooleanConstant(getBoolean(fieldName)); }
//            case VARCHAR, CHAR, CLOB -> { return new StringConstant(getString(fieldName)); }
//            default -> throw new DatabaseTypeNotImplementedException();
//        }
//    }

    /// @return Whether the table the scan is defined for has some field name.
    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    /// Sets the integer field with the provided integer value.
    public void setInt(String fieldName, int value) {
        recordPage.setInt(currentSlot, fieldName, value);
    }

    /// Sets the string field with the provided string value.
    ///
    /// @throws FieldLengthExceededException – if the length of the passed values exceeds the maximum length defined by the schema for that field.
    public void setString(String fieldName, String value) {
        recordPage.setString(currentSlot, fieldName, value);
    }

    /// Sets the integer field with the provided integer value.
    public void setBoolean(String fieldName, boolean value) {
        recordPage.setBoolean(currentSlot, fieldName, value);
    }

    /// Sets the field to the null value.
    ///
    /// @throws FieldCannotBeNullException – if the field isn't nullable according to the schema.
    public void setNull(String fieldName) {
        recordPage.setNull(currentSlot, fieldName);
    }

    // todo uncomment and give docs once chap 8. is complete
//    public void setVal(String fieldName, Constant val) {
//        switch (layout.getSchema().type(fieldName)) {
//            case INTEGER -> setInt(fieldName (Integer)val.asJavaVal());
//            case VARCHAR -> setInt(fieldName (String)val.asJavaVal());
//            case BOOLEAN -> setInt(fieldName (Boolean)val.asJavaVal());
//        }
//    }

    /// Inserts a new record in the table at the end.
    public void insert() {
        currentSlot = recordPage.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(recordPage.getBlockId().blockNum() + 1);
            }
            currentSlot = recordPage.insertAfter(currentSlot);
        }
    }

    /// Deletes the current record.
    public void delete() {
        recordPage.delete(currentSlot);
    }

    /// Move to explicitly defined record.
    public void moveToRecordId(RecordId recordId) {
        close();
        BlockId blockId = new BlockId(filename, recordId.blockNum());
        recordPage = new RecordPage(transaction, blockId, layout);
        currentSlot = recordId.slot();
    }

    /// @return The record id information about the current scan position.
    public RecordId getRecordId() {
        return new RecordId(recordPage.getBlockId().blockNum(), currentSlot);
    }

    /// Adds a new block at the end of the file for the table that the scan
    /// is defined for. Resets the current slot of that block to -1, so the
    /// sequential operations can start from the start of the block.
    private void moveToNewBlock() {
        close();
        BlockId blockId = transaction.appendEmptyBlock(filename, true);
        recordPage = new RecordPage(transaction, blockId, layout);
        recordPage.format();
        currentSlot = -1;
    }

    /// Moves to the provided block number.
    /// Resets the current slot of that block to -1, so the
    /// sequential operations can start from the start of the block.
    private void moveToBlock(int blockNumber) {
        close();
        BlockId blockId = new BlockId(filename, blockNumber);
        recordPage = new RecordPage(transaction, blockId, layout);
        currentSlot = -1;
    }

    /// @return Whether the scan is at the last block for the table file
    /// that the scan is defined for.
    private boolean atLastBlock() {
        return recordPage.getBlockId().blockNum() == transaction.lengthInBlocks(filename) - 1;
    }
}
