package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.*;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.RecordId;
import com.luka.simpledb.recordManagement.RecordPage;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.recordManagement.exceptions.FieldCannotBeNullException;
import com.luka.simpledb.recordManagement.exceptions.FieldLengthExceededException;
import com.luka.simpledb.recordManagement.exceptions.FieldNotFoundException;
import com.luka.simpledb.transactionManagement.Transaction;

import static java.sql.Types.*;

/// A table scan is a temporary object that is used for accessing one
/// table. It has the power to go through every sequentially and get / set
/// data along the way. It is used with `AutoCloseable` to allow seamless
/// releasing of managed resources, like unpinning the pins on buffers.
public class TableScan implements UpdateScan {
    private final Transaction transaction;
    private final Layout layout;
    private final String filename;
    private RecordPage recordPage;
    private int currentRecord;

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

    /// Manually unpin the buffers.
    @Override
    public void close() {
        if (recordPage != null) transaction.unpin(recordPage.getBlockId());
    }

    /// Move the scan to before the first record.
    public void beforeFirst() {
        moveToBlock(0);
    }

    /// Move the scan after the last record.
    public void afterLast() {
        moveToBlock(transaction.lengthInBlocks(filename) - 1);
        currentRecord = recordPage.numRecords();
    }

    /// Move the scan to the next record, indicating whether it exists or not.
    /// If the current record page has no more records, move to the next
    /// block and point a new record page to that block.
    ///
    /// @return True if the end of the table isn't reached, otherwise false.
    public boolean next() {
        currentRecord = recordPage.nextAfter(currentRecord);
        while (currentRecord < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.getBlockId().blockNum() + 1);
            currentRecord = recordPage.nextAfter(currentRecord);
        }

        return true;
    }

    /// Move the scan to the previous record, indicating whether it exists or not.
    /// If the current record page has no more records, move to the previous
    /// block and point a new record page to that block.
    ///
    /// @return True if the beginning of the table isn't reached, otherwise false.
    public boolean previous() {
        currentRecord = recordPage.nextBefore(currentRecord);
        while (currentRecord < 0) {
            if (atFirstBlock()) {
                return false;
            }
            moveToBlock(recordPage.getBlockId().blockNum() - 1);
            currentRecord = recordPage.nextBefore(recordPage.numRecords());
        }

        return true;
    }

    /// @return The integer for the current record, with the provided field name.
    public int getInt(String fieldName) {
        try {
            return recordPage.getInt(currentRecord, fieldName);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// @return The string for the current record, with the provided field name.
    public String getString(String fieldName) {
        try {
            return recordPage.getString(currentRecord, fieldName);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// @return The boolean for the current record, with the provided field name.
    public boolean getBoolean(String fieldName) {
        try {
            return recordPage.getBoolean(currentRecord, fieldName);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// @return Whether the field is currently null.
    public boolean isNull(String fieldName) {
        return recordPage.isNull(currentRecord, fieldName);
    }

    /// Generic value getter. All representable states in the database
    /// are abstracted over the `Constant` record which can include special
    /// cases like null values. The caller does not need to know the type
    /// of the field.
    ///
    /// @return A constant which can be of any special DB type or any type
    /// that the database implements.
    /// @throws DatabaseTypeNotImplementedException if the system does not implement
    /// the type of the field name.
    public Constant getValue(String fieldName) {
        if (isNull(fieldName)) {
            return new NullConstant();
        }
        switch (layout.getSchema().type(fieldName)) {
            case INTEGER -> { return new IntConstant(getInt(fieldName)); }
            case BOOLEAN -> { return new BooleanConstant(getBoolean(fieldName)); }
            case VARCHAR -> { return new StringConstant(getString(fieldName)); }
            default -> throw new DatabaseTypeNotImplementedException();
        }
    }

    /// @return Whether the table the scan is defined for has some field name.
    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    /// Sets the integer field with the provided integer value.
    public void setInt(String fieldName, int value) {
        try {
            recordPage.setInt(currentRecord, fieldName, value);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// Sets the string field with the provided string value.
    ///
    /// @throws FieldLengthExceededException – if the length of the passed values exceeds the maximum length defined by the schema for that field.
    public void setString(String fieldName, String value) {
        try {
            recordPage.setString(currentRecord, fieldName, value);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// Sets the integer field with the provided integer value.
    public void setBoolean(String fieldName, boolean value) {
        try {
            recordPage.setBoolean(currentRecord, fieldName, value);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// Sets the field to the null value.
    ///
    /// @throws FieldCannotBeNullException – if the field isn't nullable according to the schema.
    public void setNull(String fieldName) {
        try {
            recordPage.setNull(currentRecord, fieldName);
        } catch (FieldNotFoundException e) {
            throw new FieldNotFoundInScanException();
        }
    }

    /// Generic value setter. All representable states in the database
    /// are abstracted over the `Constant` record which can include special
    /// cases like null values. The caller does not need to know the type
    /// of the field.
    ///
    /// @throws DatabaseTypeNotImplementedException if the system does not implement
    /// the type of the field name.
    public void setValue(String fieldName, Constant value) {
        if (value instanceof NullConstant) {
            setNull(fieldName);
            return;
        }
        switch (layout.getSchema().type(fieldName)) {
            case INTEGER -> setInt(fieldName, value.asInt());
            case VARCHAR -> setString(fieldName, value.asString());
            case BOOLEAN -> setBoolean(fieldName, value.asBoolean());
            default -> throw new DatabaseTypeNotImplementedException();
        }
    }

    /// Inserts a new record in the table at the end.
    public void insert() {
        currentRecord = recordPage.insertAfter(currentRecord);
        while (currentRecord < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(recordPage.getBlockId().blockNum() + 1);
            }
            currentRecord = recordPage.insertAfter(currentRecord);
        }
    }

    /// Deletes the current record.
    public void delete() {
        recordPage.delete(currentRecord);
    }

    /// Move to explicitly defined record.
    public void moveToRecordId(RecordId recordId) {
        close();
        BlockId blockId = new BlockId(filename, recordId.blockNum());
        recordPage = new RecordPage(transaction, blockId, layout);
        currentRecord = recordId.record();
    }

    /// @return The record id information about the current scan position.
    public RecordId getRecordId() {
        return new RecordId(recordPage.getBlockId().blockNum(), currentRecord);
    }

    /// Adds a new block at the end of the file for the table that the scan
    /// is defined for. Resets the current record of that block to -1, so the
    /// sequential operations can start from the start of the block.
    private void moveToNewBlock() {
        close();
        BlockId blockId = transaction.appendEmptyBlock(filename, true);
        recordPage = new RecordPage(transaction, blockId, layout);
        recordPage.format();
        currentRecord = -1;
    }

    /// Moves to the provided block number.
    /// Resets the current record of that block to -1, so the
    /// sequential operations can start from the start of the block.
    private void moveToBlock(int blockNumber) {
        close();
        BlockId blockId = new BlockId(filename, blockNumber);
        recordPage = new RecordPage(transaction, blockId, layout);
        currentRecord = -1;
    }

    /// @return Whether the scan is at the last block for the table file
    /// that the scan is defined for.
    private boolean atLastBlock() {
        return recordPage.getBlockId().blockNum() == transaction.lengthInBlocks(filename) - 1;
    }

    /// @return Whether the scan is at the first block for the table file
    /// that the scan is defined for.
    private boolean atFirstBlock() {
        return recordPage.getBlockId().blockNum() == 0;
    }
}
