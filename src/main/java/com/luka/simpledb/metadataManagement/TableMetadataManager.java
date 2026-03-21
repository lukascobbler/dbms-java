package com.luka.simpledb.metadataManagement;

import com.luka.simpledb.metadataManagement.exceptions.TableDuplicateNameException;
import com.luka.simpledb.metadataManagement.exceptions.TableNotFoundException;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/// `TableMetadataManager` worries about what tables exist in the DBMS,
/// and what fields do these tables contain. It uses two internal metadata
/// tables called catalogs ("tablecatalog" and "fieldcatalog") to memorize this information.
public class TableMetadataManager {
    public static final int MAX_NAME_LENGTH = 32;
    private final Layout tableCatalogLayout, fieldCatalogLayout;
    private final AtomicInteger nextTableNum;

    /// Instantiates a new `TableMetadataManager` that has always in-memory layouts
    /// for catalog tables to improve efficiency. If the system is initialized for the first time
    /// (noted by `isNew`), the manager creates catalog tables as well.
    public TableMetadataManager(boolean isNew, Transaction transaction, AtomicInteger nextTableNum) {
        Schema tableCatalogSchema = new Schema();
        tableCatalogSchema.addIntField("tableid", false);
        tableCatalogSchema.addStringField("tablename", MAX_NAME_LENGTH, false);
        tableCatalogSchema.addIntField("slotsize", false);
        tableCatalogLayout = new Layout(tableCatalogSchema, transaction.blockSize());

        Schema fieldCatalogSchema = new Schema();
        fieldCatalogSchema.addIntField("type", false);
        fieldCatalogSchema.addIntField("length", false);
        fieldCatalogSchema.addIntField("offset", false);
        fieldCatalogSchema.addIntField("tableid", false);
        fieldCatalogSchema.addStringField("fieldname", MAX_NAME_LENGTH, false);
        fieldCatalogSchema.addBooleanField("nullable", false);
        fieldCatalogLayout = new Layout(fieldCatalogSchema, transaction.blockSize());

        this.nextTableNum = nextTableNum;

        if (isNew) {
            this.nextTableNum.set(1);
            createTable("tablecatalog", tableCatalogSchema, transaction);
            createTable("fieldcatalog", fieldCatalogSchema, transaction);
        } else {
            this.nextTableNum.set(getLatestTableIdNum(transaction));
        }
    }

    /// Creates a table with `tableName` that is defined by `schema` in current transaction.
    /// Internally, it adds a new row to the table catalog, and adds a new row for every field
    /// of that table in the field catalog.
    ///
    /// @throws TableDuplicateNameException if the table already exists with the same name in the system.
    public void createTable(String tableName, Schema schema, Transaction transaction) {
        TableScan tableCatalogDuplicateScan = new TableScan(transaction, "tablecatalog", tableCatalogLayout);

        try (tableCatalogDuplicateScan) {
            while (tableCatalogDuplicateScan.next()) {
                if (tableCatalogDuplicateScan.getString("tablename").equals(tableName)) {
                    throw new TableDuplicateNameException();
                }
            }
        }

        Layout layout = new Layout(schema, transaction.blockSize());
        int tableIdNum = nextTableIdNum();

        TableScan tableCatalogScan = new TableScan(transaction, "tablecatalog", tableCatalogLayout);
        try (tableCatalogScan) {
            tableCatalogScan.insert();
            tableCatalogScan.setInt("tableid", tableIdNum);
            tableCatalogScan.setString("tablename", tableName);
            tableCatalogScan.setInt("slotsize", layout.recordLength());
        }

        TableScan fieldCatalogScan = new TableScan(transaction, "fieldcatalog", fieldCatalogLayout);
        try (fieldCatalogScan) {
            for (String fieldName : schema.getFields()) {
                fieldCatalogScan.insert();
                fieldCatalogScan.setInt("tableid", tableIdNum);
                fieldCatalogScan.setString("fieldname", fieldName);
                fieldCatalogScan.setBoolean("nullable", schema.isNullable(fieldName));
                fieldCatalogScan.setInt("type", schema.type(fieldName));
                fieldCatalogScan.setInt("length", schema.length(fieldName));
                fieldCatalogScan.setInt("offset", layout.getOffset(fieldName));
            }
        }
    }

    /// Finds a table in the table catalog and retrieves it's record size. Finds all
    /// fields of that table in the field catalog, and retrieves their offsets, types and
    /// lengths. Reconstructs a layout from all that information.
    ///
    /// @return The layout matching the table name passed.
    /// @throws TableNotFoundException if the table was not found.
    public Layout getLayout(String tableName, Transaction transaction) {
        int size = -1;
        int tableId = -1;
        TableScan tableCatalogScan = new TableScan(transaction, "tablecatalog", tableCatalogLayout);

        try (tableCatalogScan) {
            while (tableCatalogScan.next()) {
                if (tableCatalogScan.getString("tablename").equals(tableName)) {
                    size = tableCatalogScan.getInt("slotsize");
                    tableId = tableCatalogScan.getInt("tableid");
                    break;
                }
            }
        }

        if (size == -1) {
            throw new TableNotFoundException();
        }

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();

        TableScan fieldCatalogScan = new TableScan(transaction, "fieldcatalog", fieldCatalogLayout);

        try (fieldCatalogScan) {
            while (fieldCatalogScan.next()) {
                if (fieldCatalogScan.getInt("tableid") == tableId) {
                    String fieldName = fieldCatalogScan.getString("fieldname");
                    int fieldType = fieldCatalogScan.getInt("type");
                    int fieldLength = fieldCatalogScan.getInt("length");
                    int fieldOffset = fieldCatalogScan.getInt("offset");
                    boolean fieldNullable = fieldCatalogScan.getBoolean("nullable");
                    offsets.put(fieldName, fieldOffset);
                    schema.addField(fieldName, fieldType, fieldLength, fieldNullable);
                }
            }
        }

        return new Layout(schema, offsets, size);
    }

    /// Removes the metadata for the field, thereby making the field
    /// not accessible anymore by any means.
    public void removeField(String tableName, String fieldName, Transaction transaction) {
        int tableId = -1;
        TableScan tableCatalogScan = new TableScan(transaction, "tablecatalog", tableCatalogLayout);

        try (tableCatalogScan) {
            while (tableCatalogScan.next()) {
                if (tableCatalogScan.getString("tablename").equals(tableName)) {
                    tableId = tableCatalogScan.getInt("tableid");
                    break;
                }
            }
        }

        if (tableId == -1) {
            throw new TableNotFoundException();
        }

        TableScan fieldCatalogScan = new TableScan(transaction, "fieldcatalog", fieldCatalogLayout);

        try (fieldCatalogScan) {
            while (fieldCatalogScan.next()) {
                if (
                    fieldCatalogScan.getInt("tableid") == tableId &&
                    fieldCatalogScan.getString("fieldname").equals(fieldName)
                ) {
                    fieldCatalogScan.delete();
                    return;
                }
            }
        }
    }

    /// @return The largest table id number. Should be run only on startup.
    private int getLatestTableIdNum(Transaction transaction) {
        int maxTableNumber = -1;

        TableScan tableCatalogScan = new TableScan(transaction, "tablecatalog", tableCatalogLayout);

        try (tableCatalogScan) {
            while (tableCatalogScan.next()) {
                int currentTableId = tableCatalogScan.getInt("tableid");
                if (currentTableId > maxTableNumber) {
                    maxTableNumber = currentTableId;
                }
            }
        }

        return maxTableNumber;
    }

    /// @return The table id number representing the next
    /// table id in the system.
    private int nextTableIdNum() {
        return nextTableNum.incrementAndGet();
    }
}
