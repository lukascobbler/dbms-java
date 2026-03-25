package com.luka.metadataManagement;

import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.TableDuplicateNameException;
import com.luka.simpledb.metadataManagement.exceptions.TableNotFoundException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.simpleDB.settings.SimpleDBSettings;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class TableMetadataManagerTests {
    // the test from the book Fig 7.2, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testTableCreationAndLayoutRetrival() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        simpleDB.getMetadataManager().createTable("testTable", sch, tx);

        Layout layout = simpleDB.getMetadataManager().getLayout("testTable", tx);

        assertEquals(sch, layout.getSchema());
        assertEquals(40, layout.recordLength());

        tx.commit();
    }

    // the test from the book Fig 7.5
    @Test
    public void testTableCatalog() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Layout layout = simpleDB.getMetadataManager().getLayout("tablecatalog", tx);
        TableScan tableCatalogScan = new TableScan(tx, "tablecatalog", layout);

        try (tableCatalogScan) {
            while (tableCatalogScan.next()) {
                String tableName = tableCatalogScan.getValue("tablename").asString();
                int size = tableCatalogScan.getValue("slotsize").asInt();
                System.out.println("Table name: " + tableName + ", table size: " + size);
            }
        }

        layout = simpleDB.getMetadataManager().getLayout("fieldcatalog", tx);
        TableScan fieldCatalogScan = new TableScan(tx, "fieldcatalog", layout);

        try (fieldCatalogScan) {
            while (fieldCatalogScan.next()) {
                int tableId = fieldCatalogScan.getValue("tableid").asInt();
                String fieldName = fieldCatalogScan.getValue("fieldname").asString();
                int type = fieldCatalogScan.getValue("type").asInt();
                int length = fieldCatalogScan.getValue("runtimeLength").asInt();
                int offset = fieldCatalogScan.getValue("offset").asInt();
                System.out.println("Table Id: " + tableId + ", field name: " +
                        fieldName + ", field type: " + type + ", runtimeLength: " + length + ", offset: " + offset);
            }
        }

        tx.commit();
    }

    @Test
    public void testDuplicateTableName() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);
        assertThrowsExactly(TableDuplicateNameException.class, () -> metadataManager.createTable("tbl1", schema, tx));

        tx.commit();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCreateTableInsertRecordRollback(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);
        Layout tableLayout = metadataManager.getLayout("tbl1", tx);

        TableScan tableScan = new TableScan(tx, "tbl1", tableLayout);

        try (tableScan) {
            for (int i = 0; i < 1000; i++) {
                tableScan.insert();
                tableScan.setValue("field", new IntConstant(100));
            }
        }

        tx.rollback();

        Transaction tx2 = simpleDB.newTransaction();

        assertThrowsExactly(TableNotFoundException.class, () -> metadataManager.getLayout("tbl1", tx2));
        assertFalse(TestUtils.fileExists(tmpDir, "tbl1.table"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCreateTableInsertRecordRollbackCreateTableSameName(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);
        Layout tableLayout = metadataManager.getLayout("tbl1", tx);

        TableScan tableScanOld = new TableScan(tx, "tbl1", tableLayout);

        try (tableScanOld) {
            for (int i = 0; i < 1000; i++) {
                tableScanOld.insert();
                tableScanOld.setValue("field", new IntConstant(100));
            }
        }

        tx.rollback();

        Transaction tx2 = simpleDB.newTransaction();

        assertThrowsExactly(TableNotFoundException.class, () -> metadataManager.getLayout("tbl1", tx2));
        assertFalse(TestUtils.fileExists(tmpDir, "tbl1.table"));

        schema.addIntField("field1", false);
        schema.addIntField("field2", false);
        schema.addIntField("field3", false);
        schema.addIntField("field4", false);
        metadataManager.createTable("tbl1", schema, tx2);

        tableLayout = metadataManager.getLayout("tbl1", tx2);

        TableScan tableScanNew = new TableScan(tx2, "tbl1", tableLayout);

        try (tableScanNew) {
            for (int i = 0; i < 1000; i++) {
                tableScanNew.insert();
                tableScanNew.setValue("field", new IntConstant(100));
                tableScanNew.setValue("field1", new IntConstant(100));
                tableScanNew.setValue("field2", new IntConstant(100));
                tableScanNew.setValue("field3", new IntConstant(100));
                tableScanNew.setValue("field4", new IntConstant(100));
            }
        }

        tx2.commit();

        Transaction tx3 = simpleDB.newTransaction();

        TableScan tableScanNewRead = new TableScan(tx3, "tbl1", tableLayout);

        int count = 0;
        try (tableScanNewRead) {
            while (tableScanNewRead.next()) {
                count += 1;
                assertEquals(100, tableScanNewRead.getValue("field").asInt());
                assertEquals(100, tableScanNewRead.getValue("field1").asInt());
                assertEquals(100, tableScanNewRead.getValue("field2").asInt());
                assertEquals(100, tableScanNewRead.getValue("field3").asInt());
                assertEquals(100, tableScanNewRead.getValue("field4").asInt());
            }
        }

        assertEquals(1000, count);
    }
}
