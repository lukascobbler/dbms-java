package com.luka.queryManagement.scanTests;

import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.recordManagement.RecordId;
import com.luka.simpledb.recordManagement.exceptions.FieldCannotBeNullException;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.simpleDB.settings.SimpleDBSettings;
import com.luka.testUtils.TestUtils;
import com.luka.simpledb.transactionManagement.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class TableScanTests {
    // the test from the book Fig 6.18, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testTableScanAddMultipleRecordsExceedBlockSize() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "B", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 100; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }

            int count = 0;
            ts.beforeFirst();
            while (ts.next()) {
                int a = ts.getInt("A");
                String b = ts.getString("B");
                if (a < 25 || a > 75) {
                    count++;
                    ts.delete();
                }
            }

            assertEquals(50, count);

            SimpleDBSettings simpleDBSettings = new SimpleDBSettings();
            int blockingFactor = Math.floorDiv(simpleDBSettings.BLOCK_SIZE, (4 + Page.maxLength(6)));
            int expectedNumberOfBlocks = Math.ceilDiv(100, blockingFactor);
            int actualNumberOfBlocks = tx.lengthInBlocks("B.table");
            assertTrue(actualNumberOfBlocks >= expectedNumberOfBlocks);

            ts.beforeFirst();
            while (ts.next()) {
                int a = ts.getInt("A");
                String b = ts.getString("B");

                assertTrue(a >= 25);
                assertTrue(a <= 75);
            }
        }

        tx.commit();
    }

    @Test
    public void testBackwardsTraversal() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "B", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 1000; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }

            ts.afterLast();
            for (int i = 1000; i >= 0; i--) {
                ts.previous();
                assertEquals(i, ts.getInt("A"));
                assertEquals(String.format("rec%03d", i), ts.getString("B"));
            }
        }

        tx.commit();
    }

    @Test
    public void testInsertAfterAfterLast() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "B", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 100; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }

            ts.beforeFirst();
            ts.afterLast();

            for (int i = 101; i <= 2000; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }

            ts.afterLast();
            for (int i = 2000; i >= 0; i--) {
                ts.previous();
                assertEquals(i, ts.getInt("A"));
                assertEquals(String.format("rec%03d", i), ts.getString("B"));
            }
        }

        tx.commit();
    }

    @Test
    public void testNextPreviousRepeated() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "B", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 100; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }

            ts.moveToRecordId(new RecordId(0, 50));
            assertEquals(50, ts.getInt("A"));

            for (int i = 0; i < 10; i ++) {
                assertEquals(50 + i, ts.getInt("A"));
                ts.next();
            }

            for (int i = 0; i < 10; i ++) {
                assertEquals(50 + 10 - i, ts.getInt("A"));
                ts.previous();
            }
        }

        tx.commit();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableScanAddMultipleRecordsCommitRollback(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "B", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 1000; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }
        }

        tx.commit();

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");

        assertEquals(10, fm.lengthInBlocks("B.table"));

        Transaction tx2 = simpleDB.newTransaction();
        TableScan ts2 = new TableScan(tx2, "B", layout);

        try (ts2) {
            ts2.beforeFirst();
            for (int i = 0; i <= 1000; i++) {
                ts2.insert();
                ts2.setInt("A", i);
                ts2.setString("B", String.format("rec%03d", i));
            }
        }

        tx2.rollback();

        assertEquals(10, fm.lengthInBlocks("B.table"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableScanAddMultipleRecordsCommitCrash(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "B", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 1000; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", String.format("rec%03d", i));
            }
        }

        tx.commit();

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");

        assertEquals(10, fm.lengthInBlocks("B.table"));

        simpleDB = new SimpleDB(tmpDir, settings);
        fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");

        assertEquals(10, fm.lengthInBlocks("B.table"));
    }

    @Test
    public void testTableScanNullValues() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("nullable", true);
        sch.addIntField("non-nullable", false);

        Layout layout = new Layout(sch, tx.blockSize());
        TableScan ts = new TableScan(tx, "nullability", layout);

        try (ts) {
            ts.beforeFirst();
            for (int i = 0; i <= 100; i++) {
                ts.insert();
                ts.setInt("nullable", i);
                ts.setInt("non-nullable", i);
            }

            for (int i = 0; i <= 100; i++) {
                ts.moveToRecordId(new RecordId(0, i));
                assertEquals(ts.getInt("nullable"), i);
                assertEquals(ts.getInt("non-nullable"), i);
            }

            for (int i = 0; i <= 100; i++) {
                ts.moveToRecordId(new RecordId(0, i));
                ts.setNull("nullable");
            }

            for (int i = 0; i <= 100; i++) {
                ts.moveToRecordId(new RecordId(0, i));
                assertTrue(ts.isNull("nullable"));
                assertEquals(NullConstant.INSTANCE, ts.getValue("nullable"));
            }

            ts.moveToRecordId(new RecordId(0, 0));
            assertThrowsExactly(FieldCannotBeNullException.class, () -> ts.setNull("non-nullable"));

            assertEquals(0, ts.getInt("nullable"));
        }

        tx.commit();
    }
}
