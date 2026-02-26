package com.luka.queryManagement;

import com.luka.simpledb.recordManagement.RecordId;
import com.luka.simpledb.recordManagement.exceptions.FieldCannotBeNullException;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.queryManagement.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.simpleDB.SimpleDBSettings;
import com.luka.testUtils.TestUtils;
import com.luka.simpledb.transactionManagement.Transaction;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class QueryManagementTests {
    // the test from the book Fig 6.18, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testTableScanAddMultipleRecordsExceedBlockSize() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_queries1");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
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
    public void testTableScanNullValues() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_queries2");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
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
//                assertEquals((Integer) null, ts.getInt("nullable")); todo when null handing is done
            }

            ts.moveToRecordId(new RecordId(0, 0));
            assertThrowsExactly(FieldCannotBeNullException.class, () -> ts.setNull("non-nullable"));

            assertEquals(0, ts.getInt("nullable"));
        }

        tx.commit();
    }
}
