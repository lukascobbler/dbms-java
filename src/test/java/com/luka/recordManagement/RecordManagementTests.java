package com.luka.recordManagement;

import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.RecordPage;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordManagementTests {
    // the test from the book Fig 6.15, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testRecordInsertionSuccessful() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_records1");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        Layout layout = new Layout(sch, tx.blockSize());

        BlockId blk = tx.appendEmptyBlock("testfile", true);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int n = 0;

        int slot = rp.insertAfter(-1);
        while (slot >= 0) {
            rp.setInt(slot, "A", n);
            rp.setString(slot, "B", "rec" + n);
            slot = rp.insertAfter(slot);
            n++;

            if (n == 100) {
                break;
            }
        }

        int count = 0;
        slot = rp.nextAfter(-1);
        while (slot >= 0) {
            int a = rp.getInt(slot, "A");
            String b = rp.getString(slot, "B");
            if (a < 25 || a > 75) {
                count++;
                rp.delete(slot);
            }
            slot = rp.nextAfter(slot);
        }

        assertEquals(50, count);

        slot = rp.nextAfter(-1);
        while (slot >= 0) {
            int a = rp.getInt(slot, "A");
            String b = rp.getString(slot, "B");

            assertTrue(a >= 25);
            assertTrue(a <= 75);

            slot = rp.nextAfter(slot);
        }
        tx.unpin(blk);
        tx.commit();
    }

    @Test
    public void testRecordNullValues() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_records2");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", true);
        sch.addStringField("B", 50, true);
        Layout layout = new Layout(sch, tx.blockSize());

        BlockId blk = tx.appendEmptyBlock("testfile", true);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(-1);

        rp.setNull(slot, "A");
        assertTrue(rp.isNull(slot, "A"));

        rp.setString(slot, "B", "TestString");
        rp.setNull(slot, "B");
        assertTrue(rp.isNull(slot, "B"));

        tx.unpin(blk);
        tx.commit();
    }
}
