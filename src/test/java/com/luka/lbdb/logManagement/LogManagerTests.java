package com.luka.lbdb.logManagement;

import com.luka.lbdb.fileManagement.FileManager;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class LogManagerTests {
    @Test
    public void testAppendSingleRecord() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        FileManager fileManager = new FileManager(tmpDir, 512);
        LogManager logManager = new LogManager(fileManager, "test.log");

        byte[] record = "single_record".getBytes();
        int lsn = logManager.append(record);
        assertEquals(1, lsn);

        Iterator<byte[]> iterator = logManager.iterator();
        assertTrue(iterator.hasNext());
        assertArrayEquals(record, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAppendMultipleRecordsSameBlock() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        FileManager fileManager = new FileManager(tmpDir, 512);
        LogManager logManager = new LogManager(fileManager, "test.log");

        byte[] record1 = "record_one".getBytes();
        byte[] record2 = "record_two".getBytes();
        byte[] record3 = "record_three".getBytes();

        logManager.append(record1);
        logManager.append(record2);
        logManager.append(record3);

        Iterator<byte[]> iterator = logManager.iterator();

        assertTrue(iterator.hasNext());
        assertArrayEquals(record3, iterator.next());

        assertTrue(iterator.hasNext());
        assertArrayEquals(record2, iterator.next());

        assertTrue(iterator.hasNext());
        assertArrayEquals(record1, iterator.next());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAppendRecordsCrossingBlockBoundary() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        FileManager fileManager = new FileManager(tmpDir, 128);
        LogManager logManager = new LogManager(fileManager, "test.log");

        byte[] record1 = new byte[120];
        byte[] record2 = new byte[120];
        byte[] record3 = new byte[120];

        record1[0] = 1;
        record2[0] = 2;
        record3[0] = 3;

        logManager.append(record1);
        logManager.append(record2);

        logManager.append(record3);

        Iterator<byte[]> iterator = logManager.iterator();

        assertTrue(iterator.hasNext());
        assertArrayEquals(record3, iterator.next());

        assertTrue(iterator.hasNext());
        assertArrayEquals(record2, iterator.next());

        assertTrue(iterator.hasNext());
        assertArrayEquals(record1, iterator.next());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testFlushLogSequenceNumber() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        FileManager fileManager = new FileManager(tmpDir, 512);
        LogManager logManager = new LogManager(fileManager, "test.log");

        byte[] record = "flush_record".getBytes();
        int lsn = logManager.append(record);

        logManager.flush(lsn);

        Iterator<byte[]> iterator = logManager.iterator();
        assertTrue(iterator.hasNext());
        assertArrayEquals(record, iterator.next());
    }

    @Test
    public void testArchiveLogFile() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        FileManager fileManager = new FileManager(tmpDir, 512);
        LogManager logManager = new LogManager(fileManager, "test.log");

        byte[] oldRecord = "old_record".getBytes();
        logManager.append(oldRecord);
        logManager.archiveLogFile();

        Path archiveDir = tmpDir.resolve("log_archive");
        assertTrue(Files.exists(archiveDir));

        Path archivedFile = archiveDir.resolve("log_file_0");
        assertTrue(Files.exists(archivedFile));

        byte[] newRecord = "new_record".getBytes();
        logManager.append(newRecord);

        Iterator<byte[]> iterator = logManager.iterator();
        assertTrue(iterator.hasNext());
        assertArrayEquals(newRecord, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRecoverExistingLogFile() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        FileManager fileManager1 = new FileManager(tmpDir, 512);
        LogManager logManager1 = new LogManager(fileManager1, "test.log");

        byte[] persistentRecord = "persistent_record".getBytes();
        int lsn = logManager1.append(persistentRecord);
        logManager1.flush(lsn);

        FileManager fileManager2 = new FileManager(tmpDir, 512);
        LogManager logManager2 = new LogManager(fileManager2, "test.log");

        Iterator<byte[]> iterator = logManager2.iterator();
        assertTrue(iterator.hasNext());
        assertArrayEquals(persistentRecord, iterator.next());
        assertFalse(iterator.hasNext());
    }
}
