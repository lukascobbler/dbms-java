package com.luka.transactionManagement;

import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.simpleDB.settings.SimpleDBSettings;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.concurrencyManagement.LockTable;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionManagementTests {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetSetInt(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;
        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);

        Transaction transaction1 = simpleDB.newTransaction();
        Transaction transaction2 = simpleDB.newTransaction();

        BlockId b0 = new BlockId("file1", 0);
        int setInt = 5;

        transaction1.pin(b0);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.commit();

        transaction2.pin(b0);
        int retunedInt = transaction2.getInt(b0, 0);
        transaction2.commit();

        assertEquals(setInt, retunedInt);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetSetIntRollback(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;
        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);

        Transaction transaction1 = simpleDB.newTransaction();
        Transaction transaction2 = simpleDB.newTransaction();

        BlockId b0 = new BlockId("file1", 0);
        int setInt = 5;

        BlockId tmp = transaction1.appendEmptyBlock("file1", true);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.rollback();

        transaction2.pin(b0);
        int retunedInt = transaction2.getInt(b0, 0);
        transaction2.commit();

        assertEquals(0, retunedInt);
        assertNotEquals(setInt, retunedInt);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConcurrentGetIntSameBlockSameTime(boolean undoOnlyRecovery) throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings settings = new SimpleDBSettings();
        settings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;
        SimpleDB simpleDB = new SimpleDB(tmpDir, settings);

        Transaction transaction0 = simpleDB.newTransaction();
        Transaction transaction1 = simpleDB.newTransaction();
        Transaction transaction2 = simpleDB.newTransaction();

        BlockId b0 = new BlockId("file1", 0);
        int setInt = 5;

        transaction0.pin(b0);
        transaction0.setInt(b0, 0, setInt, true);
        transaction0.commit();

        transaction1.pin(b0);
        transaction2.pin(b0);

        int retunedInt1 = transaction1.getInt(b0, 0);
        int retunedInt2 = transaction2.getInt(b0, 0);

        transaction1.commit();
        transaction2.commit();

        assertEquals(setInt, retunedInt2);
        assertEquals(retunedInt1, retunedInt2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSystemRecoveryUndoNonCommitedAppendBlocks(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        LogManager lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        BufferManager bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        LockTable lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");
        AtomicInteger nextTxNum = (AtomicInteger) TestUtils.getPrivateField(simpleDB, "nextTransactionNum");

        Transaction transaction0 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        transaction0.appendEmptyBlock("file1", true);
        transaction0.appendEmptyBlock("file1", true);
        transaction0.appendEmptyBlock("file1", true);
        // no commit or rollback

        // simulating system crash by manager reinstantiation
        new SimpleDB(tmpDir);

        assertFalse(TestUtils.fileExists(tmpDir, "file1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSystemRecoveryUndoNonCommitedUpdates(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);

        BlockId b0 = new BlockId("file1", 0);

        int setInt = 5;
        String setString = "test";
        boolean setBoolean = true;

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        LogManager lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        BufferManager bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        LockTable lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");
        AtomicInteger nextTxNum = (AtomicInteger) TestUtils.getPrivateField(simpleDB, "nextTransactionNum");

        Transaction transaction0 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        transaction0.appendEmptyBlock("file1", true);
        transaction0.commit();

        Transaction transaction1 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.setString(b0, 100, setString, true);
        transaction1.setBoolean(b0, 200, setBoolean, true);
        // no commit or rollback

        // simulating system crash by manager reinstantiation
        simpleDB = new SimpleDB(tmpDir);
        fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");

        Transaction transaction2 = simpleDB.newTransaction();
        transaction2.recover();

        assertEquals(1, fm.lengthInBlocks("file1"));

        transaction2.pin(b0);
        int retunedInt = transaction2.getInt(b0, 0);
        String retunedString = transaction2.getString(b0, 100);
        boolean returnedBoolean = transaction2.getBoolean(b0, 200);

        assertEquals(0, retunedInt);
        assertEquals("", retunedString);
        assertFalse(returnedBoolean);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSystemRecoveryUndoNonCommitedMixed(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);

        BlockId b0 = new BlockId("file1", 0);
        BlockId b3 = new BlockId("file1", 3);

        int setInt = 5;
        String setString = "test";
        boolean setBoolean = true;

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        LogManager lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        BufferManager bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        LockTable lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");
        AtomicInteger nextTxNum = (AtomicInteger) TestUtils.getPrivateField(simpleDB, "nextTransactionNum");

        Transaction transaction0 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        transaction0.appendEmptyBlock("file1", true);
        transaction0.commit();

        Transaction transaction1 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.setString(b0, 100, setString, true);
        transaction1.setBoolean(b0, 200, setBoolean, true);
        transaction1.appendEmptyBlock("file1", true);
        transaction1.appendEmptyBlock("file1", true);
        transaction1.appendEmptyBlock("file1", true);
        transaction1.pin(b3);
        transaction1.setInt(b3, 0, 123, true);
        // no commit or rollback

        // simulating system crash by manager reinstantiation
        simpleDB = new SimpleDB(tmpDir);
        fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");

        Transaction transaction2 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction2.recover();

        assertEquals(1, fm.lengthInBlocks("file1"));

        transaction2.pin(b0);
        int retunedInt = transaction2.getInt(b0, 0);
        String retunedString = transaction2.getString(b0, 100);
        boolean returnedBoolean = transaction2.getBoolean(b0, 200);

        assertEquals(0, retunedInt);
        assertEquals("", retunedString);
        assertFalse(returnedBoolean);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTransactionNumbersContinuingAfterRecoveryWithQuiescentCheckpointing(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings recoverySettings = new SimpleDBSettings();
        recoverySettings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        SimpleDB simpleDB = new SimpleDB(tmpDir, recoverySettings);

        Field field = SimpleDB.class.getDeclaredField("nextTransactionNum");
        field.setAccessible(true);

        BlockId b0 = new BlockId("file1", 0);

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        LogManager lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        BufferManager bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        LockTable lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");
        AtomicInteger nextTxNum = (AtomicInteger) TestUtils.getPrivateField(simpleDB, "nextTransactionNum");

        Transaction transaction0 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        transaction0.appendEmptyBlock("file1", true);
        transaction0.commit();

        Transaction transaction1 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, 1, true);
        transaction1.commit();

        Transaction transaction2 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction2.pin(b0);
        transaction2.setInt(b0, 0, 2, true);
        transaction2.commit();

        Transaction transaction3 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction3.pin(b0);
        transaction3.setInt(b0, 0, 3, true);
        transaction3.commit();

        Transaction transaction4 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction4.pin(b0);
        transaction4.setInt(b0, 0, 4, true);
        transaction4.commit();

        // simulating system crash by manager reinstantiation
        field.set(simpleDB, new AtomicInteger(0));
        simpleDB = new SimpleDB(tmpDir, recoverySettings);
        fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");

        // first test without quiescent checkpointing
        Transaction transaction5 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        assertEquals(1, fm.lengthInBlocks("file1"));

        assertEquals(7, transaction5.getTransactionNumber());

        transaction5.pin(b0);
        int retunedInt = transaction5.getInt(b0, 0);

        assertEquals(4, retunedInt);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRedoRecovery(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings recoverySettings = new SimpleDBSettings();
        recoverySettings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        SimpleDB simpleDB = new SimpleDB(tmpDir, recoverySettings);

        FileManager fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        LogManager lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        BufferManager bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        LockTable lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");
        AtomicInteger nextTxNum = (AtomicInteger) TestUtils.getPrivateField(simpleDB, "nextTransactionNum");

        Transaction transaction1 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        BlockId b0 = new BlockId("file1", 0);
        int setInt = 5;

        transaction1.pin(b0);
        transaction1.appendEmptyBlock("file1", true);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.setInt(b0, 100, 2500, true);
        transaction1.setString(b0, 200, "no", true);
        transaction1.commit();

        // simulating system crash by manager reinstantiation
        simpleDB = new SimpleDB(tmpDir, recoverySettings);
        fm = (FileManager) TestUtils.getPrivateField(simpleDB, "fileManager");
        lm = (LogManager) TestUtils.getPrivateField(simpleDB, "logManager");
        bm = (BufferManager) TestUtils.getPrivateField(simpleDB, "bufferManager");
        lt = (LockTable) TestUtils.getPrivateField(simpleDB, "lockTable");

        Transaction transaction2 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        transaction2.pin(b0);
        int retunedInt = transaction2.getInt(b0, 0);
        transaction2.commit();

        assertEquals(setInt, retunedInt);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRemoveFileAddedByTransactionOneBlockLargeIfCommitNotSuccessful(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings recoverySettings = new SimpleDBSettings();
        recoverySettings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        FileManager fm = new FileManager(tmpDir, recoverySettings.BLOCK_SIZE);
        LogManager lm = new LogManager(fm, recoverySettings.LOG_FILE);
        BufferManager bm = new BufferManager(fm, lm, recoverySettings.BUFFER_POOL_SIZE);
        LockTable lt = new LockTable();
        AtomicInteger nextTxNum = new AtomicInteger(0);

        Transaction transaction1 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);

        transaction1.appendEmptyBlock("file1", true);

        fm = new FileManager(tmpDir, recoverySettings.BLOCK_SIZE);
        lm = new LogManager(fm, recoverySettings.LOG_FILE);
        bm = new BufferManager(fm, lm, recoverySettings.BUFFER_POOL_SIZE);
        lt = new LockTable();

        Transaction transaction2 = new Transaction(fm, lm, bm, lt, undoOnlyRecovery, nextTxNum);
        transaction2.recover();

        assertFalse(TestUtils.fileExists(tmpDir, "file1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDontCreateFileDefinedInThisTransactionAfterRollback(boolean undoOnlyRecovery) throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDBSettings recoverySettings = new SimpleDBSettings();
        recoverySettings.UNDO_ONLY_RECOVERY = undoOnlyRecovery;

        FileManager fileManager = new FileManager(tmpDir, recoverySettings.BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, recoverySettings.LOG_FILE);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, recoverySettings.BUFFER_POOL_SIZE);
        LockTable lockTable = new LockTable();
        AtomicInteger nextTxNum = new AtomicInteger(0);

        BlockId b0 = new BlockId("file1", 0);

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery, nextTxNum);
        transaction1.appendEmptyBlock("file1", true);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, 100, true);
        transaction1.unpin(b0);
        transaction1.rollback();

        assertFalse(TestUtils.fileExists(tmpDir, "file1"));
    }
}
