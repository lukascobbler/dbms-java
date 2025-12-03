package com.luka.transactionManagement;

import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.concurrencyManagement.LockTable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionManagementTests {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetSetInt(boolean undoOnlyRecovery) throws IOException {
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction1");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

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
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction2");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "temp_log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

        BlockId b0 = new BlockId("file1", 0);
        int setInt = 5;

        transaction1.append("file1", true);
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
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction3");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        Transaction transaction0 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

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
    public void testSystemRecoveryUndoNonCommitedAppendBlocks(boolean undoOnlyRecovery) throws IOException {
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction4");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        Transaction transaction0 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

        transaction0.append("file1", true);
        transaction0.append("file1", true);
        transaction0.append("file1", true);
        // no commit or rollback

        // simulating system crash by manager reinstantiation
        fileManager = new FileManager(dbDir, 4096);
        logManager = new LogManager(fileManager, "log_file");
        bufferManager = new BufferManager(fileManager, logManager, 10);
        lockTable = new LockTable();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction1.recover();

        assertEquals(0, fileManager.lengthInBlocks("file1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSystemRecoveryUndoNonCommitedUpdates(boolean undoOnlyRecovery) throws IOException {
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction5");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        BlockId b0 = new BlockId("file1", 0);

        int setInt = 5;
        String setString = "test";
        boolean setBoolean = true;

        Transaction transaction0 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

        transaction0.append("file1", true);
        transaction0.commit();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.setString(b0, 100, setString, true);
        transaction1.setBoolean(b0, 200, setBoolean, true);
        // no commit or rollback

        // simulating system crash by manager reinstantiation
        fileManager = new FileManager(dbDir, 4096);
        logManager = new LogManager(fileManager, "log_file");
        bufferManager = new BufferManager(fileManager, logManager, 10);
        lockTable = new LockTable();

        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction2.recover();

        assertEquals(1, fileManager.lengthInBlocks("file1"));

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
    public void testSystemRecoveryUndoNonCommitedMixed(boolean undoOnlyRecovery) throws IOException {
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction6");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        BlockId b0 = new BlockId("file1", 0);
        BlockId b3 = new BlockId("file1", 3);

        int setInt = 5;
        String setString = "test";
        boolean setBoolean = true;

        Transaction transaction0 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

        transaction0.append("file1", true);
        transaction0.commit();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.setString(b0, 100, setString, true);
        transaction1.setBoolean(b0, 200, setBoolean, true);
        transaction1.append("file1", true);
        transaction1.append("file1", true);
        transaction1.append("file1", true);
        transaction1.pin(b3);
        transaction1.setInt(b3, 0, 123, true);
        // no commit or rollback

        // simulating system crash by manager reinstantiation
        fileManager = new FileManager(dbDir, 4096);
        logManager = new LogManager(fileManager, "log_file");
        bufferManager = new BufferManager(fileManager, logManager, 10);
        lockTable = new LockTable();

        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction2.recover();

        assertEquals(1, fileManager.lengthInBlocks("file1"));

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
    public void testTransactionNumbersContinuingAfterRecoveryWithQuiescentCheckpointing(boolean undoOnlyRecovery) throws NoSuchFieldException, IllegalAccessException, IOException {
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction7");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        BlockId b0 = new BlockId("file1", 0);

        Field field = Transaction.class.getDeclaredField("nextTransactionNum");
        field.setAccessible(true);
        field.set(null, 0);

        Transaction transaction0 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

        transaction0.append("file1", true);
        transaction0.commit();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction1.pin(b0);
        transaction1.setInt(b0, 0, 1, true);
        transaction1.commit();

        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction2.pin(b0);
        transaction2.setInt(b0, 0, 2, true);
        transaction2.commit();

        Transaction transaction3 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction3.pin(b0);
        transaction3.setInt(b0, 0, 3, true);
        transaction3.commit();

        Transaction transaction4 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction4.pin(b0);
        transaction4.setInt(b0, 0, 4, true);
        transaction4.commit();

        // simulating system crash by manager reinstantiation
        fileManager = new FileManager(dbDir, 4096);
        logManager = new LogManager(fileManager, "log_file");
        bufferManager = new BufferManager(fileManager, logManager, 10);
        lockTable = new LockTable();

        field.set(null, 0);

        // first test without quiescent checkpointing
        Transaction transaction5 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction5.recover();

        assertEquals(1, fileManager.lengthInBlocks("file1"));

        assertEquals(6, transaction5.getTransactionNumber());

        transaction5.pin(b0);
        int retunedInt = transaction5.getInt(b0, 0);

        assertEquals(4, retunedInt);

        // with quiescent checkpointing
        Transaction transaction6 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction6.recover();

        assertEquals(6, transaction5.getTransactionNumber());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRedoRecovery(boolean undoOnlyRecovery) throws IOException {
        String tempDirectory = System.getProperty("java.io.tmpdir");

        File dbDir = new File(tempDirectory + "/temp_transaction8");
        boolean s1 = dbDir.mkdirs();

        for (String filename : Objects.requireNonNull(dbDir.list())) {
            boolean s2 = new File(dbDir, filename).delete();
        }

        // log file initialization
        RandomAccessFile f = new RandomAccessFile(dbDir + "/log_file", "rws");
        f.close();

        FileManager fileManager = new FileManager(dbDir, 4096);
        LogManager logManager = new LogManager(fileManager, "log_file");
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 10);
        LockTable lockTable = new LockTable();

        Transaction transaction1 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);

        BlockId b0 = new BlockId("file1", 0);
        int setInt = 5;

        transaction1.pin(b0);
        transaction1.append("file1", true);
        transaction1.setInt(b0, 0, setInt, true);
        transaction1.setInt(b0, 100, 2500, true);
        transaction1.setString(b0, 200, "no", true);
        transaction1.commit();

        // simulating system crash by manager reinstantiation
        fileManager = new FileManager(dbDir, 4096);
        logManager = new LogManager(fileManager, "log_file");
        bufferManager = new BufferManager(fileManager, logManager, 10);
        lockTable = new LockTable();

        Transaction transaction2 = new Transaction(fileManager, logManager, bufferManager, lockTable, undoOnlyRecovery);
        transaction2.recover();

        transaction2.pin(b0);
        int retunedInt = transaction2.getInt(b0, 0);
        transaction2.commit();

        assertEquals(setInt, retunedInt);
    }
}
