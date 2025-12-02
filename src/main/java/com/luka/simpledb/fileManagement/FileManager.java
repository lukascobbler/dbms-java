package com.luka.simpledb.fileManagement;

import com.luka.simpledb.fileManagement.exceptions.FileException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/// The `FileManager` is the main API interface to the operating
/// system's files. Also called a Pager. Works exclusively with
/// blocks and pages, not direct bytes.
public class FileManager {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    /// The constructor initializes the directory where the database files will
    /// be stored by removing all files that start with "temp", or if the directory
    /// doesn't exist, it creates it.
    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();

        if (isNew) {
            if (!dbDirectory.mkdirs()) {
                throw new FileException("could not initialize directory: " + dbDirectory);
            }
        }

        for (String filename : Objects.requireNonNull(dbDirectory.list())) {
            if (filename.startsWith("temp")) {
                if (!new File(dbDirectory, filename).delete()) {
                    throw new FileException("could not delete temporary file: " + filename);
                }
            }
        }
    }

    /// Reads the given block, from the provided block id,
    /// into the provided page. Method is `synchronized` because only one
    /// access to any of the files at the same time.
    ///
    /// @return The number of bytes read.
    public synchronized int read(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.filename());
            f.seek((long) blockId.blockNum() * blockSize);
            return f.getChannel().read(page.contents());
        } catch (IOException e) {
            throw new FileException("cannot read block " + blockId);
        }
    }

    /// Writes into the given block, from the provided block id,
    /// from the provided page. Method is `synchronized` because only one
    /// access to any of the files at the same time.
    ///
    /// @return The number of bytes written.
    public synchronized int write(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.filename());
            f.seek((long) blockId.blockNum() * blockSize);
            return f.getChannel().write(page.contents());
        } catch (IOException e) {
            throw new FileException("cannot write block " + blockId);
        }
    }

    /// Appends a new empty block of the system's block size
    /// to the end of the file. Method is `synchronized` because only one
    /// access to any of the files at the same time.
    ///
    /// @return The block id that corresponds to the newly written empty
    /// block of the given file.
    public synchronized BlockId append(String filename) {
        int newBlockNum = lengthInBlocks(filename);
        BlockId blockId = new BlockId(filename, newBlockNum);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile f = getFile(blockId.filename());
            f.seek((long) blockId.blockNum() * blockSize);
            f.write(bytes);
        } catch (IOException e) {
            throw new FileException("cannot append block " + blockId);
        }

        return blockId;
    }

    /// Truncates a file by removing the last block,
    /// regardless of its contents. Method is `synchronized`
    /// because only one access to any of the files at the same time.
    public synchronized void truncate(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            long length = f.length();
            long newLength = length > blockSize ? length - blockSize : 0;
            f.setLength(newLength);
        } catch (IOException e) {
            throw new FileException("cannot truncate file " + filename);
        }
    }

    /// @return The number of blocks that the provided file consists of.
    public int lengthInBlocks(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blockSize);
        } catch (IOException e) {
            throw new FileException("cannot access " + filename);
        }
    }

    /// @return Block size of the file manager.
    /// Also called the system's block size.
    public int getBlockSize() {
        return blockSize;
    }

    /// @return A boolean representing if the database folder
    /// was created for the first time in this file manager
    /// initialization.
    public boolean isNew() {
        return isNew;
    }

    /// The implementation of this function creates a file if it doesn't
    /// exist in the map of currently opened files.
    /// Each file that is created is opened with "rws", meaning it is open
    /// for reading, writing and a note to the operating system that each file
    /// write must be immediate, skipping any buffers in between.
    /// Each created file is also inserted in the open files map.
    ///
    /// @return The file corresponding to the filename, from the file manager's
    /// open files map.
    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);

        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }

        return f;
    }

    private static boolean isAllZero(byte[] buffer) {
        for (byte b : buffer) {
            if (b != 0) return false;
        }
        return true;
    }
}
