package com.luka.simpledb.fileManagement;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();

        if (isNew) {
            dbDirectory.mkdirs();
        }

        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized int read(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.getFilename());
            f.seek(blockId.getBlockNum() * blockSize);
            return f.getChannel().read(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blockId);
        }
    }

    public synchronized int write(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.getFilename());
            f.seek(blockId.getBlockNum() * blockSize);
            return f.getChannel().write(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + blockId);
        }
    }

    public synchronized BlockId append(String filename) {
        int newBlockNum = lengthInBlocks(filename);
        BlockId blockId = new BlockId(filename, newBlockNum);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile f = getFile(blockId.getFilename());
            f.seek(blockId.getBlockNum() * blockSize);
            f.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + blockId);
        }

        return blockId;
    }

    public int lengthInBlocks(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);

        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }

        return f;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public boolean isNew() {
        return isNew;
    }
}
