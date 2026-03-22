package com.luka.fileManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.fileManagement.Page;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class FileManagementTests {
    @Test
    public void testTruncate() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        FileManager mgr = new FileManager(tmpDir, 1);

        for (int i = 0; i < 10; i++) {
            mgr.append("truncate");
        }

        for (int i = 0; i < 10; i++) {
            assertEquals(10 - i, mgr.lengthInBlocks("truncate"));
            mgr.truncate("truncate");
        }

        assertEquals(0, mgr.lengthInBlocks("truncate"));
    }

    @Test
    public void testPageWriting() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        FileManager mgr = new FileManager(tmpDir, 200);
        mgr.append("read_write");

        BlockId b1 = new BlockId("read_write", 0);
        Page p1 = new Page(mgr.getBlockSize());

        p1.setInt(0, 55);
        p1.setString(5, "string");
        p1.setBoolean(100, true);

        mgr.write(b1, p1);

        Page p2 = new Page(mgr.getBlockSize());
        mgr.read(b1, p2);

        assertEquals(p1.getInt(0), p2.getInt(0));
        assertEquals(p1.getString(0), p2.getString(0));
        assertEquals(p1.getBoolean(0), p2.getBoolean(0));
    }
}
