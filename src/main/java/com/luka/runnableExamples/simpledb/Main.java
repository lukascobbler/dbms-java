package com.luka.runnableExamples.simpledb;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.fileManagement.Page;

import java.io.File;

public class Main {
    public static void main(String[] args) {
        testEmptyString();
    }

    public static void truncationTest() {
        File dbDir = new File("./db_test2");
        FileManager mgr = new FileManager(dbDir, 8);

        for (int i = 0; i < 10; i++) {
            mgr.append("temp_file");
        }

        Page p = new Page(mgr.getBlockSize());
        p.setInt(4, 1);

        BlockId b0 = new BlockId("temp_file", 0);
        BlockId b1 = new BlockId("temp_file", 1);
        BlockId b4 = new BlockId("temp_file", 4);
        BlockId b7 = new BlockId("temp_file", 7);

        mgr.write(b0, p);
        mgr.write(b1, p);
        mgr.write(b4, p);
        mgr.write(b7, p);

        if (mgr.lengthInBlocks("temp_file") != 10) {
            throw new RuntimeException();
        }

        mgr.truncate("temp_file");

        if (mgr.lengthInBlocks("temp_file") != 8) {
            throw new RuntimeException();
        }
    }

    public static void testEmptyString() {
        Page p = new Page(4096);

        System.out.println(p.getString(57));
    }

    public static void testPages() {
        File dbDir = new File("./db_test1");
        FileManager mgr = new FileManager(dbDir, 4096);

        BlockId b1 = new BlockId("temp_table1", 0);
        Page p1 = new Page(mgr.getBlockSize());

        p1.setInt(0, 55);
        p1.setString(5, "string");
        p1.setBoolean(100, true);

        mgr.write(b1, p1);

        //mgr.append("temp_table1");

        Page p2 = new Page(mgr.getBlockSize());
        mgr.read(b1, p2);

        System.out.println("int1: " + p2.getInt(0));
        System.out.println("str1: " + p2.getString(5));
        System.out.println("bool1: " + p2.getBoolean(100));
    }
}
