package com.luka.runnableExamples.simpledb;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileMgr;
import com.luka.simpledb.fileManagement.Page;

import java.io.File;

public class Main {
    public static void main(String[] args) {
        File dbDir = new File("./db_test1");
        FileMgr mgr = new FileMgr(dbDir, 4096);

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
