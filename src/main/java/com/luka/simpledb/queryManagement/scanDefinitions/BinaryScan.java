package com.luka.simpledb.queryManagement.scanDefinitions;

/// Binary scans do not have default implementations for methods because there are
/// no sensible defaults for scans that operate on two child scans. Every scan that
/// is a binary scan will be totally different in all aspects (except `close`).
public abstract class BinaryScan extends Scan {
    protected final Scan childScan1;
    protected final Scan childScan2;

    public BinaryScan(Scan s1, Scan s2) {
        this.childScan1 = s1;
        this.childScan2 = s2;
    }

    @Override
    public void close() {
        childScan1.close();
        childScan2.close();
    }
}
