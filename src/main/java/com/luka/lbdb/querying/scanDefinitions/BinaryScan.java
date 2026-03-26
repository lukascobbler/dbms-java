package com.luka.lbdb.querying.scanDefinitions;

/// A scan class that defines default functionality for all scans
/// that build on top of **two** child scans, which are the scans
/// that operate on two tables. All scans that operate on two
/// tables should extend this class and redefine only needed behaviors.
///
/// Binary scans do not have a default implementation for any method
/// because there are no sensible defaults for scans that operate on two child scans.
public abstract class BinaryScan extends Scan {
    protected final Scan childScan1;
    protected final Scan childScan2;

    public BinaryScan(Scan childScan1, Scan childScan2) {
        this.childScan1 = childScan1;
        this.childScan2 = childScan2;
    }

    @Override
    public void close() {
        childScan1.close();
        childScan2.close();
    }
}
