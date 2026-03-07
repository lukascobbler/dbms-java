package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.BinaryScan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;

public class ProductScan extends BinaryScan {
    public ProductScan(Scan scan1, Scan scan2) {
        super(scan1, scan2);
    }

    @Override
    public void beforeFirst() {
        childScan1.beforeFirst();
        if (childScan1.next()) {
            childScan2.beforeFirst();
        }
    }

    @Override
    public void afterLast() {
        childScan1.afterLast();
        if (childScan1.previous()) {
            childScan2.afterLast();
        }
    }

    @Override
    public boolean next() {
        if (childScan2.next()) {
            return true;
        }

        if (childScan1.next()) {
            childScan2.beforeFirst();
            return childScan2.next();
        }

        return false;
    }

    @Override
    public boolean previous() {
        if (childScan2.previous()) {
            return true;
        }

        if (childScan1.previous()) {
            childScan2.afterLast();
            return childScan2.previous();
        }

        return false;
    }
}
