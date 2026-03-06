package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.UnaryUpdateScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;

public class SelectScan extends UnaryUpdateScan {
    private final Predicate predicate;

    public SelectScan(UpdateScan scan, Predicate predicate) {
        super(scan);
        this.predicate = predicate;
    }

    @Override
    public boolean next() {
        while (childScan.next()) {
            if (predicate.isSatisfied(childScan)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean previous() {
        while (childScan.previous()) {
            if (predicate.isSatisfied(childScan)) {
                return true;
            }
        }

        return false;
    }
}
