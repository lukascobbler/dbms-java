package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.BinaryScan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

public class AntijoinScan extends BinaryScan {
    private final Predicate predicate;

    public AntijoinScan(Scan childScan1, Scan childScan2, Predicate predicate) {
        super(childScan1, childScan2);
        this.predicate = predicate;
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
        while (childScan1.next()) {
            childScan2.beforeFirst();
            boolean foundMatch = false;

            while (childScan2.next()) {
                if (predicate.isSatisfied(this)) {
                    foundMatch = true;
                    break;
                }
            }

            if (!foundMatch) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean previous() {
        while (childScan1.previous()) {
            childScan2.beforeFirst();
            boolean foundMatch = false;

            while (childScan2.next()) {
                if (predicate.isSatisfied(this)) {
                    foundMatch = true;
                    break;
                }
            }

            if (!foundMatch) {
                return true;
            }
        }
        return false;
    }
}
