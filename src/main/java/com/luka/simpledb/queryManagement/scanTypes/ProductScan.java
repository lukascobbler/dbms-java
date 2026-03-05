package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.virtualEntities.constants.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;

public class ProductScan implements Scan {
    private final Scan scan1, scan2;

    public ProductScan(Scan scan1, Scan scan2) {
        this.scan1 = scan1;
        this.scan2 = scan2;
        scan1.beforeFirst();
    }

    @Override
    public void beforeFirst() {
        scan1.beforeFirst();
        scan1.next();
        scan2.beforeFirst();
    }

    @Override
    public boolean next() {
        if (scan2.next()) {
            return true;
        } else {
            scan2.beforeFirst();
            return scan2.next() && scan1.next();
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getInt(fieldName);
        }
        if (scan2.hasField(fieldName)) {
            return scan2.getInt(fieldName);
        }

        throw new FieldNotFoundInScanException();
    }

    @Override
    public String getString(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getString(fieldName);
        }
        if (scan2.hasField(fieldName)) {
            return scan2.getString(fieldName);
        }

        throw new FieldNotFoundInScanException();
    }

    @Override
    public boolean getBoolean(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getBoolean(fieldName);
        }
        if (scan2.hasField(fieldName)) {
            return scan2.getBoolean(fieldName);
        }

        throw new FieldNotFoundInScanException();
    }

    @Override
    public Constant getValue(String fieldName) {
        if (scan1.hasField(fieldName)) {
            return scan1.getValue(fieldName);
        }
        if (scan2.hasField(fieldName)) {
            return scan2.getValue(fieldName);
        }

        throw new FieldNotFoundInScanException();
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan1.hasField(fieldName) || scan1.hasField(fieldName);
    }
}
