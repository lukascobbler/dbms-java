package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;

import java.util.Collection;
import java.util.List;

public class ProjectScan implements Scan {
    private final Scan scan;
    private final Collection<String> fieldList;

    public ProjectScan(Scan scan, List<String> fieldList) {
        this.scan = scan;
        this.fieldList = fieldList;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public void afterLast() {
        scan.afterLast();
    }

    @Override
    public boolean next() {
        return scan.next();
    }

    @Override
    public boolean previous() {
        return scan.previous();
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName);
        } else {
            throw new FieldNotFoundInScanException();
        }
    }

    @Override
    public String getString(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getString(fieldName);
        } else {
            throw new FieldNotFoundInScanException();
        }
    }

    @Override
    public boolean getBoolean(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getBoolean(fieldName);
        } else {
            throw new FieldNotFoundInScanException();
        }
    }

    @Override
    public Constant getValue(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getValue(fieldName);
        } else {
            throw new FieldNotFoundInScanException();
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() throws Exception {
        scan.close();
    }
}
