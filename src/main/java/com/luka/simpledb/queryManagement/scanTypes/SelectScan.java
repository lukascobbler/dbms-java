package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.exceptions.ScanCantBeUpdateScanException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.recordManagement.RecordId;

public class SelectScan implements UpdateScan {
    private final Scan scan;
    private final Predicate predicate;

    public SelectScan(Scan scan, Predicate predicate) {
        this.scan = scan;
        this.predicate = predicate;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int getInt(String fieldName) {
        if (!hasField(fieldName)) {
            throw new FieldNotFoundInScanException();
        }
        return scan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if (!hasField(fieldName)) {
            throw new FieldNotFoundInScanException();
        }
        return scan.getString(fieldName);
    }

    @Override
    public boolean getBoolean(String fieldName) {
        if (!hasField(fieldName)) {
            throw new FieldNotFoundInScanException();
        }
        return scan.getBoolean(fieldName);
    }

    @Override
    public Constant getValue(String fieldName) {
        if (!hasField(fieldName)) {
            throw new FieldNotFoundInScanException();
        }
        return scan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan.hasField(fieldName);
    }

    @Override
    public void setInt(String fieldName, int value) {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            if (!hasField(fieldName)) {
                throw new FieldNotFoundInScanException();
            }
            updateScan.setInt(fieldName, value);
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public void setString(String fieldName, String value) {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            if (!hasField(fieldName)) {
                throw new FieldNotFoundInScanException();
            }
            updateScan.setString(fieldName, value);
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public void setBoolean(String fieldName, boolean value) {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            if (!hasField(fieldName)) {
                throw new FieldNotFoundInScanException();
            }
            updateScan.setBoolean(fieldName, value);
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public void setValue(String fieldName, Constant value) {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            if (!hasField(fieldName)) {
                throw new FieldNotFoundInScanException();
            }
            updateScan.setValue(fieldName, value);
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public void insert() {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            updateScan.insert();
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public void delete() {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            updateScan.delete();
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public RecordId getRecordId() {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            return updateScan.getRecordId();
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }

    @Override
    public void moveToRecordId(RecordId recordId) {
        try {
            UpdateScan updateScan = (UpdateScan) scan;
            updateScan.moveToRecordId(recordId);
        } catch (ClassCastException e) {
            throw new ScanCantBeUpdateScanException();
        }
    }
}
