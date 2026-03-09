package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

/// Intermediate read-only binary scan used to encapsulate logic
/// of getting values from different schema scans. It acts like a
/// scan to be compliant with the scan API, but should only be used
/// as a field of other scans that will be passed where the logic
/// for getters requires checking both scans.
public class DiffSchemaJoinContextScan extends BinaryScan {
    public DiffSchemaJoinContextScan(Scan childScan1, Scan childScan2) {
        super(childScan1, childScan2);
    }

    /// Join context can't navigate anything.
    @Override
    public void beforeFirst() { throw new UnsupportedOperationException(); }

    /// Join context can't navigate anything.
    @Override
    public void afterLast() { throw new UnsupportedOperationException(); }

    /// Join context can't navigate anything.
    @Override
    public boolean next() { throw new UnsupportedOperationException(); }

    /// Join context can't navigate anything.
    @Override
    public boolean previous() { throw new UnsupportedOperationException(); }

    /// Close does nothing because this context is used
    /// as an intermediate-result for get operations only.
    /// It is not a real scan.
    @Override
    public void close() { }

    /// @return True if either scan has the field.
    @Override
    public boolean hasField(String fieldName) {
        return childScan1.hasField(fieldName) || childScan2.hasField(fieldName);
    }

    // todo check that the field only exists in one of the scans and
    //  does not exist in the other scan
    /// @return The integer value from the scan that has that field.
    @Override
    protected int internalGetInt(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getInt(fieldName);
        }

        return childScan2.getInt(fieldName);
    }

    /// @return The string value from the scan that has that field.
    @Override
    protected String internalGetString(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getString(fieldName);
        }

        return childScan2.getString(fieldName);
    }

    /// @return The boolean value from the scan that has that field.
    @Override
    protected boolean internalGetBoolean(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getBoolean(fieldName);
        }

        return childScan2.getBoolean(fieldName);
    }

    /// @return The constant from the scan that has that field.
    @Override
    protected Constant internalGetValue(String fieldName) {
        if (childScan1.hasField(fieldName)) {
            return childScan1.getValue(fieldName);
        }

        return childScan2.getValue(fieldName);
    }
}
