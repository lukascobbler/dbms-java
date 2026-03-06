package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.RecordId;

public abstract class UpdateScan extends Scan {
    // general update scan features that must have redefined behavior
    public abstract void insert();
    public abstract void delete();
    public abstract RecordId getRecordId();
    public abstract void moveToRecordId(RecordId rid);

    // setters with field exist checks that mustn't have redefined behavior
    public final void setInt(String fld, int val) { validate(fld); internalSetInt(fld, val); }
    public final void setString(String fld, String val) { validate(fld); internalSetString(fld, val); }
    public final void setBoolean(String fld, boolean v) { validate(fld); internalSetBoolean(fld, v); }
    public final void setValue(String fld, Constant v) { validate(fld); internalSetValue(fld, v); }

    // setters with no field exist checks that must have redefined behavior
    protected abstract void internalSetInt(String fld, int val);
    protected abstract void internalSetString(String fld, String val);
    protected abstract void internalSetBoolean(String fld, boolean val);
    protected abstract void internalSetValue(String fld, Constant val);
}