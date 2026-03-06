package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.UnaryUpdateScan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

import java.util.function.BiConsumer;
import java.util.function.Function;

public class RenameScan extends UnaryUpdateScan {
    private final String oldFieldName, newFieldName;

    public RenameScan(UpdateScan updateScan, String oldFieldName, String newFieldName) {
        super(updateScan);
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;
    }

    private String map(String fieldName) {
        if (fieldName.equals(newFieldName)) return oldFieldName;
        if (fieldName.equals(oldFieldName)) return null;
        return fieldName;
    }

    @Override
    public boolean hasField(String fieldName) {
        String translated = map(fieldName);
        return translated != null && super.hasField(translated);
    }

    @Override
    protected int internalGetInt(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetInt);
    }

    @Override
    protected String internalGetString(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetString);
    }

    @Override
    protected boolean internalGetBoolean(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetBoolean);
    }

    @Override
    protected Constant internalGetValue(String fieldName) {
        return wrapGetMapping(fieldName, super::internalGetValue);
    }

    @Override
    protected void internalSetInt(String fieldName, int value) {
        wrapSetMapping(fieldName, value, super::internalSetInt);
    }

    @Override
    protected void internalSetString(String fieldName, String value) {
        wrapSetMapping(fieldName, value, super::internalSetString);
    }

    @Override
    protected void internalSetBoolean(String fieldName, boolean value) {
        wrapSetMapping(fieldName, value, super::internalSetBoolean);
    }

    @Override
    protected void internalSetValue(String fieldName, Constant value) {
        wrapSetMapping(fieldName, value, super::internalSetValue);
    }

    private <T> T wrapGetMapping(String fieldName, Function<String, T> sourceOperation) {
        String translated = map(fieldName);
        if (translated == null) {
            throw new FieldNotFoundInScanException(fieldName);
        }
        return sourceOperation.apply(translated);
    }

    private <T> void wrapSetMapping(String fieldName, T value, BiConsumer<String, T> setter) {
        String translated = map(fieldName);
        if (translated == null) {
            throw new FieldNotFoundInScanException(fieldName);
        }
        setter.accept(translated, value);
    }
}
