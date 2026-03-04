package com.luka.simpledb.queryManagement.exceptions;

public class ScanCantBeUpdateScanException extends RuntimeException {
    public ScanCantBeUpdateScanException(String message) {
        super(message);
    }

    public ScanCantBeUpdateScanException() {
        super();
    }
}
