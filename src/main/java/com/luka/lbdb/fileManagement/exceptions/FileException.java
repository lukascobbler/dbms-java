package com.luka.lbdb.fileManagement.exceptions;

public class FileException extends RuntimeException {
    public FileException(String message) {
        super(message);
    }

    public FileException() {
        super();
    }
}
