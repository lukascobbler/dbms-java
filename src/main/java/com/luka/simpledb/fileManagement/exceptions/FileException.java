package com.luka.simpledb.fileManagement.exceptions;

public class FileException extends RuntimeException {
    public FileException(String message) {
        super(message);
    }

    public FileException() {
        super();
    }
}
