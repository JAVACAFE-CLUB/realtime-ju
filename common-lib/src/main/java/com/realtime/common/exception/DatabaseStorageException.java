package com.realtime.common.exception;

public class DatabaseStorageException extends BusinessException {

    public DatabaseStorageException() {
        super(ErrorCode.DATABASE_ERROR);
    }

    public DatabaseStorageException(String message) {
        super(ErrorCode.DATABASE_ERROR, message, null);
    }

    public DatabaseStorageException(String message, Throwable cause) {
        super(ErrorCode.DATABASE_ERROR, message, cause);
    }

    public DatabaseStorageException(Throwable cause) {
        super(ErrorCode.DATABASE_ERROR, cause);
    }
}


