package com.realtime.common.exception;

public class MinioStorageException extends BusinessException {

    public MinioStorageException() {
        super(ErrorCode.STORAGE_ERROR);
    }

    public MinioStorageException(String message) {
        super(ErrorCode.STORAGE_ERROR, message, null);
    }

    public MinioStorageException(String message, Throwable cause) {
        super(ErrorCode.STORAGE_ERROR, message, cause);
    }

    public MinioStorageException(Throwable cause) {
        super(ErrorCode.STORAGE_ERROR, cause);
    }
}


