package com.yourjavaguy.pitwall.commons.exception;

public abstract class BaseException extends RuntimeException {

    protected BaseException(String message) {
        super(message);
    }
}
