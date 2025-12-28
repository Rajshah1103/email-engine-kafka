package com.raj.mailengine.exception;

public class PermanentEmailException extends RuntimeException{

    public PermanentEmailException() {
        super();
    }

    public PermanentEmailException(String message) {
        super(message);
    }

    public PermanentEmailException(String message, Throwable cause) {
        super(message, cause);
    }
}
