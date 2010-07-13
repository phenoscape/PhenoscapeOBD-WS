package org.phenoscape.obd.query;

/**
 * This is a RuntimeException used to wrap checked exceptions 
 * which must be passed up to calling code while performing data queries 
 * for web services. Checked exceptions must be converted to runtime exceptions 
 * when implementing an interface which is not declared to throw the checked 
 * exception.
 */
@SuppressWarnings("serial")
public class QueryException extends RuntimeException {

    public QueryException() {}

    public QueryException(String message) {
        super(message);
    }

    public QueryException(Throwable cause) {
        super(cause);
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }

}
