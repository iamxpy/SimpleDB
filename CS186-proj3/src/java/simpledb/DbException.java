package simpledb;

import java.lang.Exception;

/** Generic database exception class */
public class DbException extends Exception {
    private static final long serialVersionUID = 1L;

    public DbException(String s) {
        super(s);
    }
}
