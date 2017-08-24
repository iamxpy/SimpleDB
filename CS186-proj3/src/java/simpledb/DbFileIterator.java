package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator extends Serializable {
    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    void open()
            throws DbException, TransactionAbortedException;

    /**
     * @return true if there are more tuples available.
     */
    boolean hasNext()
            throws DbException, TransactionAbortedException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    void rewind() throws DbException, TransactionAbortedException;

    /**
     * Closes the iterator.
     */
    void close();
}
