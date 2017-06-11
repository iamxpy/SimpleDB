package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * DbIterator is the iterator interface that all SimpleDB operators should
 * implement. If the iterator is not open, none of the methods should work,
 * and should throw an IllegalStateException.  In addition to any
 * resource allocation/deallocation, an open method should call any
 * child iterator open methods, and in a close method, an iterator
 * should call its children's close methods.
 * <p>
 * 按照我的理解，DbIterator既是一个操作的抽象，也是这个操作的结果集的抽象，因此类似一个表，有tupleDesc(行描述)
 */
public interface DbIterator extends Serializable {
    /**
     * Opens the iterator. This must be called before any of the other methods.
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    void open()
            throws DbException, TransactionAbortedException;

    /**
     * Returns true if the iterator has more tuples.
     *
     * @return true f the iterator has more tuples.
     * @throws IllegalStateException If the iterator has not been opened
     */
    boolean hasNext() throws DbException, TransactionAbortedException;

    /**
     * Returns the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return the next tuple in the iteration.
     * @throws NoSuchElementException if there are no more tuples.
     * @throws IllegalStateException  If the iterator has not been opened
     */
    Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException           when rewind is unsupported.
     * @throws IllegalStateException If the iterator has not been opened
     */
    void rewind() throws DbException, TransactionAbortedException;

    /**
     * Returns the TupleDesc associated with this DbIterator.
     *
     * @return the TupleDesc associated with this DbIterator.
     */
    TupleDesc getTupleDesc();

    /**
     * Closes the iterator. When the iterator is closed, calling next(),
     * hasNext(), or rewind() should fail by throwing IllegalStateException.
     */
    void close();

}
