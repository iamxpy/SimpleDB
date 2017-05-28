package simpledb;

import java.util.*;

/**
 * Implements a DbIterator by wrapping an Iterable<Tuple>.
 */
public class TupleIterator implements DbIterator {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    Iterator<Tuple> i = null;
    TupleDesc td = null;
    Iterable<Tuple> tuples = null;

    /**
     * Constructs an iterator from the specified Iterable, and the specified
     * descriptor.
     * 
     * @param tuples
     *            The set of tuples to iterate over
     */
    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.td = td;
        this.tuples = tuples;

        // check that all tuples are the right TupleDesc
        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td))
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
        }
    }

    public void open() {
        i = tuples.iterator();
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Tuple next() {
        return i.next();
    }

    public void rewind() {
        close();
        open();
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        i = null;
    }
}
