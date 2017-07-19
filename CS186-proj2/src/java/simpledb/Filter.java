package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
    // TODO: 17-7-15 觉得碍眼可以删了，可以帮助了解sql执行过程的每一步
    @Override
    public String getName() {
        return "<Filter-" + predicate.toString() + " on " + child.getName() + ">";
    }

    private static final long serialVersionUID = 1L;

    private Predicate predicate;

    private TupleDesc td;

    private DbIterator child;

    //缓存过滤结果，加快hasNext和next方法
    private TupleIterator filterResult;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.predicate=p;
        this.child=child;
        this.td=child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        filterResult = filter(child, predicate);
        filterResult.open();
    }

    private TupleIterator filter(DbIterator child, Predicate predicate) throws DbException, TransactionAbortedException {
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (predicate.filter(t)) {
                tuples.add(t);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        filterResult = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        filterResult.rewind();
    }



    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(filterResult.hasNext())
            return filterResult.next();
        else return null;
    }
//    protected Tuple fetchNext() throws NoSuchElementException,
//            TransactionAbortedException, DbException {
//        // some code goes here
//        while (child.hasNext()) {
//            Tuple t = child.next();
//            if (predicate.filter(t)) {
//                return t;
//            }
//        }
//        return null;
//    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
