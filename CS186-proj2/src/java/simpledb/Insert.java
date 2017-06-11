package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;

    //insert操作影响了的tuple数量
    private int count;

    private TupleDesc td;

    private boolean hasAccessed;

    private TransactionId tid;

    private int tableId;


    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.tableId = tableid;
        this.child = child;
        count = 0;
//        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Affected_Rows"});无法通过测试，丢
        // TODO: 17-6-9 列名要用null才能通过测试，以后再自己改了
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
        child.open();
        super.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, next);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasAccessed) {
            return null;
        }
        hasAccessed = true;
        Tuple inserted_num=new Tuple(getTupleDesc());
        inserted_num.setField(0,new IntField(count));
        return inserted_num;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
