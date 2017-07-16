package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    // TODO: 17-7-15 delete this
    @Override
    public String getName() {
        return "<Scan on " + tableAlias+ ">";
    }

    private static final long serialVersionUID = 1L;

    private TransactionId tid;

    private int tableid;

    private String tableAlias;

    private DbFileIterator tupleIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableAlias = tableAlias;
        this.tableid = tableid;
        tupleIterator = Database.getCatalog().getDbFile(tableid).iterator(tid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        tupleIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc desc = Database.getCatalog().getTupleDesc(tableid);
        int fieldNum = desc.numFields();
        Type[] types = new Type[fieldNum];
        String[] names = new String[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            types[i] = desc.getFieldType(i);
            //按照构造器中所说，为了防止意外的null值使此类停止工作，故要加入一些判断
            String prefix = getAlias() == null ? "null." : getAlias() + ".";
            String fieldName = desc.getFieldName(i);
            fieldName = fieldName == null ? "null" : fieldName;
            names[i] = prefix + fieldName;
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return tupleIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return tupleIterator.next();
    }

    public void close() {
        // some code goes here
        tupleIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        tupleIterator.rewind();
    }
}
