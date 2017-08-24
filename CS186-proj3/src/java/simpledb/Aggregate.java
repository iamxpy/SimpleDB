package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 * 这个类设计为符合运算符Operator的形式(构造方法提供运算的源DbIterator)，便于递归地进行运算，
 * 而真正的聚合操作发生在Aggregator中，最后结果则需要通过其iterator方法访问
 *
 * @see Aggregator
 */
public class Aggregate extends Operator {
    // TODO: 17-7-15 觉得碍眼可以删了，可以帮助了解sql执行过程的每一步
    @Override
    public String getName() {
        return "<Aggregate-"+aggreOp+" on " + child.getName()+">";
    }

    private static final long serialVersionUID = 1L;

    private int agIndex;

    private int gbIndex;

    private Aggregator.Op aggreOp;

    private DbIterator child;

    private TupleDesc child_td;

    //真正的聚合操作是发生在聚合器Aggregator中的，而聚合的结果在iterator()方法的返回值中
    private Aggregator aggregator;

    //聚合的结果通过此AggregatorIterator访问
    private DbIterator aggregateIter;

    //指定了作为分组依据的那一列的值的类型
    private Type gbFieldType;
    private TupleDesc td;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of agIndex, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child   The DbIterator that is feeding us tuples.
     * @param agIndex The column over which we are computing an aggregate.
     * @param gbIndex The column over which we are grouping the result, or -1 if
     *                there is no grouping
     * @param aggreOp The aggregation operator to use
     */
    public Aggregate(DbIterator child, int agIndex, int gbIndex, Aggregator.Op aggreOp) {
        // some code goes here
        this.child = child;
        this.agIndex = agIndex;
        this.gbIndex = gbIndex;
        this.aggreOp = aggreOp;
        child_td = child.getTupleDesc();
        Type aggreType = child_td.getFieldType(agIndex);
        //根据进行聚合的列的类型来判断aggreator的类型
        gbFieldType = gbIndex == Aggregator.NO_GROUPING ? null : child_td.getFieldType(gbIndex);
        if (aggreType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gbIndex, gbFieldType, agIndex, aggreOp, getTupleDesc());
        } else if (aggreType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gbIndex, gbFieldType, agIndex, aggreOp, getTupleDesc());
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupFieldIndex() {
        // some code goes here
        return gbIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gbIndex == Aggregator.NO_GROUPING) return null;
        return aggregateIter.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field Index
     */
    public int aggregateFieldIndex() {
        // some code goes here
        return agIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     * <p>
     */
    public String aggregateFieldName() {
        // some code goes here
        if (gbIndex == Aggregator.NO_GROUPING) return aggregateIter.getTupleDesc().getFieldName(0);
        return aggregateIter.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aggreOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aggreOp) {
        return aggreOp.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregateIter = aggregator.iterator();
        aggregateIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggregateIter.hasNext()) {
            return aggregateIter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        aggregateIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aggreOp) (child_td.getFieldName(agIndex))" where aggreOp and agIndex are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //return aggregateIter.getTupleDesc();
        if (td != null) {
            return td;
        }
        Type[] types;
        String[] names;
        String aggName = child_td.getFieldName(agIndex);
        if (gbIndex == Aggregator.NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            // TODO: 17-7-6  names = new String[]{aggName+"("+aggreOp.toString()+")"};这样不是按照注视来的吗，结果通不过测试
            names = new String[]{aggName};
        } else {
            types = new Type[]{gbFieldType, Type.INT_TYPE};
            names = new String[]{child_td.getFieldName(gbIndex), aggName};
        }
        td = new TupleDesc(types, names);
        return td;
    }

    public void close() {
        // some code goes here
        super.close();
        aggregateIter.close();
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
