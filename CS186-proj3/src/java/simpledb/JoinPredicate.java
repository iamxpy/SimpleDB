package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 * @see Predicate 对此类的理解可以参考Predicate类的Javadoc
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int index1;

    private int index2;

    private Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param index1
     *            The field index into the first tuple in the predicate
     * @param index2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int index1, Predicate.Op op, int index2) {
        // some code goes here
        this.index1 = index1;
        this.index2 = index2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(index1).compare(op,t2.getField(index2));
    }
    
    public int getIndex1()
    {
        // some code goes here
        return index1;
    }
    
    public int getIndex2()
    {
        // some code goes here
        return index2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
