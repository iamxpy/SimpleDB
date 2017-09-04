package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    // TODO: 17-7-15 觉得碍眼可以删了，可以帮助了解sql执行过程的每一步
    @Override
    public String getName() {
        return "<Join " + child1.getName() + " with " + child2.getName() + ">";
    }

    private static final long serialVersionUID = 1L;

    private JoinPredicate joinPredicate;

    private TupleDesc td;

    private DbIterator child1, child2;

    private TupleIterator joinResults;

    // 131072是MySql中BlockNestedLoopJoin算法的默认缓冲区大小（以字节为单位）
    // 增大该参数可以更大程度减少磁盘IO，并充分利用已经优化过的内存中的Join算法
    // 对于测试案例中的两个大表的Join，使用默认大小需要25s，使用2倍大小需要15s，
    // 5倍需要10s，10倍则需要6s，所以权衡时间和空间的消耗来说，5倍比较合适
    private int blockMemory = 131072*5;
//    private int blockMemory = 131072;


    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        int length1 = child1.getTupleDesc().numFields();
        int length2 = child2.getTupleDesc().numFields();
        Type[] types = new Type[length1 + length2];
        String[] names = new String[types.length];
        //得到新表的tupleDesc(行描述)的types和names
        td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     * 这两个方法直接用getFieldName即可，而不必处理alias，这是因为在SeqScan中处理了
     * 而其他操作符是直接或间接以其来构造的(我的理解是每次都要先全表扫描一遍)
     * 他们的getTupleDesc().getFieldName最终会返回在SeqScan中得到的fieldName
     */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getIndex1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(joinPredicate.getIndex2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();
//        joinResults = nestedLoopJoin();
//        joinResults = blockNestedLoopJoin();
//        joinResults = doubleBlockNestedLoopJoin();
        joinResults = dbnlSortedJoin();
        joinResults.open();
    }

    /**
     * 传统的BlockNestedLoopJoin只是对左表做缓存，使用Block来减少对左表的访问带来的磁盘IO次数（每一个Page都要带来一次IO访问）
     * DoubleBlockNestedJoin是对右表也做缓存处理，后来发现效果并不明显，
     * 我一开始认为还是IO瓶颈，所以给HeapFile加了对多个Pages的缓存
     * 然而除了对于（左部）大表Join（右部）小表有了一些不明显的效率提高之外，对于大表Join大表仍然效率极低
     * 最后发现此时性能瓶颈在于对两个缓存区Block做Join操作，Block虽然比一个大表小，
     * 但实际上也不小（测试案例中两个Block是长度为16384和963的数组）
     * 在查阅了MergeJoin算法之后，觉得先排序再Join的思想挺好，就将两个Block的Join换成了先排序再Join的版本，得到了bnlSortedJoin
     * PS: NestedLoopJoin，BlockNestedLoopJoin和DoubleBlockNestedJoin的源码见最后的注释
     */
    private TupleIterator dbnlSortedJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        int blockSize1 = blockMemory / child1.getTupleDesc().getSize();//131072是MySql中该算法的默认缓冲区大小
        int blockSize2 = blockMemory / child2.getTupleDesc().getSize();
        Tuple[] leftCacheBlock = new Tuple[blockSize1];
        Tuple[] rightCacheBlock = new Tuple[blockSize2];
        int index1 = 0;
        int index2 = 0;
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            leftCacheBlock[index1++] = left;//将左表的tuple放入左缓存区
            if (index1 >= leftCacheBlock.length) {//如果左表缓冲区满了，就先处理缓存中的tuple
                //以下为使用另一个数组作为缓存将右表分块处理的过程，就是缓存满了就join一次，最后处理在缓存中剩下的
                //即下面的for里面的并列的while和if块是对右表的处理过程，可以看作一起来理解
                child2.rewind();//每处理完一整个左缓存区的tuples才rewind一次右表
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    rightCacheBlock[index2++] = right;//将右表的tuple放入缓存
                    if (index2 >= rightCacheBlock.length) {//如果右缓冲区满了，就先处理缓存中的tuple
                        sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
                        Arrays.fill(rightCacheBlock, null);//清空右缓存区，以供继续遍历右表
                        index2 = 0;
                    }
                }
                if (index2 > 0 && index2 < rightCacheBlock.length) {//处理缓冲区中剩下的tuple
                    sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
                    Arrays.fill(rightCacheBlock, null);//清空右缓存区，以供下一个左缓存区对右缓存区的遍历
                    index2 = 0;
                }
                Arrays.fill(leftCacheBlock, null);//清空左缓存区，以供继续遍历左表
                index1 = 0;
            }
        }
        if (index1 > 0 && index1 < leftCacheBlock.length) {//处理缓冲区中剩下的tuple
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                rightCacheBlock[index2++] = right;//将右表的tuple放入缓存
                if (index2 >= rightCacheBlock.length) {//如果缓冲区满了，就先处理缓存中的tuple
                    sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
                    Arrays.fill(rightCacheBlock, null);//清空右缓存区，以供继续遍历右表
                    index2 = 0;
                }
            }
            if (index2 > 0 && index2 < rightCacheBlock.length) {//处理缓冲区中剩下的tuple
                sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    private void sortedJoin(LinkedList<Tuple> tuples, Tuple[] lcb, Tuple[] rcb, int length1) {
        // 去掉两个Block的null值
        int m = lcb.length - 1;
        int n = rcb.length - 1;
        for (; m > 0 && lcb[m] == null; m--) ;
        for (; n > 0 && rcb[n] == null; n--) ;
        Tuple[] leftCacheBlock = new Tuple[m + 1];
        Tuple[] rightCacheBlock = new Tuple[n + 1];
        System.arraycopy(lcb, 0, leftCacheBlock, 0, leftCacheBlock.length);
        System.arraycopy(rcb, 0, rightCacheBlock, 0, rightCacheBlock.length);
        int index1 = joinPredicate.getIndex1();
        int index2 = joinPredicate.getIndex2();
        //这几个predicate用于每个数组内部的tuple比较，故每个都要需要使用各自的index
        JoinPredicate eqPredicate1 = new JoinPredicate(index1, Predicate.Op.EQUALS, index1);
        JoinPredicate eqPredicate2 = new JoinPredicate(index2, Predicate.Op.EQUALS, index2);
        JoinPredicate ltPredicate1 = new JoinPredicate(index1, Predicate.Op.LESS_THAN, index1);
        JoinPredicate ltPredicate2 = new JoinPredicate(index2, Predicate.Op.LESS_THAN, index2);
        JoinPredicate gtPredicate1 = new JoinPredicate(index1, Predicate.Op.GREATER_THAN, index1);
        JoinPredicate gtPredicate2 = new JoinPredicate(index2, Predicate.Op.GREATER_THAN, index2);
        //先对两个缓存区排序
        Comparator<Tuple> comparator1 = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                if (ltPredicate1.filter(o1, o2)) {
                    return -1;
                } else if (gtPredicate1.filter(o1, o2)) {
                    return 1;
                } else return 0;
            }
        };
        Comparator<Tuple> comparator2 = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                if (ltPredicate2.filter(o1, o2)) {
                    return -1;
                } else if (gtPredicate2.filter(o1, o2)) {
                    return 1;
                } else return 0;
            }
        };
        Arrays.sort(leftCacheBlock, comparator1);
        Arrays.sort(rightCacheBlock, comparator2);
        //两个数组的当前访问到的索引位置
        int pos1, pos2;
        switch (joinPredicate.getOperator()) {
            case EQUALS:
                pos1 = pos2 = 0;
                while (pos1 < leftCacheBlock.length && pos2 < rightCacheBlock.length) {
                    Tuple left = leftCacheBlock[pos1];
                    Tuple right = rightCacheBlock[pos2];
                    //这2个Predicate用于来自两个数组的tuple比较，故使用index1和index2
                    JoinPredicate eqPredicate = new JoinPredicate(index1, Predicate.Op.EQUALS, index2);
                    JoinPredicate ltPredicate = new JoinPredicate(index1, Predicate.Op.LESS_THAN, index2);
                    if (eqPredicate.filter(left, right)) {
                        int begin1, end1, begin2, end2;
                        begin1 = pos1;
                        begin2 = pos2;

                        for (; pos1 < leftCacheBlock.length && eqPredicate1.filter(left, leftCacheBlock[pos1]); pos1++)
                            ;
                        for (; pos2 < rightCacheBlock.length && eqPredicate2.filter(right, rightCacheBlock[pos2]); pos2++)
                            ;
                        end1 = pos1;
                        end2 = pos2;
                        for (int i = begin1; i < end1; i++) {
                            for (int j = begin2; j < end2; j++) {
                                tuples.add(mergeTuples(length1, leftCacheBlock[i], rightCacheBlock[j]));
                            }
                        }
                    } else if (ltPredicate.filter(left, right)) {
                        pos1++;
                    } else {
                        pos2++;
                    }
                }
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                pos1 = 0;
                while (pos1 < leftCacheBlock.length) {
                    Tuple left = leftCacheBlock[pos1];
                    pos2 = rightCacheBlock.length - 1;
                    while (pos2 > 0) {
                        Tuple right = rightCacheBlock[pos2];
                        pos2--;
                        if (joinPredicate.filter(left, right)) {
                            Tuple result = mergeTuples(length1, left, right);
                            tuples.add(result);
                        } else break;
                    }
                    pos1++;
                }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                pos1 = 0;
                while (pos1 < leftCacheBlock.length) {
                    Tuple left = leftCacheBlock[pos1];
                    pos2 = 0;
                    while (pos2 < rightCacheBlock.length) {
                        Tuple right = rightCacheBlock[pos2];
                        pos2++;
                        if (joinPredicate.filter(left, right)) {
                            Tuple result = mergeTuples(length1, left, right);
                            tuples.add(result);
                        } else break;
                    }
                    pos1++;
                }
                break;
            default:
                throw new RuntimeException("JoinPredicate is Illegal");
        }
    }

    private Tuple mergeTuples(int length1, Tuple left, Tuple right) {
        Tuple result = new Tuple(td);
        for (int i = 0; i < length1; i++) {
            result.setField(i, left.getField(i));
        }
        for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
            result.setField(i + length1, right.getField(i));
        }
        return result;
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        joinResults.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        joinResults.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation(串联，连结) of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * todo 什么意思？？？
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (joinResults.hasNext()) {
            return joinResults.next();
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

    /*
    private TupleIterator nestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                Tuple result;
                if (joinPredicate.filter(left, right)) {//如果符合条件就合并来自两个表的tuple作为一条结果
                    result = mergeTuples(length1, left, right);
                    tuples.add(result);
                }
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    private TupleIterator blockNestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        int blockSize = blockMemory / child1.getTupleDesc().getSize();//131072是MySql中该算法的默认缓冲区大小
        Tuple[] cacheBlock = new Tuple[blockSize];
        int index = 0;
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            cacheBlock[index++] = left;
            if (index >= cacheBlock.length) {//如果缓冲区满了，就先处理缓存中的tuple
                child2.rewind();
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    for (Tuple cacheLeft : cacheBlock) {
                        if (joinPredicate.filter(cacheLeft, right)) {//如果符合条件就合并来自两个表的tuple作为一条结果
                            Tuple result = mergeTuples(length1, cacheLeft, right);
                            tuples.add(result);
                        }
                    }
                }
                Arrays.fill(cacheBlock, null);//清空，给下一次使用
                index = 0;
            }
        }
        if (index > 0 && index < cacheBlock.length) {//处理缓冲区中剩下的tuple
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                for (Tuple cacheLeft : cacheBlock) {
                    //如果符合条件就合并来自两个表的tuple作为一条结果，加上为null的判断是因为此时cache没有满，最后有null值
                    if (cacheLeft == null) break;
                    if (joinPredicate.filter(cacheLeft, right)) {
                        Tuple result = mergeTuples(length1, cacheLeft, right);
                        tuples.add(result);
                    }
                }
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }
    private TupleIterator doubleBlockNestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        int blockSize1 = blockMemory / child1.getTupleDesc().getSize();//131072是MySql中该算法的默认缓冲区大小
        int blockSize2 = blockMemory / child2.getTupleDesc().getSize();
        Tuple[] leftCacheBlock = new Tuple[blockSize1];
        Tuple[] rightCacheBlock = new Tuple[blockSize2];
        int index1 = 0;
        int index2 = 0;
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            leftCacheBlock[index1++] = left;//将左表的tuple放入左缓存区
            if (index1 >= leftCacheBlock.length) {//如果左表缓冲区满了，就先处理缓存中的tuple
                //以下为使用另一个数组作为缓存将右表分块处理的过程，就是缓存满了就join一次，最后处理在缓存中剩下的
                //即下面的for里面的并列的while和if块是对右表的处理过程，可以看作一起来理解
                child2.rewind();//每处理完一整个左缓存区的tuples才rewind一次右表
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    rightCacheBlock[index2++] = right;//将右表的tuple放入缓存
                    if (index2 >= rightCacheBlock.length) {//如果右缓冲区满了，就先处理缓存中的tuple
                        for (Tuple cacheRight : rightCacheBlock) {
                            for (Tuple cacheLeft : leftCacheBlock) {
                                if (joinPredicate.filter(cacheLeft, cacheRight)) {//如果符合条件就合并来自两个表的tuple作为一条结果
                                    Tuple result = mergeTuples(length1, cacheLeft, cacheRight);
                                    tuples.add(result);
                                }
                            }
                        }
                        Arrays.fill(rightCacheBlock, null);//清空右缓存区，以供继续遍历右表
                        index2 = 0;
                    }
                }
                if (index2 > 0 && index2 < rightCacheBlock.length) {//处理缓冲区中剩下的tuple
                    for (Tuple cacheRight : rightCacheBlock) {
                        if (cacheRight == null) break;
                        for (Tuple cacheLeft : leftCacheBlock) {
                            //如果符合条件就合并来自两个表的tuple作为一条结果，加上为null的判断是因为此时cache没有满，最后有null值
                            if (joinPredicate.filter(cacheLeft, cacheRight)) {
                                Tuple result = mergeTuples(length1, cacheLeft, cacheRight);
                                tuples.add(result);
                            }
                        }
                    }
                    Arrays.fill(rightCacheBlock, null);//清空右缓存区，以供下一个左缓存区对右缓存区的遍历
                    index2 = 0;
                }
                Arrays.fill(leftCacheBlock, null);//清空左缓存区，以供继续遍历左表
                index1 = 0;
            }
        }
        if (index1 > 0 && index1 < leftCacheBlock.length) {//处理缓冲区中剩下的tuple
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                rightCacheBlock[index2++] = right;//将右表的tuple放入缓存
                if (index2 >= rightCacheBlock.length) {//如果缓冲区满了，就先处理缓存中的tuple
                    for (Tuple cacheRight : rightCacheBlock) {
                        for (Tuple cacheLeft : leftCacheBlock) {
                            if (cacheLeft == null) break;
                            if (joinPredicate.filter(cacheLeft, cacheRight)) {//如果符合条件就合并来自两个表的tuple作为一条结果
                                Tuple result = mergeTuples(length1, cacheLeft, cacheRight);
                                tuples.add(result);
                            }
                        }
                    }
                    Arrays.fill(rightCacheBlock, null);//清空右缓存区，以供继续遍历右表
                    index2 = 0;
                }
            }
            if (index2 > 0 && index2 < rightCacheBlock.length) {//处理缓冲区中剩下的tuple
                for (Tuple cacheRight : rightCacheBlock) {
                    if (cacheRight == null) break;
                    for (Tuple cacheLeft : leftCacheBlock) {
                        if (cacheLeft == null) break;
                        //如果符合条件就合并来自两个表的tuple作为一条结果，加上为null的判断是因为此时cache没有满，最后有null值
                        if (joinPredicate.filter(cacheLeft, cacheRight)) {
                            Tuple result = mergeTuples(length1, cacheLeft, cacheRight);
                            tuples.add(result);
                        }
                    }
                }
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }
     */

}
