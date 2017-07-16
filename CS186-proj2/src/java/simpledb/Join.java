package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    // TODO: 17-7-15 delete this
    @Override
    public String getName() {
        return "<Join " + child1.getName() + " with " + child2.getName() + ">";
    }

    // TODO: 17-7-6 delete
    private long innertimes = 0;

    private static final long serialVersionUID = 1L;

    private JoinPredicate joinPredicate;

    private TupleDesc td;

    private DbIterator child1, child2;

    private TupleIterator joinResults;


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
     * 而其他操作符是直接或间接以其来构造的(我的理解是每次都要先全表扫描一遍)todo 如果这里理解不对再回来修改想法
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
        joinResults = doubleBlockNestedLoopJoin();
        joinResults.open();
    }

    private TupleIterator nestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            child2.rewind();
            while (child2.hasNext()) {
                // TODO: 17-7-6 delete this two lines
                innertimes++;
                if (innertimes % 1000000 == 0) System.out.println("this is " + innertimes + " in inner loop");
                Tuple right = child2.next();
                Tuple result;
                if (joinPredicate.filter(left, right)) {//如果符合条件就合并来自两个表的tuple作为一条结果
                    result = new Tuple(td);
                    for (int i = 0; i < length1; i++) {
                        result.setField(i, left.getField(i));
                    }
                    for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                        result.setField(i + length1, right.getField(i));
                    }
                    tuples.add(result);
                }
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

/*
Added table : authors with schema : id, name                 14W
Added table : venues with schema : id, name, year, type      1W
Added table : papers with schema : id, title, venueid        10W
Added table : paperauths with schema : paperid, authorid     26W
 */
/*用时5s
Added scan of table p
Added scan of table a
Added scan of table pa
Added scan of table v
Added join between pa.authorid and a.id
Added join between pa.paperid and p.id
Added join between p.venueid and v.id
Added select list field p.title
Added select list field v.name
 */

    /*用时400s
    Added scan of table p
    Added scan of table a1
    Added scan of table a2
    Added scan of table pa1
    Added scan of table pa2
    Added join between pa1.authorid and a1.id
    Added join between pa1.paperid and p.id
    Added join between pa2.authorid and a2.id
    Added join between pa1.paperid and pa2.paperid
    Added select list field a2.name
     */
    private TupleIterator blockNestedLoopJoin() throws DbException, TransactionAbortedException {
        // TODO: 17-7-15 delete begin
        System.err.println("A new \"Join\" begin...");
        System.err.println("Left table is " + child1.getName());
        System.err.println("Right table is " + child2.getName());
        long begin = System.currentTimeMillis();
        long blockFullTimes = 0;
        // TODO: 17-7-15 delete end
        LinkedList<Tuple> tuples = new LinkedList<>();
        int blockSize = 131072 / child1.getTupleDesc().getSize();//131072是MySql中该算法的默认缓冲区大小
        // TODO: 17-7-16 delete this
        System.err.println("Cache array size is " + blockSize);
        Tuple[] cacheBlock = new Tuple[blockSize];
        int index = 0;
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            cacheBlock[index++] = left;
            if (index >= cacheBlock.length) {//如果缓冲区满了，就先处理缓存中的tuple
                // TODO: 17-7-16 delete this
                blockFullTimes++;
                child2.rewind();
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    for (Tuple cacheLeft : cacheBlock) {
                        if (joinPredicate.filter(cacheLeft, right)) {//如果符合条件就合并来自两个表的tuple作为一条结果
                            Tuple result = new Tuple(td);
                            for (int i = 0; i < length1; i++) {
                                result.setField(i, cacheLeft.getField(i));
                            }
                            for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                                result.setField(i + length1, right.getField(i));
                            }
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
                        Tuple result = new Tuple(td);
                        for (int i = 0; i < length1; i++) {
                            result.setField(i, cacheLeft.getField(i));
                        }
                        for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                            result.setField(i + length1, right.getField(i));
                        }
                        tuples.add(result);
                    }
                }
            }
        }
        // TODO: 17-7-15 delete this two lines
        System.err.println("\"Join\" finished and use " + (System.currentTimeMillis() - begin) / 1000.0 + " seconds");
        System.err.println("PS: Cache has been filled " + blockFullTimes + " times");
        return new TupleIterator(getTupleDesc(), tuples);
    }


    private TupleIterator doubleBlockNestedLoopJoin() throws DbException, TransactionAbortedException {
        // TODO: 17-7-15 delete begin
        System.err.println("A new \"Join\" begin...");
        System.err.println("Left table is " + child1.getName());
        System.err.println("Right table is " + child2.getName());
        long begin = System.currentTimeMillis();
        // TODO: 17-7-15 delete end
        LinkedList<Tuple> tuples = new LinkedList<>();
        int blockSize1 = 131072 / child1.getTupleDesc().getSize();//131072是MySql中该算法的默认缓冲区大小
        int blockSize2 = 131072 / child2.getTupleDesc().getSize();
        // TODO: 17-7-16 change these two lines back
        Tuple[] leftCacheBlock = new Tuple[blockSize1];
        Tuple[] rightCacheBlock = new Tuple[16384];
        // TODO: 17-7-16 delete this two
        System.out.println("left cache size is " + blockSize1);
        System.out.println("right cache size is " + blockSize2);
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
                                    Tuple result = new Tuple(td);
                                    for (int i = 0; i < length1; i++) {
                                        result.setField(i, cacheLeft.getField(i));
                                    }
                                    for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                                        result.setField(i + length1, cacheRight.getField(i));
                                    }
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
                                Tuple result = new Tuple(td);
                                for (int i = 0; i < length1; i++) {
                                    result.setField(i, cacheLeft.getField(i));
                                }
                                for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                                    result.setField(i + length1, cacheRight.getField(i));
                                }
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
                                Tuple result = new Tuple(td);
                                for (int i = 0; i < length1; i++) {
                                    result.setField(i, cacheLeft.getField(i));
                                }
                                for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                                    result.setField(i + length1, cacheRight.getField(i));
                                }
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
                            Tuple result = new Tuple(td);
                            for (int i = 0; i < length1; i++) {
                                result.setField(i, cacheLeft.getField(i));
                            }
                            for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
                                result.setField(i + length1, cacheRight.getField(i));
                            }
                            tuples.add(result);
                        }
                    }
                }
            }
        }
        // TODO: 17-7-15 delete this two lines
        System.err.println("\"Join\" finished and use " + (System.currentTimeMillis() - begin) / 1000.0 + " seconds");
        return new TupleIterator(getTupleDesc(), tuples);
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
        // TODO: 17-6-21 change
//        left = null;
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

}
