package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 *
 * 我加了参数给构造器，目的是想让聚合器的使用者(即代表aggregate操作符的Aggregate类)来
 * 负责告诉aggregator聚合后的行描述。因为聚合器在生成结果的迭代器时需要使用到td，然而这部分逻辑在Aggregate类中也需要
 * 已经在Aggregate中实现了，没必要重复，于是由Aggregate类来负责给聚合器传入聚合后的行描述
 * 那么，为什么不在聚合器中实现“得到聚合后的行描述”，然后让使用者调用就好了呢？
 * 这是因为在原设计中，Aggregate的测试类有一个方法是在新建了Aggregate类，还没有进行聚合的前提下就调用了getTupleDesc
 * 而如果它是调用聚合器的方法，由于聚合器还没有遇到任何一个tuple，所以无法确定聚合后的行描述，就会返回null或者出错
 */

public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //该索引值指定了要使用tuple的哪一个列来分组
    int gbIndex;

    //该索引指定了要使用tuple的哪一个列来聚合
    int agIndex;

    //聚合前tuple的行描述
    TupleDesc originalTd;

    //聚合后的tuple的行描述
    TupleDesc td;

    //指定了作为分组依据的那一列的值的类型
    Type gbFieldType;

    //指定使用哪种聚合操作
    Op aggreOp;

    //Key：每个不同的分组字段(groupby value)  Vlaue：聚合的结果
    HashMap<Field, Integer> gval2agval;

    //Key：每个不同的分组字段(groupby value)  Value：该分组进行平均值聚合过程处理的所有值的个数以及他们的和
    //这个map仅用于辅助在计算平均值时得到以前聚合过的总数
    HashMap<Field, Integer[]> gval2count_sum;

    /**
     * Aggregate constructor
     *
     * @param gbIndex     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param agIndex     the 0-based index of the aggregate field in the tuple
     * @param aggreOp     the aggregation operator
     * @param td          我加上的一个参数，由聚合器的使用者(一般是Aggregate类)负责传入
     */

    public IntegerAggregator(int gbIndex, Type gbFieldType, int agIndex, Op aggreOp,TupleDesc td) {
        // some code goes here
        this.gbIndex = gbIndex;
        this.gbFieldType = gbFieldType;
        this.agIndex = agIndex;
        this.aggreOp = aggreOp;
        this.td=td;
        gval2agval = new HashMap<>();
        gval2count_sum = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     * @throws IllegalArgumentException 如果该tuple的指定列不是TYPE.INT_TYPE类型或待聚合tuple的tupleDesc与之前不一致
     */
    public void mergeTupleIntoGroup(Tuple tup) throws IllegalArgumentException {
        // some code goes here
        //待聚合值所在的Field
        Field aggreField;
        //分组依据的Field
        Field gbField = null;
        //新的聚合结果
        Integer newVal;
        aggreField = tup.getField(agIndex);
        //待聚合值
        int toAggregate;
        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("该tuple的指定列不是Type.INT_TYPE类型");
        }
        toAggregate = ((IntField) aggreField).getValue();
        //初始化originalTd，并确保每一次聚合的tuple的td与其相同
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("待聚合tuple的tupleDesc不一致");
        }
        if (gbIndex != Aggregator.NO_GROUPING) {
            //如果gbIdex为NO_GROUPING，那么不用给gbField赋值，即为初始值null即可
            gbField = tup.getField(gbIndex);
        }
        //开始进行聚合操作
        //平均值的操作需要维护gval2count_sum，所以单独处理
        if (aggreOp == Op.AVG) {
            if (gval2count_sum.containsKey(gbField)) {//如果这个map已经处理过这个分组
                Integer[] oldCountAndSum = gval2count_sum.get(gbField);//之前处理该分组的总次数以及所有操作数的和
                int oldCount = oldCountAndSum[0];
                int oldSum = oldCountAndSum[1];
                //更新该分组对应的记录，将次数加1,并将总和加上待聚合的值
                gval2count_sum.put(gbField, new Integer[]{oldCount + 1, oldSum + toAggregate});
            } else {//否则为第一次处理该分组的tuple
                gval2count_sum.put(gbField, new Integer[]{1, toAggregate});
            }
            //直接由gval2count_sum这个map记录的信息得到该分组对应的聚合值并保存在gval2agval中
            Integer[] c2s=gval2count_sum.get(gbField);
            int currentCount = c2s[0];
            int currentSum = c2s[1];
            gval2agval.put(gbField, currentSum / currentCount);
            //在这里结束，此方法剩下的代码是对应除了求平均值其他的操作的
            return;
        }

        //除了求平均值的其他聚合操作
        if (gval2agval.containsKey(gbField)) {
            Integer oldVal = gval2agval.get(gbField);
            newVal = calcuNewValue(oldVal, toAggregate, aggreOp);
        } else if (aggreOp == Op.COUNT) {//如果是对应分组的第一个参加聚合操作的tuple，那么除了count操作，其他操作结果都是待聚合值
            newVal = 1;
        } else {
            newVal = toAggregate;
        }
        gval2agval.put(gbField, newVal);
    }

    /**
     * 由旧的聚合结果和新的聚合值得到新的聚合结果
     *
     * @param oldVal      旧的聚合结果
     * @param toAggregate 新的聚合值
     * @param aggreOp     聚合操作
     * @return 新的聚合值
     */
    private int calcuNewValue(int oldVal, int toAggregate, Op aggreOp) {
        switch (aggreOp) {
            case COUNT:
                return oldVal + 1;
            case MAX:
                return Math.max(oldVal, toAggregate);
            case MIN:
                return Math.min(oldVal, toAggregate);
            case SUM:
                return oldVal + toAggregate;
            default:
                throw new IllegalArgumentException("不应该到达这里");
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : gval2agval.entrySet()) {
            Tuple t = new Tuple(td);//该tuple不必setRecordId，因为RecordId对进行操作后的tuple没有意义
            //分别处理不分组与有分组的情形
            if (gbIndex == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(g2a.getValue()));
            } else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
