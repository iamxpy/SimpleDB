package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //该索引值指定了要使用tuple的哪一个列来分组
    int gbIndex;

    //该索引指定了要使用tuple的哪一个列来聚合
    int agIndex;

    //聚合前tuple的行描述
    TupleDesc originalTd;

    //指定了作为分组依据的那一列的值的类型
    Type gbFieldType;

    //指定使用哪种聚合操作
    Op aggreOp;

    //group-by value到aggregate value的映射
    HashMap<Field, Integer> gval2agval;

    //聚合后的td
    private TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbIndex     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param agIndex     the 0-based index of the aggregate field in the tuple
     * @param aggreOp     aggregation operator to use -- only supports COUNT
     * @param td          我加上的一个参数，由聚合器的使用者(一般是Aggregate类)负责传入
     * @throws IllegalArgumentException if aggreOp != COUNT
     */

    public StringAggregator(int gbIndex, Type gbfieldtype, int agIndex, Op aggreOp,TupleDesc td) {
        // some code goes here
        if (aggreOp != Op.COUNT) {
            throw new UnsupportedOperationException("String类型值只支持count操作,不支持" + aggreOp);
        }
        this.gbIndex = gbIndex;
        this.agIndex = agIndex;
        this.aggreOp = aggreOp;
        this.td = td;
        this.gbFieldType = gbfieldtype;
        gval2agval = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     * @throws IllegalArgumentException 如果该tuple的指定列不是Type.STRING_TYPE类型或待聚合tuple的tupleDesc与之前不一致
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

        if (aggreField.getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException("该tuple的指定列不是Type.STRING_TYPE类型");
        }
        //初始化originalTd，并确保每一次聚合的tuple的td与其相同
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("待聚合tuple的tupleDesc与之前不一致");
        }
        if (gbIndex != Aggregator.NO_GROUPING) {
            //如果gbIdex为NO_GROUPING，那么不用给gbField赋值，即为初始值null即可
            gbField = tup.getField(gbIndex);
        }

        //开始进行聚合操作
        if (gval2agval.containsKey(gbField)) {
            Integer oldVal = gval2agval.get(gbField);
            newVal = oldVal + 1;
        } else newVal = 1;
        gval2agval.put(gbField, newVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
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
