package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., name2hist) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing proj1 and proj2.
 *
 * 目前理解selectivity的定义：predicate应用在table后的结果集的tuple数量占原table的tuple数量的比例
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * name2hist.
     */
    static final int NUM_HIST_BINS = 100;

    private HashMap<String, Integer[]> attrs;//Key是该表的每一列的FieldName，Value是最小值和最大值的数组
    private HashMap<String, Object> name2hist;
    private HeapFile table;
    private int ntups;
    private int ioCostPerPage;
    private TupleDesc td;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        table = (HeapFile) Database.getCatalog().getDbFile(tableid);
        td = table.getTupleDesc();
        attrs = new HashMap<>();
        name2hist = new HashMap<>();
        Transaction t = new Transaction();//查询计划的Transaction是在这里新建的
        DbFileIterator iter = table.iterator(t.getId());
        process(iter);
    }

    /**
     * 计算table的tuple数量，计算每一个int类型的列的最大最小值，计算每一列的histogram
     *
     * @param iter
     */
    private void process(DbFileIterator iter) {
        try {
            iter.open();
            while (iter.hasNext()) {//计算每一个int类型的列的最大最小值
                ntups++;//计算tuple数量
                Tuple t = iter.next();
                for (int i = 0; i < td.numFields(); i++) {
                    Type type = td.getFieldType(i);
                    if (type == Type.INT_TYPE) {//只处理int类型的列，因为StringHistogram不需要最大最小的数据
                        String name = td.getFieldName(i);
                        Integer value = ((IntField) t.getField(i)).getValue();
                        if (attrs.containsKey(name)) {
                            //如果已经有了对应的Key，说明只是没有统计完，所以更新即可
                            Integer[] min_max = attrs.get(name);
                            if (value < min_max[0]) {
                                min_max[0] = value;
                            }
                            if (value > min_max[1]) {
                                min_max[1] = value;
                            }
                        } else {//否则加入新的Key
                            Integer[] min_max = new Integer[]{value, value};
                            attrs.put(name, min_max);
                        }
                    }
                }
            }

            //计算每一列的histogram
            //这里先新建每一个int类型的列的histogram
            for (Map.Entry<String, Integer[]> entry : attrs.entrySet()) {
                Integer[] min_max = entry.getValue();
                IntHistogram histogram = new IntHistogram(NUM_HIST_BINS, min_max[0], min_max[1]);
                name2hist.put(entry.getKey(), histogram);
            }

            iter.rewind();

            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                for (int i = 0; i < td.numFields(); i++) {
                    String fieldName = td.getFieldName(i);
                    Type fieldType = td.getFieldType(i);
                    if (fieldType == Type.INT_TYPE) {//int类型的列都已经新建了histogram了，只需要addValue
                        int value = ((IntField) tuple.getField(i)).getValue();
                        IntHistogram intHistogram = (IntHistogram) name2hist.get(fieldName);
                        intHistogram.addValue(value);
                        name2hist.put(fieldName, intHistogram);
                    } else {//else为String类型的列，有可能需要新建histogram
                        String value = ((StringField) tuple.getField(i)).getValue();
                        if (name2hist.containsKey(fieldName)) {
                            StringHistogram stringHistogram = (StringHistogram) name2hist.get(fieldName);
                            stringHistogram.addValue(value);
                            name2hist.put(fieldName, stringHistogram);
                        } else {
                            StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                            stringHistogram.addValue(value);
                            name2hist.put(fieldName, stringHistogram);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return table.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the name2hist.
     */
    public double avgSelectivity(int field, simpledb.Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        String fieldName = td.getFieldName(field);
        if (constant.getType() == Type.INT_TYPE) {
            int value = ((IntField)constant).getValue();
            IntHistogram histogram = (IntHistogram) name2hist.get(fieldName);
            return histogram.estimateSelectivity(op, value);
        } else {
            String value = ((StringField)constant).getValue();
            StringHistogram histogram = (StringHistogram)name2hist.get(fieldName);
            return histogram.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
