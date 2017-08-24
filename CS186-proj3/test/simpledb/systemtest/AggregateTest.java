package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AggregateTest extends SimpleDbTestBase {
    public void validateAggregate(DbFile table, Aggregator.Op operation, int aggregateColumn, int groupColumn, ArrayList<ArrayList<Integer>> expectedResult)
            throws DbException, TransactionAbortedException, IOException {
        TransactionId tid = new TransactionId();
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Aggregate ag = new Aggregate(ss, aggregateColumn, groupColumn, operation);

        SystemTestUtil.matchTuples(ag, expectedResult);
        Database.getBufferPool().transactionComplete(tid);
    }

    private int computeAggregate(ArrayList<Integer> values, Aggregator.Op operation) {
        if (operation == Aggregator.Op.COUNT) return values.size();

        int value = 0;
        if (operation == Aggregator.Op.MIN) value = Integer.MAX_VALUE;
        else if (operation == Aggregator.Op.MAX) value = Integer.MIN_VALUE;

        for (int v : values) {
            switch (operation) {
                case MAX:
                    if (v > value) value = v;
                    break;
                case MIN:
                    if (v < value) value = v;
                    break;
                case AVG:
                case SUM:
                    value += v;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operation " + operation);
            }
        }

        if (operation == Aggregator.Op.AVG) value /= values.size();
        return value;
    }

    private ArrayList<ArrayList<Integer>> aggregate(ArrayList<ArrayList<Integer>> tuples, Aggregator.Op operation, int aggregateColumn, int groupColumn) {
        // Group the values
        HashMap<Integer, ArrayList<Integer>> values = new HashMap<Integer, ArrayList<Integer>>();
        for (ArrayList<Integer> t : tuples) {
            Integer key = null;
            if (groupColumn != Aggregator.NO_GROUPING) key = t.get(groupColumn);
            Integer value = t.get(aggregateColumn);

            if (!values.containsKey(key)) values.put(key, new ArrayList<Integer>());
            values.get(key).add(value);
        }

        ArrayList<ArrayList<Integer>> results = new ArrayList<ArrayList<Integer>>();
        for (Map.Entry<Integer, ArrayList<Integer>> e : values.entrySet()) {
            ArrayList<Integer> result = new ArrayList<Integer>();
            if (groupColumn != Aggregator.NO_GROUPING) result.add(e.getKey());
            result.add(computeAggregate(e.getValue(), operation));
            results.add(result);
        }
        return results;
    }

    private final static int ROWS = 1024;
    private final static int MAX_VALUE = 64;
    private final static int COLUMNS = 3;
    private void doAggregate(Aggregator.Op operation, int groupColumn)
            throws IOException, DbException, TransactionAbortedException {
        // Create the table
        ArrayList<ArrayList<Integer>> createdTuples = new ArrayList<>();
        HeapFile table = SystemTestUtil.createRandomHeapFile(
                COLUMNS, ROWS, MAX_VALUE, null, createdTuples);

        // Compute the expected answer
        ArrayList<ArrayList<Integer>> expected =
                aggregate(createdTuples, operation, 1, groupColumn);

        // validate that we get the answer
        validateAggregate(table, operation, 1, groupColumn, expected);
    }

    @Test public void testSum() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.SUM, 0);
    }

    @Test public void testMin() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.MIN, 0);
    }

    @Test public void testMax() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.MAX, 0);
    }

    @Test public void testCount() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.COUNT, 0);
    }

    @Test public void testAverage() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.AVG, 0);
    }

    @Test public void testAverageNoGroup()
            throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.AVG, Aggregator.NO_GROUPING);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(AggregateTest.class);
    }
}
