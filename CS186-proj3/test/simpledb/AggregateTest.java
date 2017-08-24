package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class AggregateTest extends SimpleDbTestBase {

  int width1 = 2;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator scan3;

  DbIterator sum;
  DbIterator sumstring;

  DbIterator avg;
  DbIterator max;
  DbIterator min;
  DbIterator count;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleLists() throws Exception {	  
    this.scan1 = TestUtil.createTupleList(width1,
        new int[] { 1, 2,
                    1, 4,
                    1, 6,
                    3, 2,
                    3, 4,
                    3, 6,
                    5, 7 });
    this.scan2 = TestUtil.createTupleList(width1,
        new Object[] { 1, "a",
                    1, "a",
                    1, "a",
                    3, "a",
                    3, "a",
                    3, "a",
                    5, "a" });
    this.scan3 = TestUtil.createTupleList(width1,
        new Object[] { "a", 2,
                    "a", 4,
                    "a", 6,
                    "b", 2,
                    "b", 4,
                    "b", 6,
                    "c", 7 });

    this.sum = TestUtil.createTupleList(width1,
        new int[] { 1, 12,
                    3, 12,
                    5, 7 });
    this.sumstring = TestUtil.createTupleList(width1,
        new Object[] { "a", 12,
                    "b", 12,
                    "c", 7 });

    this.avg = TestUtil.createTupleList(width1,
        new int[] { 1, 4,
                    3, 4,
                    5, 7 });
    this.min = TestUtil.createTupleList(width1,
        new int[] { 1, 2,
                    3, 2,
                    5, 7 });
    this.max = TestUtil.createTupleList(width1,
        new int[] { 1, 6,
                    3, 6,
                    5, 7 });
    this.count = TestUtil.createTupleList(width1,
        new int[] { 1, 3,
                    3, 3,
                    5, 1 });

  }

  /**
   * Unit test for Aggregate.getTupleDesc()
   */
  @Test public void getTupleDesc() {
    Aggregate op = new Aggregate(scan1, 0, 0,
        Aggregator.Op.MIN);
    TupleDesc expected = Utility.getTupleDesc(2);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Aggregate.rewind()
   */
  @Test public void rewind() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
        Aggregator.Op.MIN);
    op.open();
    while (op.hasNext()) {
      assertNotNull(op.next());
    }
    assertTrue(TestUtil.checkExhausted(op));

    op.rewind();
    min.open();
    TestUtil.matchAllTuples(min, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a count aggregate with string types
   */
  @Test public void countStringAggregate() throws Exception {
    Aggregate op = new Aggregate(scan2, 1, 0,
        Aggregator.Op.COUNT);
    op.open();
    count.open();
    TestUtil.matchAllTuples(count, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a count aggregate with string types
   */
  @Test public void sumStringGroupBy() throws Exception {
    Aggregate op = new Aggregate(scan3, 1, 0,
        Aggregator.Op.SUM);
    op.open();
    sumstring.open();
    TestUtil.matchAllTuples(sumstring, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a sum aggregate
   */
  @Test public void sumAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
        Aggregator.Op.SUM);
    op.open();
    sum.open();
    TestUtil.matchAllTuples(sum, op);
  }

  /**
   * Unit test for Aggregate.getNext() using an avg aggregate
   */
  @Test public void avgAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
       Aggregator.Op.AVG);
    op.open();
    avg.open();
    TestUtil.matchAllTuples(avg, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a max aggregate
   */
  @Test public void maxAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
        Aggregator.Op.MAX);
    op.open();
    max.open();
    TestUtil.matchAllTuples(max, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a min aggregate
   */
  @Test public void minAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
       Aggregator.Op.MIN);
    op.open();
    min.open();
    TestUtil.matchAllTuples(min, op);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(AggregateTest.class);
  }
}

