package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

public class StringAggregatorTest extends SimpleDbTestBase {

  int width1 = 2;
  DbIterator scan1;
  int[][] count = null;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleList() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1,
        new Object[] { 1, "a",
                    1, "b",
                    1, "c",
                    3, "d",
                    3, "e",
                    3, "f",
                    5, "g" });

    // verify how the results progress after a few merges
    this.count = new int[][] {
      { 1, 1 },
      { 1, 2 },
      { 1, 3 },
      { 1, 3, 3, 1 }
    };

  }

  private TupleDesc getTupleDesc(TupleDesc child_td,int agIndex,int gbIndex,Type gbFieldType) {
      Type[] types;
      String[] names;
      String aggName = child_td.getFieldName(agIndex);
      if (gbIndex == Aggregator.NO_GROUPING) {
          types = new Type[]{Type.INT_TYPE};
          names = new String[]{aggName};
      } else {
          try {
              Thread.sleep(1);
          } catch (InterruptedException e) {
              e.printStackTrace();
          }
          types = new Type[]{gbFieldType, Type.INT_TYPE};
          names = new String[]{child_td.getFieldName(gbIndex), aggName};
      }
      return new TupleDesc(types, names);
  }

  /**
   * Test String.mergeTupleIntoGroup() and iterator() over a COUNT
   */
  @Test public void mergeCount() throws Exception {
    scan1.open();
    StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT,getTupleDesc(scan1.getTupleDesc(),1,0,Type.INT_TYPE));

    for (int[] step : count) {
      agg.mergeTupleIntoGroup(scan1.next());
      DbIterator it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }

  /**
   * Test StringAggregator.iterator() for DbIterator behaviour
   */
  @Test public void testIterator() throws Exception {
    // first, populate the aggregator via sum over scan1
    scan1.open();
    StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT,getTupleDesc(scan1.getTupleDesc(),1,0,Type.INT_TYPE));
    try {
      while (true)
        agg.mergeTupleIntoGroup(scan1.next());
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }

    DbIterator it = agg.iterator();
    it.open();

    // verify it has three elements
    int count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, count);

    // rewind and try again
    it.rewind();
    count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, count);

    // close it and check that we don't get anything
    it.close();
    try {
      it.next();
      throw new Exception("StringAggreator iterator yielded tuple after close");
    } catch (Exception e) {
      // explicitly ignored
    }
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(StringAggregatorTest.class);
  }
}

