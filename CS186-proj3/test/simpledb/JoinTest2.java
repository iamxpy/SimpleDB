package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;

import static org.junit.Assert.*;

public class JoinTest2 extends SimpleDbTestBase {

  int width1 = 2;
  int width2 = 3;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator eqJoin;
  DbIterator gtJoin;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleLists() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1,
        new int[] { 2, 2,
                    1, 5,
                    3, 8 });
    this.scan2 = TestUtil.createTupleList(width2,
        new int[] { 2, 2, 3,
                    2, 6, 5,
                    1, 6, 7 });
    this.eqJoin = TestUtil.createTupleList(width1 + width2,
        new int[] {
                1, 2, 1, 2, 3,
                1, 2, 1, 6, 7,
                1, 2, 1, 2, 3,
                1, 8, 1, 6, 7
    });
    this.gtJoin = TestUtil.createTupleList(width1 + width2,
        new int[] {
                    2, 2, 1, 6, 7,
                    3, 8, 2, 2, 3,
                    3, 8, 2, 6, 5
                     });
  }

  /**
   * Unit test for Join.getTupleDesc()
   */
  @Test public void getTupleDesc() {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    TupleDesc expected = Utility.getTupleDesc(width1 + width2);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Join.rewind()
   */
  @Test public void rewind() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    while (op.hasNext()) {
      assertNotNull(op.next());
    }
    assertTrue(TestUtil.checkExhausted(op));
    op.rewind();

    eqJoin.open();
    Tuple expected = eqJoin.next();
    Tuple actual = op.next();
    assertTrue(TestUtil.compareTuples(expected, actual));
  }

  /**
   * Unit test for Join.getNext() using a &gt; predicate
   */
  @Test public void gtJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.GREATER_THAN, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    gtJoin.open();
    TestUtil.matchAllTuples(gtJoin, op);
  }

  /**
   * Unit test for Join.getNext() using an = predicate
   */
  @Test public void eqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    eqJoin.open();
    TestUtil.matchAllTuples(eqJoin, op);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JoinTest2.class);
  }
}

