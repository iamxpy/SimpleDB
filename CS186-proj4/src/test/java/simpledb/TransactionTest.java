package simpledb;

import java.util.*;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

public class TransactionTest extends TestUtil.CreateHeapFile {
  private PageId p0, p1, p2;
  private TransactionId tid1, tid2;

  // just so we have a pointer shorter than Database.getBufferPool()
  private BufferPool bp;

  /**
   * Set up initial resources for each unit test.
   */
  @Before public void setUp() throws Exception {
    super.setUp();

    // clear all state from the buffer pool
    bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);

    // create a new empty HeapFile and populate it with three pages.
    // we should be able to add 512 tuples on an empty page.
    TransactionId tid = new TransactionId();
    for (int i = 0; i < 1025; ++i) {
      empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
    }

    // if this fails, complain to the TA
    assertEquals(3, empty.numPages());

    this.p0 = new HeapPageId(empty.getId(), 0);
    this.p1 = new HeapPageId(empty.getId(), 1);
    this.p2 = new HeapPageId(empty.getId(), 2);
    this.tid1 = new TransactionId();
    this.tid2 = new TransactionId();

    // forget about locks associated to tid, so they don't conflict with
    // test cases
    bp.getPage(tid, p0, Permissions.READ_WRITE).markDirty(true, tid);
    bp.getPage(tid, p1, Permissions.READ_WRITE).markDirty(true, tid);
    bp.getPage(tid, p2, Permissions.READ_WRITE).markDirty(true, tid);
    bp.flushAllPages();
    bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
  }

  /**
   * Unit test for BufferPool.transactionComplete().
   * Try to acquire locks that would conflict if old locks aren't released
   * during transactionComplete().
   */
  @Test public void attemptTransactionTwice() throws Exception {
    bp.getPage(tid1, p0, Permissions.READ_ONLY);
    bp.getPage(tid1, p1, Permissions.READ_WRITE);
    bp.transactionComplete(tid1, true);

    bp.getPage(tid2, p0, Permissions.READ_WRITE);
    bp.getPage(tid2, p0, Permissions.READ_WRITE);
  }

  /**
   * Common unit test code for BufferPool.transactionComplete() covering
   * commit and abort. Verify that commit persists changes to disk, and
   * that abort reverts pages to their previous on-disk state.
   */
  public void testTransactionComplete(boolean commit) throws Exception {
    HeapPage p = (HeapPage) bp.getPage(tid1, p2, Permissions.READ_WRITE);

    Tuple t = Utility.getHeapTuple(new int[] { 6, 830 });
    t.setRecordId(new RecordId(p2, 1));

    p.insertTuple(t);
    p.markDirty(true, tid1);
    bp.transactionComplete(tid1, commit);

    // now, flush the buffer pool and access the page again from disk.
    bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
    p = (HeapPage) bp.getPage(tid2, p2, Permissions.READ_WRITE);
    Iterator<Tuple> it = p.iterator();

    boolean found = false;
    while (it.hasNext()) {
      Tuple tup = (Tuple) it.next();
      IntField f0 = (IntField) tup.getField(0);
      IntField f1 = (IntField) tup.getField(1);

      if (f0.getValue() == 6 && f1.getValue() == 830) {
        found = true;
        break;
      }
    }

    assertEquals(commit, found);
  }

  /**
   * Unit test for BufferPool.transactionComplete() assuing commit.
   * Verify that a tuple inserted during a committed transaction is durable
   */
  @Test public void commitTransaction() throws Exception {
    testTransactionComplete(true);
  }

  /**
   * Unit test for BufferPool.transactionComplete() assuming abort.
   * Verify that a tuple inserted during a committed transaction is durable
   */
  @Test public void abortTransaction() throws Exception {
    testTransactionComplete(false);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TransactionTest.class);
  }

}

