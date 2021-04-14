package simpledb;

import simpledb.Predicate.Op;
import simpledb.BTreeUtility.*;
import simpledb.systemtest.SimpleDbTestBase;

import java.util.*;
import org.junit.Before;
import org.junit.Test;
import junit.framework.JUnit4TestAdapter;

public class BTreeDeadlockTest extends SimpleDbTestBase {
	private Random rand;

	private static final int POLL_INTERVAL = 100;
	private static final int WAIT_INTERVAL = 200;

	// just so we have a pointer shorter than Database.getBufferPool
	private BufferPool bp;
	private BTreeFile bf;
	private int item1;
	private int item2;
	private int count1;
	private int count2;

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void setUp() throws Exception {
		// create a packed B+ tree with no empty slots
		bf = BTreeUtility.createRandomBTreeFile(2, 253008, null, null, 0);
		rand = new Random();
		item1 = rand.nextInt(BTreeUtility.MAX_RAND_VALUE);
		item2 = rand.nextInt(BTreeUtility.MAX_RAND_VALUE);
		bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);

		// first make sure that item1 is not contained in our B+ tree
		TransactionId tid = new TransactionId();
		DbFileIterator it = bf.indexIterator(tid, new IndexPredicate(Op.EQUALS, new IntField(item1)));
		it.open();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		while(it.hasNext()) {
			tuples.add(it.next());
		}
		for(Tuple t : tuples) {
			bp.deleteTuple(tid, t);
		}

		// this is the number of tuples we must insert to replace the deleted tuples 
		// and cause the root node to split
		count1 = tuples.size() + 1;

		// do the same thing for item 2
		it = bf.indexIterator(tid, new IndexPredicate(Op.EQUALS, new IntField(item2)));
		it.open();
		tuples.clear();
		while(it.hasNext()) {
			tuples.add(it.next());
		}
		for(Tuple t : tuples) {
			bp.deleteTuple(tid, t);
		}

		// this is the number of tuples we must insert to replace the deleted tuples 
		// and cause the root node to split
		count2 = tuples.size() + 1;

		// clear all state from the buffer pool, increase the number of pages
		bp.flushAllPages();
		bp = Database.resetBufferPool(500);

	}

	/**
	 * Helper method to clean up the syntax of starting a BTreeWriter thread.
	 * The parameters pass through to the BTreeWriter constructor.
	 */
	public BTreeUtility.BTreeWriter startWriter(TransactionId tid, 
			int item, int count) {

		BTreeWriter bw = new BTreeWriter(tid, bf, item, count);
		bw.start();
		return bw;
	}

	/**
	 * Not-so-unit test to construct a deadlock situation.
	 * 
	 * This test causes two different transactions to update two (probably) different leaf nodes
	 * Each transaction can happily insert tuples until the page fills up, but then 
	 * it needs to obtain a write lock on the root node in order to split the page. This will cause
	 * a deadlock situation.
	 */
	@Test public void testReadWriteDeadlock() throws Exception {
		System.out.println("testReadWriteDeadlock constructing deadlock:");

		TransactionId tid1 = new TransactionId();
		TransactionId tid2 = new TransactionId();

		Database.getBufferPool().getPage(tid1, BTreeRootPtrPage.getId(bf.getId()), Permissions.READ_ONLY);
		Database.getBufferPool().getPage(tid2, BTreeRootPtrPage.getId(bf.getId()), Permissions.READ_ONLY);

		// allow read locks to acquire
		Thread.sleep(POLL_INTERVAL);
		
		BTreeWriter writer1 = startWriter(tid1, item1, count1);
		BTreeWriter writer2 = startWriter(tid2, item2, count2);

		while (true) {
			Thread.sleep(POLL_INTERVAL);

			if(writer1.succeeded() || writer2.succeeded()) break;

			if (writer1.getError() != null) {
				writer1 = null;
				bp.transactionComplete(tid1);
				Thread.sleep(rand.nextInt(WAIT_INTERVAL));

				tid1 = new TransactionId();
				writer1 = startWriter(tid1, item1, count1);
			}

			if (writer2.getError() != null) {
				writer2 = null;
				bp.transactionComplete(tid2);
				Thread.sleep(rand.nextInt(WAIT_INTERVAL));

				tid2 = new TransactionId();
				writer2 = startWriter(tid2, item2, count2);
			}

		}

		System.out.println("testReadWriteDeadlock resolved deadlock");
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeDeadlockTest.class);
	}

}

