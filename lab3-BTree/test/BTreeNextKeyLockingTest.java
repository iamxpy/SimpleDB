package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.BTreeUtility.BTreeWriter;
import simpledb.Predicate.Op;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class BTreeNextKeyLockingTest extends SimpleDbTestBase {
	private TransactionId tid;
	
	private static final int POLL_INTERVAL = 100;

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before
	public void setUp() throws Exception {
		tid = new TransactionId();
	}

	@After
	public void tearDown() throws Exception {
		Database.getBufferPool().transactionComplete(tid);
	}

	@Test
	public void nextKeyLockingTestLessThan() throws Exception {
		
		// This should create a B+ tree with 100 leaf pages
		BTreeFile bigFile = BTreeUtility.createRandomBTreeFile(2, 50200,
				null, null, 0);

		// get a key from the middle of the root page
		BTreePageId rootPtrPid = new BTreePageId(bigFile.getId(), 0, BTreePageId.ROOT_PTR);
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, rootPtrPid, Permissions.READ_ONLY);
		BTreePageId rootId = rootPtr.getRootId();
		assertEquals(rootId.pgcateg(), BTreePageId.INTERNAL);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(tid, rootId, Permissions.READ_ONLY);
		int keyIndex = 50; // this should be right in the middle since there are 100 leaf pages
		Iterator<BTreeEntry> it = root.iterator();
		Field key = null;
		int count = 0;
		while(it.hasNext()) {
			BTreeEntry e = it.next();
			if(count == keyIndex) {
				key = e.getKey();
				break;
			}
			count++;
		}
		assertTrue(key != null);

		// now find all tuples containing that key and delete them, as well as the next key
		IndexPredicate ipred = new IndexPredicate(Op.EQUALS, key);
		DbFileIterator fit = bigFile.indexIterator(tid, ipred);
		fit.open();
		while(fit.hasNext()) {
			Database.getBufferPool().deleteTuple(tid, fit.next());
		}
		fit.close();

		count = 0;
		while(count == 0) {
			key = new IntField(((IntField) key).getValue() + 1);
			ipred = new IndexPredicate(Op.EQUALS, key);
			fit = bigFile.indexIterator(tid, ipred);
			fit.open();
			while(fit.hasNext()) {
				Database.getBufferPool().deleteTuple(tid, fit.next());
				count++;
			}
			fit.close();
		}

		Database.getBufferPool().transactionComplete(tid);
		tid = new TransactionId();

		// search for tuples less than or equal to the key
		ipred = new IndexPredicate(Op.LESS_THAN_OR_EQ, key);
		fit = bigFile.indexIterator(tid, ipred);
		fit.open();
		int keyCountBefore = 0;
		while(fit.hasNext()) {
			fit.next();
			keyCountBefore++;
		}
		fit.close();

		// In a different thread, try to insert tuples containing the key
		TransactionId tid1 = new TransactionId();
		BTreeWriter bw1 = new BTreeWriter(tid1, bigFile, ((IntField) key).getValue(), 1);
		bw1.start();

		// allow thread to start
		Thread.sleep(POLL_INTERVAL);

		// search for tuples less than or equal to the key
		ipred = new IndexPredicate(Op.LESS_THAN_OR_EQ, key);
		fit = bigFile.indexIterator(tid, ipred);
		fit.open();
		int keyCountAfter = 0;
		while(fit.hasNext()) {
			fit.next();
			keyCountAfter++;
		}
		fit.close();

		// make sure our indexIterator() is working
		assertTrue(keyCountBefore > 0);

		// check that we don't have any phantoms
		assertEquals(keyCountBefore, keyCountAfter);
		assertFalse(bw1.succeeded());

		// now let the inserts happen
		Database.getBufferPool().transactionComplete(tid);

		while(!bw1.succeeded()) {
			Thread.sleep(POLL_INTERVAL);
			if(bw1.succeeded()) {
				Database.getBufferPool().transactionComplete(tid1);
			}
		}

		// clean up
		bw1 = null;
	}

	@Test
	public void nextKeyLockingTestGreaterThan() throws Exception {
		// This should create a B+ tree with 100 leaf pages
		BTreeFile bigFile = BTreeUtility.createRandomBTreeFile(2, 50200,
				null, null, 0);

		// get a key from the middle of the root page
		BTreePageId rootPtrPid = new BTreePageId(bigFile.getId(), 0, BTreePageId.ROOT_PTR);
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, rootPtrPid, Permissions.READ_ONLY);
		BTreePageId rootId = rootPtr.getRootId();
		assertEquals(rootId.pgcateg(), BTreePageId.INTERNAL);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(tid, rootId, Permissions.READ_ONLY);
		int keyIndex = 50; // this should be right in the middle since there are 100 leaf pages
		Iterator<BTreeEntry> it = root.iterator();
		Field key = null;
		int count = 0;
		while(it.hasNext()) {
			BTreeEntry e = it.next();
			if(count == keyIndex) {
				key = e.getKey();
				break;
			}
			count++;
		}
		assertTrue(key != null);

		// now find all tuples containing that key and delete them, as well as the previous key
		IndexPredicate ipred = new IndexPredicate(Op.EQUALS, key);
		DbFileIterator fit = bigFile.indexIterator(tid, ipred);
		fit.open();
		while(fit.hasNext()) {
			Database.getBufferPool().deleteTuple(tid, fit.next());
		}
		fit.close();

		count = 0;
		while(count == 0) {
			key = new IntField(((IntField) key).getValue() - 1);
			ipred = new IndexPredicate(Op.EQUALS, key);
			fit = bigFile.indexIterator(tid, ipred);
			fit.open();
			while(fit.hasNext()) {
				Database.getBufferPool().deleteTuple(tid, fit.next());
				count++;
			}
			fit.close();
		}

		Database.getBufferPool().transactionComplete(tid);
		tid = new TransactionId();

		// search for tuples greater than or equal to the key
		ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, key);
		fit = bigFile.indexIterator(tid, ipred);
		fit.open();
		int keyCountBefore = 0;
		while(fit.hasNext()) {
			fit.next();
			keyCountBefore++;
		}
		fit.close();

		// In a different thread, try to insert tuples containing the key
		TransactionId tid1 = new TransactionId();
		BTreeWriter bw1 = new BTreeWriter(tid1, bigFile, ((IntField) key).getValue(), 1);
		bw1.start();

		// allow thread to start
		Thread.sleep(POLL_INTERVAL);

		// search for tuples greater than or equal to the key
		ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, key);
		fit = bigFile.indexIterator(tid, ipred);
		fit.open();
		int keyCountAfter = 0;
		while(fit.hasNext()) {
			fit.next();
			keyCountAfter++;
		}
		fit.close();

		// make sure our indexIterator() is working
		assertTrue(keyCountBefore > 0);

		// check that we don't have any phantoms
		assertEquals(keyCountBefore, keyCountAfter);
		assertFalse(bw1.succeeded());

		// now let the inserts happen
		Database.getBufferPool().transactionComplete(tid);

		while(!bw1.succeeded()) {
			Thread.sleep(POLL_INTERVAL);
			if(bw1.succeeded()) {
				Database.getBufferPool().transactionComplete(tid1);
			}
		}

		// clean up
		bw1 = null;
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeNextKeyLockingTest.class);
	}
}
