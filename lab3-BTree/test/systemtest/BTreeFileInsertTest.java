package simpledb.systemtest;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.Predicate.Op;
import simpledb.*;

import java.io.File;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class BTreeFileInsertTest extends SimpleDbTestBase {
	private TransactionId tid;
	
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
		
		// set the page size back to the default
		BufferPool.resetPageSize();
		Database.reset();
	}

	@Test public void addTuple() throws Exception {
		// create an empty B+ tree file keyed on the second field of a 2-field tuple
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 1);

		Tuple tup = null;
		// we should be able to add 502 tuples on one page
		for (int i = 0; i < 502; ++i) {
			tup = BTreeUtility.getBTreeTuple(i, 2);
			empty.insertTuple(tid, tup);
			assertEquals(1, empty.numPages());
		}

		// the next 251 tuples should live on page 2 since they are greater than
		// all existing tuples in the file
		for (int i = 502; i < 753; ++i) {
			tup = BTreeUtility.getBTreeTuple(i, 2);
			empty.insertTuple(tid, tup);
			assertEquals(3, empty.numPages());
		}

		// one more insert greater than 502 should cause page 2 to split
		tup = BTreeUtility.getBTreeTuple(753, 2);
		empty.insertTuple(tid, tup);
		assertEquals(4, empty.numPages());

		// now make sure the records are sorted on the key field
		DbFileIterator it = empty.iterator(tid);
		it.open();
		int prev = -1;
		while(it.hasNext()) {
			Tuple t = it.next();
			int value = ((IntField) t.getField(0)).getValue();
			assertTrue(value >= prev);
			prev = value;
		} 
	}

	@Test public void addDuplicateTuples() throws Exception {
		// create an empty B+ tree file keyed on the second field of a 2-field tuple
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 1);

		Tuple tup = null;

		BTreeChecker.checkRep(empty, tid, new HashMap<PageId, Page>(), true);

		// add a bunch of identical tuples
		for (int i = 0; i < 5; ++i) {
			for(int j = 0; j < 600; ++j) {
				tup = BTreeUtility.getBTreeTuple(i, 2);
				empty.insertTuple(tid, tup);
				// BTreeChecker.checkRep(empty, tid, new HashMap<PageId, Page>(), true);
			}

		}

		BTreeChecker.checkRep(empty, tid, new HashMap<PageId, Page>(), true);

		// now search for some ranges and make sure we find all the tuples
		IndexPredicate ipred = new IndexPredicate(Op.EQUALS, new IntField(3));
		DbFileIterator it = empty.indexIterator(tid, ipred);
		it.open();
		int count = 0;
		while(it.hasNext()) {
			it.next();
			count++;
		} 
		assertEquals(600, count);

		ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, new IntField(2));
		it = empty.indexIterator(tid, ipred);
		it.open();
		count = 0;
		while(it.hasNext()) {
			it.next();
			count++;
		} 
		assertEquals(1800, count);

		ipred = new IndexPredicate(Op.LESS_THAN, new IntField(2));
		it = empty.indexIterator(tid, ipred);
		it.open();
		count = 0;
		while(it.hasNext()) {
			it.next();
			count++;
		} 
		assertEquals(1200, count);
	}

	@Test
	public void testSplitLeafPage() throws Exception {
		// This should create a B+ tree with one full page
		BTreeFile onePageFile = BTreeUtility.createRandomBTreeFile(2, 502,
				null, null, 0);

		// there should be 1 leaf page
		assertEquals(1, onePageFile.numPages());

		// now insert a tuple
		Database.getBufferPool().insertTuple(tid, onePageFile.getId(), BTreeUtility.getBTreeTuple(5000, 2));

		// there should now be 2 leaf pages + 1 internal node
		assertEquals(3, onePageFile.numPages());

		// the root node should be an internal node and have 2 children (1 entry)
		BTreePageId rootPtrPid = new BTreePageId(onePageFile.getId(), 0, BTreePageId.ROOT_PTR);
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, rootPtrPid, Permissions.READ_ONLY);
		BTreePageId rootId = rootPtr.getRootId();
		assertEquals(rootId.pgcateg(), BTreePageId.INTERNAL);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(tid, rootId, Permissions.READ_ONLY);
		assertEquals(502, root.getNumEmptySlots());

		// each child should have half of the records
		Iterator<BTreeEntry> it = root.iterator();
		assertTrue(it.hasNext());
		BTreeEntry e = it.next();
		BTreeLeafPage leftChild = (BTreeLeafPage) Database.getBufferPool().getPage(tid, e.getLeftChild(), Permissions.READ_ONLY);
		BTreeLeafPage rightChild = (BTreeLeafPage) Database.getBufferPool().getPage(tid, e.getRightChild(), Permissions.READ_ONLY);
		assertTrue(leftChild.getNumEmptySlots() <= 251);
		assertTrue(rightChild.getNumEmptySlots() <= 251);

	}

	@Test
	public void testSplitRootPage() throws Exception {
		// This should create a packed B+ tree with no empty slots
		// There are 503 keys per internal page (504 children) and 502 tuples per leaf page
		// 504 * 502 = 253008
		BTreeFile bigFile = BTreeUtility.createRandomBTreeFile(2, 253008,
				null, null, 0);

		// we will need more room in the buffer pool for this test
		Database.resetBufferPool(500);		

		// there should be 504 leaf pages + 1 internal node
		assertEquals(505, bigFile.numPages());

		// now insert a tuple
		Database.getBufferPool().insertTuple(tid, bigFile.getId(), BTreeUtility.getBTreeTuple(10, 2));

		// there should now be 505 leaf pages + 3 internal nodes
		assertEquals(508, bigFile.numPages());

		// the root node should be an internal node and have 2 children (1 entry)
		BTreePageId rootPtrPid = new BTreePageId(bigFile.getId(), 0, BTreePageId.ROOT_PTR);
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, rootPtrPid, Permissions.READ_ONLY);
		BTreePageId rootId = rootPtr.getRootId();
		assertEquals(rootId.pgcateg(), BTreePageId.INTERNAL);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(tid, rootId, Permissions.READ_ONLY);
		assertEquals(502, root.getNumEmptySlots());

		// each child should have half of the entries
		Iterator<BTreeEntry> it = root.iterator();
		assertTrue(it.hasNext());
		BTreeEntry e = it.next();
		BTreeInternalPage leftChild = (BTreeInternalPage) Database.getBufferPool().getPage(tid, e.getLeftChild(), Permissions.READ_ONLY);
		BTreeInternalPage rightChild = (BTreeInternalPage) Database.getBufferPool().getPage(tid, e.getRightChild(), Permissions.READ_ONLY);
		assertTrue(leftChild.getNumEmptySlots() <= 252);
		assertTrue(rightChild.getNumEmptySlots() <= 252);

		// now insert some random tuples and make sure we can find them
		Random rand = new Random();
		for(int i = 0; i < 100; i++) {
			int item = rand.nextInt(BTreeUtility.MAX_RAND_VALUE);
			Tuple t = BTreeUtility.getBTreeTuple(item, 2);
			Database.getBufferPool().insertTuple(tid, bigFile.getId(), t);

			IndexPredicate ipred = new IndexPredicate(Op.EQUALS, t.getField(0));
			DbFileIterator fit = bigFile.indexIterator(tid, ipred);
			fit.open();
			boolean found = false;
			while(fit.hasNext()) {
				if(fit.next().equals(t)) {
					found = true;
					break;
				}
			}
			fit.close();
			assertTrue(found);
		}
	}

	@Test
	public void testSplitInternalPage() throws Exception {
		// For this test we will decrease the size of the Buffer Pool pages
    	BufferPool.setPageSize(1024);

		// This should create a B+ tree with a packed second tier of internal pages
		// and packed third tier of leaf pages
    	// (124 entries per internal/leaf page, 125 children per internal page ->
    	// 125*2*124 = 31000)
		BTreeFile bigFile = BTreeUtility.createRandomBTreeFile(2, 31000,
				null, null, 0);
		
		// we will need more room in the buffer pool for this test
		Database.resetBufferPool(1000);

		// there should be 250 leaf pages + 3 internal nodes
		assertEquals(253, bigFile.numPages());

		// now insert some random tuples and make sure we can find them
		Random rand = new Random();
		for(int i = 0; i < 100; i++) {
			int item = rand.nextInt(BTreeUtility.MAX_RAND_VALUE);
			Tuple t = BTreeUtility.getBTreeTuple(item, 2);
			Database.getBufferPool().insertTuple(tid, bigFile.getId(), t);

			IndexPredicate ipred = new IndexPredicate(Op.EQUALS, t.getField(0));
			DbFileIterator fit = bigFile.indexIterator(tid, ipred);
			fit.open();
			boolean found = false;
			while(fit.hasNext()) {
				if(fit.next().equals(t)) {
					found = true;
					break;
				}
			}
			fit.close();
			assertTrue(found);
		}

		// now make sure we have 31100 records and they are all in sorted order
		DbFileIterator fit = bigFile.iterator(tid);
		int count = 0;
		Tuple prev = null;
		fit.open();
		while(fit.hasNext()) {
			Tuple tup = fit.next();
			if(prev != null)
				assertTrue(tup.getField(0).compare(Op.GREATER_THAN_OR_EQ, prev.getField(0)));
			prev = tup;
			count++;
		}
		fit.close();
		assertEquals(31100, count);	
		
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeFileInsertTest.class);
	}
}
