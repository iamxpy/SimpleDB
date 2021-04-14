package simpledb.systemtest;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.Predicate.Op;
import simpledb.*;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class BTreeFileDeleteTest extends SimpleDbTestBase {
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

	@Test
	public void testRedistributeLeafPages() throws Exception {
		// This should create a B+ tree with two partially full leaf pages
		BTreeFile twoLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 600,
				null, null, 0);
		BTreeChecker.checkRep(twoLeafPageFile, tid, new HashMap<PageId, Page>(), true);

		// Delete some tuples from the first page until it gets to minimum occupancy
		DbFileIterator it = twoLeafPageFile.iterator(tid);
		it.open();
		int count = 0;
		while(it.hasNext() && count < 49) {
			Tuple t = it.next();
			BTreePageId pid = (BTreePageId) t.getRecordId().getPageId();
			BTreeLeafPage p = (BTreeLeafPage) Database.getBufferPool().getPage(
					tid, pid, Permissions.READ_ONLY);
			assertEquals(202 + count, p.getNumEmptySlots());
			twoLeafPageFile.deleteTuple(tid, t);
			count++;
		}
		BTreeChecker.checkRep(twoLeafPageFile,tid, new HashMap<PageId, Page>(), true);

		// deleting a tuple now should bring the page below minimum occupancy and cause 
		// the tuples to be redistributed
		Tuple t = it.next();
		it.close();
		BTreePageId pid = (BTreePageId) t.getRecordId().getPageId();
		BTreeLeafPage p = (BTreeLeafPage) Database.getBufferPool().getPage(
				tid, pid, Permissions.READ_ONLY);
		assertEquals(251, p.getNumEmptySlots());
		twoLeafPageFile.deleteTuple(tid, t);
		assertTrue(p.getNumEmptySlots() <= 251);

		BTreePageId rightSiblingId = p.getRightSiblingId();
		BTreeLeafPage rightSibling = (BTreeLeafPage) Database.getBufferPool().getPage(
				tid, rightSiblingId, Permissions.READ_ONLY);
		assertTrue(rightSibling.getNumEmptySlots() > 202);
	} 

	@Test
	public void testMergeLeafPages() throws Exception {
		// This should create a B+ tree with one full page and two half-full leaf pages
		BTreeFile threeLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 1005,
				null, null, 0);

		BTreeChecker.checkRep(threeLeafPageFile,
				tid, new HashMap<PageId, Page>(), true);
		// there should be one internal node and 3 leaf nodes
		assertEquals(4, threeLeafPageFile.numPages());

		// delete the last two tuples
		DbFileIterator it = threeLeafPageFile.iterator(tid);
		it.open();
		Tuple secondToLast = null;
		Tuple last = null;
		while(it.hasNext()) {
			secondToLast = last;
			last = it.next();
		}
		it.close();
		threeLeafPageFile.deleteTuple(tid, secondToLast);
		threeLeafPageFile.deleteTuple(tid, last);
		BTreeChecker.checkRep(threeLeafPageFile, tid, new HashMap<PageId, Page>(), true);

		// confirm that the last two pages have merged successfully
		BTreePageId rootPtrId = BTreeRootPtrPage.getId(threeLeafPageFile.getId());
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, rootPtrId, Permissions.READ_ONLY);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(502, root.getNumEmptySlots());
		BTreeEntry e = root.iterator().next();
		BTreeLeafPage leftChild = (BTreeLeafPage) Database.getBufferPool().getPage(
				tid, e.getLeftChild(), Permissions.READ_ONLY);
		BTreeLeafPage rightChild = (BTreeLeafPage) Database.getBufferPool().getPage(
				tid, e.getRightChild(), Permissions.READ_ONLY);
		assertEquals(0, leftChild.getNumEmptySlots());
		assertEquals(1, rightChild.getNumEmptySlots());
		assertTrue(e.getKey().equals(rightChild.iterator().next().getField(0)));

	}

	@Test
	public void testDeleteRootPage() throws Exception {
		// This should create a B+ tree with two half-full leaf pages
		BTreeFile twoLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 503,
				null, null, 0);
		// there should be one internal node and 2 leaf nodes
		assertEquals(3, twoLeafPageFile.numPages());
		BTreeChecker.checkRep(twoLeafPageFile,
				tid, new HashMap<PageId, Page>(), true);

		// delete the first two tuples
		DbFileIterator it = twoLeafPageFile.iterator(tid);
		it.open();
		Tuple first = it.next();
		Tuple second = it.next();
		it.close();
		twoLeafPageFile.deleteTuple(tid, first);
		BTreeChecker.checkRep(twoLeafPageFile, tid, new HashMap<PageId, Page>(), false);
		twoLeafPageFile.deleteTuple(tid, second);
		BTreeChecker.checkRep(twoLeafPageFile,tid, new HashMap<PageId, Page>(), false);

		// confirm that the last two pages have merged successfully and replaced the root
		BTreePageId rootPtrId = BTreeRootPtrPage.getId(twoLeafPageFile.getId());
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, rootPtrId, Permissions.READ_ONLY);
		assertTrue(rootPtr.getRootId().pgcateg() == BTreePageId.LEAF);
		BTreeLeafPage root = (BTreeLeafPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(1, root.getNumEmptySlots());
		assertTrue(root.getParentId().equals(rootPtrId));
	}

	@Test
	public void testReuseDeletedPages() throws Exception {
		// this should create a B+ tree with 3 leaf nodes
		BTreeFile threeLeafPageFile = BTreeUtility.createRandomBTreeFile(2, 1005,
				null, null, 0);
		BTreeChecker.checkRep(threeLeafPageFile, tid, new HashMap<PageId, Page>(), true);

		// 3 leaf pages, 1 internal page
		assertEquals(4, threeLeafPageFile.numPages());

		// delete enough tuples to ensure one page gets deleted
		DbFileIterator it = threeLeafPageFile.iterator(tid);
		it.open();
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}
		it.close();

		// now there should be 2 leaf pages, 1 internal page, 1 unused leaf page, 1 header page
		assertEquals(5, threeLeafPageFile.numPages());

		// insert enough tuples to ensure one of the leaf pages splits
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().insertTuple(tid, threeLeafPageFile.getId(),
					BTreeUtility.getBTreeTuple(i, 2));
		}

		// now there should be 3 leaf pages, 1 internal page, and 1 header page
		assertEquals(5, threeLeafPageFile.numPages());
		BTreeChecker.checkRep(threeLeafPageFile, tid, new HashMap<PageId, Page>(), true);
	}

	@Test
	public void testRedistributeInternalPages() throws Exception {
		// This should create a B+ tree with two nodes in the second tier
		// and 602 nodes in the third tier
		BTreeFile bf = BTreeUtility.createRandomBTreeFile(2, 302204,
				null, null, 0);
		BTreeChecker.checkRep(bf, tid, new HashMap<PageId, Page>(), true);

		Database.resetBufferPool(500); // we need more pages for this test

		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(bf.getId()), Permissions.READ_ONLY);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(502, root.getNumEmptySlots());

		BTreeEntry rootEntry = root.iterator().next();
		BTreeInternalPage leftChild = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootEntry.getLeftChild(), Permissions.READ_ONLY);
		BTreeInternalPage rightChild = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootEntry.getRightChild(), Permissions.READ_ONLY);

		// delete from the right child to test redistribution from the left
		Iterator<BTreeEntry> it = rightChild.iterator();
		int count = 0;
		// bring the right internal page to minimum occupancy
		while(it.hasNext() && count < 49 * 502 + 1) {
			BTreeLeafPage leaf = (BTreeLeafPage) Database.getBufferPool().getPage(tid, 
					it.next().getLeftChild(), Permissions.READ_ONLY);
			Tuple t = leaf.iterator().next();
			Database.getBufferPool().deleteTuple(tid, t);
			it = rightChild.iterator();
			count++;
		}

		// deleting a page of tuples should bring the internal page below minimum 
		// occupancy and cause the entries to be redistributed
		assertEquals(252, rightChild.getNumEmptySlots());
		count = 0;
		while(it.hasNext() && count < 502) {
			BTreeLeafPage leaf = (BTreeLeafPage) Database.getBufferPool().getPage(tid, 
					it.next().getLeftChild(), Permissions.READ_ONLY);
			Tuple t = leaf.iterator().next();
			Database.getBufferPool().deleteTuple(tid, t);
			it = rightChild.iterator();
			count++;
		}
		assertTrue(leftChild.getNumEmptySlots() > 203);
		assertTrue(rightChild.getNumEmptySlots() <= 252);
		BTreeChecker.checkRep(bf, tid, new HashMap<PageId, Page>(), true);

		// sanity check that the entries make sense
		BTreeEntry lastLeftEntry = null;
		it = leftChild.iterator();
		while(it.hasNext()) {
			lastLeftEntry = it.next();
		}
		rootEntry = root.iterator().next();
		BTreeEntry firstRightEntry = rightChild.iterator().next();
		assertTrue(lastLeftEntry.getKey().compare(Op.LESS_THAN_OR_EQ, rootEntry.getKey()));
		assertTrue(rootEntry.getKey().compare(Op.LESS_THAN_OR_EQ, firstRightEntry.getKey()));
	}

	@Test
	public void testDeleteInternalPages() throws Exception {
    	// For this test we will decrease the size of the Buffer Pool pages
    	BufferPool.setPageSize(1024);
		
		// This should create a B+ tree with three nodes in the second tier
		// and 252 nodes in the third tier
    	// (124 entries per internal/leaf page, 125 children per internal page ->
    	// 251*124 + 1 = 31125)
		BTreeFile bigFile = BTreeUtility.createRandomBTreeFile(2, 31125,
				null, null, 0);

		BTreeChecker.checkRep(bigFile, tid, new HashMap<PageId, Page>(), true);

		Database.resetBufferPool(500); // we need more pages for this test

		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(bigFile.getId()), Permissions.READ_ONLY);
		BTreeInternalPage root = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(122, root.getNumEmptySlots());

		BTreeEntry e = root.iterator().next();
		BTreeInternalPage leftChild = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getLeftChild(), Permissions.READ_ONLY);
		BTreeInternalPage rightChild = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getRightChild(), Permissions.READ_ONLY);

		// Delete tuples causing leaf pages to merge until the first internal page 
		// gets to minimum occupancy
		DbFileIterator it = bigFile.iterator(tid);
		it.open();
		int count = 0;
		Database.getBufferPool().deleteTuple(tid, it.next());
		it.rewind();
		while(count < 62) {
			assertEquals(count, leftChild.getNumEmptySlots());
			for(int i = 0; i < 124; ++i) {
				Database.getBufferPool().deleteTuple(tid, it.next());
				it.rewind();
			}
			count++;
		}

		BTreeChecker.checkRep(bigFile, tid, new HashMap<PageId, Page>(), true);

		// deleting a page of tuples should bring the internal page below minimum 
		// occupancy and cause the entries to be redistributed
		assertEquals(62, leftChild.getNumEmptySlots());
		for(int i = 0; i < 124; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}

		BTreeChecker.checkRep(bigFile, tid, new HashMap<PageId, Page>(), true);

		assertEquals(62, leftChild.getNumEmptySlots());
		assertEquals(62, rightChild.getNumEmptySlots());

		// deleting another page of tuples should bring the page below minimum occupancy 
		// again but this time cause it to merge with its right sibling 
		for(int i = 0; i < 124; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}

		// confirm that the pages have merged
		assertEquals(123, root.getNumEmptySlots());
		e = root.iterator().next();
		leftChild = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getLeftChild(), Permissions.READ_ONLY);
		rightChild = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getRightChild(), Permissions.READ_ONLY);
		assertEquals(0, leftChild.getNumEmptySlots());
		assertTrue(e.getKey().compare(Op.LESS_THAN_OR_EQ, rightChild.iterator().next().getKey()));

		// Delete tuples causing leaf pages to merge until the first internal page 
		// gets below minimum occupancy and causes the entries to be redistributed
		count = 0;
		while(count < 62) {
			assertEquals(count, leftChild.getNumEmptySlots());
			for(int i = 0; i < 124; ++i) {
				Database.getBufferPool().deleteTuple(tid, it.next());
				it.rewind();
			}
			count++;
		}

		// deleting another page of tuples should bring the page below minimum occupancy 
		// and cause it to merge with the right sibling to replace the root
		for(int i = 0; i < 124; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}

		// confirm that the last two internal pages have merged successfully and 
		// replaced the root
		BTreePageId rootPtrId = BTreeRootPtrPage.getId(bigFile.getId());
		rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, rootPtrId, Permissions.READ_ONLY);
		assertTrue(rootPtr.getRootId().pgcateg() == BTreePageId.INTERNAL);
		root = (BTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(0, root.getNumEmptySlots());
		assertTrue(root.getParentId().equals(rootPtrId));

		it.close();
	}    

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeFileDeleteTest.class);
	}
}