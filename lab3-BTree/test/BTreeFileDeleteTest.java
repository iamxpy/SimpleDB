package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.Predicate.Op;

import java.io.File;
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
	}

	/**
	 * Unit test for BTreeFile.deleteTuple()
	 */
	@Test public void deleteTuple() throws Exception {
		BTreeFile f;
		f = BTreeUtility.createRandomBTreeFile(2, 20, null, null, 0);
		DbFileIterator it = f.iterator(tid);
		it.open();
		while(it.hasNext()) {
			Tuple t = it.next();
			f.deleteTuple(tid, t);
		}
		it.rewind();
		assertFalse(it.hasNext());

		// insert a couple of tuples
		f.insertTuple(tid, BTreeUtility.getBTreeTuple(5, 2));
		f.insertTuple(tid, BTreeUtility.getBTreeTuple(17, 2));

		it.rewind();
		assertTrue(it.hasNext());
	}

	@Test
	public void testStealFromLeftLeafPage() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0);
		int tableid = empty.getId();
		int keyField = 0;

		// create the leaf pages
		BTreePageId pageId = new BTreePageId(tableid, 1, BTreePageId.LEAF);
		BTreePageId siblingId = new BTreePageId(tableid, 2, BTreePageId.LEAF);
		BTreeLeafPage page = BTreeUtility.createRandomLeafPage(pageId, 2, keyField, 
				BTreeUtility.getNumTuplesPerPage(2)/2 - 1, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE);
		BTreeLeafPage sibling = BTreeUtility.createRandomLeafPage(siblingId, 2, keyField, 0, BTreeUtility.MAX_RAND_VALUE/2);
		
		// create the parent page and the new entry
		BTreePageId parentId = new BTreePageId(tableid, 3, BTreePageId.INTERNAL);
		BTreeInternalPage parent = new BTreeInternalPage(parentId, BTreeInternalPage.createEmptyPageData(), keyField);
		Field key = page.iterator().next().getField(keyField);
		BTreeEntry entry = new BTreeEntry(key, siblingId, pageId);
		parent.insertEntry(entry);
		
		// set all the pointers
		page.setParentId(parentId);
		sibling.setParentId(parentId);
		page.setLeftSiblingId(siblingId);
		sibling.setRightSiblingId(pageId);
		
		int totalTuples = page.getNumTuples() + sibling.getNumTuples();
		
		empty.stealFromLeafPage(page, sibling, parent, entry, false);
		assertEquals(totalTuples, page.getNumTuples() + sibling.getNumTuples());
		assertTrue(page.getNumTuples() == totalTuples/2 || page.getNumTuples() == totalTuples/2 + 1);
		assertTrue(sibling.getNumTuples() == totalTuples/2 || sibling.getNumTuples() == totalTuples/2 + 1);
		assertTrue(sibling.reverseIterator().next().getField(keyField).compare(Op.LESS_THAN_OR_EQ, 
				page.iterator().next().getField(keyField)));
	} 

	@Test
	public void testStealFromRightLeafPage() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0);
		int tableid = empty.getId();
		int keyField = 0;

		// create the leaf pages
		BTreePageId pageId = new BTreePageId(tableid, 1, BTreePageId.LEAF);
		BTreePageId siblingId = new BTreePageId(tableid, 2, BTreePageId.LEAF);
		BTreeLeafPage page = BTreeUtility.createRandomLeafPage(pageId, 2, keyField, 
				BTreeUtility.getNumTuplesPerPage(2)/2 - 1, 0, BTreeUtility.MAX_RAND_VALUE/2);
		BTreeLeafPage sibling = BTreeUtility.createRandomLeafPage(siblingId, 2, keyField, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE);
		
		// create the parent page and the new entry
		BTreePageId parentId = new BTreePageId(tableid, 3, BTreePageId.INTERNAL);
		BTreeInternalPage parent = new BTreeInternalPage(parentId, BTreeInternalPage.createEmptyPageData(), keyField);
		Field key = page.iterator().next().getField(keyField);
		BTreeEntry entry = new BTreeEntry(key, pageId, siblingId);
		parent.insertEntry(entry);
		
		// set all the pointers
		page.setParentId(parentId);
		sibling.setParentId(parentId);
		page.setRightSiblingId(siblingId);
		sibling.setLeftSiblingId(pageId);
		
		int totalTuples = page.getNumTuples() + sibling.getNumTuples();
		
		empty.stealFromLeafPage(page, sibling, parent, entry, true);
		assertEquals(totalTuples, page.getNumTuples() + sibling.getNumTuples());
		assertTrue(page.getNumTuples() == totalTuples/2 || page.getNumTuples() == totalTuples/2 + 1);
		assertTrue(sibling.getNumTuples() == totalTuples/2 || sibling.getNumTuples() == totalTuples/2 + 1);
		assertTrue(page.reverseIterator().next().getField(keyField).compare(Op.LESS_THAN_OR_EQ, 
				sibling.iterator().next().getField(keyField)));
	} 

	@Test
	public void testMergeLeafPages() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 3);
		int tableid = empty.getId();
		int keyField = 0;

		// create the leaf pages
		BTreePageId leftPageId = new BTreePageId(tableid, 2, BTreePageId.LEAF);
		BTreePageId rightPageId = new BTreePageId(tableid, 3, BTreePageId.LEAF);
		BTreeLeafPage leftPage = BTreeUtility.createRandomLeafPage(leftPageId, 2, keyField, 
				BTreeUtility.getNumTuplesPerPage(2)/2 - 1, 0, BTreeUtility.MAX_RAND_VALUE/2);
		BTreeLeafPage rightPage = BTreeUtility.createRandomLeafPage(rightPageId, 2, keyField, 
				BTreeUtility.getNumTuplesPerPage(2)/2 - 1, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE);
		
		// create the parent page and the new entry
		BTreePageId parentId = new BTreePageId(tableid, 1, BTreePageId.INTERNAL);
		BTreeInternalPage parent = BTreeUtility.createRandomInternalPage(parentId, keyField, 
				BTreePageId.LEAF, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE, 2);
		BTreeEntry entry = parent.iterator().next();
		Field siblingKey = rightPage.iterator().next().getField(keyField);
		Field parentKey = entry.getKey();
		Field minKey = (siblingKey.compare(Op.LESS_THAN, parentKey) ? siblingKey : parentKey);
		entry.setKey(minKey);
		parent.updateEntry(entry);
		int numEntries = parent.getNumEntries();
		
		// set all the pointers
		leftPage.setParentId(parentId);
		rightPage.setParentId(parentId);
		leftPage.setRightSiblingId(rightPageId);
		rightPage.setLeftSiblingId(leftPageId);
		
		int totalTuples = leftPage.getNumTuples() + rightPage.getNumTuples();
		
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();
		dirtypages.put(leftPageId, leftPage);
		dirtypages.put(rightPageId, rightPage);
		dirtypages.put(parentId, parent);
		empty.mergeLeafPages(tid, dirtypages, leftPage, rightPage, parent, entry);
		assertEquals(totalTuples, leftPage.getNumTuples());
		assertEquals(0, rightPage.getNumTuples());
		assertEquals(null, leftPage.getRightSiblingId());
		assertEquals(numEntries - 1, parent.getNumEntries());
		assertEquals(rightPageId.getPageNumber(), empty.getEmptyPageNo(tid, dirtypages));
	}

	@Test
	public void testStealFromLeftInternalPage() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		int entriesPerPage = BTreeUtility.getNumEntriesPerPage();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 5 + 3*entriesPerPage/2);
		int tableid = empty.getId();
		int keyField = 0;

		// create the internal pages
		BTreePageId pageId = new BTreePageId(tableid, 1, BTreePageId.INTERNAL);
		BTreePageId siblingId = new BTreePageId(tableid, 2, BTreePageId.INTERNAL);
		BTreeInternalPage page = BTreeUtility.createRandomInternalPage(pageId, keyField, BTreePageId.LEAF,
				entriesPerPage/2 - 1, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE, 5 + entriesPerPage);
		BTreeInternalPage sibling = BTreeUtility.createRandomInternalPage(siblingId, keyField, 
				BTreePageId.LEAF, 0, BTreeUtility.MAX_RAND_VALUE/2, 4);
		
		// create the parent page and the new entry
		BTreePageId parentId = new BTreePageId(tableid, 3, BTreePageId.INTERNAL);
		BTreeInternalPage parent = new BTreeInternalPage(parentId, BTreeInternalPage.createEmptyPageData(), keyField);
		Field key = page.iterator().next().getKey();
		BTreeEntry entry = new BTreeEntry(key, siblingId, pageId);
		parent.insertEntry(entry);
				
		// set all the pointers
		page.setParentId(parentId);
		sibling.setParentId(parentId);
		
		int totalEntries = page.getNumEntries() + sibling.getNumEntries();
		int entriesToSteal = totalEntries/2 - page.getNumEntries();
		
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();
		dirtypages.put(pageId, page);
		dirtypages.put(siblingId, sibling);
		dirtypages.put(parentId, parent);
		empty.stealFromLeftInternalPage(tid, dirtypages, page, sibling, parent, entry);
		
		// are all the entries still there?
		assertEquals(totalEntries, page.getNumEntries() + sibling.getNumEntries());
		
		// have the entries been evenly distributed?
		assertTrue(page.getNumEntries() == totalEntries/2 || page.getNumEntries() == totalEntries/2 + 1);
		assertTrue(sibling.getNumEntries() == totalEntries/2 || sibling.getNumEntries() == totalEntries/2 + 1);
		
		// are the keys in the left page less than the keys in the right page?
		assertTrue(sibling.reverseIterator().next().getKey().compare(Op.LESS_THAN_OR_EQ, 
				page.iterator().next().getKey()));
		
		// is the parent key reasonable?
		assertTrue(parent.iterator().next().getKey().compare(Op.LESS_THAN_OR_EQ, page.iterator().next().getKey()));
		assertTrue(parent.iterator().next().getKey().compare(Op.GREATER_THAN_OR_EQ, sibling.reverseIterator().next().getKey()));
		
		// are all the parent pointers set?
		Iterator<BTreeEntry> it = page.iterator();
		BTreeEntry e = null;
		int count = 0;
		while(count < entriesToSteal) {
			assertTrue(it.hasNext());
			e = it.next();
			BTreePage p = (BTreePage) dirtypages.get(e.getLeftChild());
			assertEquals(pageId, p.getParentId());
			++count;
		}
	}

	@Test
	public void testStealFromRightInternalPage() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		int entriesPerPage = BTreeUtility.getNumEntriesPerPage();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 5 + 3*entriesPerPage/2);
		int tableid = empty.getId();
		int keyField = 0;

		// create the internal pages
		BTreePageId pageId = new BTreePageId(tableid, 1, BTreePageId.INTERNAL);
		BTreePageId siblingId = new BTreePageId(tableid, 2, BTreePageId.INTERNAL);
		BTreeInternalPage page = BTreeUtility.createRandomInternalPage(pageId, keyField, BTreePageId.LEAF,
				entriesPerPage/2 - 1, 0, BTreeUtility.MAX_RAND_VALUE/2, 4);
		BTreeInternalPage sibling = BTreeUtility.createRandomInternalPage(siblingId, keyField, 
				BTreePageId.LEAF, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE, 4 + entriesPerPage/2);
		
		// create the parent page and the new entry
		BTreePageId parentId = new BTreePageId(tableid, 3, BTreePageId.INTERNAL);
		BTreeInternalPage parent = new BTreeInternalPage(parentId, BTreeInternalPage.createEmptyPageData(), keyField);
		Field key = sibling.iterator().next().getKey();
		BTreeEntry entry = new BTreeEntry(key, pageId, siblingId);
		parent.insertEntry(entry);
				
		// set all the pointers
		page.setParentId(parentId);
		sibling.setParentId(parentId);
		
		int totalEntries = page.getNumEntries() + sibling.getNumEntries();
		int entriesToSteal = totalEntries/2 - page.getNumEntries();
		
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();
		dirtypages.put(pageId, page);
		dirtypages.put(siblingId, sibling);
		dirtypages.put(parentId, parent);
		empty.stealFromRightInternalPage(tid, dirtypages, page, sibling, parent, entry);
		
		// are all the entries still there?
		assertEquals(totalEntries, page.getNumEntries() + sibling.getNumEntries());
		
		// have the entries been evenly distributed?
		assertTrue(page.getNumEntries() == totalEntries/2 || page.getNumEntries() == totalEntries/2 + 1);
		assertTrue(sibling.getNumEntries() == totalEntries/2 || sibling.getNumEntries() == totalEntries/2 + 1);
		
		// are the keys in the left page less than the keys in the right page?
		assertTrue(page.reverseIterator().next().getKey().compare(Op.LESS_THAN_OR_EQ, 
				sibling.iterator().next().getKey()));
		
		// is the parent key reasonable?
		assertTrue(parent.iterator().next().getKey().compare(Op.LESS_THAN_OR_EQ, sibling.iterator().next().getKey()));
		assertTrue(parent.iterator().next().getKey().compare(Op.GREATER_THAN_OR_EQ, page.reverseIterator().next().getKey()));
		
		// are all the parent pointers set?
		Iterator<BTreeEntry> it = page.reverseIterator();
		BTreeEntry e = null;
		int count = 0;
		while(count < entriesToSteal) {
			assertTrue(it.hasNext());
			e = it.next();
			BTreePage p = (BTreePage) dirtypages.get(e.getRightChild());
			assertEquals(pageId, p.getParentId());
			++count;
		}
	}

	@Test
	public void testMergeInternalPages() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
		int entriesPerPage = BTreeUtility.getNumEntriesPerPage();
		BTreeFile empty = BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 1 + 2*entriesPerPage);
		int tableid = empty.getId();
		int keyField = 0;

		// create the internal pages
		BTreePageId leftPageId = new BTreePageId(tableid, 2, BTreePageId.INTERNAL);
		BTreePageId rightPageId = new BTreePageId(tableid, 3, BTreePageId.INTERNAL);
		BTreeInternalPage leftPage = BTreeUtility.createRandomInternalPage(leftPageId, keyField, BTreePageId.LEAF,
				entriesPerPage/2 - 1, 0, BTreeUtility.MAX_RAND_VALUE/2, 3 + entriesPerPage);
		BTreeInternalPage rightPage = BTreeUtility.createRandomInternalPage(rightPageId, keyField, BTreePageId.LEAF,
				entriesPerPage/2 - 1, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE, 2 + 3*entriesPerPage/2);
		
		// create the parent page and the new entry
		BTreePageId parentId = new BTreePageId(tableid, 1, BTreePageId.INTERNAL);
		BTreeInternalPage parent = BTreeUtility.createRandomInternalPage(parentId, keyField, 
				BTreePageId.LEAF, BTreeUtility.MAX_RAND_VALUE/2, BTreeUtility.MAX_RAND_VALUE, 2);
		BTreeEntry entry = parent.iterator().next();
		Field siblingKey = rightPage.iterator().next().getKey();
		Field parentKey = entry.getKey();
		Field minKey = (siblingKey.compare(Op.LESS_THAN, parentKey) ? siblingKey : parentKey);
		entry.setKey(minKey);
		parent.updateEntry(entry);
		int numParentEntries = parent.getNumEntries();
		
		// set all the pointers
		leftPage.setParentId(parentId);
		rightPage.setParentId(parentId);
		
		int totalEntries = leftPage.getNumEntries() + rightPage.getNumEntries();
		
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();
		dirtypages.put(leftPageId, leftPage);
		dirtypages.put(rightPageId, rightPage);
		dirtypages.put(parentId, parent);
		empty.mergeInternalPages(tid, dirtypages, leftPage, rightPage, parent, entry);
		assertEquals(totalEntries + 1, leftPage.getNumEntries());
		assertEquals(0, rightPage.getNumEntries());
		assertEquals(numParentEntries - 1, parent.getNumEntries());
		assertEquals(rightPageId.getPageNumber(), empty.getEmptyPageNo(tid, dirtypages));

		// are all the parent pointers set?
		Iterator<BTreeEntry> it = leftPage.reverseIterator();
		BTreeEntry e = null;
		int count = 0;
		while(count < entriesPerPage/2 - 1) {
			assertTrue(it.hasNext());
			e = it.next();
			BTreePage p = (BTreePage) dirtypages.get(e.getRightChild());
			assertEquals(leftPageId, p.getParentId());
			++count;
		}
	}    

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeFileDeleteTest.class);
	}
}
