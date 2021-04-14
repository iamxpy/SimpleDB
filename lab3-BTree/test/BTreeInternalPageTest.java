package simpledb;

import simpledb.BTreeFileEncoder.EntryComparator;
import simpledb.BTreeFileEncoder.ReverseEntryComparator;
import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

//import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class BTreeInternalPageTest extends SimpleDbTestBase {
	private BTreePageId pid;

	// these entries have been carefully chosen to be valid entries when
	// inserted in order. Be careful if you change them!
	public static final int[][] EXAMPLE_VALUES = new int[][] {
		{ 2, 6350, 4 },
		{ 4, 9086, 5 },
		{ 5, 17197, 7 },
		{ 7, 22064, 9 },
		{ 9, 22189, 10 },
		{ 10, 28617, 11 },
		{ 11, 31933, 13 },
		{ 13, 33549, 14 },
		{ 14, 34784, 15 },
		{ 15, 42878, 17 },
		{ 17, 45569, 19 },
		{ 19, 56462, 20 },
		{ 20, 62778, 21 },
		{ 15, 42812, 16 },
		{ 2, 3596, 3 },
		{ 6, 17876, 7 },
		{ 1, 1468, 2 },
		{ 11, 29402, 12 },
		{ 18, 51440, 19 },
		{ 7, 19209, 8 }
	};

	public static final byte[] EXAMPLE_DATA;
	static {
		// Build the input table
		ArrayList<BTreeEntry> entries = new ArrayList<BTreeEntry>();
		for (int[] entry : EXAMPLE_VALUES) {
			BTreePageId leftChild = new BTreePageId(-1, entry[0], BTreePageId.LEAF);
			BTreePageId rightChild = new BTreePageId(-1, entry[2], BTreePageId.LEAF);
			BTreeEntry e = new BTreeEntry(new IntField(entry[1]), leftChild, rightChild);
			entries.add(e);
		}

		// Convert it to a BTreeInternalPage
		try {
			EXAMPLE_DATA = BTreeFileEncoder.convertToInternalPage(entries, 
					BufferPool.getPageSize(), Type.INT_TYPE, BTreePageId.LEAF);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void addTable() throws Exception {
		this.pid = new BTreePageId(-1, -1, BTreePageId.INTERNAL);
		Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
	}

	/**
	 * Unit test for BTreeInternalPage.getId()
	 */
	@Test public void getId() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		assertEquals(pid, page.getId());
	}

	/**
	 * Unit test for BTreeInternalPage.getParentId()
	 */
	@Test public void getParentId() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		assertEquals(new BTreePageId(pid.getTableId(), 0, BTreePageId.ROOT_PTR), page.getParentId());
	}

	/**
	 * Unit test for BTreeInternalPage.getParentId()
	 */
	@Test public void setParentId() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.INTERNAL);
		page.setParentId(id);
		assertEquals(id, page.getParentId());

		id = new BTreePageId(pid.getTableId(), 1, BTreePageId.LEAF);
		try {
			page.setParentId(id);
			throw new Exception("should not be able to set parentId to leaf node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}

		id = new BTreePageId(pid.getTableId() + 1, 1, BTreePageId.INTERNAL);
		try {
			page.setParentId(id);
			throw new Exception("should not be able to set parentId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BTreeInternalPage.iterator()
	 */
	@Test public void testIterator() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		Iterator<BTreeEntry> it = page.iterator();

		ArrayList<BTreeEntry> entries = new ArrayList<BTreeEntry>();
		for (int[] entry : EXAMPLE_VALUES) {
			BTreePageId leftChild = new BTreePageId(-1, entry[0], BTreePageId.LEAF);
			BTreePageId rightChild = new BTreePageId(-1, entry[2], BTreePageId.LEAF);
			BTreeEntry e = new BTreeEntry(new IntField(entry[1]), leftChild, rightChild);
			entries.add(e);
		}
		Collections.sort(entries, new EntryComparator());

		int row = 0;
		while (it.hasNext()) {
			BTreeEntry e = it.next();

			assertEquals(entries.get(row).getKey(), e.getKey());
			row++;
		}
	}

	/**
	 * Unit test for BTreeInternalPage.reverseIterator()
	 */
	@Test public void testReverseIterator() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		Iterator<BTreeEntry> it = page.reverseIterator();

		ArrayList<BTreeEntry> entries = new ArrayList<BTreeEntry>();
		for (int[] entry : EXAMPLE_VALUES) {
			BTreePageId leftChild = new BTreePageId(-1, entry[0], BTreePageId.LEAF);
			BTreePageId rightChild = new BTreePageId(-1, entry[2], BTreePageId.LEAF);
			BTreeEntry e = new BTreeEntry(new IntField(entry[1]), leftChild, rightChild);
			entries.add(e);
		}
		Collections.sort(entries, new ReverseEntryComparator());

		int row = 0;
		while (it.hasNext()) {
			BTreeEntry e = it.next();

			assertEquals(entries.get(row).getKey(), e.getKey());
			row++;
		}
	}

	/**
	 * Unit test for BTreeInternalPage.getNumEmptySlots()
	 */
	@Test public void getNumEmptySlots() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		assertEquals(483, page.getNumEmptySlots());
	}

	/**
	 * Unit test for BTreeInternalPage.isSlotUsed()
	 */
	@Test public void getSlot() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);

		// assuming the first slot is used for the extra child pointer
		for (int i = 0; i < 21; ++i)
			assertTrue(page.isSlotUsed(i));

		for (int i = 21; i < 504; ++i)
			assertFalse(page.isSlotUsed(i));
	}

	/**
	 * Unit test for BTreeInternalPage.isDirty()
	 */
	@Test public void testDirty() throws Exception {
		TransactionId tid = new TransactionId();
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		page.markDirty(true, tid);
		TransactionId dirtier = page.isDirty();
		assertEquals(true, dirtier != null);
		assertEquals(true, dirtier == tid);

		page.markDirty(false, tid);
		dirtier = page.isDirty();
		assertEquals(false, dirtier != null);
	}

	/**
	 * Unit test for BTreeInternalPage.addEntry()
	 */
	@Test public void addEntry() throws Exception {
		// create a blank page
		byte[] data = BTreeInternalPage.createEmptyPageData();
		BTreeInternalPage page = new BTreeInternalPage(pid, data, 0);

		// insert entries into the page
		ArrayList<BTreeEntry> entries = new ArrayList<BTreeEntry>();
		for (int[] entry : EXAMPLE_VALUES) {
			BTreePageId leftChild = new BTreePageId(pid.getTableId(), entry[0], BTreePageId.LEAF);
			BTreePageId rightChild = new BTreePageId(pid.getTableId(), entry[2], BTreePageId.LEAF);
			BTreeEntry e = new BTreeEntry(new IntField(entry[1]), leftChild, rightChild);
			entries.add(e);
			page.insertEntry(e);
		}

		// check that the entries are ordered by the key and
		// all child pointers are present
		Collections.sort(entries, new EntryComparator());
		Iterator<BTreeEntry> it0 = page.iterator();
		int childPtr = 1;
		for(BTreeEntry e : entries) {
			BTreeEntry next = it0.next();
			assertTrue(e.getKey().equals(next.getKey()));
			assertTrue(next.getLeftChild().getPageNumber() == childPtr);
			assertTrue(next.getRightChild().getPageNumber() == ++childPtr);
		}

		// now insert entries until the page fills up
		int free = page.getNumEmptySlots();

		// NOTE(ghuo): this nested loop existence check is slow, but it
		// shouldn't make a difference for n = 503 slots.

		for (int i = 0; i < free; ++i) {
			BTreeEntry addition = BTreeUtility.getBTreeEntry(i+21, 70000+i, pid.getTableId());
			page.insertEntry(addition);
			assertEquals(free-i-1, page.getNumEmptySlots());

			// loop through the iterator to ensure that the entry actually exists
			// on the page
			Iterator<BTreeEntry> it = page.iterator();
			boolean found = false;
			while (it.hasNext()) {
				BTreeEntry e = it.next();
				if (e.getKey().equals(addition.getKey()) && e.getLeftChild().equals(addition.getLeftChild()) &&
						e.getRightChild().equals(addition.getRightChild())) {
					found = true;

					// verify that the RecordId is sane
					assertTrue(page.getId().equals(e.getRecordId().getPageId()));
					break;
				}
			}
			assertTrue(found);
		}

		// now, the page should be full.
		try {
			page.insertEntry(BTreeUtility.getBTreeEntry(0, 5, pid.getTableId()));
			throw new Exception("page should be full; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BTreeInternalPage.deleteEntry() with false entries
	 */
	@Test(expected=DbException.class)
	public void deleteNonexistentEntry() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		page.deleteKeyAndRightChild(BTreeUtility.getBTreeEntry(2));
	}

	/**
	 * Unit test for BTreeInternalPage.deleteEntry()
	 */
	@Test public void deleteEntry() throws Exception {
		BTreeInternalPage page = new BTreeInternalPage(pid, EXAMPLE_DATA, 0);
		int free = page.getNumEmptySlots();

		// first, build a list of the entries on the page.
		Iterator<BTreeEntry> it = page.iterator();
		LinkedList<BTreeEntry> entries = new LinkedList<BTreeEntry>();
		while (it.hasNext())
			entries.add(it.next());
		BTreeEntry first = entries.getFirst();

		// now, delete them one-by-one from both the front and the end.
		int deleted = 0;
		while (entries.size() > 0) {
			page.deleteKeyAndRightChild(entries.removeFirst());
			page.deleteKeyAndRightChild(entries.removeLast());
			deleted += 2;
			assertEquals(free + deleted, page.getNumEmptySlots());
		}

		// now, the page should be empty.
		try {
			page.deleteKeyAndRightChild(first);
			throw new Exception("page should be empty; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeInternalPageTest.class);
	}
}
