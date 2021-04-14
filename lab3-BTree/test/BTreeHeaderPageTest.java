package simpledb;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class BTreeHeaderPageTest extends SimpleDbTestBase {
	private BTreePageId pid;

	public static final byte[] EXAMPLE_DATA;
	static {
		EXAMPLE_DATA = BTreeHeaderPage.createEmptyPageData();
	}

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void addTable() throws Exception {
		this.pid = new BTreePageId(-1, -1, BTreePageId.HEADER);
		Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
	}

	/**
	 * Unit test for BTreeHeaderPage.getId()
	 */
	@Test public void getId() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		assertEquals(pid, page.getId());
	}

	/**
	 * Unit test for BTreeHeaderPage.getPrevPageId()
	 */
	@Test public void getPrevPageId() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		assertTrue(page.getPrevPageId() == null);
	}

	/**
	 * Unit test for BTreeHeaderPage.getNextPageId()
	 */
	@Test public void getNextPageId() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		assertTrue(page.getNextPageId() == null);
	}

	/**
	 * Unit test for BTreeHeaderPage.setPrevPageId()
	 */
	@Test public void setPrevPageId() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.HEADER);
		page.setPrevPageId(id);
		assertEquals(id, page.getPrevPageId());

		id = new BTreePageId(pid.getTableId(), 1, BTreePageId.INTERNAL);
		try {
			page.setPrevPageId(id);
			throw new Exception("should not be able to set prevPageId to internal node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BTreeHeaderPage.setNextPageId()
	 */
	@Test public void setNextPageId() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.HEADER);
		page.setNextPageId(id);
		assertEquals(id, page.getNextPageId());

		id = new BTreePageId(pid.getTableId() + 1, 1, BTreePageId.HEADER);
		try {
			page.setNextPageId(id);
			throw new Exception("should not be able to set nextPageId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BTreeHeaderPage.numSlots()
	 */
	@Test public void numSlots() throws Exception {
		assertEquals(32704, BTreeHeaderPage.getNumSlots());
	}

	/**
	 * Unit test for BTreeHeaderPage.getEmptySlot()
	 */
	@Test public void getEmptySlot() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		assertEquals(0, page.getEmptySlot());
		page.init();
		assertEquals(-1, page.getEmptySlot());
		page.markSlotUsed(50, false);
		assertEquals(50, page.getEmptySlot());
	}

	/**
	 * Unit test for BTreeHeaderPage.isSlotUsed() and BTreeHeaderPage.markSlotUsed()
	 */
	@Test public void getSlot() throws Exception {
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		page.init();
		for (int i = 0; i < 20; ++i) {
			page.markSlotUsed(i, false);
		}

		for (int i = 0; i < 20; i += 2) {
			page.markSlotUsed(i, true);
		}

		for (int i = 0; i < 20; ++i) {
			if(i % 2 == 0)
				assertTrue(page.isSlotUsed(i));
			else
				assertFalse(page.isSlotUsed(i));
		}

		for (int i = 20; i < 32704; ++i)
			assertTrue(page.isSlotUsed(i));

		assertEquals(1, page.getEmptySlot());
	}

	/**
	 * Unit test for BTreeHeaderPage.getPageData()
	 */
	@Test public void getPageData() throws Exception {
		BTreeHeaderPage page0 = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		page0.init();
		for (int i = 0; i < 20; ++i) {
			page0.markSlotUsed(i, false);
		}

		for (int i = 0; i < 20; i += 2) {
			page0.markSlotUsed(i, true);
		}

		BTreeHeaderPage page = new BTreeHeaderPage(pid, page0.getPageData());

		for (int i = 0; i < 20; ++i) {
			if(i % 2 == 0)
				assertTrue(page.isSlotUsed(i));
			else
				assertFalse(page.isSlotUsed(i));
		}

		for (int i = 20; i < 32704; ++i)
			assertTrue(page.isSlotUsed(i));

		assertEquals(1, page.getEmptySlot());
	}

	/**
	 * Unit test for BTreeHeaderPage.isDirty()
	 */
	@Test public void testDirty() throws Exception {
		TransactionId tid = new TransactionId();
		BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
		page.markDirty(true, tid);
		TransactionId dirtier = page.isDirty();
		assertEquals(true, dirtier != null);
		assertEquals(true, dirtier == tid);

		page.markDirty(false, tid);
		dirtier = page.isDirty();
		assertEquals(false, dirtier != null);
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeHeaderPageTest.class);
	}
}
