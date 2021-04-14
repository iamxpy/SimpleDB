package simpledb;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

//import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

public class BTreeRootPtrPageTest extends SimpleDbTestBase {
	private BTreePageId pid;

	public static final byte[] EXAMPLE_DATA;
	static {
		// Identify the root page and page category
		int root = 1;
		int rootCategory = BTreePageId.LEAF;
		int header = 2;

		// Convert it to a BTreeRootPtrPage
		try {
			EXAMPLE_DATA = BTreeFileEncoder.convertToRootPtrPage(root, rootCategory, header);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void addTable() throws Exception {
		this.pid = new BTreePageId(-1, 0, BTreePageId.ROOT_PTR);
		Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
	}

	/**
	 * Unit test for BTreeRootPtrPage.getId()
	 */
	@Test public void getId() throws Exception {
		BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
		assertEquals(pid, page.getId());
	}

	/**
	 * Unit test for BTreeRootPtrPage.getRootId()
	 */
	@Test public void getRootId() throws Exception {
		BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
		assertEquals(new BTreePageId(pid.getTableId(), 1, BTreePageId.LEAF), page.getRootId());
	}

	/**
	 * Unit test for BTreeRootPtrPage.setRootId()
	 */
	@Test public void setRootId() throws Exception {
		BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
		BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.INTERNAL);
		page.setRootId(id);
		assertEquals(id, page.getRootId());

		id = new BTreePageId(pid.getTableId(), 1, BTreePageId.ROOT_PTR);
		try {
			page.setRootId(id);
			throw new Exception("should not be able to set rootId to RootPtr node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}

		id = new BTreePageId(pid.getTableId() + 1, 1, BTreePageId.INTERNAL);
		try {
			page.setRootId(id);
			throw new Exception("should not be able to set rootId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BTreeRootPtrPage.getHeaderId()
	 */
	@Test public void getHeaderId() throws Exception {
		BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
		assertEquals(new BTreePageId(pid.getTableId(), 2, BTreePageId.HEADER), page.getHeaderId());
	}

	/**
	 * Unit test for BTreeRootPtrPage.setHeaderId()
	 */
	@Test public void setHeaderId() throws Exception {
		BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
		BTreePageId id = new BTreePageId(pid.getTableId(), 3, BTreePageId.HEADER);
		page.setHeaderId(id);
		assertEquals(id, page.getHeaderId());

		id = new BTreePageId(pid.getTableId(), 2, BTreePageId.ROOT_PTR);
		try {
			page.setHeaderId(id);
			throw new Exception("should not be able to set headerId to RootPtr node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}

		id = new BTreePageId(pid.getTableId() + 1, 1, BTreePageId.HEADER);
		try {
			page.setHeaderId(id);
			throw new Exception("should not be able to set rootId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BTreeRootPtrPage.isDirty()
	 */
	@Test public void testDirty() throws Exception {
		TransactionId tid = new TransactionId();
		BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
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
		return new JUnit4TestAdapter(BTreeRootPtrPageTest.class);
	}
}
