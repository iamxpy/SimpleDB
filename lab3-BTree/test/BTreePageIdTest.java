package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class BTreePageIdTest extends SimpleDbTestBase {

	private BTreePageId rootPtrId;
	private BTreePageId internalId;
	private BTreePageId leafId;
	private BTreePageId headerId;

	@Before public void createPid() {
		rootPtrId = new BTreePageId(1, 0, BTreePageId.ROOT_PTR);
		internalId = new BTreePageId(1, 1, BTreePageId.INTERNAL);
		leafId = new BTreePageId(1, 2, BTreePageId.LEAF);
		headerId = new BTreePageId(1, 3, BTreePageId.HEADER);
	}

	/**
	 * Unit test for BTreePageId.getTableId()
	 */
	@Test public void getTableId() {
		assertEquals(1, rootPtrId.getTableId());
		assertEquals(1, internalId.getTableId());
		assertEquals(1, leafId.getTableId());
		assertEquals(1, headerId.getTableId());
	}

	/**
	 * Unit test for BTreePageId.pageno()
	 */
	@Test public void pageno() {
		assertEquals(0, rootPtrId.getPageNumber());
		assertEquals(1, internalId.getPageNumber());
		assertEquals(2, leafId.getPageNumber());
		assertEquals(3, headerId.getPageNumber());
	}

	/**
	 * Unit test for BTreePageId.hashCode()
	 */
	@Test public void testHashCode() {
		int code1, code2, code3, code4;

		// NOTE(ghuo): the hashCode could be anything. test determinism,
		// at least.
		code1 = rootPtrId.hashCode();
		assertEquals(code1, rootPtrId.hashCode());
		assertEquals(code1, rootPtrId.hashCode());

		code2 = internalId.hashCode();
		assertEquals(code2, internalId.hashCode());
		assertEquals(code2, internalId.hashCode());

		code3 = leafId.hashCode();
		assertEquals(code3, leafId.hashCode());
		assertEquals(code3, leafId.hashCode());

		code4 = headerId.hashCode();
		assertEquals(code4, headerId.hashCode());
		assertEquals(code4, headerId.hashCode());
	}

	/**
	 * Unit test for BTreePageId.equals()
	 */
	@Test public void equals() {
		BTreePageId pid1 = new BTreePageId(1, 1, BTreePageId.LEAF);
		BTreePageId pid1Copy = new BTreePageId(1, 1, BTreePageId.LEAF);
		BTreePageId pid2 = new BTreePageId(2, 2, BTreePageId.LEAF);
		BTreePageId pid3 = new BTreePageId(1, 1, BTreePageId.INTERNAL);

		// .equals() with null should return false
		assertFalse(pid1.equals(null));

		// .equals() with the wrong type should return false
		assertFalse(pid1.equals(new Object()));

		assertTrue(pid1.equals(pid1));
		assertTrue(pid1.equals(pid1Copy));
		assertTrue(pid1Copy.equals(pid1));
		assertTrue(pid2.equals(pid2));
		assertTrue(pid3.equals(pid3));

		assertFalse(pid1.equals(pid2));
		assertFalse(pid1Copy.equals(pid2));
		assertFalse(pid2.equals(pid1));
		assertFalse(pid2.equals(pid1Copy));
		assertFalse(pid1.equals(pid3));
		assertFalse(pid3.equals(pid1));
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreePageIdTest.class);
	}
}

