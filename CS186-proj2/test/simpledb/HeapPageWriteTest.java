package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class HeapPageWriteTest extends SimpleDbTestBase {

    private HeapPageId pid;

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void addTable() throws IOException {
        this.pid = new HeapPageId(-1, -1);
        Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
    }
    
    /**
     * Unit test for HeapPage.isDirty()
     */
    @Test public void testDirty() throws Exception {
        TransactionId tid = new TransactionId();
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        page.markDirty(true, tid);
        TransactionId dirtier = page.isDirty();
        assertEquals(true, dirtier != null);
        assertEquals(true, dirtier == tid);

        page.markDirty(false, tid);
        dirtier = page.isDirty();
        assertEquals(false, dirtier != null);
    }

    /**
     * Unit test for HeapPage.addTuple()
     */
    @Test public void addTuple() throws Exception {
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        int free = page.getNumEmptySlots();

        // NOTE(ghuo): this nested loop existence check is slow, but it
        // shouldn't make a difference for n = 504 slots.

        for (int i = 0; i < free; ++i) {
            Tuple addition = Utility.getHeapTuple(i, 2);
            page.insertTuple(addition);
            assertEquals(free-i-1, page.getNumEmptySlots());

            // loop through the iterator to ensure that the tuple actually exists
            // on the page
            Iterator<Tuple >it = page.iterator();
            boolean found = false;
            while (it.hasNext()) {
                Tuple tup = it.next();
                if (TestUtil.compareTuples(addition, tup)) {
                    found = true;

                    // verify that the RecordId is sane
                    assertEquals(page.getId(), tup.getRecordId().getPageId());
                    break;
                }
            }
            assertTrue(found);
        }

        // now, the page should be full.
        try {
            page.insertTuple(Utility.getHeapTuple(0, 2));
            throw new Exception("page should be full; expected DbException");
        } catch (DbException e) {
            // explicitly ignored
        }
    }

    /**
     * Unit test for HeapPage.deleteTuple() with false tuples
     */
    @Test(expected=DbException.class)
        public void deleteNonexistentTuple() throws Exception {
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        page.deleteTuple(Utility.getHeapTuple(2, 2));
    }

    /**
     * Unit test for HeapPage.deleteTuple()
     */
    @Test public void deleteTuple() throws Exception {
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        int free = page.getNumEmptySlots();

        // first, build a list of the tuples on the page.
        Iterator<Tuple> it = page.iterator();
        LinkedList<Tuple> tuples = new LinkedList<Tuple>();
        while (it.hasNext())
            tuples.add(it.next());
        Tuple first = tuples.getFirst();

        // now, delete them one-by-one from both the front and the end.
        int deleted = 0;
        while (tuples.size() > 0) {
            page.deleteTuple(tuples.removeFirst());
            page.deleteTuple(tuples.removeLast());
            deleted += 2;
            assertEquals(free + deleted, page.getNumEmptySlots());
        }

        // now, the page should be empty.
        try {
            page.deleteTuple(first);
            throw new Exception("page should be empty; expected DbException");
        } catch (DbException e) {
            // explicitly ignored
        }
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapPageWriteTest.class);
    }
}

