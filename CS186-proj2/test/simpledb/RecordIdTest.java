package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class RecordIdTest extends SimpleDbTestBase {

    private static RecordId hrid;
    private static RecordId hrid2;
    private static RecordId hrid3;
    private static RecordId hrid4;

    @Before public void createPids() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        HeapPageId hpid2 = new HeapPageId(-1, 2);
        HeapPageId hpid3 = new HeapPageId(-2, 2);
        hrid = new RecordId(hpid, 3);
        hrid2 = new RecordId(hpid2, 3);
        hrid3 = new RecordId(hpid, 4);
        hrid4 = new RecordId(hpid3, 3);

    }

    /**
     * Unit test for RecordId.getPageId()
     */
    @Test public void getPageId() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        assertEquals(hpid, hrid.getPageId());

    }

    /**
     * Unit test for RecordId.tupleno()
     */
    @Test public void tupleno() {
        assertEquals(3, hrid.tupleno());
    }
    
    /**
     * Unit test for RecordId.equals()
     */
    @Test public void equals() {
    	assertEquals(hrid, hrid2);
    	assertEquals(hrid2, hrid);
    	assertFalse(hrid.equals(hrid3));
    	assertFalse(hrid3.equals(hrid));
    	assertFalse(hrid2.equals(hrid4));
    	assertFalse(hrid4.equals(hrid2));
    }
    
    /**
     * Unit test for RecordId.hashCode()
     */
    @Test public void hCode() {
    	assertEquals(hrid.hashCode(), hrid2.hashCode());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(RecordIdTest.class);
    }
}

