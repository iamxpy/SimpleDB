package simpledb.systemtest;

import java.io.*;
import java.util.*;

import org.junit.Test;

import simpledb.*;

import static org.junit.Assert.*;

/**
 * Test logging, aborts, and recovery.
 */
public class LogTest extends SimpleDbTestBase {
    File file1;
    File file2;
    HeapFile hf1;
    HeapFile hf2;

    void insertRow(HeapFile hf, Transaction t, int v1, int v2)
        throws DbException, TransactionAbortedException {
        // Create a row to insert
        TupleDesc twoIntColumns = Utility.getTupleDesc(2);
        Tuple value = new Tuple(twoIntColumns);
        value.setField(0, new IntField(v1));
        value.setField(1, new IntField(v2));
        TupleIterator insertRow = new TupleIterator(Utility.getTupleDesc(2), Arrays.asList(new Tuple[]{value}));

        // Insert the row
        Insert insert = new Insert(t.getId(), insertRow, hf.getId());
        insert.open();
        Tuple result = insert.next();
        assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, result.getTupleDesc());
        assertEquals(1, ((IntField)result.getField(0)).getValue());
        assertFalse(insert.hasNext());
        insert.close();
    }

    // check that the specified tuple is, or is not, present
    void look(HeapFile hf, Transaction t, int v1, boolean present)
        throws DbException, TransactionAbortedException {
        int count = 0;
        SeqScan scan = new SeqScan(t.getId(), hf.getId(), "");
        scan.open();
        while(scan.hasNext()){
            Tuple tu = scan.next();
            int x = ((IntField)tu.getField(0)).getValue();
            if(x == v1)
                count = count + 1;
        }
        scan.close();
        if(count > 1)
            throw new RuntimeException("LogTest: tuple repeated");
        if(present && count < 1)
            throw new RuntimeException("LogTest: tuple missing");
        if(present == false && count > 0)
            throw new RuntimeException("LogTest: tuple present but shouldn't be");
    }

    // insert tuples
    void doInsert(HeapFile hf, int t1, int t2)
        throws DbException, TransactionAbortedException, IOException {
        Transaction t = new Transaction();
        t.start();
        if(t1 != -1)
            insertRow(hf, t, t1, 0);
        Database.getBufferPool().flushAllPages();
        if(t2 != -1)
            insertRow(hf, t, t2, 0);
        t.commit();
    }

    void abort(Transaction t)
        throws DbException, TransactionAbortedException, IOException {
        // t.transactionComplete(true); // abort
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        Database.getLogFile().logAbort(t.getId()); // does rollback too
        Database.getBufferPool().flushAllPages(); // prevent NO-STEAL-based abort from
                                                  // un-doing the rollback
        Database.getBufferPool().transactionComplete(t.getId(), false); // release locks
    }

    // insert tuples
    // force dirty pages to disk, defeating NO-STEAL
    // abort
    void dontInsert(HeapFile hf, int t1, int t2)
        throws DbException, TransactionAbortedException, IOException {
        Transaction t = new Transaction();
        t.start();
        if(t1 != -1)
            insertRow(hf, t, t1, 0);
        if(t2 != -1)
            insertRow(hf, t, t2, 0);
        if(t1 != -1)
            look(hf, t, t1, true);
        if(t2 != -1)
            look(hf, t, t2, true);
        abort(t);
    }

    // simulate crash
    // restart Database
    // run log recovery
    void crash()
        throws DbException, TransactionAbortedException, IOException {
        Database.reset();
        hf1 = Utility.openHeapFile(2, file1);
        hf2 = Utility.openHeapFile(2, file2);
        Database.getLogFile().recover();
    }

    // create an initial database with two empty tables
    // does *not* initiate log file recovery
    void setup()
            throws IOException, DbException, TransactionAbortedException {
        Database.reset();

        // empty heap files w/ 2 columns.
        // adds to the catalog.
        file1 = new File("simple1.db");
        file1.delete();
        file2 = new File("simple2.db");
        file2.delete();
        hf1 = Utility.createEmptyHeapFile(file1.getAbsolutePath(), 2);
        hf2 = Utility.createEmptyHeapFile(file2.getAbsolutePath(), 2);
    }

    @Test public void PatchTest()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // check that BufferPool.flushPage() calls LogFile.logWrite().
        doInsert(hf1, 1, 2);

        if(Database.getLogFile().getTotalRecords() != 4)
            throw new RuntimeException("LogTest: wrong # of log records; patch failed?");

        // *** Test:
        // check that BufferPool.transactionComplete(commit=true)
        // called Page.setBeforeImage().
        Transaction t1 = new Transaction();
        t1.start();
        Page p = Database.getBufferPool().getPage(t1.getId(),
                                                  new HeapPageId(hf1.getId(), 0),
                                                  Permissions.READ_ONLY);
        Page p1 = p.getBeforeImage();
        Boolean same = Arrays.equals(p.getPageData(),
                                     p1.getPageData());
        if(same == false)
            throw new RuntimeException("LogTest:setBeforeImage() not called? patch failed?");
    }

    @Test public void TestFlushAll()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // check that flushAllPages writes the HeapFile
        doInsert(hf1, 1, 2);

        Transaction t1 = new Transaction();
        t1.start();
        HeapPage xp1 = (HeapPage) hf1.readPage(new HeapPageId(hf1.getId(), 0));
        insertRow(hf1, t1, 3, 0);
        Database.getBufferPool().flushAllPages();
        HeapPage xp2 = (HeapPage) hf1.readPage(new HeapPageId(hf1.getId(), 0));

        if(xp1.getNumEmptySlots() == xp2.getNumEmptySlots())
            throw new RuntimeException("LogTest: flushAllPages() had no effect");
    }

    @Test public void TestCommitCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // insert, crash, recover: data should still be there

        doInsert(hf1, 1, 2);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        t.commit();
    }

    @Test public void TestAbort()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // insert, abort: data should not be there
        // flush pages directly to heap file to defeat NO-STEAL policy

        dontInsert(hf1, 4, -1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();
    }

    @Test public void TestAbortCommitInterleaved()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 start, T2 start and commit, T1 abort

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 3, 0);

        Transaction t2 = new Transaction();
        t2.start();
        insertRow(hf2, t2, 21, 0);
        Database.getLogFile().logCheckpoint();
        insertRow(hf2, t2, 22, 0);
        t2.commit();

        insertRow(hf1, t1, 4, 0);
        abort(t1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        look(hf2, t, 21, true);
        look(hf2, t, 22, true);
        t.commit();
    }

    @Test public void TestAbortCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        dontInsert(hf1, 4, -1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();

        // *** Test:
        // crash and recover: data should still not be there

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();
    }

    @Test public void TestCommitAbortCommitCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 inserts and commits
        // T2 inserts but aborts
        // T3 inserts and commit
        // only T1 and T3 data should be there

        doInsert(hf1, 5, -1);
        dontInsert(hf1, 6, -1);
        doInsert(hf1, 7, -1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 5, true);
        look(hf1, t, 6, false);
        look(hf1, t, 7, true);
        t.commit();

        // *** Test:
        // crash: should not change visible data

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        look(hf1, t, 5, true);
        look(hf1, t, 6, false);
        look(hf1, t, 7, true);
        t.commit();
    }

    @Test public void TestOpenCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // insert but no commit
        // crash
        // data should not be there

        Transaction t = new Transaction();
        t.start();
        insertRow(hf1, t, 8, 0);
        Database.getBufferPool().flushAllPages(); // XXX something to UNDO
        insertRow(hf1, t, 9, 0);

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 8, false);
        look(hf1, t, 9, false);
        t.commit();
    }

    @Test public void TestOpenCommitOpenCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 inserts but does not commit
        // T2 inserts and commits
        // T3 inserts but does not commit
        // crash
        // only T2 data should be there

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 10, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf1, t1, 11, 0);

        // T2 commits
        doInsert(hf2, 22, 23);

        Transaction t3 = new Transaction();
        t3.start();
        insertRow(hf2, t3, 24, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf2, t3, 25, 0);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 10, false);
        look(hf1, t, 11, false);
        look(hf2, t, 22, true);
        look(hf2, t, 23, true);
        look(hf2, t, 24, false);
        look(hf2, t, 25, false);
        t.commit();
    }

    @Test public void TestOpenCommitCheckpointOpenCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 inserts but does not commit
        // T2 inserts and commits
        // checkpoint
        // T3 inserts but does not commit
        // crash
        // only T2 data should be there

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 12, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf1, t1, 13, 0);

        // T2 commits
        doInsert(hf2, 26, 27);

        Database.getLogFile().logCheckpoint();

        Transaction t3 = new Transaction();
        t3.start();
        insertRow(hf2, t3, 28, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf2, t3, 29, 0);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 12, false);
        look(hf1, t, 13, false);
        look(hf2, t, 22, false);
        look(hf2, t, 23, false);
        look(hf2, t, 24, false);
        look(hf2, t, 25, false);
        look(hf2, t, 26, true);
        look(hf2, t, 27, true);
        look(hf2, t, 28, false);
        look(hf2, t, 29, false);
        t.commit();
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(LogTest.class);
    }
}
