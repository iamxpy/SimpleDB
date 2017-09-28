package simpledb.systemtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;

import org.junit.Test;

import simpledb.*;

import static org.junit.Assert.*;

/**
 * Tests running concurrent transactions.
 * You do not need to pass this test until lab3.
 */
public class TransactionTest extends SimpleDbTestBase {
    // Wait up to 10 minutes for the test to complete
    private static final int TIMEOUT_MILLIS = 10 * 60 * 1000;
    private void validateTransactions(int threads)
            throws DbException, TransactionAbortedException, IOException {
        // Create a table with a single integer value = 0
        HashMap<Integer, Integer> columnSpecification = new HashMap<Integer, Integer>();
        columnSpecification.put(0, 0);
        DbFile table = SystemTestUtil.createRandomHeapFile(1, 1, columnSpecification, null);

        ModifiableCyclicBarrier latch = new ModifiableCyclicBarrier(threads);
        XactionTester[] list = new XactionTester[threads];
        for(int i = 0; i < list.length; i++) {
            list[i] = new XactionTester(table.getId(), latch);
            list[i].start();
        }

        long stopTestTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
        for (XactionTester tester : list) {
            long timeout = stopTestTime - System.currentTimeMillis();
            if (timeout <= 0) {
                fail("Timed out waiting for transaction to complete");
            }
            try {
                tester.join(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (tester.isAlive()) {
                fail("Timed out waiting for transaction to complete");
            }

            if (tester.exception != null) {
                // Rethrow any exception from a child thread
                assert tester.exception != null;
                throw new RuntimeException("Child thread threw an exception.", tester.exception);
            }
            assert tester.completed;
        }

        // Check that the table has the correct value
        TransactionId tid = new TransactionId();
        DbFileIterator it = table.iterator(tid);
        it.open();
        Tuple tup = it.next();
        assertEquals(threads, ((IntField) tup.getField(0)).getValue());
        it.close();
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
    }

    private static class XactionTester extends Thread {
        private final int tableId;
        private final ModifiableCyclicBarrier latch;
        public Exception exception = null;
        public boolean completed = false;

        public XactionTester(int tableId, ModifiableCyclicBarrier latch) {
            this.tableId = tableId;
            this.latch = latch;
        }

        public void run() {
            try {
                // Try to increment the value until we manage to successfully commit
                while (true) {
                    // Wait for all threads to be ready
                    latch.await();
                    Transaction tr = new Transaction();
                    try {
                        tr.start();
                        SeqScan ss1 = new SeqScan(tr.getId(), tableId, "");
                        SeqScan ss2 = new SeqScan(tr.getId(), tableId, "");

                        // read the value out of the table
                        Query q1 = new Query(ss1, tr.getId());
                        q1.start();
                        Tuple tup = q1.next();
                        IntField intf = (IntField) tup.getField(0);
                        int i = intf.getValue();

                        // create a Tuple so that Insert can insert this new value
                        // into the table.
                        Tuple t = new Tuple(SystemTestUtil.SINGLE_INT_DESCRIPTOR);
                        t.setField(0, new IntField(i+1));

                        // sleep to get some interesting thread interleavings
                        Thread.sleep(1);

                        // race the other threads to finish the transaction: one will win
                        q1.close();

                        // delete old values (i.e., just one row) from table
                        Delete delOp = new Delete(tr.getId(), ss2);

                        Query q2 = new Query(delOp, tr.getId());

                        q2.start();
                        q2.next();
                        q2.close();

                        // set up a Set with a tuple that is one higher than the old one.
                        HashSet<Tuple> hs = new HashSet<Tuple>();
                        hs.add(t);
                        TupleIterator ti = new TupleIterator(t.getTupleDesc(), hs);

                        // insert this new tuple into the table
                        Insert insOp = new Insert(tr.getId(), ti, tableId);
                        Query q3 = new Query(insOp, tr.getId());
                        q3.start();
                        q3.next();
                        q3.close();

                        tr.commit();
                        break;
                    } catch (TransactionAbortedException te) {
                        //System.out.println("thread " + tr.getId() + " killed");
                        // give someone else a chance: abort the transaction
                        tr.transactionComplete(true);
                        latch.stillParticipating();
                    }
                }
                //System.out.println("thread " + id + " done");
            } catch (Exception e) {
                // Store exception for the master thread to handle
                exception = e;
            }
            
            try {
                latch.notParticipating();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            completed = true;
        }
    }
    
    private static class ModifiableCyclicBarrier {
        private CountDownLatch awaitLatch;
        private CyclicBarrier participationLatch;
        private AtomicInteger nextParticipants;
        
        public ModifiableCyclicBarrier(int parties) {
            reset(parties);
        }
        
        private void reset(int parties) {
            nextParticipants = new AtomicInteger(0);
            awaitLatch = new CountDownLatch(parties);
            participationLatch = new CyclicBarrier(parties, new UpdateLatch(this, nextParticipants));
        }
        
        public void await() throws InterruptedException, BrokenBarrierException {
            awaitLatch.countDown();
            awaitLatch.await();
        }

        public void notParticipating() throws InterruptedException, BrokenBarrierException {
            participationLatch.await();
        }

        public void stillParticipating() throws InterruptedException, BrokenBarrierException {
            nextParticipants.incrementAndGet();
            participationLatch.await();
        }

        private static class UpdateLatch implements Runnable {
            ModifiableCyclicBarrier latch;
            AtomicInteger nextParticipants;
            
            public UpdateLatch(ModifiableCyclicBarrier latch, AtomicInteger nextParticipants) {
                this.latch = latch;
                this.nextParticipants = nextParticipants;
            }

            public void run() {
                // Reset this barrier if there are threads still running
                int participants = nextParticipants.get();
                if (participants > 0) {
                    latch.reset(participants);
                }
            }           
        }
    }
    
    @Test public void testSingleThread()
            throws IOException, DbException, TransactionAbortedException {
        validateTransactions(1);
    }

    @Test public void testTwoThreads()
            throws IOException, DbException, TransactionAbortedException {
        validateTransactions(2);
    }

    @Test public void testFiveThreads()
            throws IOException, DbException, TransactionAbortedException {
        validateTransactions(5);
    }
    
    @Test public void testTenThreads()
    throws IOException, DbException, TransactionAbortedException {
        validateTransactions(10);
    }

    @Test public void testAllDirtyFails()
            throws IOException, DbException, TransactionAbortedException {
        // Allocate a file with ~10 pages of data
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 512*10, null, null);
        Database.resetBufferPool(1);

        // BEGIN TRANSACTION
        Transaction t = new Transaction();
        t.start();

        // Insert a new row
        EvictionTest.insertRow(f, t);

        // Scanning the table must fail because it can't evict the dirty page
        try {
            EvictionTest.findMagicTuple(f, t);
            fail("Expected scan to run out of available buffer pages");
        } catch (DbException e) {}
        t.commit();
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(TransactionTest.class);
    }
}
