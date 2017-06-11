package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
// TODO: 17-5-23 未完成
public class BufferPool {
    //这个类没有设计为单例类，是因为作者认为：
    // The Database class provides a static method, Database.getBufferPool(),
    // that returns a reference to the single BufferPool instance for the entire SimpleDB process
    //但是我还是觉得应该设计为单例类

    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    //页的最大数量
    public final int PAGES_NUM;

    //当前的缓存页
    private LruCache<PageId,Page> lruPagesPool;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        PAGES_NUM = numPages;
        lruPagesPool = new LruCache<>(PAGES_NUM);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // TODO: 17-5-26 怎么用tid和perm？？？？
        HeapPage page = (HeapPage) lruPagesPool.get(pid);
        if (page != null) {//直接命中
            return page;
        }
        //未命中，访问磁盘并将其缓存
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
        HeapPage newPage = (HeapPage) table.readPage(pid);
        Page removedPage = lruPagesPool.put(pid, newPage);
        if (removedPage != null) {
            try {
                flushPage(removedPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> affectedPages = table.insertTuple(tid, t);
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t   the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        int tableId=t.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        Page affectedPage = table.deleteTuple(tid, t);
        affectedPage.markDirty(true,tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     * 我加上deprecated了，因为课程解释了这个方法是便于测试用的，原文如下
     * flushAllPages() method is not something you would ever need in a real implementation of a buffer pool.
     * However, we need this method for testing purposes. You should never call this method from any real code.
     */
    @Deprecated
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        Iterator<Page> it = lruPagesPool.iterator();
        while (it.hasNext()) {
            flushPage(it.next());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     *
     * //@param  pid an ID indicating the page to flush
     * 我改了方法参数，因为想用自己写的LruCache来实现替换策略，而不是将相关代码嵌入到这个类的设计中
     */
    // TODO: 17-6-10 改了原来的方法参数
//    private synchronized void flushPage(PageId pid) throws IOException {
    private synchronized void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        HeapPage dirty_page = (HeapPage) page;
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(page.getId().getTableId());
        table.writePage(dirty_page);
        dirty_page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 这个方法不实现了，具体的替换策略已经在LruCache中体现了，标志deprecated好了
     */
    @Deprecated
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    }


}
