package simpledb;

import java.util.HashSet;

/**
 * Unique identifier for HeapPage objects.
 * 我将其实现为每个HeapPageId都为唯一的对象，即多次新建为同一个对象
 */
public class HeapPageId implements PageId {

    //缓存所有新建过的pid
    private volatile static HashSet<HeapPageId> pids = new HashSet<>();

    private int tableId;

    private int pageNum;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    private HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pageNum = pgNo;
    }

    public static HeapPageId getOrNewId(int tableId, int paNo) {
        HeapPageId newId = new HeapPageId(tableId, paNo);
        for (HeapPageId existId : pids) {
            //已经存在则返回
            if (existId.equals(newId)) return existId;
        }
        //不存在则进入同步代码块
        synchronized (HeapPageId.class) {
            for (HeapPageId existId : pids) {
                //再次检查是否存在，如果存在直接返回
                if (existId.equals(newId)) return existId;
            }
            //否则将新建的pid加入集合，并将其返回
            pids.add(newId);
            return newId;
        }
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        // some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int pageNumber() {
        // some code goes here
        return pageNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */

    @Override
    public int hashCode() {
        int result = 31 * tableId + pageNum;
        return result;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }
        if (o instanceof PageId) {
            PageId another = (PageId) o;
            return this.pageNum == another.pageNumber()
                    && this.tableId == another.getTableId();
        } else return false;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
