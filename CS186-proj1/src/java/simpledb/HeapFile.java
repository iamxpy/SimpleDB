package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private TupleDesc tupleDesc;

    private File file;

    private int numPage;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        numPage = (int) (file.length() / BufferPool.PAGE_SIZE);
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs

    /**
     * 根据PageId从磁盘读取一个页，注意此方法只应该在BufferPool类被直接调用
     * 在其他需要page的地方需要通过BufferPool访问。这样才能实现缓存功能
     *
     * @param pid
     * @return 读取得到的Page
     * @throws IllegalArgumentException
     */
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        Page page = null;
        byte[] data = new byte[BufferPool.PAGE_SIZE];

        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            // page在HeapFile的偏移量
            int pos = pid.pageNumber() * BufferPool.PAGE_SIZE;
            raf.seek(pos);
            raf.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    /**
     * 这个类在实现时有不少疑惑，参考了别人的代码才清楚以下一些点：
     * 1.tableid就是heapfile的id，即通过getId。。但是这个不是从0开始的，按照课程源码推荐，这是文件的哈希码。。
     * 2.PageId是从0开始的。。。(哪里说了，这个可以默认的么，谁知道这个作业的设计是不是从0开始。。。)
     * 3.transactionId哪里来的让我非常困惑，现在知道不用理，反正iterator方法的调用者会提供，应该是以后章节的内容
     * 4.我觉得别人的一个想法挺好，就是存储一个当前正在遍历的页的tuples迭代器的引用，这样一页一页来遍历
     */
    private class HeapFileIterator implements DbFileIterator {

        private int pagePos;

        private Iterator<Tuple> tuplesInPage;

        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            // 不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page，理由见readPage()方法的Javadoc
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            HeapPageId pid = new HeapPageId(getId(), pagePos);
            //加载第一页的tuples
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuplesInPage == null) {
                //说明已经被关闭
                return false;
            }
            //如果当前页还有tuple未遍历
            if (tuplesInPage.hasNext()) {
                return true;
            }
            //如果遍历完当前页，测试是否还有页未遍历
            //注意要减一，这里与for循环的一般判断逻辑（迭代变量<长度）不同，是因为我们要在接下来代码中将pagePos加1才使用
            //如果不理解，可以自己举一个例子想象运行过程
            if (pagePos < numPages() - 1) {
                pagePos++;
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                tuplesInPage = getTuplesInPage(pid);
                //这时不能直接return ture，有可能返回的新的迭代器是不含有tuple的
                return tuplesInPage.hasNext();
            } else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //直接初始化一次。。。。。
            open();
        }

        @Override
        public void close() {
            pagePos = 0;
            tuplesInPage = null;
        }
    }
}
