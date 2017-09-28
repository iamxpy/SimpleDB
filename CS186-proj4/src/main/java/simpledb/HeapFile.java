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
        if (pid.getTableId() != getId()) {
            throw new IllegalArgumentException();
        }
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
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
            byte[] data = page.getPageData();
            raf.write(data);
        }
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
        ArrayList<Page> affectedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                //page的insertTuple已经负责修改tuple信息表明其存储在该page上
                page.insertTuple(t);
                page.markDirty(true, tid);
                affectedPages.add(page);
                break;
            }
        }
        if (affectedPages.size() == 0) {//说明page都已经满了
            //创建一个新的空白的Page
            HeapPageId npid = new HeapPageId(getId(), numPages());
            HeapPage blankPage = new HeapPage(npid, HeapPage.createEmptyPageData());
            numPage++;
            //将其写入磁盘
            writePage(blankPage);
            //通过BufferPool来访问该新的page
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, npid, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            affectedPages.add(newPage);
        }
        return affectedPages;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage affectedPage = null;
        for (int i = 0; i < numPages(); i++) {
            if (i == pid.pageNumber()) {
                affectedPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                affectedPage.deleteTuple(t);
            }
        }
        if (affectedPage == null) {
            throw new DbException("tuple " + t + " is not in this table");
        }
        return affectedPage;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    /**
     * 对HeapFile的一部分Page进行缓存，并只允许一次性使用(貌似有点浪费。。)
     * clear()-> addPage()-> init()为设计好的常用调用次序,这三个方法和getNum()用于管理状态
     * next()，和hasNext()是向外提供的使用接口
     */
    private class OneOffCachePages {

        //能够缓存的Page数目的最大值(实际缓存的可能小于这个值),至少为1
        private int num;

        //当前访问到的Page索引
        private int index;

        //缓存的Pages
        private List<Iterator<Tuple>> cachePages;

        //当前正在访问的Page
        private Iterator<Tuple> current;

        /**
         * @param cacheRate 缓存的Page比例
         * @param pageNum   HeapFile所有的Page数目
         */
        public OneOffCachePages(double cacheRate, int pageNum) {
            cachePages = new ArrayList<>();
            int tmp = (int) (pageNum * cacheRate);
            num = tmp < 1 ? 1 : tmp;//计算得到需要缓存的page数目，如果总的page数目小于1,则设置为1
        }

        /**
         * 需要在缓存完page后调用
         */
        public void init() {
            if (cachePages.size() == 0) {
                throw new RuntimeException("cache has no page");
            }
            index = 0;
            current = cachePages.get(index);
        }

        public Tuple next() {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return current.next();
        }

        public boolean hasNext() {
            if (cachePages.size() == 0 || current == null) {//还未调用addPage或init
                return false;
            }
            if (current.hasNext()) {//判断当前访问的Page是否还有tuple未访问
                return true;
            }
            if (index+1 < cachePages.size()) {//判断是否还有缓存的Page未访问
                index++;
                current = cachePages.get(index);
                return current.hasNext();
            }
            return false;
        }

        public int getNum() {
            return num;
        }

        /**
         * 清空缓存
         */
        public void clear() {
            cachePages.clear();
        }

        public void addPage(Iterator<Tuple> tuples) {
            if (cachePages.size() <= num) {
                cachePages.add(tuples);
            } else throw new RuntimeException("cache is full");
        }
    }

    /**
     * 这个类在实现时有不少疑惑，参考了别人的代码才清楚以下一些点：
     * 1.tableid就是heapfile的id，即通过getId。。但是这个不是从0开始的，按照课程源码推荐，这是文件的哈希码。。
     * 2.PageId是从0开始的。。。(哪里说了?这个可以默认的么，谁知道这个作业的设计是不是从0开始。。。)
     * 3.transactionId哪里来的让我非常困惑，现在知道不用理，反正iterator方法的调用者会提供，应该是以后章节的内容
     * 4.我觉得别人的一个想法挺好，就是存储一个当前正在遍历的页的tuples迭代器的引用，这样一页一页来遍历
     * 5.在写Join算法时，我发现这个类的next和hasNext是性能瓶颈(是由于频繁的磁盘访问导致的)，故准备将一部分Page先缓存起来，
     * 设置了一个cacheRate表示缓存比例，表示每一次磁盘访问先缓存下来多大比例的Page
     */
    private class HeapFileIterator implements DbFileIterator {

        private final double cacheRate = 0.1;

        //缓存了一部分Page
        private OneOffCachePages cachePool;

        //作为起始偏移量，从这一页开始缓存Page
        private int initPos;

        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            initPos = 0;
            cachePool = new OneOffCachePages(cacheRate, numPage);
            initPos += fillCache(initPos);//缓存Pages并修改initPos为下一次需要作为起始缓存的Page偏移量
        }

        /**
         * @param initPos 作为起始偏移量，从这一页开始缓存Page，直到访问完当前HeapFile的所有Page或缓存用完
         * @return 实际缓存了的Page数目
         * @throws TransactionAbortedException
         * @throws DbException
         */
        private int fillCache(int initPos) throws TransactionAbortedException, DbException {
            int addNum = 0;//这次调用实际添加的Page数目
            //先清空之前的缓存页
            cachePool.clear();
            int pagePos = initPos;
            for (; pagePos < numPage && addNum < cachePool.getNum(); ) {
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                Iterator<Tuple> tuples = getTuplesInPage(pid);
                cachePool.addPage(tuples);
                addNum = ++pagePos - initPos;
            }
            if (addNum != 0) {
                cachePool.init();
            }
            return addNum;
        }

        /**
         * 由HeapPageId得到该Page的所有Tuple，以迭代器形式返回
         *
         * @param pid
         * @return
         * @throws TransactionAbortedException
         * @throws DbException
         */
        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            // 不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page，理由见readPage()方法的Javadoc
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (cachePool == null) {
                //说明已经被关闭
                return false;
            }
            if (cachePool.hasNext()) {//如果缓存还访问完
                return true;
            }
            //从上一个if出来说明当前缓存访问完了，所以加载新的Page到缓存
            int addNum = fillCache(initPos);
            //实际缓存的page数量为0，说明这个HeapFile已经访问完
            if (addNum == 0) {
                return false;
            }
            initPos += addNum;
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return cachePool.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //直接初始化一次。。。。。
            open();
        }

        @Override
        public void close() {
            initPos = 0;
            cachePool = null;
        }
    }
}
