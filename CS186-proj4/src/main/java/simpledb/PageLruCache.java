package simpledb;

public class PageLruCache extends LruCache<PageId, Page> {

    public PageLruCache(int capacity) {
        super(capacity);
    }

    @Override
    public synchronized Page put(PageId key, Page value) throws CacheException {
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }
        if (isCached(key)) {
            //该结点存在于cache中，则更新其值，然后调整最近使用的条目，返回null(因为没有被删除的条目)
            Node ruNode = cachedEntries.get(key);
            ruNode.value = value;
            unlink(ruNode);
            linkFirst(ruNode);
            return null;
        } else {
            //不存在的话先判断是否已经达到容量
            //如果到达容量，判断尾节点是否是dirty的page，如果是则取其前一个page
            //否则先删除尾结点最后将其返回
            //未到达容量的话只需要新建结点，然后插入到表头，返回null
            Page removed = null;
            if (cachedEntries.size() == capacity) {
                Page toRemoved = null;
                Node n = tail;
                while ((toRemoved = n.value).isDirty() != null) {
                    n = n.front;
                    if (n == head)
                        throw new CacheException("Page Cache is full and all pages in cache are dirty, not supported to put now");
                }
                //在链表中删除该node,以及缓存中删除page
                removePage(toRemoved.getId());
                removed = cachedEntries.remove(toRemoved.getId()).value;
            }
            Node ruNode = new Node(key, value);
            linkFirst(ruNode);
            cachedEntries.put(key, ruNode);
            return removed;
        }
    }


    /**
     * 删除cache中pageId对应的page
     *
     * @param pid
     */
    private synchronized void removePage(PageId pid) {
        if (!isCached(pid)) {
            throw new IllegalArgumentException();
        }
        Node toRemoved = head;
        //这里不需要超出链表尾的判断（toRemoved为null），因为到这里肯定存在该page
        while (!(toRemoved = toRemoved.next).key.equals(pid)) ;
        if (toRemoved == tail) {
            removeTail();
        } else {
            toRemoved.next.front = toRemoved.front;
            toRemoved.front.next = toRemoved.next;
        }
    }

    /**
     * 将pid对应的page从磁盘中再次读入，即将其恢复为磁盘中该page的状态
     *
     * @param pid
     */
    public synchronized void reCachePage(PageId pid) {
        if (!isCached(pid)) {
            throw new IllegalArgumentException();
        }
        //访问磁盘获得该page
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
        HeapPage originalPage = (HeapPage) table.readPage(pid);
        Node node = new Node(pid, originalPage);
        cachedEntries.put(pid, node);
        Node toRemoved = head;
        //这里不需要超出链表尾的判断（toRemoved为null），因为到这里肯定存在该page
        while (!(toRemoved = toRemoved.next).key.equals(pid)) ;
        node.front = toRemoved.front;
        node.next = toRemoved.next;
        toRemoved.front.next = node;
        if (toRemoved.next != null) {
            toRemoved.next.front = node;
        } else {
            //reCache的是尾节点，需要修改tail指针
            tail = node;
        }
    }
}

