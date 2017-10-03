package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LruCache<K,V> {

    //存放当前缓存的条目
    protected HashMap<K,Node> cachedEntries;

    //允许缓存的最大条目数量
    protected int capacity;

    //不含内容的头结点
    protected Node head;

    //最后一个结点
    protected Node tail;

    protected class Node{
        Node front;
        Node next;
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public LruCache(int capacity) {
        this.capacity = capacity;
        cachedEntries = new HashMap<>(capacity);
        head = new Node(null, null);
    }

    /**
     * 删除结点
     * @param ruNode the recently used Node
     */
    protected void unlink(Node ruNode) {
        //如果是最后一个结点
        if (ruNode.next == null) {
            ruNode.front.next = null;
        } else {
            ruNode.front.next=ruNode.next;
            ruNode.next.front=ruNode.front;
        }
    }

    /**
     * 把节点插入到表头作为第一个结点(在头结点之后)
     * @param ruNode  the recently used Node
     */
    protected void linkFirst(Node ruNode) {
        Node first= this.head.next;
        this.head.next=ruNode;
        ruNode.front= this.head;
        ruNode.next=first;
        if (first == null) {
            tail = ruNode;
        } else {
            first.front=ruNode;
        }
    }

    /**
     * 删除链表的最后一个元素
     */
    protected void removeTail() {
        Node newTail = tail.front;
        tail.front=null;
        newTail.next=null;
        tail=newTail;
    }

    /**
     *
     * @param key
     * @param value
     * @return     被删除出缓存的条目，如果没有，返回null
     * @throws CacheException 如果put操作出错
     */
    public synchronized V put(K key, V value) throws CacheException{
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }
        if (isCached(key)) {
            //该结点存在于cache中，则更新其值，然后调整最近使用的条目，返回null(因为没有被删除的条目)
            Node ruNode = cachedEntries.get(key);
            ruNode.value=value;
            unlink(ruNode);
            linkFirst(ruNode);
            return null;
        } else  {
            //不存在的话先判断是否已经达到容量，是的话要先删除尾结点最后将其返回
            //还没有的话只需要新建结点，然后插入到表头，返回null
            V removed=null;
            if (cachedEntries.size() == capacity) {
                removed = cachedEntries.remove(tail.key).value;
                removeTail();
            }
            Node ruNode = new Node(key, value);
            linkFirst(ruNode);
            cachedEntries.put(key, ruNode);
            return removed;
        }
    }

    /**
     *
     * @param key
     * @return  返回存在于缓存中的条目，不存在则返回null
     */
    public synchronized V get(K key) {
        if (isCached(key)) {
            //调整最近使用的条目
            Node ruNode = cachedEntries.get(key);
            if (tail == ruNode && ruNode.front != head) {
                //如果是尾节点且其前一个不为头结点，则设其前一个节点为新的尾节点
                tail = ruNode.front;
            }
            unlink(ruNode);
            linkFirst(ruNode);
            return ruNode.value;
        }
        return null;
    }

    public synchronized boolean isCached(K key) {
        return cachedEntries.containsKey(key);
    }

    protected void displayCache() {
        //用于测试的
        Node n=head;
        while ((n = n.next) != null) {
            System.out.print(n.value+", ");
        }
        System.out.println();
    }

    /**
     *
     * @return 当前缓存的所有value
     */
    public Iterator<V> iterator() {
        return new LruIter();
    }

    protected class LruIter implements Iterator<V> {
        Node n = head;

        @Override
        public synchronized boolean hasNext() {
            return n.next!=null;
        }

        @Override
        public synchronized V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n=n.next;
            return n.value;
        }
    }
}
