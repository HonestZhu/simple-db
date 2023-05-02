package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private LockManager lockManager;

    private int numPages;
    private ConcurrentHashMap<PageId, Page> bufferPool;

    private LRUCache cache;

    private class Node {
        public PageId pageId;
        public Page page;
        public Node pre;
        public Node next;

        public Node() {
            this.pageId = null;
            this.page = null;
            this.pre = null;
            this.next = null;
        }

        public Node(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
            this.pre = null;
            this.next = null;
        }

        public Node(PageId pageId, Page page, Node pre, Node next) {
            this.pageId = pageId;
            this.page = page;
            this.pre = pre;
            this.next = next;
        }
    }
    private class LRUCache {
        private ConcurrentHashMap<PageId, Node> cache;
        private Node tail;
        private Node head;
        private int size;
        private int capacity;

        public ConcurrentHashMap<PageId, Node> getCache() {
            return cache;
        }

        public LRUCache(int capacity) {
            this.cache = new ConcurrentHashMap<>();
            this.tail = new Node();
            this.head = new Node();
            this.tail.pre = this.head;
            this.head.next = this.tail;
            this.capacity = capacity;
            this.size = 0;
        }

        public Page get(PageId pid) {
            if(cache.containsKey(pid)){
//                remove(cache.get(pid));
                moveToHead(cache.get(pid));
                return cache.get(pid).page;
            }else{
                return null;
            }
        }

        private void moveToHead(Node node) {
            if(node.pre == head) return;
            Node pre = node.pre;
            Node next = node.next;
            pre.next = next;
            next.pre = pre;
            node.next = head.next;
            head.next.pre = node;
            node.pre = head;
            head.next = node;
        }

        private void deleteBack() {
            Node node = tail.pre;
            if(node == head) return;
            Node pre = node.pre;
            pre.next = tail;
            tail.pre = pre;
        }

        public void remove(Node node) {
            if(node == null) return;
            Node pre = node.pre, next = node.next;
            pre.next = next;
            next.pre = pre;

        }

        public void removeByKey(PageId pageId) {
            Node target = cache.get(pageId);
            this.remove(target);
        }

        public void put(PageId pid, Page page) throws DbException {
            if(cache.containsKey(pid)) {
                cache.get(pid).page = page;
                moveToHead(cache.get(pid));
                return;
            }
            if(size >= capacity) {
                Node target = tail.pre;
                while(target != head && target.page.isDirty() != null)
                    target = target.pre;
                if(target == head) throw new DbException("There is no suitable page storage space or all pages are dirty.");
                cache.remove(target.pageId);
                deleteBack();
                size --;
            }

            Node node = new Node(pid, page, head, head.next);
            head.next.pre = node;
            head.next = node;
            cache.put(pid, node);
            size ++;
        }
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
//        this.bufferPool = new ConcurrentHashMap<>();
        this.cache = new LRUCache(numPages);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        long start = System.currentTimeMillis();
        while(true) {
            try {
                boolean hasGetLock = lockManager.acquireLock(tid, pid, perm);
                // 未获取到锁即回滚
                if(hasGetLock) break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if(now - start > 500)
                throw new TransactionAbortedException();
        }
        if (cache.get(pid) == null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            cache.put(pid, page);
        }
        return cache.get(pid);
//        if(!bufferPool.containsKey(pid)) {
//            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = file.readPage(pid);
//            bufferPool.put(pid, page);
//        }
//        return bufferPool.get(pid);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.unLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {

        }
        lockManager.unLockByTId(tid);
    }

    public synchronized void rollback(TransactionId tid) {
        for (Map.Entry<PageId, Node> group : cache.getCache().entrySet()) {
            PageId pid = group.getKey();
            Page page = group.getValue().page;
            if(tid.equals(page.isDirty())) {
                int tableId = pid.getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                Page readPage = file.readPage(pid);
                cache.removeByKey(pid);
                try {
                    cache.put(pid, readPage);
                } catch (DbException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        updateBufferPool(pages, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile updateFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = updateFile.deleteTuple(tid, t);
        updateBufferPool(pages, tid);
    }

    public void updateBufferPool(List<Page> updatePages, TransactionId tid) throws DbException {
        for (Page page : updatePages) {
            page.markDirty(true, tid);
            // update bufferPool
            cache.put(page.getId(), page);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Map.Entry<PageId, Node> group : cache.getCache().entrySet()) {
            Page page = group.getValue().page;
            if (page.isDirty() != null) {
                this.flushPage(group.getKey());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        cache.removeByKey(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page target = cache.get(pid);
        if(target == null){
            return;
        }
        TransactionId tid = target.isDirty();
        if (tid != null) {
            Page before = target.getBeforeImage();
            Database.getLogFile().logWrite(tid, before,target);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(target);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<PageId, Node> group : this.cache.getCache().entrySet()) {
            PageId pid = group.getKey();
            Page flushPage = group.getValue().page;
            TransactionId flushPageDirty = flushPage.isDirty();
            Page before = flushPage.getBeforeImage();
            // 涉及到事务就应该setBeforeImage
            flushPage.setBeforeImage();
            if (flushPageDirty != null && flushPageDirty.equals(tid)) {
                Database.getLogFile().logWrite(tid, before, flushPage);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(flushPage);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    public class LockManager
    {
        private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Permissions>> lockMap;

        public LockManager() {
            this.lockMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            ConcurrentHashMap<TransactionId, Permissions> tidMap = lockMap.get(pid);
            if(tidMap == null) return false;
            return tidMap.get(tid) != null;
        }

        public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws InterruptedException {
            ConcurrentHashMap<TransactionId, Permissions> tidMap = lockMap.get(pid);
            if(tidMap == null) {
                put(tid, pid, perm);
                return true;
            }

            if(!holdsLock(tid, pid)) {
                if(perm == Permissions.READ_WRITE) {
                    return false;
                } else {
                    if(tidMap.size() > 1) {
                        put(tid, pid, perm);
                        return true;
                    } else {
                        for (Permissions value : tidMap.values()) {
                            if(value == Permissions.READ_WRITE) {
                                return false;
                            } else {
                                put(tid, pid, perm);
                                return true;
                            }
                        }
                    }
                }
            } else {
                Permissions holdPerm = tidMap.get(tid);
                if(holdPerm == perm) return true;
                else {
                    // 降级
                    if(perm == Permissions.READ_ONLY) {
                        tidMap.remove(tid);
                        put(tid, pid, perm);
                        return true;
                    } else {
                        if(tidMap.size() > 1) {
                            return false;
                        } else {
                            tidMap.remove(tid);
                            put(tid, pid, perm);
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public synchronized void put(TransactionId tid, PageId pid, Permissions perm) {
            ConcurrentHashMap<TransactionId, Permissions> tidMap = lockMap.get(pid);
            if(tidMap == null) {
                tidMap = new ConcurrentHashMap<>();
            }
            tidMap.put(tid, perm);
            lockMap.put(pid, tidMap);
        }

        public synchronized void unLock(TransactionId tid, PageId pid) {
            if(holdsLock(tid, pid)) {
                ConcurrentHashMap<TransactionId, Permissions> tidMap = lockMap.get(pid);
                tidMap.remove(tid);
                if(tidMap.size() == 0) {
                    lockMap.remove(pid);
                }
            }
        }

        public synchronized void unLockByTId(TransactionId tid) {
            Set<PageId> pageIds = lockMap.keySet();
            for (PageId pageId : pageIds){
                unLock(tid, pageId);
            }
        }
    }
}
