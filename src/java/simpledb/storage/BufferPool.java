package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096; //这里一个页面的大小是4KB，正好是操作系统一个页面的大小，但我记得MySQL是16KB，有区别

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private HashMap<PageId, Page> bufferedPages;
    private LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        bufferedPages = new HashMap<>();
        lockManager = new LockManager();
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //acquire lock here
        lockManager.lock(tid, pid, perm);

        if (bufferedPages.containsKey(pid))
            return bufferedPages.get(pid);
        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = df.readPage(pid); //应该不用判null，如果null会报错
        if (bufferedPages.size() >= numPages)
            evictPage();
        bufferedPages.put(pid, page);
        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else { //abort
            discardPages(tid);
        }
        //unlock
        //注意是遍历lockManager.pageContexts，而不是遍历bufferPages，因为上边的代码钟如果commit=false，那么可能page
        //已经被删除了，所以后边无法unlock这个页面
        for (PageId pageId : lockManager.pageContexts.keySet()) {
            lockManager.unlock(tid, pageId);
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // 这里非常的绕，而且有坑。我刚开始考虑道直接调用getPage来insertTuple，但此时并没有指定这个tuple应该被放在哪个page中，
        // 所以应该在table层面，也就是DBFile层面进行insert
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = databaseFile.insertTuple(tid, t);
        for (Page page : pages) {
            // FIXME: 2022/6/29 测试{@link BufferPoolWriteTest#handleManyDirtyPages}我不是很懂，里边使用的是HeapFile的子类HeapFileDuplicates，
            //  但是其中的insertTuple实现就很让人迷惑，并没有更新page至file，只是空操作了一下，然后还返回了虚假的dirty page。
            //  目前感觉只是为了强制要求insert的结果要立即更新至buffer
            if (!bufferedPages.containsKey(page.getId())){
                evictPage();
                bufferedPages.put(page.getId(), page);//很细节！可能会增加一个page
            }
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());//非常有意思，可以通过recordId一直往上找，找到tableId
        List<Page> pages = dbFile.deleteTuple(tid, t); // effected pages
        for (Page page : pages) {
            //todo 考虑一下需要这样做吗
            page.markDirty(true, tid);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId : bufferedPages.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferedPages.remove(pid);
    }

    // discard pages in buffer pool held by tid.
    public synchronized void discardPages(TransactionId tid) {
        // 为了避免ConcurrentModificationException
        bufferedPages.values().removeIf(page -> {
            if (page.isDirty() == tid) {
                page.markDirty(false, null);
                lockManager.pageContexts.remove(page.getId());
                return true;
            }
            return false;
        });
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = bufferedPages.get(pid);
        page.markDirty(false, null);
        dbFile.writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page : bufferedPages.values()) {
            if (page.isDirty() == tid) flushPage(page.getId());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //todo 这里是一个大的优化点，可以实现一个lru、lru之类的
        if (bufferedPages.size() < numPages)
            return;
        Iterator<Page> iter = bufferedPages.values().iterator();
        while (iter.hasNext()) {
            Page page = iter.next();
            if (page.isDirty() == null) {
                iter.remove();
                return;
            }
        }
        throw new DbException("No available pages");
    }

}
